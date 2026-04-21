#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$script_dir/_edge_common.sh"

repo_root="$(edge_repo_root)"
cd "$repo_root"

mapfile -t changed_files < <(
  {
    git diff --name-only HEAD
    git ls-files --others --exclude-standard
  } | sort -u
)

if [ "${#changed_files[@]}" -eq 0 ]; then
  exit 0
fi

proposal_files=()
forbidden_hits=()
contract_hits=()
event_registry_hits=()
packaging_hits=()
architecture_hits=()
chatgpt_hits=()
plugin_hits=()
generated_doc_hits=()
doc_coupled_hits=()

for path in "${changed_files[@]}"; do
  case "$path" in
    spec/proposals/*.yaml)
      proposal_files+=("$path")
      ;;
  esac

  case "$path" in
    spec/events/event_registry_unified.yaml|\
    spec/events/regime_routing.yaml|\
    project/contracts/pipeline_registry.py|\
    project/contracts/schemas.py|\
    project/engine/schema.py|\
    project/research/experiment_engine_schema.py|\
    project/strategy/dsl/schema.py|\
    project/strategy/models/executable_strategy_spec.py)
      forbidden_hits+=("$path")
      ;;
  esac

  case "$path" in
    project/*|spec/*|project/tests/*|pyproject.toml|pytest.ini|Makefile|requirements-dev.txt)
      contract_hits+=("$path")
      ;;
  esac

  case "$path" in
    project/events/*|project/spec_validation/*|project/configs/registries/*|spec/events/*|spec/ontology/*|spec/states/*|spec/templates/*)
      event_registry_hits+=("$path")
      ;;
  esac

  case "$path" in
    project/research/live_export.py|project/research/services/promotion_service.py|project/portfolio/*|project/live/*|data/live/theses/*)
      packaging_hits+=("$path")
      ;;
  esac

  case "$path" in
    project/pipelines/*|project/domain/*|docs/reference/architecture.md|docs/reference/repository_map.md|docs/generated/system_map.*)
      architecture_hits+=("$path")
      ;;
  esac

  case "$path" in
    project/apps/chatgpt/*)
      chatgpt_hits+=("$path")
      ;;
  esac

  case "$path" in
    plugins/edge-agents/*|.agents/plugins/marketplace.json)
      plugin_hits+=("$path")
      ;;
  esac

  case "$path" in
    docs/generated/*)
      generated_doc_hits+=("$path")
      ;;
  esac

  case "$path" in
    README.md|docs/README.md|docs/reference/*.md|docs/lifecycle/*.md|docs/operator/*.md)
      doc_coupled_hits+=("$path")
      ;;
  esac
done

if [ "${#forbidden_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Forbidden contract surface touched:"
  printf '  - %s\n' "${forbidden_hits[@]}"
  echo "[edge-hook] Stop and get explicit human approval before continuing."
fi

if [ "${#proposal_files[@]}" -gt 0 ]; then
  echo "[edge-hook] Proposal edit detected:"
  printf '  - %s\n' "${proposal_files[@]}"
  first_proposal="${proposal_files[0]}"
  echo "[edge-hook] Next commands:"
  echo "  ./plugins/edge-agents/scripts/edge_preflight_proposal.sh $first_proposal"
  echo "  ./plugins/edge-agents/scripts/edge_lint_proposal.sh $first_proposal"
  echo "  ./plugins/edge-agents/scripts/edge_explain_proposal.sh $first_proposal"
  echo "  edge discover plan --proposal $first_proposal"
fi

if [ "${#contract_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Contract-sensitive repo change detected."
  echo "[edge-hook] Run:"
  echo "  ./plugins/edge-agents/scripts/edge_validate_repo.sh contracts"
fi

if [ "${#event_registry_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Event / ontology / registry surface change detected."
  echo "[edge-hook] Maintenance loop:"
  echo "  make governance"
  echo "  ./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green"
  echo "[edge-hook] Review docs:"
  echo "  docs/generated/event_contract_reference.md"
  echo "  docs/generated/event_ontology_mapping.md"
  echo "  docs/generated/system_map.md"
  echo "  docs/reference/spec_authoring.md"
fi

if [ "${#packaging_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Runtime-thesis / packaging / overlap surface change detected."
  echo "[edge-hook] Maintenance loop:"
  echo "  edge promote export --run_id <run_id>"
  echo "  PYTHONPATH=. ./.venv/bin/python -m project.scripts.build_thesis_overlap_artifacts --run_id <run_id>"
  echo "[edge-hook] Review docs and artifacts:"
  echo "  data/live/theses/<run_id>/promoted_theses.json"
  echo "  data/live/theses/index.json"
  echo "  docs/lifecycle/promote.md"
  echo "  docs/lifecycle/deploy.md"
fi

if [ "${#architecture_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Architecture-surface change detected."
  echo "[edge-hook] Maintenance loop:"
  echo "  ./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green"
  echo "  PYTHONPATH=. ./.venv/bin/python -m project.scripts.build_system_map --check"
  echo "[edge-hook] Review docs:"
  echo "  docs/reference/architecture.md"
  echo "  docs/reference/repository_map.md"
  echo "  docs/generated/system_map.md"
fi

if [ "${#chatgpt_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] ChatGPT app surface change detected."
  echo "[edge-hook] Useful commands:"
  echo "  ./plugins/edge-agents/scripts/edge_chatgpt_app.sh backlog"
  echo "  ./plugins/edge-agents/scripts/edge_chatgpt_app.sh blueprint"
  echo "  ./plugins/edge-agents/scripts/edge_chatgpt_app.sh widget"
  echo "  ./plugins/edge-agents/scripts/edge_chatgpt_app.sh serve --host 127.0.0.1 --port 8000 --path /mcp"
fi

if [ "${#generated_doc_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Generated-doc files changed."
  echo "[edge-hook] Prefer the generator/check command associated with the artifact before manual edits."
fi

if [ "${#doc_coupled_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Test-coupled docs changed."
  echo "[edge-hook] Run targeted tests or the minimum green gate:"
  echo "  ./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green"
fi

if [ "${#plugin_hits[@]}" -gt 0 ]; then
  echo "[edge-hook] Plugin surface change detected."
  echo "[edge-hook] Sync and check the installed plugin copy:"
  echo "  ./plugins/edge-agents/scripts/edge_sync_plugin.sh check"
  echo "  ./plugins/edge-agents/scripts/edge_sync_plugin.sh sync"
fi
