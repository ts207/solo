from pathlib import Path
import json
from project.live.thesis_store import ThesisStore
from project.live.contracts.promoted_thesis import LIVE_TRADEABLE_STATES, PromotedThesis
from project.core.exceptions import CompatibilityRequiredError, DataIntegrityError
from project.promote.paper_gate import evaluate_paper_gate

def _assert_forward_confirmation_passes(fc_path: Path) -> None:
    if not fc_path.exists():
         raise PermissionError(f"Trading mode blocked: forward confirmation missing at {fc_path}")
    
    fc = json.loads(fc_path.read_text(encoding="utf-8"))

    if fc.get("method") != "oos_frozen_thesis_replay_v1":
        raise PermissionError(
            "Trading mode blocked: forward confirmation method must be oos_frozen_thesis_replay_v1"
        )

    metrics = fc.get("metrics", {})
    if isinstance(metrics, dict) and metrics.get("status") == "fail":
        raise PermissionError(
            f"Trading mode blocked: forward confirmation failed: {metrics.get('reason', 'unknown')}"
        )

    if int(metrics.get("event_count", 0) or 0) <= 0:
        raise PermissionError("Trading mode blocked: forward confirmation has no OOS events")

    if float(metrics.get("mean_return_net_bps", 0.0) or 0.0) <= 0:
        raise PermissionError("Trading mode blocked: forward confirmation net bps is nonpositive")

    if float(metrics.get("t_stat_net", 0.0) or 0.0) <= 0:
        raise PermissionError("Trading mode blocked: forward confirmation t_stat_net is nonpositive")

def assert_deploy_admission(
    *,
    thesis_path: Path,
    runtime_mode: str,
    monitor_report_path: Path | None = None,
    data_root: Path | None = None,
) -> None:
    """
    Gates deployment based on validated thesis artifacts and monitor readiness.
    
    Rules:
    - monitor_only:
      allow monitor_only, paper_only, promoted, paper_enabled, paper_approved, live_eligible, live_enabled
    - simulation:
      allow paper_enabled, paper_approved, live_eligible, live_enabled
      block monitor_only/promoted unless explicitly paper gate is passed (via deployment_ready)
    - trading:
      allow live_enabled only
      require deployment_ready=true
      require forward_confirmation.json pass (OOS replay)
      require paper_quality_summary.json pass (Paper Gate)
      require DeploymentGate pass (already checked by ThesisStore)
    """
    runtime_mode = runtime_mode.lower()
    
    # 1. Load and validate via ThesisStore (applies DeploymentGate)
    try:
        store = ThesisStore.from_path(thesis_path, strict_live_gate=True)
        theses = store.all()
    except (CompatibilityRequiredError, DataIntegrityError):
        if runtime_mode == "monitor_only":
            from project.live.thesis_store import _load_payload
            payload = _load_payload(thesis_path)
            theses = [
                PromotedThesis.model_validate(item)
                for item in payload.get("theses", [])
                if isinstance(item, dict)
            ]
        else:
            raise

    if not theses:
        raise ValueError(f"Thesis artifact {thesis_path} contains no theses")

    # 2. Determine monitor readiness
    deployment_ready = False
    if monitor_report_path and monitor_report_path.exists():
        try:
            report = json.loads(monitor_report_path.read_text(encoding="utf-8"))
            deployment_ready = report.get("deployment_ready", False)
        except Exception:
             pass

    # 3. Mode-specific admission
    for thesis in theses:
        state = str(thesis.deployment_state or "").strip().lower()
        run_id = thesis.lineage.run_id if hasattr(thesis, "lineage") and thesis.lineage else None
        
        if runtime_mode == "trading":
            if state not in LIVE_TRADEABLE_STATES:
                raise PermissionError(
                    f"Trading mode blocked: thesis {thesis.thesis_id} is in state '{state}'. "
                    "Requires 'live_enabled'."
                )
            if not deployment_ready:
                 raise PermissionError(
                     f"Trading mode blocked: monitor report deployment_ready=False for thesis {thesis.thesis_id}."
                 )

            # Require Forward Confirmation Pass
            if not run_id or not data_root:
                 raise PermissionError(f"Trading mode blocked: cannot resolve run_id or data_root for thesis {thesis.thesis_id}")
            
            fc_path = data_root / "reports" / "validation" / str(run_id) / "forward_confirmation.json"
            _assert_forward_confirmation_passes(fc_path)

            # Require Paper Gate Pass
            paper_summary_path = data_root / "reports" / "paper" / str(thesis.thesis_id) / "paper_quality_summary.json"
            paper_gate = evaluate_paper_gate(paper_summary_path)
            if paper_gate.status != "pass":
                 raise PermissionError(
                     f"Trading mode blocked: paper gate failed for thesis {thesis.thesis_id}. "
                     f"Reasons: {', '.join(paper_gate.reason_codes)}"
                 )

        if runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
            if state not in paper_compatible:
                if state == "promoted" and deployment_ready:
                    continue
                raise PermissionError(
                    f"Simulation mode blocked: thesis {thesis.thesis_id} is in state '{state}'. "
                    "Requires paper-enabled state."
                )
