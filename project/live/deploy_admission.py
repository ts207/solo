from pathlib import Path
import json
from project.live.thesis_store import ThesisStore
from project.live.contracts.promoted_thesis import LIVE_TRADEABLE_STATES, PromotedThesis
from project.core.exceptions import CompatibilityRequiredError, DataIntegrityError

def assert_deploy_admission(
    *,
    thesis_path: Path,
    runtime_mode: str,
    monitor_report_path: Path | None = None,
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
      require DeploymentGate pass (already checked by ThesisStore)
    """
    runtime_mode = runtime_mode.lower()
    
    # 1. Load and validate via ThesisStore (applies DeploymentGate)
    # Raises RuntimeError on schema mismatch or DeploymentGate violations (if strict_live_gate=True)
    # ThesisStore.from_path performs artifact trust checks as well.
    try:
        store = ThesisStore.from_path(thesis_path, strict_live_gate=True)
        theses = store.all()
    except (CompatibilityRequiredError, DataIntegrityError):
        # Allow bypass for monitor_only mode ONLY. 
        # Research artifacts might not have full trust headers yet.
        if runtime_mode == "monitor_only":
            # Fallback to direct loading without trust checks
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

        if runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
            if state not in paper_compatible:
                # Special case: allow robust 'promoted' theses to enter simulation if monitor ready
                if state == "promoted" and deployment_ready:
                    continue
                raise PermissionError(
                    f"Simulation mode blocked: thesis {thesis.thesis_id} is in state '{state}'. "
                    "Requires paper-enabled state."
                )

    # monitor_only is always allowed if ThesisStore.from_path didn't raise
