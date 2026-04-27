from pathlib import Path

def assert_deploy_admission(
    *,
    thesis_state: str,
    runtime_mode: str,
    deployment_ready: bool = False,
) -> None:
    """
    Gates deployment based on thesis state and monitor readiness.
    
    Rules:
    - If runtime_mode == "trading":
        - requires thesis_state == "live_enabled"
    - If runtime_mode == "simulation":
        - requires thesis_state in ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
        - OR (legacy/research) if thesis_state == "promoted" and deployment_ready is True
    - If runtime_mode == "monitor_only":
        - always allowed for any promoted state
    """
    runtime_mode = runtime_mode.lower()
    
    if runtime_mode == "trading":
        if thesis_state != "live_enabled":
            raise PermissionError(f"Trading mode blocked for thesis in state '{thesis_state}'. Requires 'live_enabled'.")
        if not deployment_ready:
             raise PermissionError(f"Trading mode blocked for thesis in state '{thesis_state}' because deployment_ready=False.")

    if runtime_mode == "simulation":
        allowed_states = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
        if thesis_state not in allowed_states:
            if thesis_state == "promoted" and deployment_ready:
                return # Special case for highly robust research theses
            raise PermissionError(f"Simulation mode blocked for thesis in state '{thesis_state}'. Requires paper-enabled state or robust 'promoted' state.")

    # monitor_only is always allowed if it reached deploy stage (which implies it's promoted/exported)
