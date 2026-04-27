def validate_runtime_mode_against_theses(runtime_mode: str, theses: list) -> None:
    """
    Ensures that the requested runtime mode is compatible with the deployment states of the theses.
    
    Rules:
    - trading: Requires ALL theses to be 'live_enabled'.
    - simulation: Requires ALL theses to be in a paper-enabled state (paper_enabled, paper_approved, live_eligible, live_enabled).
    - monitor_only: Allowed for any thesis state.
    """
    runtime_mode = runtime_mode.lower()
    
    for thesis in theses:
        state = getattr(thesis, "deployment_state", "unknown")
        if runtime_mode == "trading":
            if state != "live_enabled":
                raise ValueError(f"Thesis in state '{state}' cannot run in trading mode. Requires 'live_enabled'.")
        elif runtime_mode == "simulation":
            paper_compatible = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
            if state not in paper_compatible:
                raise ValueError(f"Thesis in state '{state}' cannot run in simulation mode. Requires paper-enabled state.")
