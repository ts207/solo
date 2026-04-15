from project.events.event_specs import EVENT_REGISTRY_SPECS

spec = EVENT_REGISTRY_SPECS.get("LIQUIDATION_EXHAUSTION_REVERSAL")
print(f"Event Type: {spec.event_type}")
print(f"Reports Dir: {spec.reports_dir}")
print(f"Events File: {spec.events_file}")
