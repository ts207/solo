import json
import os
from pathlib import Path

def main():
    root = Path("data/reports")
    
    # 1. Load data quality
    audit_dirs = [d for d in (root / "data_quality_audit").iterdir() if d.is_dir() and "data_quality_audit" in d.name]
    latest_audit = max(audit_dirs, key=lambda d: d.name)
    with open(latest_audit / "mechanism_data_quality.json") as f:
        data_quality = json.load(f)["mechanisms"]
        
    data_quality_map = {m["mechanism_id"]: m for m in data_quality}
    
    # 2. Load mechanism inventory
    with open(root / "regime_event_inventory" / "mechanism_inventory.json") as f:
        inventory = json.load(f)["rows"]
    
    # 3. Load regime scorecard
    with open(root / "regime_baselines" / "regime_scorecard.json") as f:
        scorecard_rows = json.load(f)["rows"]
        
    forced_flow_failed = any(
        r["matrix_id"] == "forced_flow_crisis_v1" 
        and r.get("proposal_path_eligible") 
        and r.get("classification") == "negative"
        for r in scorecard_rows
    )
    
    vol_compression_failed = any(
        r["matrix_id"] == "volatility_compression_release_v1" 
        and r.get("proposal_path_eligible") 
        and r.get("classification") == "negative"
        for r in scorecard_rows
    )
    
    results = []
    
    for row in inventory:
        mech_id = row["id"]
        dq = data_quality_map.get(mech_id, {})
        
        status = dq.get("status", "draft")
        dq_decision = dq.get("data_quality_decision", "draft_only")
        req_obs = dq.get("required_observables", [])
        blocked_fields = dq.get("blocked_fields", [])
        proxy_fields = dq.get("proxy_fields", [])
        
        prior_state = "untested"
        if mech_id == "forced_flow_reversal" and forced_flow_failed:
            prior_state = "parked"
        elif mech_id == "funding_squeeze":
            prior_state = "parked"
        elif mech_id == "volatility_compression_release" and vol_compression_failed:
            prior_state = "parked"
            
        readiness = ""
        next_action = ""
        
        has_data_issues = bool(blocked_fields or proxy_fields)
        
        if prior_state in ("parked", "killed"):
            readiness = "remain_parked"
            next_action = "none"
        elif has_data_issues:
            readiness = "data_repair_first"
            fields_to_repair = blocked_fields + proxy_fields
            next_action = "repair_" + "_".join(fields_to_repair)
        elif status == "draft":
            readiness = "draft_only"
            next_action = "none"
        else:
            readiness = "ready_for_thesis"
            next_action = "define_one_ex_ante_thesis"
            
        res = {
            "mechanism_id": mech_id,
            "status": status,
            "data_quality_decision": dq_decision,
            "required_observables": req_obs,
            "blocked_fields": blocked_fields,
            "proxy_fields": proxy_fields,
            "prior_research_state": prior_state,
            "readiness": readiness,
            "next_action": next_action
        }
        results.append(res)
        
    out_dir = root / "mechanism_readiness"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    with open(out_dir / "mechanism_readiness.json", "w") as f:
        json.dump({"schema_version": "mechanism_readiness_v1", "rows": results}, f, indent=2)
        
    with open(out_dir / "mechanism_readiness.md", "w") as f:
        f.write("# Mechanism Readiness Scorecard\n\n")
        f.write("| Mechanism | Status | Prior State | Readiness | Next Action | Blocked/Proxy Fields |\n")
        f.write("|---|---|---|---|---|---|\n")
        for r in results:
            bf = ", ".join(r["blocked_fields"] + r["proxy_fields"]) if (r["blocked_fields"] or r["proxy_fields"]) else "None"
            f.write(f"| `{r['mechanism_id']}` | `{r['status']}` | `{r['prior_research_state']}` | `{r['readiness']}` | `{r['next_action']}` | {bf} |\n")
            
    print("Wrote mechanism readiness scorecard.")

if __name__ == "__main__":
    main()
