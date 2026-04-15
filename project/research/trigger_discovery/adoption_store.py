import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

log = logging.getLogger(__name__)

def _get_store_path(out_dir: Path) -> Path:
    return out_dir / "adoption_state.json"

def _load_store(store_path: Path) -> Dict[str, Any]:
    if store_path.exists():
        try:
            with store_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            log.warning(f"Failed to decode {store_path}. Returning empty store.")
            return {}
    return {}

def _save_store(store_path: Path, data: Dict[str, Any]) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    with store_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def register_proposals(proposals: List[Dict[str, Any]], out_dir: Path, source_lane: str) -> None:
    """Registers newly generated proposals with 'proposed' status."""
    store_path = _get_store_path(out_dir)
    store = _load_store(store_path)
    
    timestamp = datetime.now(timezone.utc).isoformat()
    
    for p in proposals:
        cid = p.get("candidate_trigger_id")
        if not cid:
            continue
            
        if cid not in store:
            store[cid] = {
                "candidate_id": cid,
                "source_lane": source_lane,
                "status": "proposed",
                "created_at": timestamp,
                "history": [],
                "metadata": {}
            }
            log.debug(f"Registered new proposal {cid} in adoption store.")
            
    _save_store(store_path, store)

def get_proposal(candidate_id: str, out_dir: Path) -> Optional[Dict[str, Any]]:
    store_path = _get_store_path(out_dir)
    store = _load_store(store_path)
    return store.get(candidate_id)

def list_proposals(out_dir: Path) -> List[Dict[str, Any]]:
    store_path = _get_store_path(out_dir)
    store = _load_store(store_path)
    return list(store.values())

def transition_state(
    candidate_id: str, 
    new_status: str, 
    out_dir: Path, 
    reviewer: str = "system",
    reason: Optional[str] = None
) -> bool:
    """Transitions a proposal's status, enforcing state machine rules."""
    store_path = _get_store_path(out_dir)
    store = _load_store(store_path)
    
    if candidate_id not in store:
        log.error(f"Candidate {candidate_id} not found in adoption store.")
        return False
        
    current = store[candidate_id]
    old_status = current.get("status", "proposed")
    
    # State machine rules
    valid_transitions = {
        "proposed": ["under_review", "rejected", "approved"],
        "under_review": ["approved", "rejected", "proposed"],
        "approved": ["adopted", "rejected", "under_review"],
        "rejected": ["under_review", "proposed"],
        "adopted": ["approved", "rejected"]
    }
    
    if new_status not in valid_transitions.get(old_status, []):
        log.error(f"Invalid transition from {old_status} to {new_status} for {candidate_id}.")
        return False
        
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Record history
    current.setdefault("history", []).append({
        "from": old_status,
        "to": new_status,
        "timestamp": timestamp,
        "reviewer": reviewer,
        "reason": reason
    })
    
    current["status"] = new_status
    current["updated_at"] = timestamp
    
    if reason:
        current.setdefault("metadata", {})["decision_reason"] = reason
        
    store[candidate_id] = current
    _save_store(store_path, store)
    log.info(f"Transitioned {candidate_id} from {old_status} to {new_status}.")
    return True
