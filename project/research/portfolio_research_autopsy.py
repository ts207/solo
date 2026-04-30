from typing import Any

def generate_portfolio_autopsy() -> dict[str, Any]:
    return {
        "funding_squeeze": {
            "decision": "park",
            "reopen_only_if": [
                "new data-complete ex-ante regime with independent structural rationale",
                "and baseline passes after costs"
            ]
        },
        "forced_flow_reversal": {
            "decision": "park",
            "reopen_only_if": [
                "new liquidation/depth observable is materially better",
                "or current detector/materialization bug is found"
            ]
        },
        "volatility_compression_release": {
            "decision": "park",
            "reopen_only_if": [
                "materially different compression definition",
                "or execution model/cost model changes with justification"
            ]
        }
    }
