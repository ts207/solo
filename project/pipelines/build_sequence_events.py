import pandas as pd
from pathlib import Path
from project.domain.compiled_registry import get_domain_registry
from project.events.sequence_analyzer import detect_sequences


def build_sequences(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all registered sequence motifs to the event stream.
    """
    sequences_cfg = get_domain_registry().sequence_rows()

    sequence_events = []
    for seq in sequences_cfg:
        print(f"Detecting sequence: {seq['name']}...")
        seq_df = detect_sequences(
            df, seq["events"], seq.get("max_gap", [6] * (len(seq["events"]) - 1)), seq["name"]
        )
        if not seq_df.empty:
            sequence_events.append(seq_df)

    if not sequence_events:
        return pd.DataFrame()

    return pd.concat(sequence_events, ignore_index=True)
