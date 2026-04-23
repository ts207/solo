#!/bin/bash
set -e

cd /home/irene/Edge

YEARS=("2023" "2024" "2025")
SLICES=("FULL" "H1" "H2")
FAMILIES=("FPT" "VOL")
SYMBOL="BTCUSDT"

for YEAR in "${YEARS[@]}"; do
    for SLICE in "${SLICES[@]}"; do
        if [ "$SLICE" == "FULL" ]; then
            START="${YEAR}-01-01T00:00:00"
            END="${YEAR}-12-31T23:59:59"
        elif [ "$SLICE" == "H1" ]; then
            START="${YEAR}-01-01T00:00:00"
            END="${YEAR}-06-30T23:59:59"
        elif [ "$SLICE" == "H2" ]; then
            START="${YEAR}-07-01T00:00:00"
            END="${YEAR}-12-31T23:59:59"
        fi

        for FAMILY in "${FAMILIES[@]}"; do
            RUN_ID="CHOP_BTC_${FAMILY}_48B_${YEAR}_${SLICE}"
            
            if [ "$FAMILY" == "FPT" ]; then
                SPEC="spec/discovery/chop_probe_fpt"
            else
                SPEC="spec/discovery/chop_probe_vol"
            fi

            echo "Running $RUN_ID: $START to $END"

            if make discover-cells-run RUN_ID="$RUN_ID" START="$START" END="$END" SYMBOLS="$SYMBOL" SPEC_DIR="$SPEC"; then
                echo "Success: $RUN_ID"
                
                OUT_DIR="analysis/chop_probe/${RUN_ID}"
                mkdir -p "$OUT_DIR"
                
                SRC="data/reports/phase2/${RUN_ID}"
                cp "$SRC/edge_scoreboard.parquet" "$OUT_DIR/"
                cp "$SRC/phase2_candidates.parquet" "$OUT_DIR/"
                cp "$SRC/phase2_candidate_fold_metrics.parquet" "$OUT_DIR/"
                cp "$SRC/edge_cell_data_contract.json" "$OUT_DIR/"
                echo "Snapshotted $RUN_ID to $OUT_DIR"
            else
                echo "FAILED: $RUN_ID"
            fi
        done
    done
done
