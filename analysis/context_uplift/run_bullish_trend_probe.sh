#!/bin/bash
set -e

cd /home/irene/Edge

YEARS=("2023" "2024" "2025")
SLICES=("FULL" "H1" "H2")
SYMBOLS=("BTCUSDT" "ETHUSDT")
SPEC="spec/discovery/bullish_trend_probe"

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

        for SYMBOL in "${SYMBOLS[@]}"; do
            RUN_ID="BULL_PROBE_${SYMBOL}_${YEAR}_${SLICE}"
            echo "Running $RUN_ID: $START to $END for $SYMBOL"

            if make discover-cells-run RUN_ID="$RUN_ID" START="$START" END="$END" SYMBOLS="$SYMBOL" SPEC_DIR="$SPEC"; then
                echo "Success: $RUN_ID"
                
                OUT_DIR="analysis/bullish_trend_probe/${SYMBOL}/${YEAR}_${SLICE}"
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
