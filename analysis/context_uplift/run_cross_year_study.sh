#!/bin/bash
set -e

cd /home/irene/Edge

YEARS=("2025")
SLICES=("FULL" "H1" "H2")
SYMBOLS=("BTCUSDT" "ETHUSDT")

for YEAR in "${YEARS[@]}"; do
    for SLICE in "${SLICES[@]}"; do
        if [ "$SLICE" == "FULL" ]; then
            START="${YEAR}-01-01T00:00:00"
            END="${YEAR}-12-31T23:59:59"
            OUT_SLICE="full_${YEAR}"
        elif [ "$SLICE" == "H1" ]; then
            START="${YEAR}-01-01T00:00:00"
            END="${YEAR}-06-30T23:59:59"
            OUT_SLICE="h1_${YEAR}"
        elif [ "$SLICE" == "H2" ]; then
            START="${YEAR}-07-01T00:00:00"
            END="${YEAR}-12-31T23:59:59"
            OUT_SLICE="h2_${YEAR}"
        fi

        for SYMBOL in "${SYMBOLS[@]}"; do
            RUN_ID="CTXEXP_${SYMBOL}_${YEAR}_${SLICE}"
            echo "Running $RUN_ID: $START to $END for $SYMBOL"

            if make discover-cells-run RUN_ID="$RUN_ID" START="$START" END="$END" SYMBOLS="$SYMBOL" SPEC_DIR="spec/discovery"; then
                echo "Success: $RUN_ID"
                
                OUT_DIR="analysis/context_uplift_expanded/${SYMBOL}/${OUT_SLICE}"
                mkdir -p "$OUT_DIR"
                
                if [ -d "data/reports/phase2/${RUN_ID}" ]; then
                    cp -r data/reports/phase2/${RUN_ID}/* "$OUT_DIR/"
                    echo "Snapshotted $RUN_ID to $OUT_DIR"
                else
                    echo "WARNING: Output directory data/reports/phase2/${RUN_ID} not found!"
                fi
            else
                echo "FAILED: $RUN_ID. Skipping snapshot."
            fi
        done
    done
done
