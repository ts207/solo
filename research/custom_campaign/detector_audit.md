# Detector Audit: State of Events and Runtime Compatibility

This audit documents the current implementation, data dependencies, and runtime status of core detectors in the Edge repository as of 2026-05-01.

## 1. Detector Summary Table

| Detector ID | Family | Primary Logic | Runtime Support | Maturity |
| :--- | :--- | :--- | :--- | :--- |
| **VOL_SHOCK** | Volatility | $rv\_z \geq th_{rv}$ AND $move_{bps} \geq th_{move}$ | Heuristic / Governed | Production |
| **VOL_SPIKE** | Volatility | $rv\_z \geq th_{rv}$ | Heuristic / Governed | Production |
| **BREAKOUT_TRIGGER** | Volatility | Compression release + Price breakout | Governed | Specialized |
| **LIQUIDATION_CASCADE** | Liquidation | $Liq_{notional} \geq th_{liq}$ AND $\Delta OI \leq th_{oi}$ | Heuristic / Governed | Production |
| **OI_SPIKE_POS/NEG** | Positioning | $OI\_z \geq th_{oi}$ AND $Ret_{sign}$ | Governed | Standard |
| **OI_FLUSH** | Positioning | $OI_{pct\_change} \leq th_{flush}$ | Governed | Standard |
| **FUNDING_EXTREME_ONSET**| Positioning | $FR_{pct} \geq th_{fr}$ AND $Accel_{rank} \geq th_{accel}$ | Governed | Standard |
| **BASIS_DISLOC** | Basis | $Basis_{z} \geq th_{basis}$ | Governed | Standard |
| **LIQUIDITY_VACUUM** | Liquidity | $Spread \geq th_{spread}$ OR $Depth \leq th_{depth}$ | Heuristic / Governed | Context |

## 2. Tier 1 Deep Dive (Highest Priority)

### 2.1 VOL_SHOCK
- **Offline Implementation**: `VolShockDetectorV2` ([volatility_base.py](file:///home/irene/Edge/project/events/detectors/volatility_base.py#L94))
- **Data Dependencies**: `timestamp`, `close`, `rv_96`, `range_96`, `range_med_2880`
- **Runtime Parity**:
    - **Heuristic**: Uses only a one-bar absolute move threshold. **Significant semantic divergence.**
    - **Governed**: Full parity with offline V2 logic.
- **Recommendation**: Standardize `source_features` to include `signed_move_bps` and `rv_z` to allow proposal directionality.

### 2.2 VOL_SPIKE
- **Offline Implementation**: `VolSpikeDetectorV2` ([volatility_base.py](file:///home/irene/Edge/project/events/detectors/volatility_base.py#L56))
- **Data Dependencies**: `timestamp`, `close`, `rv_96`, `range_96`, `range_med_2880`
- **Runtime Parity**:
    - **Heuristic**: Uses absolute move and volume. **Semantic divergence.**
    - **Governed**: Full parity with offline V2.
- **Recommendation**: Align heuristic with realized-vol expansion to prevent "fake" spikes during low-vol regime shifts.

### 2.3 BREAKOUT_TRIGGER
- **Offline Implementation**: `BreakoutTriggerDetectorV2` ([volatility_base.py](file:///home/irene/Edge/project/events/detectors/volatility_base.py#L260))
- **Data Dependencies**: `timestamp`, `close`, `high`, `low`, `rv_96`, `range_96`, `range_med_2880`
- **Runtime Status**: Supported in `governed_runtime_core` if registered in domain spec.
- **Recommendation**: Add `breakout_side` to metadata. Current implementation detects the breakout but doesn't explicitly label direction in the emitted event.

### 2.4 LIQUIDATION_CASCADE
- **Offline Implementation**: `LiquidationCascadeDetectorV2` ([liquidation_base.py](file:///home/irene/Edge/project/events/detectors/liquidation_base.py#L80))
- **Data Dependencies**: `timestamp`, `liquidation_notional`, `oi_delta_1h`, `oi_notional`, `close`, `high`, `low`
- **Runtime Parity**:
    - **Heuristic**: Requires $|move| \geq 80$, $OI_{drop} \geq 3\%$, and $|funding| \geq 0.0005$.
    - **Governed**: Matches offline logic.
- **Recommendation**: Move heuristic thresholds to dynamic snapshots to match offline calibration. Record `cascade_side` based on funding/price signs.

## 3. Data Dependency Audit

| Feature | Source | Availability | Staleness Risk |
| :--- | :--- | :--- | :--- |
| `rv_96` | Derived (Close) | High | Low |
| `oi_notional` | Direct (Venue) | Medium | High (Venue lag) |
| `funding_rate` | Direct (Venue) | Medium | Very High (Low freq) |
| `liq_notional` | Direct (Venue) | Medium | High (Event-driven) |
| `ms_*` states | Derived (Model) | High | Medium (Window lag) |

## 4. Runtime Compatibility Status

### 4.1 Heuristic Adapter (`heuristic`)
- **Supported**: `VOL_SHOCK`, `VOL_SPIKE`, `LIQUIDITY_VACUUM`, `LIQUIDATION_CASCADE`.
- **Risk**: Hardcoded thresholds and simplified logic create "hidden" differences between research and production results.

### 4.2 Governed Adapter (`governed_runtime_core`)
- **Supported**: Any spec-registered event.
- **Requirement**: Must have an entry in `_RUNTIME_CORE_EVENT_INPUT_BINDINGS` in `project/live/context_builder.py`.
- **Current Bindings**: `BASIS_DISLOC`, `FND_DISLOC`, `LIQUIDATION_CASCADE`, `LIQUIDITY_SHOCK`, `LIQUIDITY_STRESS_DIRECT`, `LIQUIDITY_VACUUM`, `OI_SPIKE_NEGATIVE`, `SPOT_PERP_BASIS_SHOCK`, `VOL_SHOCK`, `VOL_SPIKE`.

## 5. Recommended Next Steps

1. **Harden VOL_SHOCK**: Update `VolShockDetectorV2` to include `signed_move_bps` and explicit `directionality` metadata.
2. **Align VOL_SPIKE**: Resolve the semantic divergence between the move-based heuristic and the vol-based offline detector.
3. **Explicit Sides**: Update `BREAKOUT_TRIGGER` and `LIQUIDATION_CASCADE` to emit `breakout_side` and `cascade_side` respectively.
4. **Staleness Guards**: Implement `oi_age_minutes` and `funding_age_minutes` in the input surface to prevent detectors from firing on stale data forward-filled by the runtime.
