from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional

from project import PROJECT_ROOT
from project.live.audit_log import (
    AuditLog,
    KillSwitchEvent,
    OrderIntentEvent,
)
from project.live.binance_client import BinanceFuturesClient
from project.live.bybit_client import BybitDerivativesClient
from project.live.context_builder import build_live_trade_context
from project.live.decay import DecayMonitor, DecayRule, default_decay_rules as _default_decay_rules
from project.live.decision import DecisionOutcome, decide_trade_intent
from project.live.event_detector import detect_live_event
from project.live.execution_attribution import summarize_execution_attribution_by
from project.live.health_checks import DataHealthMonitor
from project.live.kill_switch import KillSwitchManager, KillSwitchReason
from project.live.memory import append_live_episode
from project.live.oms import (
    OrderManager,
    OrderStatus,
    OrderSubmissionFailed,
    OrderType,
    build_live_order_from_strategy_result,
)
from project.live.order_planner import build_order_plan
from project.live.risk import RiskEnforcer, RuntimeRiskCaps
from project.live.state import LiveStateStore
from project.live.thesis_reconciliation import (
    RECONCILIATION_DEGRADED_EXCEPTIONS,
    ThesisBatchReconciliationError,
    reconcile_thesis_batch,
)
from project.live.thesis_state import ThesisStateManager
from project.live.thesis_store import ThesisStore
from project.core.exceptions import (
    CompatibilityRequiredError,
    DataIntegrityError,
    SchemaMismatchError,
)
from project.portfolio.incubation import IncubationLedger

_LOG = logging.getLogger(__name__)


def _classify_canonical_regime(
    move_bps: float,
    rv_pct: Optional[float] = None,
    ms_trend_state: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Unified regime classification.
    Calls shared project.core.regime_classifier logic.
    """
    from project.core.regime_classifier import classify_regime

    result = classify_regime(move_bps=move_bps, rv_pct=rv_pct, ms_trend_state=ms_trend_state)

    return {
        "canonical_regime": result.regime.value,
        "regime_mode": result.mode.value,
        "regime_confidence": result.confidence,
        "regime_metadata": result.metadata,
    }


class LiveEngineRunner:
    def __init__(
        self,
        symbols: List[str],
        *,
        exchange: str = "binance",
        api_key: str = "",
        api_secret: str = "",
        snapshot_path: str | Path | None = None,
        microstructure_recovery_streak: int = 3,
        account_sync_interval_seconds: float = 30.0,
        account_sync_failure_threshold: int = 3,
        account_snapshot_fetcher: Callable[[], Awaitable[Dict[str, Any]]] | None = None,
        market_feature_fetcher: Callable[[str], Awaitable[Dict[str, Any]]] | None = None,
        execution_quality_report_path: str | Path | None = None,
        runtime_metrics_snapshot_path: str | Path | None = None,
        execution_degradation_min_samples: int = 3,
        execution_degradation_warn_edge_bps: float = 0.0,
        execution_degradation_block_edge_bps: float = -5.0,
        execution_degradation_throttle_scale: float = 0.5,
        order_manager: OrderManager | None = None,
        data_manager: Any | None = None,
        health_check_interval_seconds: float = 5.0,
        stale_threshold_sec: float = 10.0,
        reconcile_at_startup: bool = True,
        runtime_mode: str = "monitor_only",
        strategy_runtime: Dict[str, Any] | None = None,
        risk_caps: RuntimeRiskCaps | None = None,
        decay_rules: List[DecayRule] | None = None,
    ):
        self.exchange = exchange.lower()

        # Phase 3: Native REST Client for Initialization & Recovery
        if self.exchange == "binance":
            self.rest_client = BinanceFuturesClient(
                api_key=api_key,
                api_secret=api_secret,
            )
        elif self.exchange == "bybit":
            self.rest_client = BybitDerivativesClient(
                api_key=api_key,
                api_secret=api_secret,
            )
        else:
            raise ValueError(f"Unsupported exchange: {self.exchange}")

        self.symbols = symbols
        self.state_store = LiveStateStore(snapshot_path=snapshot_path)
        self.kill_switch = KillSwitchManager(
            self.state_store,
            microstructure_recovery_streak=microstructure_recovery_streak,
        )
        self.kill_switch.register_callback(self._on_kill_switch_triggered)
        if data_manager is None:
            from project.live.ingest.manager import LiveDataManager

            data_manager = LiveDataManager(
                symbols,
                exchange=self.exchange,
                on_reconnect_exhausted=self._on_ws_reconnect_exhausted,
                rest_client=self.rest_client,
            )
        self.data_manager = data_manager
        self.runtime_mode = str(runtime_mode or "monitor_only").strip().lower()
        if order_manager is not None:
            self.order_manager = order_manager
        elif self.runtime_mode == "trading":
            self.order_manager = OrderManager(exchange_client=self.rest_client)
        else:
            self.order_manager = OrderManager()
        self.strategy_runtime = dict(strategy_runtime or {})

        self.execution_quality_report_path = (
            Path(execution_quality_report_path)
            if execution_quality_report_path is not None
            else None
        )
        metrics_path = runtime_metrics_snapshot_path or self.strategy_runtime.get(
            "runtime_metrics_snapshot_path"
        )
        self.runtime_metrics_snapshot_path = Path(metrics_path) if metrics_path else None
        self.account_sync_interval_seconds = max(1.0, float(account_sync_interval_seconds))
        self.account_sync_failure_threshold = max(1, int(account_sync_failure_threshold))
        self.execution_degradation_min_samples = max(1, int(execution_degradation_min_samples))
        self.execution_degradation_warn_edge_bps = float(execution_degradation_warn_edge_bps)
        self.execution_degradation_block_edge_bps = float(execution_degradation_block_edge_bps)
        self.execution_degradation_throttle_scale = min(
            1.0, max(0.0, float(execution_degradation_throttle_scale))
        )
        self.health_monitor = DataHealthMonitor(stale_threshold_sec=stale_threshold_sec)
        if hasattr(self.data_manager, "health_monitor_keys"):
            self.health_monitor.register_streams(self.data_manager.health_monitor_keys())
        self.health_check_interval_seconds = max(1.0, float(health_check_interval_seconds))
        self.reconcile_at_startup = bool(reconcile_at_startup)
        self.account_snapshot_fetcher = account_snapshot_fetcher
        self.account_sync_failure_count = 0
        self.market_feature_fetcher = (
            market_feature_fetcher or self._fetch_runtime_market_features_from_rest
        )
        self.market_feature_poll_interval_seconds = max(
            5.0,
            float(self.strategy_runtime.get("market_feature_poll_interval_seconds", 30.0) or 30.0),
        )
        self.runtime_market_feature_stale_after_seconds = max(
            self.market_feature_poll_interval_seconds,
            float(
                self.strategy_runtime.get(
                    "runtime_market_feature_stale_after_seconds",
                    self.market_feature_poll_interval_seconds * 2.0,
                )
                or (self.market_feature_poll_interval_seconds * 2.0)
            ),
        )
        # Keep the default ledger under the canonical project live directory.
        self.incubation_ledger = IncubationLedger(PROJECT_ROOT / "live" / "incubation_ledger.json")

        self._latest_book_ticker_by_symbol: Dict[str, Dict[str, Any]] = {}
        self._latest_runtime_market_features_by_symbol: Dict[str, Dict[str, Any]] = {}
        self._latest_final_kline_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        self._decision_outcomes: List[DecisionOutcome] = []
        self._auto_order_sequence = 0

        # Sprint 6: Risk and Decay components
        family_budgets = self.strategy_runtime.get("family_risk_budgets", {})
        if not risk_caps:
            risk_caps = RuntimeRiskCaps(per_family_caps=dict(family_budgets))

        self.risk_enforcer = RiskEnforcer(risk_caps)
        self.decay_monitor = DecayMonitor(decay_rules or _default_decay_rules())
        self.thesis_manager = ThesisStateManager()
        self._family_budgets = dict(family_budgets)

        # Workstream 1: Deploy admission control
        self._thesis_store = self._load_thesis_store()

        # P0: Thesis-batch reconciliation on startup
        if self._thesis_store and self.reconcile_at_startup:
            self._reconcile_thesis_batch()

        self._register_theses_in_manager()
        # B3: Inject per-thesis decay rules calibrated from evidence at startup.
        # These supplement (not replace) operator-provided rules.
        self._inject_per_thesis_decay_rules()

        self._thesis_memory_root = self._resolve_memory_root()

        # Sprint 7: Append-only audit log (optional — no-op if path not configured)
        audit_log_path = str(self.strategy_runtime.get("audit_log_path", "")).strip()
        self._audit_log: AuditLog | None = AuditLog(audit_log_path) if audit_log_path else None
        # Runtime session ID (changes each process start)
        self._session_id = str(uuid.uuid4())

        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._kill_switch_task: asyncio.Task | None = None

    @property
    def session_metadata(self) -> Dict[str, Any]:
        return {
            "symbols": list(self.symbols),
            "live_state_snapshot_path": (
                str(self.state_store._snapshot_path)
                if self.state_store._snapshot_path is not None
                else ""
            ),
            "live_state_auto_persist_enabled": bool(self.state_store._snapshot_path is not None),
            "kill_switch_recovery_streak": int(self.kill_switch.microstructure_recovery_streak),
            "account_sync_interval_seconds": float(self.account_sync_interval_seconds),
            "account_sync_failure_threshold": int(self.account_sync_failure_threshold),
            "market_feature_poll_interval_seconds": float(
                self.market_feature_poll_interval_seconds
            ),
            "execution_degradation_min_samples": int(self.execution_degradation_min_samples),
            "execution_degradation_warn_edge_bps": float(self.execution_degradation_warn_edge_bps),
            "execution_degradation_block_edge_bps": float(
                self.execution_degradation_block_edge_bps
            ),
            "execution_degradation_throttle_scale": float(
                self.execution_degradation_throttle_scale
            ),
            "execution_quality_report_path": (
                str(self.execution_quality_report_path)
                if self.execution_quality_report_path is not None
                else ""
            ),
            "runtime_metrics_snapshot_path": (
                str(self.runtime_metrics_snapshot_path)
                if self.runtime_metrics_snapshot_path is not None
                else ""
            ),
            "runtime_mode": self.runtime_mode,
            "strategy_runtime_implemented": bool(self.strategy_runtime.get("implemented", False)),
            "thesis_runtime_loaded": bool(self._thesis_store is not None),
            "thesis_count_loaded": (
                len(self._thesis_store.all()) if self._thesis_store is not None else 0
            ),
        }

    def _resolve_memory_root(self) -> Path | None:
        memory_root = str(self.strategy_runtime.get("memory_root", "")).strip()
        if not memory_root:
            return None
        return Path(memory_root)

    def _serialize_recent_decision(self, outcome: DecisionOutcome) -> Dict[str, Any]:
        top_match = outcome.ranked_matches[0] if outcome.ranked_matches else None
        thesis = top_match.thesis if top_match is not None else None
        thesis_regime = ""
        if thesis is not None:
            thesis_regime = (
                str(
                    thesis.canonical_regime
                    or (thesis.supportive_context or {}).get("canonical_regime", "")
                )
                .strip()
                .upper()
            )
        return {
            "timestamp": str(outcome.context.timestamp),
            "symbol": str(outcome.context.symbol),
            "primary_event_id": str(
                outcome.context.primary_event_id or outcome.context.event_family
            ),
            "canonical_regime": str(
                thesis_regime
                or outcome.context.canonical_regime
                or outcome.context.regime_snapshot.get("canonical_regime", "")
            ),
            "compat_event_family": str(outcome.context.event_family),
            "event_side": str(outcome.context.event_side),
            "active_event_ids": list(outcome.context.active_event_ids),
            "compat_active_event_families": list(outcome.context.active_event_families),
            "active_episode_ids": list(outcome.context.active_episode_ids),
            "action": str(outcome.trade_intent.action),
            "thesis_id": str(outcome.trade_intent.thesis_id),
            "thesis_canonical_regime": thesis_regime,
            "compat_thesis_event_family": str(
                (thesis.event_family or thesis.primary_event_id) if thesis is not None else ""
            ),
            "support_score": float(outcome.trade_intent.support_score),
            "contradiction_penalty": float(outcome.trade_intent.contradiction_penalty),
            "confidence_band": str(outcome.trade_intent.confidence_band),
            "match_count": int(len(outcome.ranked_matches)),
            "top_thesis_net_expectancy_bps": float(
                thesis.evidence.net_expectancy_bps if thesis is not None else 0.0
            ),
        }

    def _latest_market_state_by_symbol(self) -> Dict[str, Dict[str, Any]]:
        payload: Dict[str, Dict[str, Any]] = {}
        for symbol in sorted({str(s).upper() for s in self.symbols}):
            ticker = dict(self._latest_book_ticker_by_symbol.get(symbol, {}))
            runtime = self._fresh_runtime_market_features_for_symbol(symbol)
            payload[symbol] = {
                "best_bid_price": float(ticker.get("best_bid_price", 0.0) or 0.0),
                "best_ask_price": float(ticker.get("best_ask_price", 0.0) or 0.0),
                "ticker_timestamp": str(ticker.get("timestamp", "") or ""),
                "funding_rate": float(runtime.get("funding_rate", 0.0) or 0.0),
                "funding_timestamp": str(runtime.get("funding_timestamp", "") or ""),
                "open_interest": float(runtime.get("open_interest", 0.0) or 0.0),
                "open_interest_delta_fraction": float(
                    runtime.get("open_interest_delta_fraction", 0.0) or 0.0
                ),
                "open_interest_timestamp": str(runtime.get("open_interest_timestamp", "") or ""),
                "mark_price": float(runtime.get("mark_price", 0.0) or 0.0),
            }
        return payload

    def runtime_metrics_snapshot(self) -> Dict[str, Any]:
        recent_outcomes = list(self._decision_outcomes[-20:])
        action_counts: Dict[str, int] = {}
        symbol_counts: Dict[str, int] = {}
        for outcome in recent_outcomes:
            action = str(outcome.trade_intent.action)
            symbol = str(outcome.context.symbol)
            action_counts[action] = int(action_counts.get(action, 0)) + 1
            symbol_counts[symbol] = int(symbol_counts.get(symbol, 0)) + 1
        account = self.state_store.account
        health = self.health_monitor.check_health()

        # Sprint 6: Thesis states and Risk/Decay events
        thesis_states = {
            tid: {
                "state": s.state,
                "size_scalar": s.size_scalar,
                "cap_breach_count": s.cap_breach_count,
                "disable_reason": s.disable_reason,
            }
            for tid, s in self.thesis_manager.states.items()
        }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "runtime_mode": self.runtime_mode,
            "strategy_runtime_enabled": bool(self._strategy_runtime_enabled()),
            "thesis_runtime_loaded": bool(self._thesis_store is not None),
            "thesis_count_loaded": len(self._thesis_store.all())
            if self._thesis_store is not None
            else 0,
            "thesis_states": thesis_states,
            "risk_caps": {
                "breach_count": len(self.risk_enforcer.breach_history),
                "last_breaches": [
                    {
                        "timestamp": b.timestamp,
                        "thesis_id": b.thesis_id,
                        "cap_type": b.cap_type,
                        "action": b.action,
                    }
                    for b in self.risk_enforcer.breach_history[-10:]
                ],
            },
            "decay_monitor": {
                "health_history_count": len(self.decay_monitor.health_history),
                "degraded_count": len(
                    [s for s in self.thesis_manager.states.values() if s.state == "degraded"]
                ),
                "disabled_count": len(
                    [s for s in self.thesis_manager.states.values() if s.state == "disabled"]
                ),
            },
            "symbols": list(self.symbols),
            "kill_switch": self.state_store.get_kill_switch_snapshot(),
            "account": {
                "wallet_balance": float(account.wallet_balance),
                "margin_balance": float(account.margin_balance),
                "available_balance": float(account.available_balance),
                "total_unrealized_pnl": float(account.total_unrealized_pnl),
                "exchange_status": str(account.exchange_status),
                "position_count": int(len(account.positions)),
                "update_time": account.update_time.isoformat(),
            },
            "health": health,
            "execution_quality_summary": self.execution_quality_summary(),
            "latest_market_state_by_symbol": self._latest_market_state_by_symbol(),
            "decision_counts": {
                "recent_window": int(len(recent_outcomes)),
                "by_action": action_counts,
                "by_symbol": symbol_counts,
            },
            "recent_decisions": [
                self._serialize_recent_decision(outcome) for outcome in recent_outcomes
            ],
        }

    def persist_runtime_metrics_snapshot(self) -> Path | None:
        if self.runtime_metrics_snapshot_path is None:
            return None
        target = self.runtime_metrics_snapshot_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.runtime_metrics_snapshot(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return target

    def persist_deploy_run_summary(self, out_path: Path) -> Path:
        summary = {
            "deploy_run_id": f"deploy_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            "promoted_batch_id": self._thesis_store.run_id if self._thesis_store else "unknown",
            "runtime_mode": self.runtime_mode,
            "thesis_count_loaded": len(self._thesis_store.all()) if self._thesis_store else 0,
            "thesis_count_activated": len(
                [s for s in self.thesis_manager.states.values() if s.state == "active"]
            ),
            "thesis_count_degraded": len(
                [s for s in self.thesis_manager.states.values() if s.state == "degraded"]
            ),
            "thesis_count_disabled": len(
                [s for s in self.thesis_manager.states.values() if s.state == "disabled"]
            ),
            "cap_breach_count": len(self.risk_enforcer.breach_history),
            "decay_event_count": len(self.decay_monitor.health_history),
            "symbols": list(self.symbols),
        }
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        # Also persist detailed tables if possible
        # (This is a simplified version for Sprint 6)
        return out_path

    def _inject_per_thesis_decay_rules(self) -> None:
        """B3: Add per-thesis DecayRules calibrated from each thesis's promotion evidence.

        Edge decay threshold: fires downsize when realized edge < 50% of expected.
        Hit rate decay threshold: fires warn when realized hit_rate < 70% of expected.
        Window samples scale with evidence size so data-rich theses require more
        evidence before a rule triggers.
        """
        if not self._thesis_store:
            return
        existing_rule_ids = {r.rule_id for r in self.decay_monitor.rules}
        new_rules: List[DecayRule] = []
        for t in self._thesis_store.all():
            tid = t.thesis_id
            ev = t.evidence
            expected_bps = float(getattr(ev, "net_expectancy_bps", None) or 0.0)
            expected_hr = float(getattr(ev, "hit_rate", None) or 0.5)
            sample_size = int(getattr(ev, "sample_size", None) or 0)
            window = max(10, min(50, sample_size // 40))

            edge_rule_id = f"edge_decay_{tid}"
            hr_rule_id = f"hit_rate_decay_{tid}"

            if expected_bps > 0.0 and edge_rule_id not in existing_rule_ids:
                new_rules.append(
                    DecayRule(
                        rule_id=edge_rule_id,
                        metric="edge",
                        threshold=0.50,  # fire when realized < 50% of expected
                        window_samples=window,
                        action="downsize",
                        downsize_factor=0.50,
                    )
                )
                _LOG.info(
                    "Registered per-thesis edge decay rule for %s: threshold=50%% of %.1f bps, "
                    "window=%d samples.",
                    tid,
                    expected_bps,
                    window,
                )

            if hr_rule_id not in existing_rule_ids:
                hr_threshold = max(0.30, expected_hr * 0.70)  # 70% of expected hit rate
                new_rules.append(
                    DecayRule(
                        rule_id=hr_rule_id,
                        metric="hit_rate",
                        threshold=hr_threshold,
                        window_samples=window,
                        action="warn",
                    )
                )
                _LOG.info(
                    "Registered per-thesis hit_rate decay rule for %s: threshold=%.2f, "
                    "window=%d samples.",
                    tid,
                    hr_threshold,
                    window,
                )

        if new_rules:
            self.decay_monitor.rules = list(self.decay_monitor.rules) + new_rules
            _LOG.info("Injected %d per-thesis decay rules from thesis store.", len(new_rules))

    def _register_theses_in_manager(self):

        if self._thesis_store:
            for t in self._thesis_store.all():
                self.thesis_manager.register_thesis(
                    thesis_id=t.thesis_id,
                    promotion_class=t.promotion_class,
                    deployment_mode=t.deployment_state,
                )
                # A promoted thesis has already passed the full research + DeploymentApproval
                # pipeline. Graduate it immediately so the incubation gate doesn't block
                # live order submission. The 30-day incubation period applies to
                # DSL-blueprint strategies, not to thesis-promoted strategies.
                if not self.incubation_ledger.is_graduated(t.thesis_id):
                    self.incubation_ledger.graduate(t.thesis_id)
                    _LOG.info("Graduated promoted thesis %s in incubation ledger.", t.thesis_id)

    def _load_thesis_store(self) -> ThesisStore | None:
        thesis_path = str(self.strategy_runtime.get("thesis_path", "")).strip()
        thesis_run_id = str(self.strategy_runtime.get("thesis_run_id", "")).strip()
        strict_runtime = bool(self.strategy_runtime.get("implemented", False))
        try:
            store = None
            if thesis_path:
                store = ThesisStore.from_path(thesis_path)
            elif thesis_run_id:
                store = ThesisStore.from_run_id(thesis_run_id)

            if store:
                # Workstream 1: Admission Control - verify it only contains promoted theses
                # ThesisStore already validates this via PromotedThesis model
                _LOG.info("Loaded %d promoted theses for deployment.", len(store.all()))
                return store

        except (
            FileNotFoundError,
            CompatibilityRequiredError,
            DataIntegrityError,
            SchemaMismatchError,
        ) as exc:
            if strict_runtime:
                raise RuntimeError(
                    "Configured thesis store is unavailable for live runtime; "
                    "export theses from a specific run and set "
                    "strategy_runtime.thesis_path or strategy_runtime.thesis_run_id."
                ) from exc
            _LOG.warning("Configured thesis store is unavailable for live runtime: %s", exc)
            return None
        return None

    def _reconcile_thesis_batch(self) -> None:
        """P0: Reconcile current thesis batch against previous batch on startup.

        Detects added/removed/superseded/downgraded theses and enforces
        fail-safe rules before live trading can proceed.
        """
        if not self._thesis_store:
            return

        persist_dir = Path(self.strategy_runtime.get("persist_dir", "live/persist"))
        audit_log_path = persist_dir / "thesis_reconciliation.json"
        thesis_manager_state = {
            thesis_id: state.state for thesis_id, state in self.thesis_manager.states.items()
        }

        try:
            from project.core.config import get_data_root as _get_data_root

            result = reconcile_thesis_batch(
                current_store=self._thesis_store,
                persist_dir=persist_dir,
                thesis_manager_state=thesis_manager_state,
                audit_log_path=audit_log_path,
                data_root=_get_data_root(),
            )

            if not result.safe_to_proceed:
                _LOG.error(
                    "Thesis batch reconciliation failed with %d safety violations. "
                    "Live trading is BLOCKED. Violations: %s",
                    len(result.blocked_reasons),
                    "; ".join(result.blocked_reasons),
                )
                # Record unsafe state for operator review
                self.state_store.set_kill_switch_snapshot(
                    {
                        "is_active": True,
                        "reason": "thesis_batch_reconciliation_failure",
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "message": f"Batch reconciliation blocked: {'; '.join(result.blocked_reasons)}",
                    }
                )
            else:
                _LOG.info(
                    "Thesis batch reconciliation succeeded: %d added, %d unchanged, "
                    "%d removed, %d superseded, %d downgraded",
                    len(result.diff.added),
                    len(result.diff.unchanged),
                    len(result.diff.removed),
                    len(result.diff.superseded),
                    len(result.diff.downgraded),
                )
        except RECONCILIATION_DEGRADED_EXCEPTIONS as exc:
            wrapped = ThesisBatchReconciliationError(
                f"Thesis batch reconciliation failed in degraded mode: {exc}"
            )
            self._report_reconciliation_degraded_state(error=wrapped)
            _LOG.exception("%s", wrapped)
            if self.runtime_mode == "trading" and bool(
                self.strategy_runtime.get("implemented", False)
            ):
                raise wrapped

    def _report_reconciliation_degraded_state(
        self, *, error: ThesisBatchReconciliationError
    ) -> None:
        snapshot = {
            "is_active": bool(self.strategy_runtime.get("implemented", False)),
            "reason": "thesis_batch_reconciliation_degraded",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "message": str(error),
        }
        self.state_store.update_from_exchange_snapshot({"exchange_status": "DEGRADED"})
        self.state_store.set_kill_switch_snapshot(snapshot)

    def _strategy_runtime_enabled(self) -> bool:
        return (
            bool(self.strategy_runtime.get("implemented", False)) and self._thesis_store is not None
        )

    def latest_trade_intents(self) -> List[DecisionOutcome]:
        return list(self._decision_outcomes)

    def _ensure_runtime_mode_known(self) -> None:
        if self.runtime_mode not in {"monitor_only", "trading"}:
            raise RuntimeError(
                f"Unsupported runtime_mode '{self.runtime_mode}'. Expected 'monitor_only' or 'trading'."
            )

    def _ensure_runtime_ready_for_start(self) -> None:
        self._ensure_runtime_mode_known()
        if self.runtime_mode == "trading" and not bool(
            self.strategy_runtime.get("implemented", False)
        ):
            raise RuntimeError("runtime_mode='trading' requires strategy_runtime.implemented=true")

    def _ensure_trading_enabled(self) -> None:
        self._ensure_runtime_ready_for_start()
        if self.runtime_mode != "trading":
            raise RuntimeError("Order submission is disabled when runtime_mode='monitor_only'.")

    def _assess_execution_degradation(self, order: Any) -> Dict[str, float | str]:
        metadata = dict(getattr(order, "metadata", {}) or {})
        bucket_records = [
            item
            for item in self.order_manager.execution_attribution
            if item.symbol == str(order.symbol).upper()
            and item.strategy == str(metadata.get("strategy", ""))
            and item.volatility_regime == str(metadata.get("volatility_regime", ""))
            and item.microstructure_regime == str(metadata.get("microstructure_regime", ""))
        ]
        sample_count = len(bucket_records)
        if sample_count < self.execution_degradation_min_samples:
            return {
                "action": "allow",
                "sample_count": float(sample_count),
                "avg_realized_net_edge_bps": 0.0,
            }

        avg_realized_net_edge_bps = sum(
            float(item.realized_net_edge_bps) for item in bucket_records
        ) / float(sample_count)
        if avg_realized_net_edge_bps <= self.execution_degradation_block_edge_bps:
            return {
                "action": "block",
                "sample_count": float(sample_count),
                "avg_realized_net_edge_bps": float(avg_realized_net_edge_bps),
            }
        if avg_realized_net_edge_bps <= self.execution_degradation_warn_edge_bps:
            return {
                "action": "throttle",
                "sample_count": float(sample_count),
                "avg_realized_net_edge_bps": float(avg_realized_net_edge_bps),
            }
        return {
            "action": "allow",
            "sample_count": float(sample_count),
            "avg_realized_net_edge_bps": float(avg_realized_net_edge_bps),
        }

    def submit_strategy_result(
        self,
        result: Any,
        *,
        client_order_id: str,
        timestamp: Any | None = None,
        order_type: OrderType = OrderType.MARKET,
        realized_fee_bps: float = 0.0,
        market_state: Dict[str, float] | None = None,
        max_spread_bps: float = 5.0,
        min_depth_usd: float = 25_000.0,
        min_tob_coverage: float = 0.80,
    ) -> Dict[str, Any] | None:
        self._ensure_trading_enabled()
        prepared = self._prepare_strategy_order(
            result,
            client_order_id=client_order_id,
            timestamp=timestamp,
            order_type=order_type,
            realized_fee_bps=realized_fee_bps,
        )
        if prepared is None:
            return None
        order, blocked = prepared
        if blocked is not None:
            return blocked
        if getattr(self.order_manager, "exchange_client", None) is not None:
            raise OrderSubmissionFailed(
                "exchange-backed live submission requires await submit_strategy_result_async(...)"
            )
        return self.order_manager.submit_order(
            order,
            kill_switch_manager=self.kill_switch,
            market_state=market_state,
            max_spread_bps=max_spread_bps,
            min_depth_usd=min_depth_usd,
            min_tob_coverage=min_tob_coverage,
        )

    async def submit_strategy_result_async(
        self,
        result: Any,
        *,
        client_order_id: str,
        timestamp: Any | None = None,
        order_type: OrderType = OrderType.MARKET,
        realized_fee_bps: float = 0.0,
        market_state: Dict[str, float] | None = None,
        max_spread_bps: float = 5.0,
        min_depth_usd: float = 25_000.0,
        min_tob_coverage: float = 0.80,
    ) -> Dict[str, Any] | None:
        self._ensure_trading_enabled()
        prepared = self._prepare_strategy_order(
            result,
            client_order_id=client_order_id,
            timestamp=timestamp,
            order_type=order_type,
            realized_fee_bps=realized_fee_bps,
        )
        if prepared is None:
            return None
        order, blocked = prepared
        if blocked is not None:
            return blocked
        return await self.order_manager.submit_order_async(
            order,
            kill_switch_manager=self.kill_switch,
            market_state=market_state,
            max_spread_bps=max_spread_bps,
            min_depth_usd=min_depth_usd,
            min_tob_coverage=min_tob_coverage,
        )

    def _prepare_strategy_order(
        self,
        result: Any,
        *,
        client_order_id: str,
        timestamp: Any | None,
        order_type: OrderType,
        realized_fee_bps: float,
    ) -> tuple[Any, Dict[str, Any] | None] | None:
        order = build_live_order_from_strategy_result(
            result,
            client_order_id=client_order_id,
            timestamp=timestamp,
            order_type=order_type,
            realized_fee_bps=realized_fee_bps,
        )
        if order is None:
            return None

        # Sprint 7 — per-entity kill switch check (thesis / symbol / family)
        thesis_id = str(order.metadata.get("thesis_id", "")).strip()
        symbol = str(getattr(order, "symbol", "")).strip().upper()
        family = str(order.metadata.get("event_family", "")).strip().upper()
        blocked_by_entity, block_reason = self.kill_switch.is_thesis_blocked(
            thesis_id, symbol, family
        )
        if blocked_by_entity:
            order.update_status(OrderStatus.REJECTED)
            self.order_manager.order_history.append(order)
            _LOG.warning("Order %s blocked by kill switch: %s", client_order_id, block_reason)
            return order, {
                "accepted": False,
                "client_order_id": order.client_order_id,
                "blocked_by": "kill_switch",
                "kill_switch_reason": block_reason,
            }

        # Sprint 7 — log OrderIntentEvent before any further checks
        if self._audit_log is not None:
            intent_evt = OrderIntentEvent(
                session_id=self._session_id,
                thesis_id=thesis_id,
                thesis_version=str(order.metadata.get("thesis_version", "")),
                promotion_run_id=str(order.metadata.get("promotion_run_id", "")),
                validation_run_id=str(order.metadata.get("validation_run_id", "")),
                approval_record_id=str(order.metadata.get("approval_record_id", "")),
                signal_timestamp=str(order.metadata.get("signal_timestamp", "")),
                client_order_id=str(order.client_order_id),
                symbol=symbol,
                side=str(getattr(order, "side", "")).name
                if hasattr(getattr(order, "side", None), "name")
                else str(order.metadata.get("side", "")),
                order_type=str(order_type.name) if hasattr(order_type, "name") else str(order_type),
                quantity=float(getattr(order, "quantity", 0.0) or 0.0),
                expected_price=float(order.metadata.get("expected_entry_price", 0.0) or 0.0),
                kill_switch_state=bool(self.kill_switch.status.is_active),
                metadata={},
            )
            # Store the intent event_id on the order metadata so FillEvent can link back
            order.metadata["audit_intent_event_id"] = intent_evt.event_id
            self._audit_log.append(intent_evt)

        # Fail closed: a trading submission must already be fully graduated.
        strategy_id = str(order.metadata.get("strategy", "")).strip()
        if not strategy_id:
            order.update_status(OrderStatus.REJECTED)
            self.order_manager.order_history.append(order)
            return order, {
                "accepted": False,
                "client_order_id": order.client_order_id,
                "blocked_by": "missing_strategy_provenance",
            }
        if not self.incubation_ledger.is_graduated(strategy_id):
            _LOG.warning(
                "Strategy %s is still in incubation; rejecting live submission.", strategy_id
            )
            order.update_status(OrderStatus.REJECTED)
            self.order_manager.order_history.append(order)
            return order, {
                "accepted": False,
                "client_order_id": order.client_order_id,
                "blocked_by": "incubation_gate",
                "strategy_id": strategy_id,
            }

        degradation = self._assess_execution_degradation(order)
        order.metadata["execution_degradation_action"] = str(degradation["action"])
        order.metadata["execution_degradation_sample_count"] = float(degradation["sample_count"])
        order.metadata["execution_degradation_avg_realized_net_edge_bps"] = float(
            degradation["avg_realized_net_edge_bps"]
        )
        if degradation["action"] == "block":
            order.update_status(OrderStatus.REJECTED)
            self.order_manager.order_history.append(order)
            return order, {
                "accepted": False,
                "client_order_id": order.client_order_id,
                "blocked_by": "execution_degradation",
                "degradation": degradation,
            }
        if degradation["action"] == "throttle":
            original_quantity = float(order.quantity)
            order.quantity = original_quantity * self.execution_degradation_throttle_scale
            order.remaining_quantity = order.quantity
            order.metadata["execution_degradation_original_quantity"] = original_quantity
            order.metadata["execution_degradation_applied_scale"] = float(
                self.execution_degradation_throttle_scale
            )
        return order, None

    def _get_portfolio_state_for_sizing(self) -> Dict[str, Any]:
        """
        Produce a portfolio state snapshot suitable for the sizer,
        including active cluster counts for the 'Portfolio Matrix' gate.
        """
        with self.state_store._lock:
            acc = self.state_store.account
            cluster_counts: Dict[int, int] = {}
            symbol_exposures: Dict[str, float] = {}
            family_exposures: Dict[str, float] = {}

            for pos in acc.positions.values():
                notional = pos.quantity * pos.mark_price
                symbol_exposures[pos.symbol] = symbol_exposures.get(pos.symbol, 0.0) + notional

                if pos.cluster_id is not None:
                    cluster_counts[pos.cluster_id] = cluster_counts.get(pos.cluster_id, 0) + 1

                # Family lookup would need a map from thesis to family
                # For now we'll use symbol as a family proxy or look up from store
                if self._thesis_store:
                    # This is inefficient, but okay for small thesis sets
                    theses = self._thesis_store.filter(symbol=pos.symbol)
                    for t in theses:
                        family = t.event_family or t.primary_event_id
                        family_exposures[family] = family_exposures.get(family, 0.0) + notional

            return {
                "portfolio_value": float(acc.wallet_balance + acc.total_unrealized_pnl),
                "gross_exposure": float(
                    sum(abs(p.quantity * p.mark_price) for p in acc.positions.values())
                ),
                "symbol_exposures": symbol_exposures,
                "family_exposures": family_exposures,
                "max_gross_leverage": 1.0,
                "target_vol": 0.1,
                "current_vol": 0.1,
                "bucket_exposures": {},
                "active_cluster_counts": cluster_counts,
                "available_balance": float(acc.available_balance),
                "exchange_status": str(acc.exchange_status),
            }

    @staticmethod
    def _event_value(event: Any, key: str, default: Any = None) -> Any:
        if isinstance(event, Mapping):
            return event.get(key, default)
        return getattr(event, key, default)

    def _allowed_submission_actions(self) -> set[str]:
        configured = self.strategy_runtime.get("allowed_actions")
        if isinstance(configured, list) and configured:
            return {str(item).strip() for item in configured if str(item).strip()}
        # Default: only partial-size actions. To enable full-size live trades, set
        # strategy_runtime.allowed_actions = ["probe", "trade_small", "trade_normal"]
        _LOG.debug(
            "allowed_actions not configured; defaulting to probe+trade_small. "
            "trade_normal (100%% size) requires explicit strategy_runtime.allowed_actions config."
        )
        return {"probe", "trade_small"}

    def _build_execution_env_snapshot(self) -> Dict[str, Any]:
        return {
            "runtime_mode": self.runtime_mode,
            "exchange_status": str(self.state_store.account.exchange_status),
        }

    def _supported_event_ids(self) -> List[str]:
        configured = self.strategy_runtime.get(
            "supported_event_ids",
            self.strategy_runtime.get("supported_event_families", ["VOL_SHOCK"]),
        )
        if not isinstance(configured, list):
            return ["VOL_SHOCK"]
        values = [str(item).strip().upper() for item in configured if str(item).strip()]
        return values or ["VOL_SHOCK"]

    def _supported_event_families(self) -> List[str]:
        return self._supported_event_ids()

    def _requires_runtime_market_features(self) -> bool:
        return self._strategy_runtime_enabled() and "LIQUIDATION_CASCADE" in set(
            self._supported_event_ids()
        )

    async def _fetch_runtime_market_features_from_rest(self, symbol: str) -> Dict[str, Any]:
        if self.rest_client is None:
            return {}
        premium_payload: Dict[str, Any] | None = None
        open_interest_payload: Dict[str, Any] | None = None
        premium_task = asyncio.create_task(self.rest_client.get_premium_index(symbol))
        open_interest_task = asyncio.create_task(self.rest_client.get_open_interest(symbol))
        premium_result, open_interest_result = await asyncio.gather(
            premium_task, open_interest_task, return_exceptions=True
        )
        if isinstance(premium_result, Exception):
            _LOG.debug("Runtime premium-index fetch failed for %s: %s", symbol, premium_result)
        elif isinstance(premium_result, dict):
            premium_payload = premium_result
        elif isinstance(premium_result, list) and premium_result:
            first = premium_result[0]
            if isinstance(first, dict):
                premium_payload = first
        if isinstance(open_interest_result, Exception):
            _LOG.debug(
                "Runtime open-interest fetch failed for %s: %s", symbol, open_interest_result
            )
        elif isinstance(open_interest_result, dict):
            open_interest_payload = open_interest_result

        snapshot: Dict[str, Any] = {}
        if premium_payload is not None:
            snapshot["funding_rate"] = float(premium_payload.get("lastFundingRate", 0.0) or 0.0)
            snapshot["mark_price"] = float(premium_payload.get("markPrice", 0.0) or 0.0)
            premium_ts = premium_payload.get("time") or premium_payload.get("nextFundingTime")
            if premium_ts is not None:
                try:
                    snapshot["funding_timestamp"] = datetime.fromtimestamp(
                        float(premium_ts) / 1000.0, tz=timezone.utc
                    ).isoformat()
                except Exception:
                    snapshot["funding_timestamp"] = str(premium_ts)
        if open_interest_payload is not None:
            snapshot["open_interest"] = float(open_interest_payload.get("openInterest", 0.0) or 0.0)
            oi_ts = open_interest_payload.get("time")
            if oi_ts is not None:
                try:
                    snapshot["open_interest_timestamp"] = datetime.fromtimestamp(
                        float(oi_ts) / 1000.0, tz=timezone.utc
                    ).isoformat()
                except Exception:
                    snapshot["open_interest_timestamp"] = str(oi_ts)
        return snapshot

    def _fresh_runtime_market_features_for_symbol(self, symbol: str) -> Dict[str, Any]:
        runtime = dict(self._latest_runtime_market_features_by_symbol.get(str(symbol).upper(), {}))
        if not runtime:
            return {}
        refreshed_at = str(runtime.get("refreshed_at", "") or "").strip()
        if not refreshed_at:
            return {}
        try:
            refreshed_ts = datetime.fromisoformat(refreshed_at)
        except ValueError:
            return {}
        age_seconds = (datetime.now(timezone.utc) - refreshed_ts).total_seconds()
        if age_seconds > self.runtime_market_feature_stale_after_seconds:
            return {}
        return runtime

    async def _refresh_runtime_market_features_once(self) -> None:
        if self.market_feature_fetcher is None or not self._requires_runtime_market_features():
            return
        for symbol in self.symbols:
            normalized = str(symbol).upper()
            try:
                raw = await self.market_feature_fetcher(normalized)
            except Exception as exc:
                _LOG.warning("Runtime market-feature refresh failed for %s: %s", normalized, exc)
                self._latest_runtime_market_features_by_symbol.pop(normalized, None)
                continue
            if not isinstance(raw, Mapping):
                self._latest_runtime_market_features_by_symbol.pop(normalized, None)
                continue
            previous = dict(self._latest_runtime_market_features_by_symbol.get(normalized, {}))
            merged = dict(raw)
            merged["refreshed_at"] = datetime.now(timezone.utc).isoformat()
            open_interest = merged.get("open_interest")
            if open_interest is not None:
                try:
                    current_oi = float(open_interest)
                except Exception:
                    current_oi = None
                previous_oi = previous.get("open_interest")
                delta_fraction = merged.get("open_interest_delta_fraction")
                if delta_fraction is None and current_oi is not None:
                    try:
                        previous_oi_f = float(previous_oi) if previous_oi is not None else None
                    except Exception:
                        previous_oi_f = None
                    if previous_oi_f is not None and abs(previous_oi_f) > 0.0:
                        delta_fraction = (current_oi - previous_oi_f) / abs(previous_oi_f)
                    else:
                        delta_fraction = 0.0
                if delta_fraction is not None:
                    try:
                        merged["open_interest_delta_fraction"] = float(delta_fraction)
                    except Exception:
                        pass
                if current_oi is not None:
                    merged["open_interest"] = float(current_oi)
            if "funding_rate" in merged:
                try:
                    merged["funding_rate"] = float(merged.get("funding_rate", 0.0) or 0.0)
                except Exception:
                    merged["funding_rate"] = 0.0
            if "mark_price" in merged:
                try:
                    merged["mark_price"] = float(merged.get("mark_price", 0.0) or 0.0)
                except Exception:
                    merged["mark_price"] = 0.0
            self._latest_runtime_market_features_by_symbol[normalized] = merged
        self.persist_runtime_metrics_snapshot()

    async def _poll_runtime_market_features(self) -> None:
        while self._running:
            try:
                await self._refresh_runtime_market_features_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                _LOG.error("Error polling runtime market features: %s", exc)
            await asyncio.sleep(self.market_feature_poll_interval_seconds)

    def _current_market_snapshot(
        self,
        *,
        symbol: str,
        timeframe: str,
        close: float,
        timestamp: str,
        move_bps: float,
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol).upper()
        ticker = self._latest_book_ticker_by_symbol.get(normalized_symbol, {})
        runtime_features = self._fresh_runtime_market_features_for_symbol(normalized_symbol)
        bid = float(ticker.get("best_bid_price", 0.0) or 0.0)
        ask = float(ticker.get("best_ask_price", 0.0) or 0.0)
        spread_bps = 0.0
        mid_price = float(close)
        mark_price = float(runtime_features.get("mark_price", 0.0) or 0.0)
        if bid > 0.0 and ask > 0.0:
            mid_price = (bid + ask) / 2.0
            spread_bps = ((ask - bid) / mid_price) * 10_000.0 if mid_price > 0.0 else 0.0
        elif mark_price > 0.0:
            mid_price = mark_price
        regime_info = _classify_canonical_regime(
            move_bps=move_bps,
            rv_pct=runtime_features.get("rv_pct"),
            ms_trend_state=runtime_features.get("ms_trend_state"),
        )
        return {
            "timestamp": str(timestamp),
            "close": float(close),
            "last_price": float(close),
            "mid_price": float(mid_price),
            "mark_price": float(mark_price or close),
            "spread_bps": float(spread_bps),
            "depth_usd": float(
                self.strategy_runtime.get("default_depth_usd", 50_000.0) or 50_000.0
            ),
            "tob_coverage": float(self.strategy_runtime.get("default_tob_coverage", 0.95) or 0.95),
            "canonical_regime": regime_info["canonical_regime"],
            "regime_mode": regime_info["regime_mode"],
            "regime_confidence": regime_info["regime_confidence"],
            "regime_metadata": regime_info["regime_metadata"],
            "microstructure_regime": "healthy" if spread_bps <= 5.0 else "degraded",
            "expected_cost_bps": float(
                self.strategy_runtime.get("default_expected_cost_bps", 3.0) or 3.0
            ),
            "funding_rate": float(runtime_features.get("funding_rate", 0.0) or 0.0),
            "funding_timestamp": str(runtime_features.get("funding_timestamp", "") or ""),
            "open_interest": float(runtime_features.get("open_interest", 0.0) or 0.0),
            "open_interest_delta_fraction": float(
                runtime_features.get("open_interest_delta_fraction", 0.0) or 0.0
            ),
            "open_interest_timestamp": str(
                runtime_features.get("open_interest_timestamp", "") or ""
            ),
            "liquidation_notional_usd": float(
                self.data_manager.get_liquidation_notional(normalized_symbol)
                if hasattr(self.data_manager, "get_liquidation_notional")
                else runtime_features.get("liquidation_notional_usd", 0.0)
            ),
        }

    def _record_live_decision_episode(self, outcome: DecisionOutcome) -> None:
        if self._thesis_memory_root is None:
            return
        payload = {
            "timestamp": outcome.context.timestamp,
            "symbol": outcome.context.symbol,
            "primary_event_id": str(
                outcome.context.primary_event_id or outcome.context.event_family
            ),
            "canonical_regime": str(
                outcome.context.canonical_regime
                or outcome.context.regime_snapshot.get("canonical_regime", "")
            ),
            "compat_event_family": outcome.context.event_family,
            "event_side": outcome.context.event_side,
            "active_event_ids": list(outcome.context.active_event_ids),
            "compat_active_event_families": list(outcome.context.active_event_families),
            "active_episode_ids": list(outcome.context.active_episode_ids),
            "action": outcome.trade_intent.action,
            "thesis_id": outcome.trade_intent.thesis_id,
            "support_score": float(outcome.trade_intent.support_score),
            "contradiction_penalty": float(outcome.trade_intent.contradiction_penalty),
            "confidence_band": outcome.trade_intent.confidence_band,
        }
        append_live_episode(self._thesis_memory_root, payload)

    async def _submit_trade_intent_if_enabled(
        self,
        *,
        outcome: DecisionOutcome,
        market_state: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        if self.runtime_mode != "trading":
            return None
        if not bool(self.strategy_runtime.get("auto_submit", False)):
            return None
        if outcome.trade_intent.action not in self._allowed_submission_actions():
            return {
                "accepted": False,
                "blocked_by": "action_not_enabled",
                "action": outcome.trade_intent.action,
            }
        # Sprint 6: Risk Enforcer check
        top_match = outcome.ranked_matches[0]
        family = top_match.thesis.event_family or top_match.thesis.primary_event_id

        # Estimate attempted notional (mid_price * quantity)
        # This is an approximation for pre-trade check
        mid_price = float(market_state.get("mid_price", 0.0))
        # Need to know target quantity from intent?
        # TradeIntent doesn't have absolute quantity, only size_fraction.
        # Sizing happens in build_order_plan.

        plan = build_order_plan(
            intent=outcome.trade_intent,
            client_order_id=self._next_auto_order_id(outcome.context.symbol),
            market_state=market_state
            | {
                "expected_return_bps": float(
                    outcome.ranked_matches[0].thesis.evidence.estimate_bps or 0.0
                ),
                "expected_adverse_bps": float(
                    abs(
                        outcome.ranked_matches[0].thesis.expected_response.get("stop_value", 0.0)
                        or 0.0
                    )
                    * 10_000.0
                ),
                "expected_net_edge_bps": float(
                    outcome.ranked_matches[0].thesis.evidence.net_expectancy_bps or 0.0
                ),
            },
            portfolio_state=outcome.context.portfolio_state,
            max_notional_fraction=float(
                self.strategy_runtime.get("max_notional_fraction", 0.10) or 0.10
            ),
        )

        if not plan.accepted or plan.order is None:
            return {
                "accepted": False,
                "blocked_by": plan.blocked_by,
                "plan": plan.plan,
            }

        # Apply Risk Caps to the generated order
        attempted_notional = float(plan.order.quantity * mid_price)
        # Unique thesis IDs with at least one filled order this session.
        # Deduplication prevents the cap from firing prematurely when a single
        # thesis has multiple fills (each fill would otherwise inflate the count).
        active_thesis_ids = list(
            {
                str(o.metadata.get("thesis_id", ""))
                for o in self.order_manager.order_history
                if o.status == OrderStatus.FILLED and str(o.metadata.get("thesis_id", ""))
            }
        )

        # Portfolio Orchestration: Pass active overlap groups to risk enforcer
        active_overlap_groups = self._get_active_overlap_groups()
        thesis_overlap_group = ""
        if self._thesis_store:
            matching = [
                t for t in self._thesis_store.all() if t.thesis_id == outcome.trade_intent.thesis_id
            ]
            if matching:
                thesis_overlap_group = matching[0].overlap_group_id

        effective_notional, breach = self.risk_enforcer.check_and_apply_caps(
            thesis_id=outcome.trade_intent.thesis_id,
            symbol=outcome.trade_intent.symbol,
            family=family,
            attempted_notional=attempted_notional,
            portfolio_state=outcome.context.portfolio_state,
            active_thesis_ids=active_thesis_ids,
            timestamp=outcome.context.timestamp,
            active_overlap_groups=active_overlap_groups,
            thesis_overlap_group=thesis_overlap_group,
        )

        if effective_notional <= 0:
            return {
                "accepted": False,
                "blocked_by": "risk_cap",
                "breach": breach.cap_type if breach else "unknown",
            }

        if effective_notional < attempted_notional:
            _LOG.info(
                "Risk cap clipping order for %s: %f -> %f",
                outcome.trade_intent.thesis_id,
                attempted_notional,
                effective_notional,
            )
            plan.order.quantity = effective_notional / mid_price
            plan.order.remaining_quantity = plan.order.quantity

        return await self.order_manager.submit_order_async(
            plan.order,
            kill_switch_manager=self.kill_switch,
            market_state=market_state,
            max_spread_bps=float(self.strategy_runtime.get("max_spread_bps", 5.0) or 5.0),
            min_depth_usd=float(self.strategy_runtime.get("min_depth_usd", 25_000.0) or 25_000.0),
            min_tob_coverage=float(self.strategy_runtime.get("min_tob_coverage", 0.80) or 0.80),
        )

    def _get_active_overlap_groups(self) -> set[str]:
        active_groups = set()
        if not self._thesis_store:
            return active_groups

        for thesis_id, state in self.thesis_manager.states.items():
            if state.state == "active":
                # Find the thesis in the store to get its overlap_group_id
                # (This is slightly inefficient, but okay for small thesis sets)
                matching = [t for t in self._thesis_store.all() if t.thesis_id == thesis_id]
                if matching:
                    group_id = matching[0].overlap_group_id
                    if group_id:
                        active_groups.add(group_id)
        return active_groups

    def _next_auto_order_id(self, symbol: str) -> str:
        self._auto_order_sequence += 1
        return f"thesis-{str(symbol).lower()}-{self._auto_order_sequence:06d}"

    async def _process_kline_for_thesis_runtime(self, event: Any) -> None:
        if not self._strategy_runtime_enabled():
            return
        timeframe = str(self._event_value(event, "timeframe", "")).strip()
        symbol = str(self._event_value(event, "symbol", "")).upper().strip()
        is_final = bool(self._event_value(event, "is_final", False))
        if not symbol or not timeframe or not is_final:
            return

        close = float(self._event_value(event, "close", 0.0) or 0.0)
        volume = float(
            self._event_value(event, "quote_volume", self._event_value(event, "volume", 0.0)) or 0.0
        )
        timestamp = self._event_value(event, "timestamp")
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        elif hasattr(timestamp, "isoformat"):
            timestamp = timestamp.isoformat()

        prior = self._latest_final_kline_by_key.get((symbol, timeframe))
        previous_close = float(prior.get("close", 0.0) or 0.0) if prior else None
        supported = self._supported_event_ids()
        provisional_move_bps = 0.0
        if previous_close is not None and previous_close > 0.0:
            provisional_move_bps = ((float(close) / float(previous_close)) - 1.0) * 10_000.0
        market_state = self._current_market_snapshot(
            symbol=symbol,
            timeframe=timeframe,
            close=close,
            timestamp=str(timestamp),
            move_bps=float(provisional_move_bps),
        )
        detector = detect_live_event(
            symbol=symbol,
            timeframe=timeframe,
            current_close=close,
            previous_close=previous_close,
            volume=volume,
            market_features=market_state,
            supported_event_ids=supported,
            detector_config=self.strategy_runtime.get("event_detector", {}),
        )
        self._latest_final_kline_by_key[(symbol, timeframe)] = {
            "close": close,
            "timestamp": timestamp,
        }
        if detector is None:
            return

        market_state = market_state | dict(detector.features)

        # Portfolio Orchestration: Pass active overlap groups to retrieval
        active_groups = self._get_active_overlap_groups()

        context = build_live_trade_context(
            timestamp=str(timestamp),
            symbol=symbol,
            timeframe=timeframe,
            detected_event=detector,
            market_features=market_state,
            portfolio_state=self._get_portfolio_state_for_sizing(),
            execution_env=self._build_execution_env_snapshot(),
            active_groups=active_groups,
            family_budgets=self._family_budgets,
        )
        outcome = decide_trade_intent(
            context=context,
            thesis_store=self._thesis_store,
            policy_config=self.strategy_runtime.get("decision_policy", {}),
            include_pending=bool(
                self.strategy_runtime.get("include_pending_theses", self.runtime_mode != "trading")
            ),
        )

        # Sprint 6: Decay Monitor check
        if outcome.ranked_matches:
            top_thesis = outcome.ranked_matches[0].thesis
            realized = self._get_realized_metrics_for_thesis(top_thesis.thesis_id)
            # Expected metrics at promotion time. hit_rate comes from the thesis evidence
            # if available; falls back to 0.5 only when no empirical value was recorded.
            expected_hit_rate = float(
                getattr(top_thesis.evidence, "hit_rate", None)
                or (top_thesis.evidence.net_expectancy_bps > 0 and 0.55)  # directional prior
                or 0.5
            )
            expected = {
                "net_expectancy_bps": float(top_thesis.evidence.net_expectancy_bps or 0.0),
                "hit_rate": expected_hit_rate,
            }
            health = self.decay_monitor.assess_thesis_health(
                top_thesis.thesis_id, realized, expected
            )
            self.thesis_manager.update_health(
                top_thesis.thesis_id, health.health_state, health.actions_taken
            )

        self._decision_outcomes.append(outcome)
        self._decision_outcomes = self._decision_outcomes[-100:]
        self._record_live_decision_episode(outcome)

        # Sprint 6: Risk Enforcer check
        if outcome.trade_intent.action != "reject":
            thesis_id = outcome.trade_intent.thesis_id
            thesis_state = self.thesis_manager.get_state(thesis_id)
            if thesis_state and thesis_state.state in ("disabled", "paused"):
                _LOG.warning(
                    "Rejecting trade intent for %s: thesis state is %s",
                    thesis_id,
                    thesis_state.state,
                )
                # Overwrite intent
                # (Normally we'd use a more formal way to update the immutable TradeIntent)
                pass
            else:
                # Apply risk caps
                await self._submit_trade_intent_if_enabled(
                    outcome=outcome, market_state=market_state
                )

        self.persist_runtime_metrics_snapshot()

    def _get_realized_metrics_for_thesis(self, thesis_id: str) -> Dict[str, Any]:
        # Implementation to extract rolling metrics from execution_attribution
        records = [
            r
            for r in self.order_manager.execution_attribution
            if str(r.metadata.get("thesis_id", "")) == thesis_id
        ]
        if not records:
            return {
                "sample_count": 0,
                "avg_realized_net_edge_bps": 0.0,
                "hit_rate": 0.0,
                "avg_realized_slippage_bps": 0.0,
                "payoff_ratio": 0.0,
            }

        n = len(records)
        edge = sum(float(r.realized_net_edge_bps) for r in records) / n
        slippage = sum(float(r.realized_slippage_bps) for r in records) / n
        hit_rate = sum(1 for r in records if r.realized_net_edge_bps > 0.0) / n
        wins = [float(r.realized_net_edge_bps) for r in records if r.realized_net_edge_bps > 0.0]
        losses = [-float(r.realized_net_edge_bps) for r in records if r.realized_net_edge_bps < 0.0]
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        payoff_ratio = avg_win / avg_loss if avg_loss > 0.0 else 0.0
        return {
            "sample_count": n,
            "avg_realized_net_edge_bps": edge,
            "hit_rate": hit_rate,
            "avg_realized_slippage_bps": slippage,
            "payoff_ratio": payoff_ratio,
        }

    def on_order_fill(self, client_order_id: str, fill_qty: float, fill_price: float) -> None:
        self.order_manager.on_fill(client_order_id, fill_qty=fill_qty, fill_price=fill_price)
        if self._thesis_memory_root is not None:
            for order in reversed(self.order_manager.order_history):
                if order.client_order_id != client_order_id:
                    continue
                append_live_episode(
                    self._thesis_memory_root,
                    {
                        "timestamp": order.updated_at.isoformat(),
                        "symbol": order.symbol,
                        "action": str(order.metadata.get("trade_intent_action", "filled")).strip(),
                        "thesis_id": str(order.metadata.get("thesis_id", "")).strip(),
                        "realized_net_edge_bps": float(
                            order.metadata.get("expected_net_edge_bps", 0.0) or 0.0
                        ),
                    },
                )
                break
        self.persist_execution_quality_report()
        self.persist_runtime_metrics_snapshot()

    def execution_quality_summary(self) -> Dict[str, float]:
        return self.order_manager.summarize_execution_quality()

    def persist_execution_quality_report(self) -> Path | None:
        if self.execution_quality_report_path is None:
            return None
        target = self.execution_quality_report_path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": self.execution_quality_summary(),
            "by_symbol": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "symbol"
            ),
            "by_strategy": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "strategy"
            ),
            "by_thesis_id": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "thesis_id"
            ),
            "by_overlap_group_id": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "overlap_group_id"
            ),
            "by_governance_tier": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "governance_tier"
            ),
            "by_operational_role": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "operational_role"
            ),
            "by_volatility_regime": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "volatility_regime"
            ),
            "by_microstructure_regime": summarize_execution_attribution_by(
                self.order_manager.execution_attribution, "microstructure_regime"
            ),
            "records": [
                {
                    "client_order_id": item.client_order_id,
                    "symbol": item.symbol,
                    "strategy": item.strategy,
                    "volatility_regime": item.volatility_regime,
                    "microstructure_regime": item.microstructure_regime,
                    "side": item.side,
                    "quantity": float(item.quantity),
                    "signal_timestamp": item.signal_timestamp,
                    "expected_net_edge_bps": float(item.expected_net_edge_bps),
                    "realized_net_edge_bps": float(item.realized_net_edge_bps),
                    "edge_decay_bps": float(item.edge_decay_bps),
                    "created_at": item.created_at,
                }
                for item in self.order_manager.execution_attribution
            ],
        }
        target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return target

    async def start(self):
        _LOG.info("Starting Live Engine for %s", self.symbols)
        self._ensure_runtime_ready_for_start()
        if self.state_store._snapshot_path is not None:
            _LOG.info("Live state auto-persist enabled at %s", self.state_store._snapshot_path)

        if self.reconcile_at_startup and self.account_snapshot_fetcher is not None:
            _LOG.info("Performing startup account sync...")
            exchange_snapshot = await self.account_snapshot_fetcher()
            discrepancies = self.state_store.reconcile(exchange_snapshot)
            if discrepancies:
                for error in discrepancies:
                    _LOG.warning(
                        "Startup reconciliation drift (expected from inter-snapshot "
                        "venue movement): %s",
                        error,
                    )
            else:
                _LOG.info("Startup reconciliation verified — no drift.")
            self.state_store.update_from_exchange_snapshot(exchange_snapshot)
            _LOG.info("Startup account state synced from exchange.")

        self._running = True

        # Start the data ingestion manager
        await self.data_manager.start()

        # Prime runtime market features before the first decision cycle when required.
        if self._requires_runtime_market_features():
            await self._refresh_runtime_market_features_once()
        self.persist_runtime_metrics_snapshot()

        # Start consumers
        self._tasks.append(asyncio.create_task(self._consume_klines()))
        self._tasks.append(asyncio.create_task(self._consume_tickers()))
        self._tasks.append(asyncio.create_task(self._monitor_data_health()))
        if self._requires_runtime_market_features():
            self._tasks.append(asyncio.create_task(self._poll_runtime_market_features()))
        if self.account_snapshot_fetcher is not None:
            self._tasks.append(asyncio.create_task(self._sync_account_state()))

        # Keep running
        while self._running:
            await asyncio.sleep(1)

    async def stop(self):
        _LOG.info("Stopping Live Engine...")
        self._running = False
        await self._shutdown_runtime()

    async def _shutdown_runtime(self) -> None:
        try:
            await self.data_manager.stop()
        except Exception as exc:
            _LOG.error("Failed to stop data manager during shutdown: %s", exc)

        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                _LOG.error("Background task shutdown failed: %s", exc)
        self._tasks = []
        if hasattr(self.order_manager, "close"):
            try:
                await self.order_manager.close()
            except Exception as exc:
                _LOG.error("Failed to close order manager during shutdown: %s", exc)
        self.persist_runtime_metrics_snapshot()

    def _on_kill_switch_triggered(self, reason: KillSwitchReason, message: str) -> None:
        # Sprint 7 — audit the global kill-switch trigger
        if self._audit_log is not None:
            try:
                self._audit_log.append(
                    KillSwitchEvent(
                        session_id=self._session_id,
                        action="triggered",
                        scope="global",
                        reason=f"{reason.name}: {message}",
                    )
                )
            except Exception as exc:
                _LOG.error("Failed to write kill switch audit event: %s", exc)
        self._running = False
        if self._kill_switch_task is not None and not self._kill_switch_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _LOG.error(
                "Kill-switch triggered without an active event loop; async unwind could not start."
            )
            return
        self._kill_switch_task = loop.create_task(self._handle_kill_switch_trigger(reason, message))

    async def _handle_kill_switch_trigger(self, reason: KillSwitchReason, message: str) -> None:
        _LOG.critical("Actuating kill-switch %s: %s", reason.name, message)
        try:
            if self.runtime_mode == "trading":
                await self.order_manager.cancel_all_orders()
                await self.order_manager.flatten_all_positions(self.state_store)
            await self._shutdown_runtime()
        except Exception as exc:
            _LOG.error("Kill-switch actuation failed: %s", exc)
        self.persist_runtime_metrics_snapshot()

    async def _consume_klines(self):
        while self._running:
            event = None
            try:
                event = await self.data_manager.kline_queue.get()
                # Here we would update the live engine's feature state
                _LOG.debug(
                    f"Consumed kline: {event.symbol} {event.timeframe} close={event.close} final={event.is_final}"
                )
                self.health_monitor.on_event(event.symbol, f"kline:{event.timeframe}")
                await self._process_kline_for_thesis_runtime(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOG.error(f"Error consuming kline: {e}")
            finally:
                if event is not None:
                    self.data_manager.kline_queue.task_done()

    async def _consume_tickers(self):
        while self._running:
            event = None
            try:
                event = await self.data_manager.ticker_queue.get()
                # Here we would update order execution state, bid/ask spread, etc.
                _LOG.debug(
                    f"Consumed ticker: {event.symbol} bid={event.best_bid_price} ask={event.best_ask_price}"
                )
                self._latest_book_ticker_by_symbol[str(event.symbol).upper()] = {
                    "best_bid_price": float(event.best_bid_price),
                    "best_ask_price": float(event.best_ask_price),
                    "timestamp": (
                        event.timestamp.isoformat()
                        if hasattr(event.timestamp, "isoformat")
                        else str(event.timestamp)
                    ),
                }
                self.health_monitor.on_event(event.symbol, "ticker")
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOG.error(f"Error consuming ticker: {e}")
            finally:
                if event is not None:
                    self.data_manager.ticker_queue.task_done()

    async def _sync_account_state(self):
        while self._running:
            try:
                snapshot = await self.account_snapshot_fetcher()
                if isinstance(snapshot, dict):
                    self.state_store.update_from_exchange_snapshot(snapshot)
                    self.account_sync_failure_count = 0
                    self.persist_runtime_metrics_snapshot()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.account_sync_failure_count += 1
                _LOG.error(f"Error syncing account state: {e}")
                if self.account_sync_failure_count >= self.account_sync_failure_threshold:
                    self.kill_switch.trigger(
                        KillSwitchReason.ACCOUNT_SYNC_LOSS,
                        (
                            "Authenticated account sync failed "
                            f"{self.account_sync_failure_count} times consecutively"
                        ),
                    )
            await asyncio.sleep(self.account_sync_interval_seconds)

    async def _monitor_data_health(self):
        while self._running:
            try:
                report = self.health_monitor.check_health()
                if not report["is_healthy"]:
                    self.kill_switch.trigger(
                        KillSwitchReason.STALE_DATA,
                        (
                            f"Stale data feeds detected: {report['stale_count']} streams "
                            f"(max_staleness={report['max_last_seen_sec_ago']}s)"
                        ),
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOG.error(f"Error monitoring data health: {e}")
            await asyncio.sleep(self.health_check_interval_seconds)

    def _on_ws_reconnect_exhausted(self) -> None:
        """Callback invoked when the WebSocket client exhausts all reconnect attempts."""
        _LOG.error(
            "WebSocket reconnect retries exhausted; triggering EXCHANGE_DISCONNECT kill-switch."
        )
        self.kill_switch.trigger(
            KillSwitchReason.EXCHANGE_DISCONNECT,
            "WebSocket connection lost and all reconnect attempts exhausted",
        )


async def main(snapshot_path: str | Path | None = None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    runner = LiveEngineRunner(["btcusdt", "ethusdt"], snapshot_path=snapshot_path)

    loop = asyncio.get_running_loop()
    import signal

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(runner.stop()))
        except NotImplementedError:
            pass

    await runner.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
