from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import yaml
_DEFAULT_PATH = Path('spec/governance/data_capabilities.yaml')
@dataclass(frozen=True)
class DataCapabilityProfile:
    name: str
    available_feeds: dict[str, bool] = field(default_factory=dict)
    disabled_detectors: frozenset[str] = field(default_factory=frozenset)
    context_only_detectors: frozenset[str] = field(default_factory=frozenset)
    never_trade_standalone: frozenset[str] = field(default_factory=frozenset)
    trade_candidate_events: frozenset[str] = field(default_factory=frozenset)
    paper_approved_events: frozenset[str] = field(default_factory=frozenset)
    live_approved_events: frozenset[str] = field(default_factory=frozenset)
    research_only_events: frozenset[str] = field(default_factory=frozenset)
    killed_events: frozenset[str] = field(default_factory=frozenset)
    rejected_events: frozenset[str] = field(default_factory=frozenset)
    research_candidate_variants: dict[str, Any] = field(default_factory=dict)
    frozen_families: dict[str, Any] = field(default_factory=dict)
    requires_pairing: dict[str, Any] = field(default_factory=dict)
    composite_theses: dict[str, Any] = field(default_factory=dict)
    runtime_detectable_detectors: frozenset[str] = field(default_factory=frozenset)
    def detector_disabled(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.disabled_detectors
    def context_only(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.context_only_detectors
    def standalone_forbidden(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.never_trade_standalone
    def trade_candidate(self, event_id: str) -> bool:
        token = str(event_id or '').strip().upper()
        return token in self.trade_candidate_events and token not in self.killed_events
    def paper_approved(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.paper_approved_events
    def live_approved(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.live_approved_events
    def research_only(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.research_only_events
    def killed(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.killed_events
    def rejected(self, event_id: str) -> bool:
        return str(event_id or '').strip().upper() in self.rejected_events
    def family_frozen(self, family_id: str) -> bool:
        return str(family_id or '').strip().upper() in self.frozen_families
    def feed_available(self, feed_name: str) -> bool:
        return bool(self.available_feeds.get(str(feed_name or '').strip(), False))
def _profile_from_mapping(name: str, payload: dict[str, Any]) -> DataCapabilityProfile:
    return DataCapabilityProfile(
        name=str(name),
        available_feeds={str(k): bool(v) for k, v in dict(payload.get('available_feeds', {})).items()},
        disabled_detectors=frozenset(str(x).strip().upper() for x in payload.get('disabled_detectors', []) if str(x).strip()),
        context_only_detectors=frozenset(str(x).strip().upper() for x in payload.get('context_only_detectors', []) if str(x).strip()),
        never_trade_standalone=frozenset(str(x).strip().upper() for x in payload.get('never_trade_standalone', []) if str(x).strip()),
        trade_candidate_events=frozenset(str(x).strip().upper() for x in payload.get('trade_candidate_events', []) if str(x).strip()),
        paper_approved_events=frozenset(str(x).strip().upper() for x in payload.get('paper_approved_events', []) if str(x).strip()),
        live_approved_events=frozenset(str(x).strip().upper() for x in payload.get('live_approved_events', []) if str(x).strip()),
        research_only_events=frozenset(str(x).strip().upper() for x in payload.get('research_only_events', []) if str(x).strip()),
        killed_events=frozenset(str(x).strip().upper() for x in payload.get('killed_events', []) if str(x).strip()),
        rejected_events=frozenset(str(x).strip().upper() for x in payload.get('rejected_events', []) if str(x).strip()),
        research_candidate_variants=dict(payload.get('research_candidate_variants', {}) or {}),
        frozen_families={str(k).strip().upper(): v for k, v in dict(payload.get('frozen_families', {}) or {}).items()},
        requires_pairing=dict(payload.get('requires_pairing', {}) or {}),
        composite_theses=dict(payload.get('composite_theses', {}) or {}),
        runtime_detectable_detectors=frozenset(str(x).strip().upper() for x in payload.get('runtime_detectable_detectors', []) if str(x).strip()),
    )
def load_data_capability_profile(profile_name: str | None = None, *, path: str | Path = _DEFAULT_PATH) -> DataCapabilityProfile:
    config_path = Path(path)
    if not config_path.exists(): return DataCapabilityProfile(name=profile_name or 'no_liquidations_v1')
    payload = yaml.safe_load(config_path.read_text()) or {}; profiles = dict(payload.get('profiles', {}) or {})
    name = str(profile_name or payload.get('default_profile') or 'no_liquidations_v1')
    if name not in profiles: raise KeyError(f'unknown data capability profile: {name}')
    return _profile_from_mapping(name, dict(profiles[name] or {}))
def detector_trade_eligible(event_id: str, *, profile: DataCapabilityProfile | None = None) -> bool:
    active = profile or load_data_capability_profile(); token = str(event_id or '').strip().upper()
    return bool(token and active.trade_candidate(token) and not active.detector_disabled(token))
