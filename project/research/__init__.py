from __future__ import annotations

from typing import Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "EffectEstimate": ("project.research.validation.schemas", "EffectEstimate"),
    "EvidenceBundle": ("project.research.validation.schemas", "EvidenceBundle"),
    "FalsificationResult": ("project.research.validation.schemas", "FalsificationResult"),
    "MultiplicityResult": ("project.research.validation.schemas", "MultiplicityResult"),
    "PromotionDecision": ("project.research.validation.schemas", "PromotionDecision"),
    "PromotionPolicy": ("project.research.validation.evidence_bundle", "PromotionPolicy"),
    "StabilityResult": ("project.research.validation.schemas", "StabilityResult"),
    "ValidationSplit": ("project.research.validation.schemas", "ValidationSplit"),
    "adjust_pvalues_bh": ("project.research.validation.multiple_testing", "adjust_pvalues_bh"),
    "adjust_pvalues_by": ("project.research.validation.multiple_testing", "adjust_pvalues_by"),
    "adjust_pvalues_holm": ("project.research.validation.multiple_testing", "adjust_pvalues_holm"),
    "apply_embargo": ("project.research.validation.purging", "apply_embargo"),
    "apply_multiple_testing": ("project.research.validation.multiple_testing", "apply_multiple_testing"),
    "assign_split_labels": ("project.research.validation.splits", "assign_split_labels"),
    "assign_test_families": ("project.research.validation.multiple_testing", "assign_test_families"),
    "build_evidence_bundle": ("project.research.validation.evidence_bundle", "build_evidence_bundle"),
    "build_stability_result_from_row": ("project.research.validation.regime_tests", "build_stability_result_from_row"),
    "build_validation_splits": ("project.research.validation.splits", "build_validation_splits"),
    "bundle_to_flat_record": ("project.research.validation.evidence_bundle", "bundle_to_flat_record"),
    "compute_event_windows": ("project.research.validation.purging", "compute_event_windows"),
    "compute_regime_labels": ("project.research.validation.regime_tests", "compute_regime_labels"),
    "estimate_effect": ("project.research.validation.estimators", "estimate_effect"),
    "estimate_effect_from_frame": ("project.research.validation.estimators", "estimate_effect_from_frame"),
    "evaluate_by_regime": ("project.research.validation.regime_tests", "evaluate_by_regime"),
    "evaluate_cross_symbol_stability": ("project.research.validation.regime_tests", "evaluate_cross_symbol_stability"),
    "evaluate_negative_controls": ("project.research.validation.falsification", "evaluate_negative_controls"),
    "evaluate_promotion_bundle": ("project.research.validation.evidence_bundle", "evaluate_promotion_bundle"),
    "generate_placebo_events": ("project.research.validation.falsification", "generate_placebo_events"),
    "purge_overlapping_events": ("project.research.validation.purging", "purge_overlapping_events"),
    "resolve_split_scheme": ("project.research.validation.splits", "resolve_split_scheme"),
    "rolling_stability_metrics": ("project.research.validation.regime_tests", "rolling_stability_metrics"),
    "run_permutation_test": ("project.research.validation.falsification", "run_permutation_test"),
    "serialize_evidence_bundles": ("project.research.validation.evidence_bundle", "serialize_evidence_bundles"),
    "serialize_splits": ("project.research.validation.splits", "serialize_splits"),
    "validate_evidence_bundle": ("project.research.validation.evidence_bundle", "validate_evidence_bundle"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    from importlib import import_module

    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value
