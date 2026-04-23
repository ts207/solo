__all__ = [
    "CandidateDiscoveryConfig",
    "CandidateDiscoveryResult",
    "execute_candidate_discovery",
    "PromotionConfig",
    "PromotionServiceResult",
    "execute_promotion",
    "ReportBundleResult",
    "write_candidate_reports",
    "write_promotion_reports",
]


def __getattr__(name: str):
    if name in {
        "CandidateDiscoveryConfig",
        "CandidateDiscoveryResult",
        "execute_candidate_discovery",
    }:
        from project.research.services.candidate_discovery_service import (
            CandidateDiscoveryConfig,
            CandidateDiscoveryResult,
            execute_candidate_discovery,
        )

        exports = {
            "CandidateDiscoveryConfig": CandidateDiscoveryConfig,
            "CandidateDiscoveryResult": CandidateDiscoveryResult,
            "execute_candidate_discovery": execute_candidate_discovery,
        }
        return exports[name]
    if name in {"PromotionConfig", "PromotionServiceResult", "execute_promotion"}:
        from project.research.services.promotion_service import (
            PromotionConfig,
            PromotionServiceResult,
            execute_promotion,
        )

        exports = {
            "PromotionConfig": PromotionConfig,
            "PromotionServiceResult": PromotionServiceResult,
            "execute_promotion": execute_promotion,
        }
        return exports[name]
    if name in {"ReportBundleResult", "write_candidate_reports", "write_promotion_reports"}:
        from project.research.services.reporting_service import (
            ReportBundleResult,
            write_candidate_reports,
            write_promotion_reports,
        )

        exports = {
            "ReportBundleResult": ReportBundleResult,
            "write_candidate_reports": write_candidate_reports,
            "write_promotion_reports": write_promotion_reports,
        }
        return exports[name]
    raise AttributeError(name)
