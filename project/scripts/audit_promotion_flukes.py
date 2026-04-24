import numpy as np
import pandas as pd

from project.eval.audit_stats import detect_selection_bias, permutation_test


def run_fluke_audit():
    print("Auditing Promotion Results for Statistical Flukes...")

    np.random.seed(42)
    n_hypotheses = 1000
    n_obs = 500

    # Simulate 1000 noisy strategies
    all_p_values = []
    strategies = []

    for i in range(n_hypotheses):
        returns = pd.Series(np.random.normal(0, 0.01, n_obs))
        # Simple t-test p-value (approx)
        sr = returns.mean() / returns.std()
        t_stat = sr * np.sqrt(n_obs)
        # Use a simple normal approximation for p-value
        from scipy.stats import norm

        p_val = 2 * (1 - norm.cdf(abs(t_stat)))
        all_p_values.append(p_val)

        if p_val < 0.05:
            strategies.append({"id": f"strat_{i}", "p_value": p_val, "returns": returns})

    print(f"Found {len(strategies)} candidates with nominal p < 0.05 out of {n_hypotheses} trials.")

    # Apply Audit 1: Selection Bias Detection
    bias_report = detect_selection_bias(all_p_values, n_hypotheses)
    print("\nAudit 1: Selection Bias (Multiple Comparison Check)")
    print(f"  Best observed p: {bias_report['best_observed_p']:.6f}")
    print(f"  Expected best p by chance: {bias_report['expected_best_p']:.6f}")
    print(f"  Suspicious? {bias_report['is_suspicious']}")

    # Apply Audit 2: Permutation Test on the 'best' candidate
    best_strat = min(strategies, key=lambda x: x["p_value"])
    print(f"\nAudit 2: Permutation Test on Best Candidate ({best_strat['id']})")
    perm_p = permutation_test(best_strat["returns"], n_permutations=500)
    print(f"  Nominal p-value: {best_strat['p_value']:.6f}")
    print(f"  Permutation p-value: {perm_p:.6f}")

    if perm_p > 0.05:
        print("  RESULT: Flagged as Statistical Fluke! (Failed permutation test)")
    else:
        print("  RESULT: Robust (Passed permutation test)")


if __name__ == "__main__":
    run_fluke_audit()
