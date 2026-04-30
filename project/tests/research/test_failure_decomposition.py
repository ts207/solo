from project.research.failure_decomposition import classify_failure

def test_classify_failure_insufficient_data():
    assert classify_failure(
        effective_n=40,
        mean_gross_bps=10.0,
        mean_net_bps=5.0,
        entry_lag_1_net=4.0,
        entry_lag_2_net=3.0,
        max_year_pnl_share=0.2,
        positive_year_count=3
    ) == "insufficient_data"

def test_classify_failure_no_gross_edge():
    assert classify_failure(
        effective_n=100,
        mean_gross_bps=-5.0,
        mean_net_bps=-10.0,
        entry_lag_1_net=-11.0,
        entry_lag_2_net=-12.0,
        max_year_pnl_share=0.2,
        positive_year_count=3
    ) == "no_gross_edge"

def test_classify_failure_cost_killed():
    assert classify_failure(
        effective_n=100,
        mean_gross_bps=5.0,
        mean_net_bps=-2.0,
        entry_lag_1_net=-3.0,
        entry_lag_2_net=-4.0,
        max_year_pnl_share=0.2,
        positive_year_count=3
    ) == "cost_killed"

def test_classify_failure_adverse_timing():
    assert classify_failure(
        effective_n=100,
        mean_gross_bps=5.0,
        mean_net_bps=-2.0,
        entry_lag_1_net=3.0,
        entry_lag_2_net=-4.0,
        max_year_pnl_share=0.2,
        positive_year_count=3
    ) == "adverse_timing"

def test_classify_failure_one_year_artifact():
    assert classify_failure(
        effective_n=100,
        mean_gross_bps=15.0,
        mean_net_bps=10.0,
        entry_lag_1_net=9.0,
        entry_lag_2_net=8.0,
        max_year_pnl_share=0.6,
        positive_year_count=1
    ) == "one_year_artifact"
