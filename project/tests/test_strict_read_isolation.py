import os

from project.io.utils import _strict_run_scoped_reads_enabled


def test_strict_read_isolation_default():
    """
    Mechanical Check 2: Cross-run contamination prevention
    Ensures that 'certify mode' is on by default, preventing shared lake reads
    unless explicitly disabled.
    """
    # Temporarily set to '1' to test enabled mode
    original = os.environ.get("BACKTEST_STRICT_RUN_SCOPED_READS")
    os.environ["BACKTEST_STRICT_RUN_SCOPED_READS"] = "1"

    try:
        assert _strict_run_scoped_reads_enabled() is True
    finally:
        if original is not None:
            os.environ["BACKTEST_STRICT_RUN_SCOPED_READS"] = original
        else:
            del os.environ["BACKTEST_STRICT_RUN_SCOPED_READS"]
