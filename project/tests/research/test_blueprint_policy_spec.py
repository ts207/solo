import numpy as np

from project.research.blueprint_compilation import _derive_time_stop


def test_time_stop_uses_policy_bounds():
    out = _derive_time_stop(np.array([1000.0]), {"n_events": 10})
    assert out <= 192
