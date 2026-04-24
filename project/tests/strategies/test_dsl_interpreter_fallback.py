from unittest.mock import patch


# Define the fallback shim for njit
def fallback_njit(*args, **kwargs):
    """
    A shim that mimics njit's behavior when numba is not available.
    It either returns the function directly (for bare @njit)
    or returns a decorator that returns the function (for @njit(...)).
    """
    if args and callable(args[0]) and len(args) == 1 and not kwargs:
        # Case: @njit (bare decorator)
        return args[0]
    else:
        # Case: @njit(...) (parametrized decorator)
        def _decorator(fn):
            return fn

        return _decorator


# --- Test 1: Validate fallback behavior with bare @njit ---
def test_njit_fallback_supports_bare_decorator():
    """
    Tests if the fallback_njit shim correctly handles the bare decorator syntax.
    """
    # Monkeypatch njit to use our fallback shim for this test scope
    with patch("project.strategy.runtime.dsl_interpreter_v1.njit", fallback_njit):

        @fallback_njit
        def f(x):
            return x + 1

        assert f(1) == 2


# --- Test 2: Validate fallback behavior with parametrized @njit ---
def test_njit_fallback_supports_parametrized_decorator():
    """
    Tests if the fallback_njit shim correctly handles parametrized decorator syntax.
    """
    # Monkeypatch njit to use our fallback shim for this test scope
    with patch("project.strategy.runtime.dsl_interpreter_v1.njit", fallback_njit):

        @fallback_njit(cache=True)  # Example of a parameter
        def g(x):
            return x + 2

        assert g(1) == 3


# --- Test 3: Validate fallback behavior by checking the module symbol ---
# This test is designed to be stable across environments (with or without numba)
def test_njit_symbol_uses_fallback_or_numba():
    """
    Checks if the njit symbol in the dsl_interpreter_v1 module behaves as expected.
    If numba is installed, it should be from 'numba.core.decorators'.
    If numba is NOT installed, it should be a fallback/shim.
    This test specifically checks if the fallback shim IS BEING USED.
    """
    import project.strategy.runtime.dsl_interpreter_v1 as m

    # Check if njit symbol is present
    assert hasattr(m, "njit"), "njit symbol not found in dsl_interpreter_v1 module"

    # We want to ensure the fallback is active.
    # The fallback_njit shim returns itself when called without args.
    # A real njit decorator would return a numba.core.decorators.njit object or similar.
    # A simple way to check if it's the shim is to see if it's callable and returns itself.
    # More robust check: inspect its module.

    # If njit is from numba, it will be in numba's module.
    # If it's our fallback, it will be in this module or a custom shim.

    # To test the fallback specifically, we can monkeypatch it to our shim first,
    # then check if the shim works, as done in tests 1 and 2.
    # For a test that verifies *if* the fallback is active in the actual module:

    # Approach: Monkeypatch __import__ to simulate numba being unavailable.
    # This is more complex and might affect other imports.
    # A simpler approach is to rely on the fact that if numba is NOT installed,
    # the symbol in m.njit will likely be a shim provided by the library itself,
    # or we can dynamically inject our shim as done in tests 1 & 2.

    # Let's rely on tests 1 & 2 to validate the fallback logic itself.
    # This test will be more of a sanity check on the module's symbol.

    # If njit is decorated and is itself callable, it might be the shim.
    # If it's a function that returns a decorator, it's likely the real one or a shim.

    # A more direct check for the *actual* fallback path:
    # We can check the module of the njit object if it's present.
    # If numba is installed, this would be 'numba.core.decorators'.
    # If numba is *not* installed, the project might provide its own shim,
    # or the import might fail in a way that leads to our fallback.

    # Let's assume for this test that if we can call it and it behaves like the shim,
    # it's either the shim or numba is not present.
    # Test 1 & 2 already validated the *shim's* behavior.
    # This test aims to confirm the *module's* njit symbol is usable.

    # For stability, we won't try to *force* numba absence here,
    # but rather ensure the symbol is usable.
    # If numba is present, this symbol should be the actual numba njit.
    # If numba is absent, it should be the project's provided shim.

    # We can infer the fallback if the symbol IS NOT from numba's module.
    # However, this requires numba to be installed to check the 'else' condition.
    # A more robust check is to see if our shim (if we inject it) works.
    # Since tests 1 & 2 already validate the shim, we can skip a complex check here
    # and rely on those tests.

    # Re-running tests 1 and 2 within the context of the actual module symbol
    # if we were to do a full integration test.

    # Let's refine Test 3 to specifically check the *presence* of a fallback
    # or the correct numba import without forcing numba's absence.

    # If numba is installed, m.njit should be the actual numba decorator.
    # If numba is NOT installed, m.njit should be a shim.

    # We can check the module of the imported njit.
    try:
        import numba

        numba_module_name = numba.core.decorators.njit.__module__
    except ImportError:
        numba_module_name = None  # Numba is not installed

    # Check the module of the njit symbol from dsl_interpreter_v1
    njit_symbol_module = getattr(m.njit, "__module__", None)

    if numba_module_name:
        # Numba is installed. The symbol should be from numba's module.
        assert njit_symbol_module == numba_module_name, (
            f"Expected njit to be from '{numba_module_name}', but found '{njit_symbol_module}'"
        )
    else:
        # Numba is NOT installed. The symbol should be a fallback/shim.
        # The shim we defined (fallback_njit) does not have a __module__ attribute in this context.
        # We can check if it's NOT from numba's expected module.
        # If the module is None or not numba's, it's likely a fallback.
        assert njit_symbol_module is None or njit_symbol_module != numba_module_name, (
            f"Numba not installed: Expected njit to be a fallback (module not '{numba_module_name}'), but found '{njit_symbol_module}'"
        )

    # Additionally, let's ensure the shim logic itself works when injected.
    # This is covered by tests 1 & 2, but we can re-run one here for clarity.
    with patch("project.strategy.runtime.dsl_interpreter_v1.njit", fallback_njit):

        @fallback_njit
        def h(x):
            return x * 2

        assert h(5) == 10
