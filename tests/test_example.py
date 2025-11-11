"""Example test to verify pytest setup."""

import gpuport_collectors


def test_version() -> None:
    """Test that the package version is defined."""
    assert hasattr(gpuport_collectors, "__version__")
    assert isinstance(gpuport_collectors.__version__, str)
    assert gpuport_collectors.__version__ == "0.1.0"
