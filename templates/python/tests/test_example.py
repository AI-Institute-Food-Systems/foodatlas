"""Example test file."""

from mypackage import __version__


def test_version() -> None:
    """Test that version is set."""
    assert __version__ == "0.1.0"
