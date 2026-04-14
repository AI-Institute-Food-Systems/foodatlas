"""Example test file."""

from src import __version__


def test_version() -> None:
    """Test that version is set."""
    assert __version__ == "0.1.0"
