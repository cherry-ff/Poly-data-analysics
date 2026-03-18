class ConfigurationError(Exception):
    """Raised when runtime configuration is invalid."""


class StateMismatchError(Exception):
    """Raised when local and exchange states cannot be reconciled."""
