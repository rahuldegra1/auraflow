class DAGValidationError(Exception):
    """Base exception for invalid workflow schemas."""


class CircularDependencyError(DAGValidationError):
    """Raised when the workflow graph contains at least one cycle."""


class DanglingDependencyError(DAGValidationError):
    """Raised when a task depends on an ID not defined in the workflow."""
