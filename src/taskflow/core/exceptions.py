"""TaskFlow exceptions."""


class TaskFlowError(Exception):
    """Base exception for all TaskFlow errors."""


class TaskRetryError(TaskFlowError):
    """Exception raised when task retries are exhausted."""

    def __init__(
        self,
        message: str,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialise TaskRetryError."""
        super().__init__(message)
        self.original_exception = original_exception


class CircuitBreakerError(TaskFlowError):
    """Exception raised when circuit breaker is open."""


class TaskTimeoutError(TaskFlowError):
    """Exception raised when a task times out."""


class TaskNotFoundError(TaskFlowError):
    """Exception raised when a task cannot be found."""


class QueueFullError(TaskFlowError):
    """Exception raised when a queue is full."""


class WorkerShutdownError(TaskFlowError):
    """Exception raised during worker shutdown."""


class SerializationError(TaskFlowError):
    """Exception raised when serialization fails."""


class InvalidTaskError(TaskFlowError):
    """Exception raised when task configuration is invalid."""


class BrokerConnectionError(TaskFlowError):
    """Exception raised when broker connection fails."""


class ResultBackendError(TaskFlowError):
    """Exception raised when result backend operations fail."""


class RateLimitError(TaskFlowError):
    """Exception raised when rate limit is exceeded."""


class SchedulingError(TaskFlowError):
    """Exception raised when task scheduling fails."""
