class ApplicationError(Exception):
    """A classified error crossing an adapter boundary."""


class PaymentNotFound(ApplicationError):
    pass


class IdempotencyConflict(ApplicationError):
    pass


class DuplicateKey(ApplicationError):
    pass


class DependencyUnavailable(ApplicationError):
    """Wait for infrastructure without starting a new processing attempt."""


class TransientProcessingError(ApplicationError):
    """An application attempt failed and may be retried."""


class PermanentProcessingError(ApplicationError):
    """Retry cannot repair this input or operation."""


class PublicationFailed(ApplicationError):
    """No positive, routed publish confirmation was obtained."""
