"""
TurboBulk client exceptions.
"""

from typing import Dict, Any


class TurboBulkError(Exception):
    """Base exception for TurboBulk client errors."""

    pass


class JobFailedError(TurboBulkError):
    """Raised when a bulk job fails."""

    def __init__(self, message: str, job_result: Dict[str, Any]):
        super().__init__(message)
        self.job_result = job_result


class ValidationError(TurboBulkError):
    """Raised when data validation fails."""

    def __init__(self, message: str, errors: list):
        super().__init__(message)
        self.errors = errors


class ConnectionError(TurboBulkError):
    """Raised when connection to NetBox fails."""

    pass


class AuthenticationError(TurboBulkError):
    """Raised when authentication fails."""

    pass
