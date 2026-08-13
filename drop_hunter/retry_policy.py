from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Mapping

RETRYABLE_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def status_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_retryable_status(value) -> bool:
    code = status_int(value)
    return code in RETRYABLE_HTTP_STATUSES if code is not None else False


def retry_after_seconds(headers: Mapping | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After as delay-seconds or an HTTP date.

    Returns None when the header is missing or invalid. The caller should then use
    its normal exponential backoff policy.
    """
    if not headers:
        return None

    value = None
    for key, candidate in headers.items():
        if str(key).lower() == "retry-after":
            value = str(candidate).strip()
            break
    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        target = parsedate_to_datetime(value)
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (target - current).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def backoff_seconds(
    attempt: int,
    *,
    base_seconds: float = 30.0,
    cap_seconds: float = 300.0,
    retry_after: float | None = None,
) -> float:
    """Return a conservative delay, honoring Retry-After when it is longer."""
    attempt = max(1, int(attempt))
    exponential = min(cap_seconds, base_seconds * (2 ** (attempt - 1)))
    if retry_after is None:
        return exponential
    return min(cap_seconds, max(exponential, max(0.0, float(retry_after))))
