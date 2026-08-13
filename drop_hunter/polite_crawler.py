from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

from crawl4ai import AsyncWebCrawler

from .crawler import crawl_site as _base_crawl_site
from .rate_limited_crawler import _repair_checkpoint
from .retry_policy import backoff_seconds, retry_after_seconds, status_int
from .utils import canonicalize_url


def _status(result) -> int | None:
    code = status_int(getattr(result, "redirected_status_code", None))
    return code if code is not None else status_int(getattr(result, "status_code", None))


def _deferred(url: str):
    return SimpleNamespace(
        url=canonicalize_url(url),
        redirected_url=None,
        status_code=429,
        redirected_status_code=None,
        success=False,
        html="",
        metadata={},
        response_headers={},
        error_message="HTTP 429; deferred to a later resume",
    )


async def crawl_site(
    start_url: str,
    output_dir: Path,
    max_pages: int = 0,
    max_runtime_minutes: int = 315,
    full_page_scan: bool = False,
    use_current_discovery: bool = True,
    batch_size: int = 4,
    progress=print,
    resume: bool = False,
    max_query_variants_per_path: int = 100,
):
    restored = _repair_checkpoint(output_dir) if resume else 0
    if restored:
        progress(f"[resume] restored {restored} retryable URLs")

    original_arun = AsyncWebCrawler.arun
    original_arun_many = AsyncWebCrawler.arun_many
    circuit_open = False
    last_request_at = 0.0
    wait_total = 0.0
    rate_limit_events = 0

    async def live_request(self, *args, **kwargs):
        nonlocal last_request_at
        wait = 1.0 - (time.monotonic() - last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        # Browser navigation can otherwise consume the entire Actions job on one
        # stalled URL.  It is recorded as an access error by the base crawler and
        # the rest of the site can still be processed.
        result = await asyncio.wait_for(original_arun(self, *args, **kwargs), timeout=60)
        last_request_at = time.monotonic()
        return result

    async def polite_arun(self, *args, **kwargs):
        nonlocal circuit_open, wait_total, rate_limit_events
        url = kwargs.get("url") or (args[0] if args else "")
        if circuit_open:
            return _deferred(str(url))

        try:
            result = await live_request(self, *args, **kwargs)
        except asyncio.TimeoutError:
            progress(f"[crawl] request timed out after 60s: {url}")
            raise
        if _status(result) != 429:
            return result

        rate_limit_events += 1
        headers = getattr(result, "response_headers", None)
        retry_after = retry_after_seconds(headers if isinstance(headers, dict) else None)
        if retry_after is not None and retry_after > 180:
            circuit_open = True
            progress(f"[throttle] Retry-After={retry_after:.1f}s; deferring remaining URLs")
            return result

        delay = backoff_seconds(1, base_seconds=60, cap_seconds=180, retry_after=retry_after)
        wait_total += delay
        progress(f"[throttle] 429; waiting {delay:.1f}s once before one probe")
        await asyncio.sleep(delay)
        try:
            probe = await live_request(self, *args, **kwargs)
        except asyncio.TimeoutError:
            progress(f"[crawl] 429 probe timed out after 60s: {url}")
            circuit_open = True
            return result
        if _status(probe) == 429:
            rate_limit_events += 1
            circuit_open = True
            progress("[throttle] Probe also returned 429; deferring remaining URLs")
        return probe

    async def polite_many(self, urls, *args, **kwargs):
        config = kwargs.get("config") or (args[0] if args else None)
        results = []
        for url in list(urls):
            if circuit_open:
                results.append(_deferred(url))
            else:
                results.append(await self.arun(url=url, config=config))
        return results

    AsyncWebCrawler.arun = polite_arun
    AsyncWebCrawler.arun_many = polite_many
    try:
        links, stats = await _base_crawl_site(
            start_url,
            output_dir,
            max_pages=max_pages,
            max_runtime_minutes=max_runtime_minutes,
            full_page_scan=full_page_scan,
            use_current_discovery=use_current_discovery and not resume,
            batch_size=max(1, min(int(batch_size), 4)),
            progress=progress,
            resume=resume,
            max_query_variants_per_path=max_query_variants_per_path,
        )
    finally:
        AsyncWebCrawler.arun = original_arun
        AsyncWebCrawler.arun_many = original_arun_many

    deferred = _repair_checkpoint(output_dir)
    if deferred:
        state = json.loads((output_dir / "crawl_checkpoint.json").read_text(encoding="utf-8"))
        stats["pages_crawled"] = int(state.get("fetched_count", 0))
        stats["remaining_queue_urls"] = int(state.get("queued_count", 0))
        stats["crawl_complete"] = False
        stats["stopped_by_rate_limit"] = circuit_open

    stats["recovered_retryable_urls"] = restored
    stats["deferred_retryable_urls"] = deferred
    stats["rate_limit_events"] = rate_limit_events
    stats["rate_limit_probe_wait_seconds"] = round(wait_total, 2)
    stats["rate_limit_circuit_open"] = circuit_open
    stats["rate_limit_policy"] = "single-probe-then-defer"
    return links, stats
