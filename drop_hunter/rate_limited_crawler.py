from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from crawl4ai import AsyncWebCrawler

from .crawler import crawl_site as _base_crawl_site
from .retry_policy import backoff_seconds, is_retryable_status, retry_after_seconds, status_int
from .utils import atomic_write_text, canonicalize_url


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _rewrite_jsonl(path: Path, rows: list[dict]) -> None:
    text = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)
    atomic_write_text(path, text)


def _repair_checkpoint(run_dir: Path) -> int:
    """Return retryable HTTP failures from older runs to the pending queue."""
    checkpoint_path = run_dir / "crawl_checkpoint.json"
    errors_path = run_dir / "partial_errors.jsonl"
    if not checkpoint_path.exists() or not errors_path.exists():
        return 0

    try:
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0

    errors = _load_jsonl(errors_path)
    retryable: dict[str, dict] = {}
    terminal: list[dict] = []
    for err in errors:
        url = canonicalize_url(err.get("url", ""))
        if url and is_retryable_status(err.get("status")):
            retryable[url] = err
        else:
            terminal.append(err)

    if not retryable:
        return 0

    fetched = {canonicalize_url(x) for x in state.get("fetched_urls", []) if x}
    pending = [canonicalize_url(x) for x in state.get("pending_urls", []) if x]
    pending_set = set(pending)

    for url in retryable:
        fetched.discard(url)
        if url not in pending_set:
            pending.append(url)
            pending_set.add(url)

    state["fetched_urls"] = sorted(fetched)
    state["fetched_count"] = len(fetched)
    state["pending_urls"] = pending
    state["queued_count"] = len(pending)
    state["rate_limit_recovered"] = len(retryable)
    state["saved_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    atomic_write_text(checkpoint_path, json.dumps(state, ensure_ascii=False, indent=2))
    _rewrite_jsonl(errors_path, terminal)
    return len(retryable)


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
    """Run the existing crawler with conservative request pacing.

    A 429 response is retried only after Retry-After (when supplied) or exponential
    backoff. Residual retryable responses are restored to the checkpoint so they are
    not counted as completed coverage and can be resumed later.
    """
    recovered_before_run = _repair_checkpoint(output_dir) if resume else 0
    if recovered_before_run:
        progress(f"[resume] restored {recovered_before_run} retryable URLs to pending queue")

    original_arun = AsyncWebCrawler.arun
    original_arun_many = AsyncWebCrawler.arun_many
    host_not_before = 0.0

    async def paced_arun(self, *args, **kwargs):
        nonlocal host_not_before
        last_result = None
        for attempt in range(1, 4):
            wait = host_not_before - time.time()
            if wait > 0:
                progress(f"[throttle] waiting {wait:.1f}s before next request")
                await asyncio.sleep(wait)

            result = await original_arun(self, *args, **kwargs)
            last_result = result
            code = status_int(getattr(result, "redirected_status_code", None))
            if code is None:
                code = status_int(getattr(result, "status_code", None))
            if code != 429:
                return result

            headers = getattr(result, "response_headers", None)
            server_delay = retry_after_seconds(headers if isinstance(headers, dict) else None)
            delay = backoff_seconds(
                attempt,
                base_seconds=60.0,
                cap_seconds=300.0,
                retry_after=server_delay,
            )
            host_not_before = max(host_not_before, time.time() + delay)
            progress(f"[throttle] HTTP 429; next request allowed in {delay:.1f}s")

        return last_result

    async def paced_arun_many(self, urls, *args, **kwargs):
        config = kwargs.get("config")
        if config is None and args:
            config = args[0]
        results = []
        for url in list(urls):
            result = await self.arun(url=url, config=config)
            results.append(result)
            await asyncio.sleep(2.0)
        return results

    AsyncWebCrawler.arun = paced_arun
    AsyncWebCrawler.arun_many = paced_arun_many
    try:
        links, stats = await _base_crawl_site(
            start_url,
            output_dir,
            max_pages=max_pages,
            max_runtime_minutes=max_runtime_minutes,
            full_page_scan=full_page_scan,
            use_current_discovery=use_current_discovery,
            batch_size=max(1, min(int(batch_size), 4)),
            progress=progress,
            resume=resume,
            max_query_variants_per_path=max_query_variants_per_path,
        )
    finally:
        AsyncWebCrawler.arun = original_arun
        AsyncWebCrawler.arun_many = original_arun_many

    recovered_after_run = _repair_checkpoint(output_dir)
    recovered_total = recovered_before_run + recovered_after_run
    if recovered_after_run:
        checkpoint_path = output_dir / "crawl_checkpoint.json"
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        stats["pages_crawled"] = int(state.get("fetched_count", stats.get("pages_crawled", 0)))
        stats["remaining_queue_urls"] = int(state.get("queued_count", 0))
        stats["crawl_complete"] = False
        stats["stopped_by_rate_limit"] = True

    stats["recovered_retryable_urls"] = recovered_total
    stats["rate_limit_policy"] = "retry-after-or-exponential-backoff"
    return links, stats
