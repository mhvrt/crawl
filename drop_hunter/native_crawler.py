from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree

import httpx
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, HTTPCrawlerConfig
from crawl4ai.async_crawler_strategy import AsyncHTTPCrawlerStrategy
from crawl4ai.async_dispatcher import RateLimiter, SemaphoreDispatcher

from .crawl_priority import YieldPriorityQueue
from .link_extractor import extract_external_links, extract_internal_urls
from .retry_policy import retry_after_seconds, status_int
from .utils import atomic_write_text, canonicalize_url, hostname_from_url, is_valid_public_domain

RETRYABLE = {408, 425, 429, 500, 502, 503, 504}
DEFAULT_429_BACKOFF = 60.0


def _allowed_hosts(start_url: str) -> set[str]:
    host = hostname_from_url(start_url)
    bare = host.removeprefix("www.")
    return {host, bare, "www." + bare}


def _status(result) -> int | None:
    code = status_int(getattr(result, "redirected_status_code", None))
    return code if code is not None else status_int(getattr(result, "status_code", None))


def _title(result) -> str:
    meta = getattr(result, "metadata", None) or {}
    return str(meta.get("title") or meta.get("og:title") or "") if isinstance(meta, dict) else ""


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    atomic_write_text(path, "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in rows))


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def _browser_needed(html: str, internal: list[str], external: list[dict]) -> bool:
    if not html or len(html.strip()) < 300:
        return True
    if internal or external:
        return False
    low = html.lower()
    markers = ('__next_data__', 'id="__next"', 'id="root"', 'id="app"', "data-reactroot", "webpack")
    return low.count("<a ") < 2 and low.count("<script") >= 3 and any(m in low for m in markers)


async def _iter_results(value):
    if hasattr(value, "__aiter__"):
        async for item in value:
            yield item
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield item
    elif value is not None:
        yield value


async def _discover(start_url: str) -> list[str]:
    allowed = _allowed_hosts(start_url)
    todo = deque([urljoin(start_url, "/sitemap.xml")])
    seen, found = set(), set()
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(20),
        headers={"User-Agent": "LiveDropHunter/1.0 (+https://github.com/mhvrt/crawl)"},
    ) as client:
        try:
            robots = await client.get(urljoin(start_url, "/robots.txt"))
            if robots.is_success:
                for line in robots.text.splitlines():
                    key, _, value = line.partition(":")
                    if key.strip().lower() == "sitemap" and value.strip():
                        todo.append(value.strip())
        except httpx.HTTPError:
            pass

        while todo:
            sitemap = canonicalize_url(todo.popleft())
            if not sitemap or sitemap in seen or hostname_from_url(sitemap) not in allowed:
                continue
            seen.add(sitemap)
            try:
                response = await client.get(sitemap)
                if response.status_code == 429:
                    break
                if not response.is_success:
                    continue
                root = ElementTree.fromstring(response.content)
            except (httpx.HTTPError, ElementTree.ParseError):
                continue

            is_index = root.tag.rsplit("}", 1)[-1].lower() == "sitemapindex"
            for node in root.iter():
                if node.tag.rsplit("}", 1)[-1].lower() != "loc" or not node.text:
                    continue
                url = canonicalize_url(node.text)
                if not url:
                    continue
                if is_index:
                    todo.append(url)
                elif hostname_from_url(url) in allowed:
                    found.add(url)
            if todo:
                await asyncio.sleep(0.5)
    return sorted(found)


async def crawl_site(
    start_url: str,
    output_dir: Path,
    max_pages: int = 0,
    max_runtime_minutes: int = 315,
    full_page_scan: bool = False,
    use_current_discovery: bool = True,
    batch_size: int = 4,
    progress: Callable[[str], None] = print,
    resume: bool = False,
    max_query_variants_per_path: int = 100,
) -> tuple[list[dict], dict]:
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = output_dir / "crawl_checkpoint.json"
    links_file = output_dir / "partial_links.jsonl"
    errors_file = output_dir / "partial_errors.jsonl"
    start_url = canonicalize_url(start_url)
    allowed = _allowed_hosts(start_url)
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    started_epoch = time.time()
    deadline = time.monotonic() + max_runtime_minutes * 60

    live, terminal, queued = set(), set(), set()
    queue = YieldPriorityQueue()
    deferred: dict[str, dict] = {}
    errors: dict[str, dict] = {}
    links: list[dict] = []
    query_variants: dict[tuple[str, str], set[str]] = {}
    seen_target_domains: set[str] = set()

    resumed = False
    recovered = rate_limits = skipped_traps = 0
    browser_attempts = browser_successes = http_successes = 0
    pages_attempted_this_run = 0
    new_domains_this_run = 0
    stopped_by_rate_limit = False
    host_not_before = 0.0

    def completed_count() -> int:
        return len(live | terminal)

    def enqueue(url: str) -> None:
        nonlocal skipped_traps
        url = canonicalize_url(url)
        if not url or hostname_from_url(url) not in allowed:
            return
        if url in live or url in terminal or url in queued or url in deferred:
            return
        parts = urlsplit(url)
        if parts.query:
            key = (parts.hostname or "", parts.path or "/")
            seen = query_variants.setdefault(key, set())
            if parts.query not in seen and len(seen) >= max_query_variants_per_path:
                skipped_traps += 1
                return
            seen.add(parts.query)
        queued.add(url)
        queue.append(url)

    def defer(url: str, code: int | None, message: str, not_before: float = 0) -> None:
        url = canonicalize_url(url)
        live.discard(url)
        terminal.discard(url)
        queued.discard(url)
        row = {
            "url": url,
            "status": code or "",
            "error": message,
            "not_before": round(not_before, 3),
        }
        deferred[url] = row
        errors[url] = {"url": url, "status": code or "", "error": message}

    if resume and checkpoint.exists():
        try:
            state = json.loads(checkpoint.read_text(encoding="utf-8"))
            if canonicalize_url(state.get("start_url", "")) == start_url:
                live = {
                    canonicalize_url(x)
                    for x in state.get("successful_urls", state.get("fetched_urls", []))
                    if x
                }
                terminal = {
                    canonicalize_url(x)
                    for x in state.get("terminal_urls", [])
                    if x
                }
                queue.load_stats(state.get("section_yield_stats"))
                host_not_before = float(state.get("host_not_before", 0) or 0)

                for x in state.get("pending_urls", []):
                    u = canonicalize_url(x)
                    if u and u not in live and u not in terminal:
                        queued.add(u)
                        queue.append(u)

                for row in state.get("deferred_urls", []) or []:
                    if isinstance(row, dict) and row.get("url"):
                        u = canonicalize_url(row["url"])
                        deferred[u] = dict(row)
                        errors[u] = {
                            "url": u,
                            "status": row.get("status", ""),
                            "error": row.get("error", "retryable failure"),
                        }

                links = _read_jsonl(links_file)
                seen_target_domains = {
                    str(row.get("target_domain") or "").lower()
                    for row in links
                    if is_valid_public_domain(str(row.get("target_domain") or ""))
                }

                # Repair legacy checkpoints where retryable failures, especially
                # HTTP 429, were incorrectly counted as fetched.
                for row in _read_jsonl(errors_file):
                    u = canonicalize_url(row.get("url", ""))
                    if not u:
                        continue
                    code = status_int(row.get("status"))
                    if code in RETRYABLE or code is None:
                        live.discard(u)
                        terminal.discard(u)
                        deferred.setdefault(
                            u,
                            {
                                "url": u,
                                "status": code or "",
                                "error": row.get("error", "retryable failure"),
                                "not_before": 0,
                            },
                        )
                        errors[u] = {
                            "url": u,
                            "status": code or "",
                            "error": row.get("error", "retryable failure"),
                        }
                        recovered += 1
                    else:
                        terminal.add(u)
                        errors[u] = {
                            "url": u,
                            "status": code or "",
                            "error": row.get("error", "crawl failed"),
                        }

                now = time.time()
                for u, row in list(deferred.items()):
                    if float(row.get("not_before", 0) or 0) <= now:
                        deferred.pop(u, None)
                        errors.pop(u, None)
                        if u not in live and u not in terminal and u not in queued:
                            queued.add(u)
                            queue.append(u)

                resumed = True
                progress(
                    f"[resume] live={len(live)} terminal={len(terminal)} "
                    f"pending={len(queue)} deferred={len(deferred)} "
                    f"known_external_domains={len(seen_target_domains)} "
                    f"recovered_retryable={recovered}"
                )
        except Exception as exc:
            progress(f"[resume] checkpoint ignored: {type(exc).__name__}: {exc}")

    if not resumed:
        links_file.unlink(missing_ok=True)
        errors_file.unlink(missing_ok=True)
        enqueue(start_url)

    def save() -> None:
        atomic_write_text(
            checkpoint,
            json.dumps(
                {
                    "schema_version": 5,
                    "start_url": start_url,
                    "fetched_urls": sorted(live),
                    "successful_urls": sorted(live),
                    "terminal_urls": sorted(terminal),
                    "pending_urls": list(queue),
                    "deferred_urls": list(deferred.values()),
                    "successful_count": len(live),
                    "terminal_count": len(terminal),
                    "queued_count": len(queue),
                    "deferred_count": len(deferred),
                    "known_external_domains": len(seen_target_domains),
                    "section_yield_stats": queue.export_stats(),
                    "host_not_before": round(host_not_before, 3),
                    "partial_links_file": links_file.name,
                    "partial_errors_file": errors_file.name,
                    "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
            ),
        )
        _write_jsonl(errors_file, list(errors.values()))

    if resumed and host_not_before > time.time():
        progress(
            f"[resume] host Retry-After/backoff active for "
            f"{host_not_before-time.time():.1f}s; no requests sent"
        )
        save()
        return links, _stats(
            start_url,
            started_at,
            started_epoch,
            True,
            completed_count() + len(queue) + len(deferred),
            live,
            terminal,
            queue,
            deferred,
            links,
            errors,
            rate_limits,
            browser_attempts,
            browser_successes,
            http_successes,
            recovered,
            skipped_traps,
            False,
            False,
            True,
            pages_attempted_this_run,
            new_domains_this_run,
            seen_target_domains,
        )

    if use_current_discovery and not resumed:
        progress("[discovery] reading current robots/sitemaps")
        try:
            for url in await asyncio.wait_for(_discover(start_url), timeout=120):
                enqueue(url)
        except asyncio.TimeoutError:
            progress("[discovery] timed out after 120s; continuing from homepage")

    seed_count = completed_count() + len(queue) + len(deferred)
    effective_batch_size = max(1, min(int(batch_size), 8))

    # Fast default for static HTML: at most two simultaneous requests to a host,
    # with Crawl4AI's native per-domain pacing/backoff. A 429 still immediately
    # opens our host-wide circuit breaker and preserves the queue for resume.
    http_limiter = RateLimiter(
        base_delay=(0.5, 1.0),
        max_delay=30,
        max_retries=1,
        rate_limit_codes=[429, 503],
    )
    http_dispatcher = SemaphoreDispatcher(
        semaphore_count=2,
        max_session_permit=2,
        rate_limiter=http_limiter,
    )
    browser_limiter = RateLimiter(
        base_delay=(1.0, 2.0),
        max_delay=30,
        max_retries=1,
        rate_limit_codes=[429, 503],
    )
    browser_dispatcher = SemaphoreDispatcher(
        semaphore_count=1,
        max_session_permit=1,
        rate_limiter=browser_limiter,
    )

    http_strategy = AsyncHTTPCrawlerStrategy(
        browser_config=HTTPCrawlerConfig(
            method="GET",
            verify_ssl=True,
            follow_redirects=True,
        )
    )
    http_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        stream=False,
        verbose=False,
    )
    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        light_mode=True,
        avoid_ads=True,
        verbose=False,
    )
    browser_run = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        exclude_all_images=True,
        scan_full_page=full_page_scan,
        scroll_delay=0.15 if full_page_scan else 0,
        flatten_shadow_dom=True,
        remove_consent_popups=True,
        remove_overlay_elements=True,
        page_timeout=60000,
        stream=False,
        verbose=False,
    )

    async with AsyncExitStack() as stack:
        http = await stack.enter_async_context(
            AsyncWebCrawler(crawler_strategy=http_strategy)
        )
        browser = None

        async def browser_fetch(url: str):
            nonlocal browser, browser_attempts, browser_successes
            browser_attempts += 1
            if browser is None:
                progress("[browser] starting Chromium fallback")
                browser = await stack.enter_async_context(
                    AsyncWebCrawler(config=browser_cfg)
                )
            values = await browser.arun_many(
                urls=[url],
                config=browser_run,
                dispatcher=browser_dispatcher,
            )
            rows = [x async for x in _iter_results(values)]
            if rows and getattr(rows[0], "success", False):
                browser_successes += 1
            return rows[0] if rows else None

        async def handle(result, requested: str) -> tuple[bool, int]:
            nonlocal rate_limits, http_successes, host_not_before, new_domains_this_run
            requested = canonicalize_url(requested)

            if result is None:
                defer(
                    requested,
                    None,
                    "crawler returned no result",
                    time.time() + 30,
                )
                return False, 0

            code = _status(result)
            final = canonicalize_url(
                getattr(result, "redirected_url", None)
                or getattr(result, "url", "")
                or requested
            )
            html = getattr(result, "html", None) or ""

            if code == 429:
                rate_limits += 1
                headers = getattr(result, "response_headers", None)
                retry_after = retry_after_seconds(
                    headers if isinstance(headers, dict) else None
                )
                delay = retry_after if retry_after is not None else DEFAULT_429_BACKOFF
                host_not_before = max(host_not_before, time.time() + max(1, delay))
                defer(
                    requested,
                    429,
                    getattr(result, "error_message", "")
                    or "HTTP 429 Too Many Requests",
                    host_not_before,
                )
                progress(
                    f"[throttle] HTTP 429; stopping this site run for "
                    f"{max(1, delay):.1f}s"
                )
                return True, 0

            if hostname_from_url(final) not in allowed:
                terminal.add(requested)
                errors.pop(requested, None)
                queue.record_result(requested, 0)
                return False, 0

            ok = (
                bool(getattr(result, "success", False))
                and bool(html)
                and (code is None or code < 400)
            )

            if not ok and (code in RETRYABLE or code is None):
                defer(
                    requested,
                    code,
                    getattr(result, "error_message", "")
                    or "retryable source failure",
                    time.time() + 30,
                )
                return False, 0

            if not ok:
                terminal.add(requested)
                errors[requested] = {
                    "url": requested,
                    "status": code or "",
                    "error": getattr(result, "error_message", "")
                    or "non-live source page",
                }
                queue.record_result(requested, 0)
                return False, 0

            external = extract_external_links(final, html, code, _title(result))
            internal = extract_internal_urls(final, html, allowed)

            if full_page_scan or _browser_needed(html, internal, external):
                br = await browser_fetch(requested)
                if br is not None:
                    bcode = _status(br)
                    if bcode == 429:
                        headers = getattr(br, "response_headers", None)
                        retry_after = retry_after_seconds(
                            headers if isinstance(headers, dict) else None
                        )
                        delay = (
                            retry_after
                            if retry_after is not None
                            else DEFAULT_429_BACKOFF
                        )
                        host_not_before = max(
                            host_not_before,
                            time.time() + max(1, delay),
                        )
                        defer(
                            requested,
                            429,
                            getattr(br, "error_message", "")
                            or "HTTP 429 during browser fallback",
                            host_not_before,
                        )
                        rate_limits += 1
                        progress(
                            f"[throttle] browser fallback received HTTP 429; "
                            f"stopping this site run for {max(1, delay):.1f}s"
                        )
                        return True, 0

                    bhtml = getattr(br, "html", None) or ""
                    if (
                        getattr(br, "success", False)
                        and bhtml
                        and (bcode is None or bcode < 400)
                    ):
                        final = canonicalize_url(
                            getattr(br, "redirected_url", None)
                            or getattr(br, "url", "")
                            or requested
                        )
                        external = extract_external_links(
                            final,
                            bhtml,
                            bcode,
                            _title(br),
                        )
                        internal = extract_internal_urls(final, bhtml, allowed)

            page_domains = {
                str(row.get("target_domain") or "").lower()
                for row in external
                if is_valid_public_domain(str(row.get("target_domain") or ""))
            }
            new_domains = page_domains - seen_target_domains
            seen_target_domains.update(page_domains)
            new_count = len(new_domains)
            new_domains_this_run += new_count
            queue.record_result(requested, new_count)

            live.add(requested)
            terminal.discard(requested)
            deferred.pop(requested, None)
            errors.pop(requested, None)
            http_successes += 1

            if external:
                links.extend(external)
                _append_jsonl(links_file, external)
            for url in internal:
                enqueue(url)

            return False, new_count

        while queue and time.monotonic() < deadline:
            if max_pages > 0 and completed_count() >= max_pages:
                break

            remaining_capacity = effective_batch_size
            if max_pages > 0:
                remaining_capacity = min(
                    remaining_capacity,
                    max_pages - completed_count(),
                )
            if remaining_capacity <= 0:
                break

            batch: list[str] = []
            while queue and len(batch) < remaining_capacity:
                url = queue.popleft()
                queued.discard(url)
                if url in live or url in terminal or url in deferred:
                    continue
                batch.append(url)

            if not batch:
                continue

            try:
                values = await http.arun_many(
                    urls=batch,
                    config=http_cfg,
                    dispatcher=http_dispatcher,
                )
                rows = [x async for x in _iter_results(values)]
            except Exception as exc:
                progress(
                    f"[crawl] HTTP batch failed: {type(exc).__name__}: {exc}"
                )
                rows = []

            batch_limited = False
            batch_new_domains = 0
            for index, requested in enumerate(batch):
                result = rows[index] if index < len(rows) else None
                limited, new_count = await handle(result, requested)
                pages_attempted_this_run += 1
                batch_new_domains += new_count
                if limited:
                    batch_limited = True

            save()

            if (
                pages_attempted_this_run % 25 < len(batch)
                or batch_new_domains > 0
                or batch_limited
            ):
                progress(
                    f"[crawl] live={len(live)} terminal={len(terminal)} "
                    f"queue={len(queue)} deferred={len(deferred)} "
                    f"external_domains={len(seen_target_domains)} "
                    f"new_domains_this_run={new_domains_this_run} "
                    f"outbound_rows={len(links)}"
                )

            if batch_limited:
                stopped_by_rate_limit = True
                break

    deduped, seen = [], set()
    for row in links:
        key = (
            row.get("source_url"),
            row.get("target_url"),
            row.get("anchor"),
            row.get("rel"),
            row.get("xpath"),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    links = deduped
    _write_jsonl(links_file, links)
    save()

    remaining = len(queue) + len(deferred)
    stopped_runtime = time.monotonic() >= deadline and remaining > 0
    stopped_page = (
        max_pages > 0
        and completed_count() >= max_pages
        and remaining > 0
    )

    return links, _stats(
        start_url,
        started_at,
        started_epoch,
        resumed,
        seed_count,
        live,
        terminal,
        queue,
        deferred,
        links,
        errors,
        rate_limits,
        browser_attempts,
        browser_successes,
        http_successes,
        recovered,
        skipped_traps,
        stopped_runtime,
        stopped_page,
        stopped_by_rate_limit,
        pages_attempted_this_run,
        new_domains_this_run,
        seen_target_domains,
    )


def _stats(
    start_url,
    started_at,
    started_epoch,
    resumed,
    seed_count,
    live,
    terminal,
    queue,
    deferred,
    links,
    errors,
    rate_limits,
    browser_attempts,
    browser_successes,
    http_successes,
    recovered,
    skipped_traps,
    stopped_runtime,
    stopped_page,
    stopped_rate,
    pages_attempted_this_run,
    new_domains_this_run,
    seen_target_domains,
):
    remaining = len(queue) + len(deferred)
    yield_stats = queue.export_stats()
    ranked_sections = sorted(
        (
            {
                "section": section,
                "pages": values.get("pages", 0),
                "new_domains": values.get("new_domains", 0),
                "new_domains_per_page": round(
                    values.get("new_domains", 0)
                    / max(1, values.get("pages", 0)),
                    4,
                ),
            }
            for section, values in yield_stats.items()
        ),
        key=lambda row: (
            row["new_domains_per_page"],
            row["new_domains"],
        ),
        reverse=True,
    )[:20]

    return {
        "start_url": start_url,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "resumed": resumed,
        "initial_seed_urls": seed_count,
        "pages_crawled": len(live),
        "terminal_urls": len(terminal),
        "pages_attempted_this_run": pages_attempted_this_run,
        "remaining_queue_urls": remaining,
        "deferred_retryable_urls": len(deferred),
        "outbound_link_rows": len(links),
        "unique_external_domains_seen": len(seen_target_domains),
        "new_external_domains_this_run": new_domains_this_run,
        "crawl_errors": len(errors),
        "rate_limit_events": rate_limits,
        "browser_fallback_attempts": browser_attempts,
        "browser_fallback_successes": browser_successes,
        "http_successes_this_run": http_successes,
        "recovered_retryable_urls": recovered,
        "skipped_trap_urls": skipped_traps,
        "top_yield_sections": ranked_sections,
        "runtime_seconds": round(time.time() - started_epoch, 2),
        "crawl_complete": remaining == 0,
        "stopped_by_runtime_limit": stopped_runtime,
        "stopped_by_page_limit": stopped_page,
        "stopped_by_rate_limit": stopped_rate,
        "crawler_mode": "crawl4ai-http-first-yield-priority",
        "errors": list(errors.values()),
    }
