from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, DomainMapperConfig

from .link_extractor import extract_external_links, extract_internal_urls
from .utils import atomic_write_text, canonicalize_url, hostname_from_url, registrable_domain


def _metadata_title(result) -> str:
    metadata = getattr(result, "metadata", None) or {}
    if isinstance(metadata, dict):
        return str(metadata.get("title") or metadata.get("og:title") or "")
    return ""


def _allowed_hosts(start_url: str) -> set[str]:
    host = hostname_from_url(start_url)
    bare = host.removeprefix("www.")
    return {host, bare, "www." + bare}


async def _discover_current_urls(crawler: AsyncWebCrawler, start_url: str) -> list[str]:
    """Seed exhaustive queue from current site discovery sources."""
    root = registrable_domain(hostname_from_url(start_url))
    allowed = _allowed_hosts(start_url)
    try:
        mapped = await crawler.amap_domain(
            root,
            DomainMapperConfig(
                source="sitemap+robots+feed+homepage",
                max_urls=-1,
                concurrency=20,
                hits_per_sec=5,
                extract_head=False,
                filter_nonsense_urls=True,
                soft_404_detection=True,
                use_browser_for_homepage=False,
            ),
        )
    except Exception:
        return []

    urls: list[str] = []
    for item in mapped or []:
        url = item.get("url", "") if isinstance(item, dict) else ""
        if not url:
            continue
        url = canonicalize_url(url)
        if hostname_from_url(url) in allowed:
            urls.append(url)
    return sorted(set(urls))


async def _iterate_results(result_or_stream):
    if hasattr(result_or_stream, "__aiter__"):
        async for item in result_or_stream:
            yield item
    elif isinstance(result_or_stream, (list, tuple)):
        for item in result_or_stream:
            yield item
    elif result_or_stream is not None:
        yield result_or_stream


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    out.append(item)
            except json.JSONDecodeError:
                continue
    return out


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


async def crawl_site(
    start_url: str,
    output_dir: Path,
    max_pages: int = 0,
    max_runtime_minutes: int = 315,
    full_page_scan: bool = False,
    use_current_discovery: bool = True,
    batch_size: int = 30,
    progress: Callable[[str], None] = print,
    resume: bool = False,
    max_query_variants_per_path: int = 100,
) -> tuple[list[dict], dict]:
    """Crawl every discoverable live page on one host until the queue is exhausted.

    Current homepage/sitemap/feed URLs seed the queue. Every internal URL found on every
    live page is recursively queued. Checkpoint + JSONL partial link storage supports
    continuation after a partial GitHub run.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / "crawl_checkpoint.json"
    partial_links_path = output_dir / "partial_links.jsonl"
    errors_path = output_dir / "partial_errors.jsonl"

    start_url = canonicalize_url(start_url)
    allowed_hosts = _allowed_hosts(start_url)
    started_epoch = time.time()
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    deadline = time.monotonic() + max_runtime_minutes * 60

    errors: list[dict] = []
    link_rows: list[dict] = []
    fetched_pages: set[str] = set()
    queue: deque[str] = deque()
    queued: set[str] = set()
    query_variants: dict[tuple[str, str], set[str]] = {}
    skipped_trap_urls = 0
    resumed = False

    if resume and checkpoint_path.exists():
        try:
            state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            if canonicalize_url(state.get("start_url", "")) == start_url:
                fetched_pages = {canonicalize_url(x) for x in state.get("fetched_urls", []) if x}
                for item in state.get("pending_urls", []):
                    u = canonicalize_url(item)
                    if u and u not in fetched_pages:
                        queue.append(u)
                        queued.add(u)
                link_rows = _load_jsonl(partial_links_path)
                errors = _load_jsonl(errors_path)
                resumed = True
                progress(f"[resume] fetched={len(fetched_pages)} pending={len(queue)} links={len(link_rows)}")
        except Exception as exc:
            progress(f"[resume] checkpoint ignored: {type(exc).__name__}: {exc}")

    if not resumed:
        partial_links_path.unlink(missing_ok=True)
        errors_path.unlink(missing_ok=True)

    browser_cfg = BrowserConfig(
        browser_type="chromium",
        headless=True,
        light_mode=True,
        text_mode=False,
        avoid_ads=True,
        verbose=False,
    )
    run_cfg = CrawlerRunConfig(
        prefetch=True,
        cache_mode=CacheMode.BYPASS,
        exclude_all_images=True,
        scan_full_page=full_page_scan,
        scroll_delay=0.15 if full_page_scan else 0.0,
        flatten_shadow_dom=True,
        remove_consent_popups=True,
        remove_overlay_elements=True,
        verbose=False,
    )

    def enqueue(url: str) -> None:
        nonlocal skipped_trap_urls
        url = canonicalize_url(url)
        if not url or hostname_from_url(url) not in allowed_hosts:
            return
        if url in queued or url in fetched_pages:
            return

        parts = urlsplit(url)
        if parts.query:
            key = (parts.hostname or "", parts.path or "/")
            seen_queries = query_variants.setdefault(key, set())
            if parts.query not in seen_queries and len(seen_queries) >= max_query_variants_per_path:
                skipped_trap_urls += 1
                return
            seen_queries.add(parts.query)

        queued.add(url)
        queue.append(url)

    if not resumed:
        enqueue(start_url)
    seed_count = len(queued) or 1

    def save_checkpoint() -> None:
        atomic_write_text(checkpoint_path, json.dumps({
            "start_url": start_url,
            "fetched_count": len(fetched_pages),
            "queued_count": len(queue),
            "fetched_urls": sorted(fetched_pages),
            "pending_urls": list(queue),
            "partial_links_file": partial_links_path.name,
            "partial_errors_file": errors_path.name,
            "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, ensure_ascii=False, indent=2))

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        if use_current_discovery:
            progress("[discovery] seeding queue from current sitemap/robots/feed/homepage")
            discovered = await _discover_current_urls(crawler, start_url)
            for url in discovered:
                enqueue(url)
            seed_count = len(queued) + len(fetched_pages)
            progress(f"[discovery] total known URLs after seeding: {seed_count}")

        async def process_result(result) -> tuple[list[dict], list[str], dict | None]:
            final_url = canonicalize_url(getattr(result, "redirected_url", None) or getattr(result, "url", "") or "")
            if not final_url:
                return [], [], {"url": "", "status": "", "error": "crawl returned no final URL"}

            initial_status = getattr(result, "status_code", None)
            redirected_status = getattr(result, "redirected_status_code", None)
            final_status = redirected_status if redirected_status is not None else initial_status
            success = bool(getattr(result, "success", False))
            html_text = getattr(result, "html", None) or ""
            final_host = hostname_from_url(final_url)
            live = success and bool(html_text) and (final_status is None or int(final_status) < 400)

            # A same-site URL redirecting outside the site is not treated as an external
            # link embedded on the source page; we also do not crawl the destination DOM.
            if final_host not in allowed_hosts:
                return [], [], None

            fetched_pages.add(final_url)
            if not live:
                return [], [], {
                    "url": final_url,
                    "status": final_status or initial_status or "",
                    "error": getattr(result, "error_message", "crawl failed") or "non-live source page",
                }

            new_links = extract_external_links(
                source_url=final_url,
                html_text=html_text,
                source_status=final_status,
                source_title=_metadata_title(result),
            )
            internal = extract_internal_urls(final_url, html_text, allowed_hosts)
            return new_links, internal, None

        async def crawl_one_with_retry(url: str, attempts: int = 3) -> None:
            last_error: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    result = await crawler.arun(url=url, config=run_cfg)
                    new_links, internal, err = await process_result(result)
                    fetched_pages.add(url)
                    if new_links:
                        link_rows.extend(new_links)
                        _append_jsonl(partial_links_path, new_links)
                    for u in internal:
                        enqueue(u)
                    if err:
                        errors.append(err)
                        _append_jsonl(errors_path, [err])
                    return
                except Exception as exc:
                    last_error = exc
                    if attempt < attempts:
                        await asyncio.sleep(min(2 ** (attempt - 1), 4))
            fetched_pages.add(url)
            err = {"url": url, "status": "", "error": f"retry exhausted: {type(last_error).__name__}: {last_error}"}
            errors.append(err)
            _append_jsonl(errors_path, [err])

        while queue and time.monotonic() < deadline:
            if max_pages > 0 and len(fetched_pages) >= max_pages:
                break

            remaining = batch_size
            if max_pages > 0:
                remaining = min(remaining, max_pages - len(fetched_pages))
            batch: list[str] = []
            while queue and len(batch) < remaining:
                url = queue.popleft()
                queued.discard(url)
                if url not in fetched_pages:
                    batch.append(url)

            if not batch:
                continue

            try:
                result_set = await crawler.arun_many(batch, config=run_cfg)
                seen_results = 0
                async for result in _iterate_results(result_set):
                    seen_results += 1
                    new_links, internal, err = await process_result(result)
                    if new_links:
                        link_rows.extend(new_links)
                        _append_jsonl(partial_links_path, new_links)
                    for u in internal:
                        enqueue(u)
                    if err:
                        errors.append(err)
                        _append_jsonl(errors_path, [err])

                # Do not silently mark a whole batch complete if the dispatcher returns
                # fewer CrawlResult objects than requested URLs. In exhaustive mode, retry
                # unresolved inputs individually so a partial batch cannot hide missed pages.
                if seen_results == len(batch):
                    fetched_pages.update(batch)
                else:
                    progress(
                        f"[crawl] batch returned {seen_results}/{len(batch)} results; "
                        "retrying unresolved URLs individually"
                    )
                    for url in batch:
                        if url not in fetched_pages:
                            await crawl_one_with_retry(url)
            except Exception as exc:
                progress(f"[crawl] batch failed ({type(exc).__name__}); retrying {len(batch)} URLs individually")
                for url in batch:
                    if time.monotonic() >= deadline:
                        enqueue(url)
                        continue
                    await crawl_one_with_retry(url)

            save_checkpoint()
            if len(fetched_pages) % 100 < max(1, len(batch)):
                progress(f"[crawl] fetched={len(fetched_pages)} queue={len(queue)} outbound_rows={len(link_rows)}")

    # Deduplicate exact DOM link occurrences while preserving separate source pages.
    deduped: list[dict] = []
    keys: set[tuple] = set()
    for r in link_rows:
        key = (r.get("source_url"), r.get("target_url"), r.get("anchor"), r.get("rel"), r.get("xpath"))
        if key not in keys:
            keys.add(key)
            deduped.append(r)

    save_checkpoint()
    stopped_by_runtime = time.monotonic() >= deadline and bool(queue)
    stopped_by_page_limit = max_pages > 0 and len(fetched_pages) >= max_pages and bool(queue)
    finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    stats = {
        "start_url": start_url,
        "started_at": started_at,
        "finished_at": finished_at,
        "resumed": resumed,
        "initial_seed_urls": seed_count,
        "pages_crawled": len(fetched_pages),
        "remaining_queue_urls": len(queue),
        "outbound_link_rows": len(deduped),
        "crawl_errors": len(errors),
        "skipped_trap_urls": skipped_trap_urls,
        "runtime_seconds": round(time.time() - started_epoch, 2),
        "crawl_complete": not queue,
        "stopped_by_runtime_limit": stopped_by_runtime,
        "stopped_by_page_limit": stopped_by_page_limit,
        "errors": errors,
    }
    return deduped, stats
