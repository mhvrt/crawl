from __future__ import annotations

import asyncio
import json
from urllib.parse import urlsplit

import httpx

from .utils import canonicalize_url, hostname_from_url


COMMON_CRAWL_COLLECTIONS_URL = "https://index.commoncrawl.org/collinfo.json"
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"


def _domain(start_url: str) -> str:
    return hostname_from_url(start_url).removeprefix("www.")


def _host(start_url: str) -> str:
    return hostname_from_url(start_url)


def _allowed_hosts(start_url: str) -> set[str]:
    host = _host(start_url)
    bare = host.removeprefix("www.")
    return {host, bare, "www." + bare}


def _accept(url: str, domain: str) -> str:
    try:
        normalized = canonicalize_url(url)
    except (TypeError, ValueError):
        return ""
    host = hostname_from_url(normalized)
    if not normalized or not (host == domain or host.endswith("." + domain)):
        return ""
    path = urlsplit(normalized).path.lower()
    if path.endswith((
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
        ".css", ".js", ".json", ".xml", ".pdf", ".zip", ".gz",
        ".mp3", ".mp4", ".woff", ".woff2", ".ttf",
    )):
        return ""
    return normalized


async def discover_wayback_urls(
    start_url: str,
    *,
    limit: int = 100_000,
    timeout_seconds: float = 90,
) -> list[str]:
    """Return unique historical page URLs from the Wayback CDX index."""
    domain = _domain(start_url)
    host = _host(start_url)
    allowed_hosts = _allowed_hosts(start_url)
    params = [
        ("url", host),
        ("matchType", "host"),
        ("output", "txt"),
        ("fl", "original"),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
        ("collapse", "urlkey"),
        ("limit", str(max(1, limit))),
    ]
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": "LiveDropHunter/2.0 (+https://github.com/mhvrt/crawl)"},
    ) as client:
        response = None
        for attempt in range(2):
            try:
                response = await client.get(WAYBACK_CDX_URL, params=params)
                response.raise_for_status()
                break
            except httpx.HTTPError:
                if attempt == 0:
                    await asyncio.sleep(1)
        if response is None or not response.is_success:
            return []
    found = set()
    for line in response.text.splitlines():
        url = _accept(line.strip(), domain)
        if url and hostname_from_url(url) in allowed_hosts:
            found.add(url)
    found.discard("")
    return sorted(found)


async def discover_commoncrawl_urls(
    start_url: str,
    *,
    collections: int = 12,
    timeout_seconds: float = 60,
) -> list[str]:
    """Merge page URLs from several recent Common Crawl CDX collections."""
    domain = _domain(start_url)
    allowed_hosts = _allowed_hosts(start_url)
    timeout = httpx.Timeout(timeout_seconds)
    headers = {"User-Agent": "LiveDropHunter/2.0 (+https://github.com/mhvrt/crawl)"}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        info_response = None
        for attempt in range(2):
            try:
                info_response = await client.get(COMMON_CRAWL_COLLECTIONS_URL)
                info_response.raise_for_status()
                break
            except httpx.HTTPError:
                if attempt == 0:
                    await asyncio.sleep(1)
        if info_response is None or not info_response.is_success:
            return []
        indexes = info_response.json()[: max(1, collections)]

        async def one(index: dict) -> set[str]:
            endpoint = str(index.get("cdx-api") or "")
            if not endpoint:
                return set()
            params = {
                "url": domain,
                "matchType": "domain",
                "output": "json",
                "page": "0",
            }
            response = None
            for attempt in range(2):
                try:
                    response = await client.get(endpoint, params=params)
                    response.raise_for_status()
                    break
                except httpx.HTTPError:
                    if attempt == 0:
                        await asyncio.sleep(1)
            if response is None or not response.is_success:
                return set()
            output = set()
            for line in response.text.splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(row.get("status") or "") != "200":
                    continue
                mime = str(row.get("mime-detected") or row.get("mime") or "").lower()
                if mime and "html" not in mime:
                    continue
                url = _accept(str(row.get("url") or ""), domain)
                if url and hostname_from_url(url) in allowed_hosts:
                    output.add(url)
            return output

        semaphore = asyncio.Semaphore(3)

        async def bounded(index: dict) -> set[str]:
            async with semaphore:
                try:
                    return await asyncio.wait_for(
                        one(index),
                        timeout=min(timeout_seconds, 25),
                    )
                except asyncio.TimeoutError:
                    return set()

        results = await asyncio.gather(*(bounded(index) for index in indexes))
    return sorted(set().union(*results))


async def discover_archive_urls(
    start_url: str,
    *,
    wayback_limit: int = 100_000,
    commoncrawl_collections: int = 12,
) -> dict[str, list[str]]:
    """Run independent archive sources without one failure hiding the other."""
    async def safe_wayback():
        try:
            return await discover_wayback_urls(start_url, limit=wayback_limit)
        except (httpx.HTTPError, ValueError):
            return []

    async def safe_commoncrawl():
        try:
            return await discover_commoncrawl_urls(
                start_url,
                collections=commoncrawl_collections,
            )
        except (httpx.HTTPError, ValueError):
            return []

    wayback, commoncrawl = await asyncio.gather(safe_wayback(), safe_commoncrawl())
    return {"wayback": wayback, "commoncrawl": commoncrawl}
