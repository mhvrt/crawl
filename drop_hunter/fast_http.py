from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field

import httpx


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


@dataclass
class FastHTTPResult:
    url: str
    redirected_url: str | None = None
    status_code: int | None = None
    redirected_status_code: int | None = None
    success: bool = False
    html: str = ""
    metadata: dict = field(default_factory=dict)
    response_headers: dict = field(default_factory=dict)
    error_message: str = ""


class FastHTTPFetcher:
    """Small HTTP-only fetcher optimized for link extraction."""

    def __init__(
        self,
        *,
        concurrency: int = 8,
        timeout_seconds: float = 25,
        max_response_bytes: int = 5 * 1024 * 1024,
    ):
        self.semaphore = asyncio.Semaphore(max(1, concurrency))
        self.max_response_bytes = max_response_bytes
        self.client = httpx.AsyncClient(
            http2=True,
            follow_redirects=True,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=max(4, concurrency * 2),
                max_keepalive_connections=max(2, concurrency),
            ),
            headers={
                "User-Agent": "LiveDropHunter/2.0 (+https://github.com/mhvrt/crawl)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.1",
                "Accept-Encoding": "gzip, deflate, br",
            },
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.aclose()
        return False

    async def fetch(self, url: str) -> FastHTTPResult:
        async with self.semaphore:
            try:
                async with self.client.stream("GET", url) as response:
                    content_type = response.headers.get("content-type", "").lower()
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        remaining = self.max_response_bytes - len(body)
                        if remaining <= 0:
                            break
                        body.extend(chunk[:remaining])
                    encoding = response.encoding or "utf-8"
                    text = bytes(body).decode(encoding, errors="replace")
                    if "html" not in content_type and not text.lstrip().lower().startswith(("<!doctype html", "<html")):
                        text = ""
                    match = _TITLE_RE.search(text)
                    title = re.sub(r"\s+", " ", match.group(1)).strip() if match else ""
                    return FastHTTPResult(
                        url=str(response.url),
                        redirected_url=str(response.url) if str(response.url) != url else None,
                        status_code=response.status_code,
                        redirected_status_code=response.status_code,
                        success=response.is_success and bool(text),
                        html=text,
                        metadata={"title": title},
                        response_headers=dict(response.headers),
                    )
            except httpx.HTTPError as exc:
                return FastHTTPResult(url=url, error_message=f"{type(exc).__name__}: {exc}")

    async def fetch_many(self, urls: list[str]) -> list[FastHTTPResult]:
        return list(await asyncio.gather(*(self.fetch(url) for url in urls)))
