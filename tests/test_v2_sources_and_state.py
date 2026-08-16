import asyncio

import httpx

from drop_hunter.archive_sources import _accept
from drop_hunter.fast_http import FastHTTPFetcher
from drop_hunter.state_store import CrawlStore


def test_archive_url_filter_keeps_pages_and_rejects_assets_and_other_hosts():
    assert _accept("https://news.example.com/a?utm_source=x", "example.com") == (
        "https://news.example.com/a"
    )
    assert _accept("https://example.com/image.jpg", "example.com") == ""
    assert _accept("https://other.example/page", "example.com") == ""
    assert _accept("https://example.com: broken", "example.com") == ""


def test_sqlite_store_roundtrip(tmp_path):
    store = CrawlStore(tmp_path / "crawl.sqlite3")
    store.set_page("https://example.com/", "live", status=200)
    store.set_page("https://example.com/next", "pending")
    store.set_page(
        "https://example.com/retry",
        "deferred",
        status=429,
        error="limited",
        not_before=123,
    )
    store.add_source("https://example.com/next", "wayback")
    store.add_links([
        {
            "source_url": "https://example.com/",
            "target_url": "https://outside.test/a",
            "target_domain": "outside.test",
            "anchor": "a",
            "rel": "",
            "xpath": "/html/body/a",
        }
    ])
    store.commit()

    pages = store.load_pages()
    assert pages["successful_urls"] == ["https://example.com/"]
    assert pages["pending_urls"] == ["https://example.com/next"]
    assert pages["deferred_urls"][0]["status"] == 429
    assert store.load_links()[0]["target_domain"] == "outside.test"
    assert store.source_counts() == {"wayback": 1}
    assert store.load_errors() == [
        {
            "url": "https://example.com/retry",
            "status": 429,
            "error": "limited",
        }
    ]
    store.close()


def test_fast_http_fetcher_parses_html_and_limits_non_html():
    async def run():
        def handler(request):
            if request.url.path == "/html":
                return httpx.Response(
                    200,
                    headers={"content-type": "text/html; charset=utf-8"},
                    text="<html><head><title>Hello</title></head><body>x</body></html>",
                )
            return httpx.Response(200, headers={"content-type": "image/png"}, content=b"png")

        async with FastHTTPFetcher(concurrency=2) as fetcher:
            await fetcher.client.aclose()
            fetcher.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            html, image = await fetcher.fetch_many(
                ["https://example.com/html", "https://example.com/image"]
            )
        return html, image

    html, image = asyncio.run(run())
    assert html.success is True
    assert html.metadata["title"] == "Hello"
    assert image.success is False
    assert image.html == ""
