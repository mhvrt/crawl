import asyncio
import json
import time
from types import SimpleNamespace

from drop_hunter import native_crawler


class FakeCrawler:
    results = {}

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def arun_many(self, urls, **kwargs):
        return [self.results[url] for url in urls]


def result(url, status=200, html="<html><body><a href='https://outside.example/x'>x</a></body></html>", **kwargs):
    return SimpleNamespace(
        url=url,
        redirected_url=kwargs.get("redirected_url"),
        status_code=status,
        redirected_status_code=kwargs.get("redirected_status_code"),
        success=kwargs.get("success", status < 400),
        html=html,
        metadata={"title": "test"},
        response_headers=kwargs.get("response_headers", {}),
        error_message=kwargs.get("error_message", ""),
    )


def test_browser_fallback_only_for_js_shell():
    normal = "<html><body><p>hello</p></body></html>" * 20
    shell = "<html><body><div id=\"root\"></div>" + "<script></script>" * 4 + "x" * 500 + "</body></html>"
    assert not native_crawler._browser_needed(normal, [], [])
    assert native_crawler._browser_needed(shell, [], [])
    assert not native_crawler._browser_needed(shell, ["https://example.com/a"], [])


def test_redirected_status_wins():
    assert native_crawler._status(result("https://example.com", status=301, redirected_status_code=200)) == 200


def test_429_is_not_fetched_and_is_saved_for_resume(tmp_path, monkeypatch):
    url = "https://example.com/"
    FakeCrawler.results = {
        url: result(
            url,
            status=429,
            html="",
            success=False,
            response_headers={"Retry-After": "120"},
            error_message="Too Many Requests",
        )
    }
    monkeypatch.setattr(native_crawler, "AsyncWebCrawler", FakeCrawler)

    _links, stats = asyncio.run(
        native_crawler.crawl_site(
            url,
            tmp_path,
            use_current_discovery=False,
            max_runtime_minutes=1,
        )
    )

    state = json.loads((tmp_path / "crawl_checkpoint.json").read_text())
    assert stats["pages_crawled"] == 0
    assert stats["deferred_retryable_urls"] == 1
    assert stats["stopped_by_rate_limit"] is True
    assert url not in state["fetched_urls"]
    assert state["deferred_urls"][0]["url"] == url
    assert state["deferred_urls"][0]["not_before"] > time.time() + 100


def test_resume_recovers_legacy_429_from_fetched(tmp_path, monkeypatch):
    url = "https://example.com/"
    (tmp_path / "crawl_checkpoint.json").write_text(json.dumps({
        "start_url": url,
        "fetched_urls": [url],
        "pending_urls": [],
    }))
    (tmp_path / "partial_errors.jsonl").write_text(json.dumps({
        "url": url,
        "status": 429,
        "error": "old crawler marked this fetched",
    }) + "\n")
    (tmp_path / "partial_links.jsonl").write_text("")

    FakeCrawler.results = {url: result(url, status=200)}
    monkeypatch.setattr(native_crawler, "AsyncWebCrawler", FakeCrawler)

    _links, stats = asyncio.run(
        native_crawler.crawl_site(
            url,
            tmp_path,
            resume=True,
            use_current_discovery=False,
            max_runtime_minutes=1,
        )
    )

    state = json.loads((tmp_path / "crawl_checkpoint.json").read_text())
    assert stats["recovered_retryable_urls"] == 1
    assert stats["pages_crawled"] == 1
    assert stats["remaining_queue_urls"] == 0
    assert url in state["successful_urls"]
