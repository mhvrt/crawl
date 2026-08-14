import asyncio

from drop_hunter.crawl_priority import YieldPriorityQueue, base_priority, section_key
from drop_hunter.domain_checker import check_domains
from drop_hunter.link_extractor import extract_external_links
from drop_hunter.utils import is_valid_public_domain


def test_priority_puts_resource_pages_before_utility_pages():
    assert base_priority("https://example.com/company/team") < base_priority(
        "https://example.com/account/login"
    )
    assert section_key("https://example.com/analysis/some-article") == "/analysis"


def test_yield_learning_reorders_existing_queue():
    q = YieldPriorityQueue()
    generic = "https://example.com/products/a"
    crypto = "https://example.com/crypto/a"
    q.append(generic)
    q.append(crypto)

    # Initially content already has a modest static advantage.
    assert q.popleft() == crypto

    # Requeue both; sustained yield from /products should now make already queued
    # URLs from that section more valuable than ordinary content.
    q.append(crypto)
    q.append(generic)
    for i in range(4):
        q.record_result(f"https://example.com/products/{i}", 3)
    assert q.popleft() == generic


def test_priority_stats_roundtrip():
    q = YieldPriorityQueue()
    q.record_result("https://example.com/company/a", 5)
    q.record_result("https://example.com/company/b", 1)
    payload = q.export_stats()

    restored = YieldPriorityQueue()
    restored.load_stats(payload)
    assert restored.export_stats() == payload


def test_public_domain_validation_rejects_scheme_fragments_and_ips():
    assert is_valid_public_domain("example.com")
    assert not is_valid_public_domain("https")
    assert not is_valid_public_domain("ttp")
    assert not is_valid_public_domain("127.0.0.1")
    assert not is_valid_public_domain("localhost")


def test_malformed_external_links_are_not_emitted():
    html = """
    <html><body>
      <a href="https://https//www.fxstreet.com/bad">bad https</a>
      <a href="https://ttp//pubads.g.doubleclick.net/bad">bad ttp</a>
      <a href="https://valid-example.com/page">valid</a>
    </body></html>
    """
    rows = extract_external_links("https://source-example.com/page", html, 200, "x")
    assert [row["target_domain"] for row in rows] == ["valid-example.com"]


def test_invalid_domains_never_reach_dns_or_rdap():
    result = asyncio.run(check_domains(["https", "ttp", "127.0.0.1", "localhost"]))
    assert result == []
