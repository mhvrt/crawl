from drop_hunter.link_extractor import extract_external_links
from drop_hunter.report import summarize_domains


def test_extract_rel_anchor_position_and_external_only():
    html = """
    <html><head><title>X</title></head><body>
      <nav><a href="https://external.com/a" rel="nofollow">External A</a></nav>
      <article>
        <a href="https://external.com/b" rel="sponsored ugc" title="Deal">Best deal</a>
        <a href="https://external.com/c">Normal link</a>
        <a href="/internal">Internal</a>
      </article>
    </body></html>
    """
    rows = extract_external_links("https://example.com/post", html, 200, "Post")
    assert len(rows) == 3

    a = next(r for r in rows if r["target_url"].endswith("/a"))
    assert a["nofollow"] is True
    assert a["follow"] is False
    assert a["position"] == "nav"

    b = next(r for r in rows if r["target_url"].endswith("/b"))
    assert b["sponsored"] is True
    assert b["ugc"] is True
    assert b["follow"] is False
    assert b["anchor"] == "Best deal"
    assert b["position"] == "article"

    c = next(r for r in rows if r["target_url"].endswith("/c"))
    assert c["follow"] is True
    assert c["rel"] == ""


def test_domain_summary_counts_unique_source_pages():
    html = """
    <html><body><article>
      <a href="https://dead.example.net/a">one</a>
      <a href="https://dead.example.net/b" rel="nofollow">two</a>
    </article></body></html>
    """
    rows = extract_external_links("https://example.com/post", html, 200, "Post")
    summary = summarize_domains(rows)
    assert len(summary) == 1
    assert summary[0]["link_count"] == 2
    assert summary[0]["source_pages"] == 1
    assert summary[0]["follow_links"] == 1
    assert summary[0]["nofollow_links"] == 1


def test_extract_internal_urls_exhaustive_queue():
    from drop_hunter.link_extractor import extract_internal_urls
    html = '''
    <html><body>
      <a href="/a">A</a>
      <a href="https://www.example.com/b?x=1&utm_source=z">B</a>
      <a href="https://other.com/out">Out</a>
      <a href="/image.jpg">Image</a>
    </body></html>
    '''
    urls = extract_internal_urls(
        "https://example.com/",
        html,
        {"example.com", "www.example.com"},
    )
    assert "https://example.com/a" in urls
    assert "https://www.example.com/b?x=1" in urls
    assert all("other.com" not in x for x in urls)
    assert all(not x.endswith("image.jpg") for x in urls)
