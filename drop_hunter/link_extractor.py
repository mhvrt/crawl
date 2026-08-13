from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlsplit

from lxml import html as lxml_html

from .utils import canonicalize_url, hostname_from_url, registrable_domain

QUALIFYING_REL = {"nofollow", "sponsored", "ugc"}
# These are resources, not HTML pages we want to enqueue for crawling.
NON_PAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tif", ".tiff",
    ".css", ".js", ".mjs", ".map", ".json", ".xml", ".txt", ".csv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".7z", ".gz", ".tar", ".bz2",
    ".mp3", ".wav", ".ogg", ".m4a", ".mp4", ".webm", ".mov", ".avi", ".mkv",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
}


def _position(element) -> str:
    tags = []
    node = element
    while node is not None:
        tag = getattr(node, "tag", "")
        if isinstance(tag, str):
            tags.append(tag.lower())
        node = node.getparent()

    for tag, label in (
        ("nav", "nav"),
        ("footer", "footer"),
        ("header", "header"),
        ("aside", "aside"),
        ("article", "article"),
        ("main", "main"),
    ):
        if tag in tags:
            return label
    return "body"


def _clean_text(value: str, limit: int = 500) -> str:
    return " ".join((value or "").split())[:limit]


def _doc_and_base(source_url: str, html_text: str):
    if not html_text:
        return None, source_url
    try:
        doc = lxml_html.fromstring(html_text)
    except Exception:
        return None, source_url
    base_url = source_url
    base_nodes = doc.xpath("//base[@href]")
    if base_nodes:
        candidate = base_nodes[0].get("href")
        if candidate:
            base_url = urljoin(source_url, candidate)
    return doc, base_url


def _looks_like_page(url: str) -> bool:
    path = urlsplit(url).path.lower()
    leaf = path.rsplit("/", 1)[-1]
    if "." not in leaf:
        return True
    return not any(path.endswith(ext) for ext in NON_PAGE_EXTENSIONS)


def extract_internal_urls(source_url: str, html_text: str, allowed_hosts: set[str]) -> list[str]:
    """Return same-site HTML-like URLs found in the final DOM/HTML.

    This powers exhaustive graph expansion: every newly found internal page URL is queued.
    """
    doc, base_url = _doc_and_base(source_url, html_text)
    if doc is None:
        return []

    urls: set[str] = set()
    for a in doc.xpath("//a[@href]"):
        raw_href = (a.get("href") or "").strip()
        if not raw_href:
            continue
        absolute = urljoin(base_url, raw_href)
        parts = urlsplit(absolute)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            continue
        target = canonicalize_url(absolute)
        if hostname_from_url(target) not in allowed_hosts:
            continue
        if not _looks_like_page(target):
            continue
        urls.add(target)
    return sorted(urls)


def extract_external_links(
    source_url: str,
    html_text: str,
    source_status: int | None = None,
    source_title: str = "",
) -> list[dict[str, Any]]:
    """Extract link-level outbound data from the final HTML snapshot."""
    doc, base_url = _doc_and_base(source_url, html_text)
    if doc is None:
        return []

    source_url = canonicalize_url(source_url)
    source_host = hostname_from_url(source_url)
    source_domain = registrable_domain(source_host)

    tree = doc.getroottree()
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    for a in doc.xpath("//a[@href]"):
        raw_href = (a.get("href") or "").strip()
        if not raw_href:
            continue

        absolute = urljoin(base_url, raw_href)
        parts = urlsplit(absolute)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            continue

        target_url = canonicalize_url(absolute)
        target_host = hostname_from_url(target_url)
        target_domain = registrable_domain(target_host)
        if not target_domain or target_domain == source_domain:
            continue

        rel_tokens = sorted({x.lower() for x in (a.get("rel") or "").split() if x.strip()})
        rel_set = set(rel_tokens)
        anchor = _clean_text(" ".join(a.itertext()))
        title = _clean_text(a.get("title") or "")
        position = _position(a)
        try:
            xpath = tree.getpath(a)
        except Exception:
            xpath = ""

        key = (source_url, target_url, anchor, tuple(rel_tokens), xpath)
        if key in seen:
            continue
        seen.add(key)

        nofollow = "nofollow" in rel_set
        sponsored = "sponsored" in rel_set
        ugc = "ugc" in rel_set
        follow = not bool(rel_set & QUALIFYING_REL)

        rows.append({
            "source_url": source_url,
            "source_domain": source_domain,
            "source_status": source_status or "",
            "source_title": _clean_text(source_title, 300),
            "target_url": target_url,
            "target_host": target_host,
            "target_domain": target_domain,
            "anchor": anchor,
            "title_attr": title,
            "rel": " ".join(rel_tokens),
            "follow": follow,
            "nofollow": nofollow,
            "sponsored": sponsored,
            "ugc": ugc,
            "position": position,
            "xpath": xpath,
            "discovered_at": now,
        })
    return rows
