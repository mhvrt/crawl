from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

from .utils import is_valid_public_domain

LINK_FIELDS = [
    "source_url", "source_domain", "source_status", "source_title",
    "target_url", "target_host", "target_domain", "anchor", "title_attr",
    "rel", "follow", "nofollow", "sponsored", "ugc", "position", "xpath",
    "discovered_at",
]

DOMAIN_FIELDS = [
    "target_domain", "link_count", "source_pages", "follow_links",
    "follow_source_pages", "nofollow_links", "sponsored_links", "ugc_links",
    "article_links", "main_links", "nav_links", "footer_links",
    "top_anchors", "first_source_url", "priority_score",
]


def write_csv(path: Path, rows: Iterable[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def summarize_domains(link_rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in link_rows:
        domain = str(row.get("target_domain", "") or "").strip().lower().rstrip(".")
        if is_valid_public_domain(domain):
            grouped[domain].append(row)

    output = []
    for domain, rows in grouped.items():
        source_pages = {r["source_url"] for r in rows if r.get("source_url")}
        follow_rows = [r for r in rows if str(r.get("follow", "")).lower() in {"true", "1", "yes"} or r.get("follow") is True]
        follow_source_pages = {r["source_url"] for r in follow_rows if r.get("source_url")}
        anchors = Counter(r.get("anchor", "") for r in rows if r.get("anchor"))
        positions = Counter(r.get("position", "") for r in rows)
        priority_score = (
            len(follow_source_pages) * 5
            + len(source_pages) * 2
            + positions.get("article", 0) * 2
            + positions.get("main", 0)
        )
        output.append({
            "target_domain": domain,
            "link_count": len(rows),
            "source_pages": len(source_pages),
            "follow_links": len(follow_rows),
            "follow_source_pages": len(follow_source_pages),
            "nofollow_links": sum(str(r.get("nofollow", "")).lower() in {"true", "1", "yes"} or r.get("nofollow") is True for r in rows),
            "sponsored_links": sum(str(r.get("sponsored", "")).lower() in {"true", "1", "yes"} or r.get("sponsored") is True for r in rows),
            "ugc_links": sum(str(r.get("ugc", "")).lower() in {"true", "1", "yes"} or r.get("ugc") is True for r in rows),
            "article_links": positions.get("article", 0),
            "main_links": positions.get("main", 0),
            "nav_links": positions.get("nav", 0),
            "footer_links": positions.get("footer", 0),
            "top_anchors": " | ".join(a for a, _ in anchors.most_common(5)),
            "first_source_url": rows[0].get("source_url", ""),
            "priority_score": priority_score,
        })

    return sorted(
        output,
        key=lambda x: (x["priority_score"], x["follow_source_pages"], x["source_pages"], x["link_count"]),
        reverse=True,
    )


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
