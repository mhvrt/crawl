from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


HIGH_VALUE_SEGMENTS = {
    "about", "company", "companies", "resource", "resources", "research",
    "report", "reports", "guide", "guides", "tools", "tool", "partners",
    "partner", "press", "media", "directory", "directories", "links",
    "reference", "references", "academy", "education", "learn",
}
CONTENT_SEGMENTS = {
    "article", "articles", "blog", "blogs", "news", "analysis", "crypto",
    "insights", "insight", "stories", "story", "features", "feature",
    "opinion", "reviews", "review", "markets", "market",
}
LOW_VALUE_SEGMENTS = {
    "account", "accounts", "login", "signin", "sign-in", "signup", "sign-up",
    "register", "registration", "auth", "profile", "preferences", "settings",
    "search", "tag", "tags", "category", "categories", "author", "authors",
    "privacy", "terms", "cookies", "cookie", "legal", "contact", "support",
}


def section_key(url: str) -> str:
    parts = [p.lower() for p in urlsplit(url).path.split("/") if p]
    if not parts:
        return "/"
    # Two levels separate broad sections such as /crypto/news without exploding
    # article slugs into thousands of independent buckets.
    return "/" + "/".join(parts[:2])


def base_priority(url: str) -> float:
    parts = [p.lower() for p in urlsplit(url).path.split("/") if p]
    if not parts:
        return 5.0
    segment_set = set(parts[:3])
    if segment_set & LOW_VALUE_SEGMENTS:
        return 90.0
    if segment_set & HIGH_VALUE_SEGMENTS:
        return 10.0
    if segment_set & CONTENT_SEGMENTS:
        return 25.0
    return 45.0


@dataclass
class SectionYield:
    pages: int = 0
    new_domains: int = 0

    @property
    def rate(self) -> float:
        # One pseudo-page prevents a single lucky page from dominating the queue.
        return self.new_domains / (self.pages + 1)


class YieldPriorityQueue:
    """Small dynamic priority queue optimized for discovery yield, not FIFO order.

    Scores are recomputed when an item is popped, so already queued URLs benefit
    immediately when their section starts producing new external domains.
    Exhaustiveness is preserved: low-yield URLs remain queued and are processed
    after higher-yield work.
    """

    def __init__(self) -> None:
        self._items: dict[str, int] = {}
        self._serial = 0
        self._sections: dict[str, SectionYield] = {}

    def append(self, url: str) -> None:
        if url in self._items:
            return
        self._serial += 1
        self._items[url] = self._serial

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __iter__(self):
        for url in self.ordered():
            yield url

    def score(self, url: str) -> float:
        stats = self._sections.get(section_key(url))
        # Cap the learned boost so utility/auth URLs never jump ahead merely due
        # to one anomalous page. A sustained 1 new-domain/page section receives
        # roughly a 12-point boost.
        learned_boost = min(30.0, (stats.rate * 12.0) if stats else 0.0)
        return base_priority(url) - learned_boost

    def ordered(self) -> list[str]:
        return sorted(self._items, key=lambda u: (self.score(u), self._items[u]))

    def popleft(self) -> str:
        if not self._items:
            raise IndexError("pop from empty YieldPriorityQueue")
        url = min(self._items, key=lambda u: (self.score(u), self._items[u]))
        self._items.pop(url, None)
        return url

    def record_result(self, url: str, new_domains: int) -> None:
        key = section_key(url)
        stats = self._sections.setdefault(key, SectionYield())
        stats.pages += 1
        stats.new_domains += max(0, int(new_domains))

    def export_stats(self) -> dict[str, dict[str, int]]:
        return {
            key: {"pages": value.pages, "new_domains": value.new_domains}
            for key, value in self._sections.items()
        }

    def load_stats(self, payload: dict | None) -> None:
        if not isinstance(payload, dict):
            return
        for key, value in payload.items():
            if not isinstance(value, dict):
                continue
            try:
                pages = max(0, int(value.get("pages", 0)))
                new_domains = max(0, int(value.get("new_domains", 0)))
            except (TypeError, ValueError):
                continue
            self._sections[str(key)] = SectionYield(pages=pages, new_domains=new_domains)
