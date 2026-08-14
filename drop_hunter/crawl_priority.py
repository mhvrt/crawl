from __future__ import annotations

import heapq
from dataclasses import dataclass
from urllib.parse import urlsplit


HIGH_VALUE_SEGMENTS = {
    "about", "company", "companies", "resource", "resources", "research",
    "report", "reports", "guide", "guides", "tools", "tool", "partners",
    "partner", "press", "press-releases", "media", "directory", "directories",
    "links", "reference", "references", "academy", "education", "learn",
    "info",
}
CONTENT_SEGMENTS = {
    "article", "articles", "blog", "blogs", "news", "analysis", "crypto",
    "cryptocurrencies", "insights", "insight", "stories", "story",
    "features", "feature", "opinion", "reviews", "review", "markets",
    "market",
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
    # Learn at the top-level section (/analysis, /crypto, /company). Using an
    # article slug here would fragment the signal and make adaptive ordering inert.
    return "/" + parts[0]


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
    """Dynamic priority queue optimized for discovery yield, not FIFO order.

    URLs are kept in small per-section heaps. Choosing the next section scans the
    section heads rather than every queued URL, so a 100k URL sitemap does not
    turn every pop into a 100k-item scan. Learned section yield is applied when a
    section is selected, so already queued URLs are reprioritized immediately.
    Exhaustiveness is preserved: low-yield URLs remain queued.
    """

    def __init__(self) -> None:
        self._items: dict[str, int] = {}
        self._buckets: dict[str, list[tuple[float, int, str]]] = {}
        self._serial = 0
        self._sections: dict[str, SectionYield] = {}

    def append(self, url: str) -> None:
        if url in self._items:
            return
        self._serial += 1
        self._items[url] = self._serial
        key = section_key(url)
        heapq.heappush(
            self._buckets.setdefault(key, []),
            (base_priority(url), self._serial, url),
        )

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __iter__(self):
        # Checkpoint restore recomputes priorities from section stats, so pending
        # URLs only need a stable, linear-time snapshot here.
        return iter(self.pending_urls())

    def score(self, url: str) -> float:
        stats = self._sections.get(section_key(url))
        # Cap the learned boost so utility/auth URLs never jump ahead merely due
        # to one anomalous page. A sustained 1 new-domain/page section receives
        # roughly a 12-point boost.
        learned_boost = min(30.0, (stats.rate * 12.0) if stats else 0.0)
        return base_priority(url) - learned_boost

    def ordered(self) -> list[str]:
        """Return a fully ranked debug view; checkpoints should use pending_urls."""
        return sorted(self._items, key=lambda u: (self.score(u), self._items[u]))

    def pending_urls(self) -> list[str]:
        """Return all queued URLs in insertion order without sorting them."""
        return list(self._items)

    def _prune_bucket(self, key: str) -> None:
        bucket = self._buckets.get(key)
        if not bucket:
            return
        while bucket:
            _priority, serial, url = bucket[0]
            if self._items.get(url) == serial:
                break
            heapq.heappop(bucket)
        if not bucket:
            self._buckets.pop(key, None)

    def _section_boost(self, key: str) -> float:
        stats = self._sections.get(key)
        return min(30.0, (stats.rate * 12.0) if stats else 0.0)

    def popleft(self) -> str:
        if not self._items:
            raise IndexError("pop from empty YieldPriorityQueue")

        candidates: list[tuple[float, int, str]] = []
        for key in list(self._buckets):
            self._prune_bucket(key)
            bucket = self._buckets.get(key)
            if bucket:
                priority, serial, _url = bucket[0]
                candidates.append((priority - self._section_boost(key), serial, key))

        if not candidates:
            raise RuntimeError("priority queue indexes are inconsistent")

        _score, _serial, key = min(candidates)
        _priority, serial, url = heapq.heappop(self._buckets[key])
        if self._items.get(url) != serial:
            raise RuntimeError("priority queue returned a stale item")
        del self._items[url]
        self._prune_bucket(key)
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
