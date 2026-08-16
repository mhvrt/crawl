from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from .report import LINK_FIELDS


class CrawlStore:
    """Crash-safe crawl state and link storage.

    The JSON checkpoint remains as a portable compatibility snapshot, while
    SQLite is the authoritative incremental store for new crawler versions.
    """

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                status INTEGER,
                error TEXT NOT NULL DEFAULT '',
                not_before REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS pages_state_idx ON pages(state);

            CREATE TABLE IF NOT EXISTS url_sources (
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (url, source)
            );

            CREATE TABLE IF NOT EXISTS outbound_links (
                source_url TEXT NOT NULL,
                target_url TEXT NOT NULL,
                anchor TEXT NOT NULL DEFAULT '',
                rel TEXT NOT NULL DEFAULT '',
                xpath TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL,
                evidence_source TEXT NOT NULL DEFAULT 'current',
                observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_url, target_url, anchor, rel, xpath, evidence_source)
            );
            CREATE INDEX IF NOT EXISTS outbound_target_idx ON outbound_links(target_url);

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    def reset(self) -> None:
        self.db.executescript(
            """
            DELETE FROM pages;
            DELETE FROM url_sources;
            DELETE FROM outbound_links;
            DELETE FROM metadata;
            """
        )
        self.db.commit()

    def set_page(
        self,
        url: str,
        state: str,
        *,
        status: int | None = None,
        error: str = "",
        not_before: float = 0,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO pages(url, state, status, error, not_before)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                state=excluded.state,
                status=excluded.status,
                error=excluded.error,
                not_before=excluded.not_before,
                updated_at=CURRENT_TIMESTAMP
            """,
            (url, state, status, error, not_before),
        )

    def add_source(self, url: str, source: str) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO url_sources(url, source) VALUES (?, ?)",
            (url, source),
        )

    def add_links(self, rows: Iterable[dict], evidence_source: str = "current") -> None:
        values = []
        for row in rows:
            payload = {field: row.get(field, "") for field in LINK_FIELDS}
            values.append(
                (
                    str(row.get("source_url") or ""),
                    str(row.get("target_url") or ""),
                    str(row.get("anchor") or ""),
                    str(row.get("rel") or ""),
                    str(row.get("xpath") or ""),
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    evidence_source,
                )
            )
        self.db.executemany(
            """
            INSERT OR IGNORE INTO outbound_links(
                source_url, target_url, anchor, rel, xpath, payload, evidence_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )

    def load_links(self, evidence_source: str = "current") -> list[dict]:
        rows = self.db.execute(
            "SELECT payload FROM outbound_links WHERE evidence_source=?",
            (evidence_source,),
        )
        return [json.loads(row[0]) for row in rows]

    def load_pages(self) -> dict[str, list]:
        output = {
            "successful_urls": [],
            "terminal_urls": [],
            "pending_urls": [],
            "deferred_urls": [],
        }
        for url, state, status, error, not_before in self.db.execute(
            "SELECT url, state, status, error, not_before FROM pages"
        ):
            if state == "live":
                output["successful_urls"].append(url)
            elif state == "terminal":
                output["terminal_urls"].append(url)
            elif state == "pending":
                output["pending_urls"].append(url)
            elif state == "deferred":
                output["deferred_urls"].append(
                    {
                        "url": url,
                        "status": status or "",
                        "error": error,
                        "not_before": not_before,
                    }
                )
        return output

    def source_counts(self) -> dict[str, int]:
        return {
            source: count
            for source, count in self.db.execute(
                "SELECT source, COUNT(*) FROM url_sources GROUP BY source ORDER BY source"
            )
        }

    def load_errors(self) -> list[dict]:
        return [
            {"url": url, "status": status or "", "error": error}
            for url, status, error in self.db.execute(
                """
                SELECT url, status, error FROM pages
                WHERE state IN ('terminal', 'deferred') AND error != ''
                """
            )
        ]

    def set_metadata(self, key: str, value) -> None:
        self.db.execute(
            """
            INSERT INTO metadata(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, json.dumps(value, ensure_ascii=False)),
        )

    def get_metadata(self, key: str, default=None):
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def commit(self) -> None:
        self.db.commit()
