from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from typing import Iterable

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_HEADERS: dict[str, list[str]] = {
    "Dashboard": [
        "source_site", "last_run_id", "last_run_at", "crawl_status",
        "pages_crawled", "remaining_queue_urls", "outbound_link_rows",
        "unique_external_domains", "follow_link_rows", "nofollow_link_rows",
        "potential_drops", "high_confidence_drops", "crawl_errors",
        "runtime_seconds", "github_run_url",
    ],
    "Runs": [
        "run_id", "source_site", "started_at", "finished_at", "crawl_status",
        "pages_crawled", "remaining_queue_urls", "outbound_link_rows",
        "unique_external_domains", "follow_link_rows", "nofollow_link_rows",
        "potential_drops", "high_confidence_drops", "crawl_errors",
        "runtime_seconds", "github_run_url",
    ],
    "Drop Candidates": [
        "run_id", "source_site", "target_domain", "link_count", "source_pages",
        "follow_links", "follow_source_pages", "nofollow_links", "sponsored_links",
        "ugc_links", "article_links", "main_links", "nav_links", "footer_links",
        "top_anchors", "first_source_url", "priority_score", "rdap_status",
        "dns_status", "confidence", "rdap_server", "detail",
    ],
    "Outbound Domains": [
        "run_id", "source_site", "target_domain", "link_count", "source_pages",
        "follow_links", "follow_source_pages", "nofollow_links", "sponsored_links",
        "ugc_links", "article_links", "main_links", "nav_links", "footer_links",
        "top_anchors", "first_source_url", "priority_score",
    ],
    "Outbound Links": [
        "run_id", "source_site", "source_url", "source_domain", "source_status",
        "source_title", "target_url", "target_host", "target_domain", "anchor",
        "title_attr", "rel", "follow", "nofollow", "sponsored", "ugc",
        "position", "xpath", "discovered_at",
    ],
    "Errors": ["run_id", "source_site", "url", "status", "error"],
}


def _load_service_account_info() -> dict | None:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        return json.loads(raw)
    raw_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
    if raw_b64:
        return json.loads(base64.b64decode(raw_b64).decode("utf-8"))
    return None


def sheets_enabled() -> bool:
    return bool(os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip() and _load_service_account_info())


def _service():
    info = _load_service_account_info()
    if not info:
        raise RuntimeError("Google service-account credentials are not configured")
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _status(stats: dict) -> str:
    if stats.get("crawl_in_progress"):
        return "RUNNING"
    if not stats.get("crawl_complete"):
        return "PARTIAL"
    if int(stats.get("crawl_errors", 0) or 0) > 0:
        return "COMPLETE_WITH_ERRORS"
    return "COMPLETE"


def _clean(value):
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False)


def _row_from_dict(headers: list[str], row: dict) -> list:
    return [_clean(row.get(h, "")) for h in headers]


def _ensure_sheets(service, spreadsheet_id: str) -> None:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    requests = []
    for title in SHEET_HEADERS:
        if title not in existing:
            requests.append({"addSheet": {"properties": {"title": title}}})
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    for title, headers in SHEET_HEADERS.items():
        existing_header = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{title}'!1:1",
        ).execute().get("values", [])
        if not existing_header or existing_header[0][: len(headers)] != headers:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{title}'!A1",
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()


def _append_rows(service, spreadsheet_id: str, title: str, rows: Iterable[list], chunk_size: int = 500) -> int:
    rows = list(rows)
    written = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        if not chunk:
            continue
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{title}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": chunk},
        ).execute()
        written += len(chunk)
    return written


def _upsert_dashboard(service, spreadsheet_id: str, dashboard: dict) -> None:
    headers = SHEET_HEADERS["Dashboard"]
    col = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="'Dashboard'!A2:A",
    ).execute().get("values", [])
    site = dashboard["source_site"]
    row_number = None
    for idx, row in enumerate(col, start=2):
        if row and row[0] == site:
            row_number = idx
            break
    values = [_row_from_dict(headers, dashboard)]
    if row_number is None:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="'Dashboard'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    else:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'Dashboard'!A{row_number}",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()


def sync_run_to_google_sheets(
    *,
    run_id: str,
    source_site: str,
    stats: dict,
    domains: list[dict],
    drops: list[dict],
    links: list[dict],
    errors: list[dict],
    write_raw_links: bool = False,
) -> dict:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        return {"enabled": False, "reason": "GOOGLE_SHEETS_SPREADSHEET_ID is not set"}
    if not _load_service_account_info():
        return {"enabled": False, "reason": "Google service-account credentials are not set"}

    service = _service()
    _ensure_sheets(service, spreadsheet_id)

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    github_run_url = ""
    if os.getenv("GITHUB_REPOSITORY") and os.getenv("GITHUB_RUN_ID"):
        github_run_url = f"https://github.com/{os.getenv('GITHUB_REPOSITORY')}/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

    crawl_status = _status(stats)
    common = {
        "run_id": run_id,
        "source_site": source_site,
        "github_run_url": github_run_url,
    }
    run_row = {
        **common,
        "started_at": stats.get("started_at", ""),
        "finished_at": stats.get("finished_at", now),
        "crawl_status": crawl_status,
        "pages_crawled": stats.get("pages_crawled", 0),
        "remaining_queue_urls": stats.get("remaining_queue_urls", 0),
        "outbound_link_rows": stats.get("outbound_link_rows", 0),
        "unique_external_domains": stats.get("unique_external_domains", 0),
        "follow_link_rows": stats.get("follow_link_rows", 0),
        "nofollow_link_rows": stats.get("nofollow_link_rows", 0),
        "potential_drops": stats.get("potential_drops", 0),
        "high_confidence_drops": stats.get("high_confidence_drops", 0),
        "crawl_errors": stats.get("crawl_errors", 0),
        "runtime_seconds": stats.get("runtime_seconds", 0),
    }
    dashboard = {
        "source_site": source_site,
        "last_run_id": run_id,
        "last_run_at": run_row["finished_at"],
        "crawl_status": crawl_status,
        "pages_crawled": run_row["pages_crawled"],
        "remaining_queue_urls": run_row["remaining_queue_urls"],
        "outbound_link_rows": run_row["outbound_link_rows"],
        "unique_external_domains": run_row["unique_external_domains"],
        "follow_link_rows": run_row["follow_link_rows"],
        "nofollow_link_rows": run_row["nofollow_link_rows"],
        "potential_drops": run_row["potential_drops"],
        "high_confidence_drops": run_row["high_confidence_drops"],
        "crawl_errors": run_row["crawl_errors"],
        "runtime_seconds": run_row["runtime_seconds"],
        "github_run_url": github_run_url,
    }

    _upsert_dashboard(service, spreadsheet_id, dashboard)
    _append_rows(service, spreadsheet_id, "Runs", [_row_from_dict(SHEET_HEADERS["Runs"], run_row)])

    domain_rows = [{**common, **r} for r in domains]
    drop_rows = [{**common, **r} for r in drops]
    error_rows = [{**common, **r} for r in errors]
    link_rows = [{**common, **r} for r in links] if write_raw_links else []

    counts = {
        "dashboard": 1,
        "runs": 1,
        "outbound_domains": _append_rows(service, spreadsheet_id, "Outbound Domains", (_row_from_dict(SHEET_HEADERS["Outbound Domains"], r) for r in domain_rows)),
        "drop_candidates": _append_rows(service, spreadsheet_id, "Drop Candidates", (_row_from_dict(SHEET_HEADERS["Drop Candidates"], r) for r in drop_rows)),
        "errors": _append_rows(service, spreadsheet_id, "Errors", (_row_from_dict(SHEET_HEADERS["Errors"], r) for r in error_rows)),
        "outbound_links": _append_rows(service, spreadsheet_id, "Outbound Links", (_row_from_dict(SHEET_HEADERS["Outbound Links"], r) for r in link_rows)),
    }
    return {"enabled": True, "spreadsheet_id": spreadsheet_id, "counts": counts}
