from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from drop_hunter.native_crawler import crawl_site
from drop_hunter.domain_checker import check_domains
from drop_hunter.google_sheets import sync_run_to_google_sheets
from drop_hunter.notifier import telegram_send_document, telegram_send_message
from drop_hunter.report import DOMAIN_FIELDS, LINK_FIELDS, summarize_domains, write_csv, write_json
from drop_hunter.utils import ensure_url, safe_slug

CHECK_FIELDS = ["domain", "rdap_status", "dns_status", "candidate", "confidence", "rdap_server", "detail"]
DROP_FIELDS = DOMAIN_FIELDS + ["rdap_status", "dns_status", "confidence", "rdap_server", "detail"]
ERROR_FIELDS = ["url", "status", "error"]


def parse_args():
    p = argparse.ArgumentParser(description="Find live outbound links and potential dropped target domains.")
    p.add_argument("url")
    p.add_argument("--output", default="output")
    p.add_argument("--run-id", default="")
    p.add_argument("--max-pages", type=int, default=0, help="0 = unlimited (subject to runtime limit)")
    p.add_argument("--max-runtime-minutes", type=int, default=315)
    p.add_argument("--full-page-scan", action="store_true")
    p.add_argument("--no-current-discovery", action="store_true")
    p.add_argument("--rdap-concurrency", type=int, default=6)
    p.add_argument("--write-raw-links-to-sheets", action="store_true")
    p.add_argument("--resume", action="store_true")
    return p.parse_args()


async def main_async():
    args = parse_args()
    url = ensure_url(args.url)
    slug = safe_slug(url)
    run_id = args.run_id or f"local-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{slug}"
    out_root = Path(args.output)
    run_dir = out_root / slug
    run_dir.mkdir(parents=True, exist_ok=True)

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "")
    await telegram_send_message(tg_token, tg_chat, f"🚀 Crawl started\n{url}\nRun: {run_id}")

    # Make the source site visible in the Dashboard immediately.  Previously the
    # first Sheets write happened only after a potentially multi-hour full crawl,
    # which looked exactly like a stalled workflow.
    try:
        await asyncio.to_thread(
            sync_run_to_google_sheets,
            run_id=run_id,
            source_site=url,
            stats={
                "crawl_in_progress": True,
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "pages_crawled": 0,
                "remaining_queue_urls": 0,
                "outbound_link_rows": 0,
                "crawl_errors": 0,
                "runtime_seconds": 0,
            },
            domains=[], drops=[], links=[], errors=[], write_raw_links=False,
        )
    except Exception as exc:
        print(f"[sheets] initial RUNNING status was not written: {type(exc).__name__}: {exc}")

    links, stats = await crawl_site(
        url,
        run_dir,
        max_pages=args.max_pages,
        max_runtime_minutes=args.max_runtime_minutes,
        full_page_scan=args.full_page_scan,
        use_current_discovery=not args.no_current_discovery,
        resume=args.resume,
    )

    all_links_path = run_dir / "all_outbound.csv"
    write_csv(all_links_path, links, LINK_FIELDS)

    summary = summarize_domains(links)
    domains_path = run_dir / "domains_summary.csv"
    write_csv(domains_path, summary, DOMAIN_FIELDS)

    checks = await check_domains([r["target_domain"] for r in summary], concurrency=args.rdap_concurrency)
    check_map = {r["domain"]: r for r in checks}
    write_csv(run_dir / "domain_status.csv", checks, CHECK_FIELDS)

    drops = []
    for row in summary:
        status = check_map.get(row["target_domain"], {})
        if status.get("candidate"):
            merged = dict(row)
            merged.update({
                "rdap_status": status.get("rdap_status", ""),
                "dns_status": status.get("dns_status", ""),
                "confidence": status.get("confidence", ""),
                "rdap_server": status.get("rdap_server", ""),
                "detail": status.get("detail", ""),
            })
            drops.append(merged)
    drops.sort(key=lambda r: (r.get("confidence") == "HIGH", int(r.get("priority_score", 0))), reverse=True)
    drops_path = run_dir / "drop_candidates.csv"
    write_csv(drops_path, drops, DROP_FIELDS)

    errors = stats.pop("errors", [])
    write_csv(run_dir / "crawl_errors.csv", errors, ERROR_FIELDS)
    stats.update({
        "run_id": run_id,
        "source_site": url,
        "unique_external_domains": len(summary),
        "follow_link_rows": sum(1 for r in links if r.get("follow")),
        "nofollow_link_rows": sum(1 for r in links if r.get("nofollow")),
        "potential_drops": len(drops),
        "high_confidence_drops": sum(1 for r in drops if r.get("confidence") == "HIGH"),
    })

    sheets_result = {"enabled": False}
    try:
        sheets_result = await asyncio.to_thread(
            sync_run_to_google_sheets,
            run_id=run_id,
            source_site=url,
            stats=stats,
            domains=summary,
            drops=drops,
            links=links,
            errors=errors,
            write_raw_links=args.write_raw_links_to_sheets,
        )
    except Exception as exc:
        sheets_result = {"enabled": False, "error": f"{type(exc).__name__}: {exc}"}
    stats["google_sheets"] = sheets_result
    write_json(run_dir / "stats.json", stats)

    archive_base = out_root / f"{slug}-report"
    zip_path = Path(shutil.make_archive(str(archive_base), "zip", root_dir=run_dir))

    if not stats.get("crawl_complete"):
        crawl_label = "⚠️ PARTIAL"
    elif stats.get("crawl_errors"):
        crawl_label = "⚠️ COMPLETE WITH ACCESS ERRORS"
    else:
        crawl_label = "✅ COMPLETE"

    text = (
        f"{crawl_label}: {url}\n\n"
        f"Pages crawled: {stats['pages_crawled']}\n"
        f"Outbound link rows: {stats['outbound_link_rows']}\n"
        f"Unique external domains: {stats['unique_external_domains']}\n"
        f"Follow links: {stats['follow_link_rows']}\n"
        f"Nofollow links: {stats['nofollow_link_rows']}\n"
        f"Potential drops: {stats['potential_drops']}\n"
        f"High confidence: {stats['high_confidence_drops']}\n"
        f"Errors: {stats.get('crawl_errors', 0)}\n"
        f"Runtime: {stats['runtime_seconds']} sec"
    )
    if not stats.get("crawl_complete"):
        text += f"\nRemaining queue: {stats.get('remaining_queue_urls', 0)}"
    if sheets_result.get("enabled"):
        text += "\n📊 Saved to Google Sheets"
    elif sheets_result.get("error"):
        text += f"\n⚠️ Google Sheets export failed: {sheets_result['error'][:180]}"
    else:
        text += "\nℹ️ Google Sheets export not configured"

    await telegram_send_message(tg_token, tg_chat, text)
    await telegram_send_document(tg_token, tg_chat, zip_path, caption=f"Report: {slug}")

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"REPORT_ZIP={zip_path}")


if __name__ == "__main__":
    asyncio.run(main_async())
