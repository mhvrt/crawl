# Live Drop Hunter v3

Exhaustive current-site outbound-link crawler for finding target domains that may have dropped.

## Workflow

Paste one or many domains into **Actions → Crawl sites → Run workflow**. Each domain becomes an independent matrix job. The workflow runs **2 site jobs in parallel** so finished sites return results immediately without waiting for the whole list.

For each site the crawler:

1. seeds the queue from homepage + current sitemap/feed/homepage discovery;
2. follows every newly discovered same-site internal URL until the queue is exhausted (or runtime/page limit is reached);
3. extracts every external DOM `<a>` link from every live source page;
4. stores source URL, target URL/domain, anchor, `rel`, follow/nofollow/sponsored/ugc, approximate DOM position and XPath;
5. aggregates external target domains;
6. checks suspicious target domains with DNS/RDAP;
7. sends Telegram results and a ZIP artifact;
8. optionally appends summarized results into one shared Google Sheet.

No Wayback/Common Crawl historical URLs are used. A true orphan page that is not linked anywhere and is absent from current discovery sources cannot be discovered by any normal crawler.

## Google Sheets structure

One shared spreadsheet is recommended, not a tab per source site:

- **Dashboard** — latest result per source site (upserted)
- **Runs** — append-only history of every crawl
- **Drop Candidates** — all candidates from all sites, with `run_id` and `source_site`
- **Outbound Domains** — aggregated current external target domains
- **Outbound Links** — optional link-level raw rows; disabled by default because it can become huge
- **Errors** — crawl/access errors

Full link-level CSV is always preserved in each site's ZIP/GitHub artifact even when raw Google Sheets export is disabled.

### Required GitHub repository secrets

Telegram:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Google Sheets:

- `GOOGLE_SHEETS_SPREADSHEET_ID`
- either `GOOGLE_SERVICE_ACCOUNT_JSON` (raw service-account JSON) **or** `GOOGLE_SERVICE_ACCOUNT_JSON_B64` (base64-encoded JSON)

Share the destination Google Sheet with the service account's `client_email` as **Editor**.

If Google secrets are absent or invalid, crawling still completes; Sheets export is skipped/marked failed and CSV/ZIP/Telegram continue working.

## Reports per site

- `all_outbound.csv`
- `domains_summary.csv`
- `domain_status.csv`
- `drop_candidates.csv`
- `crawl_errors.csv`
- `stats.json`
- `crawl_checkpoint.json`

## Multi-domain input

Accepted examples:

```text
example.com
site2.org
https://site3.net/
```

or:

```text
example.com, site2.org; site3.net
```

Maximum per single GitHub matrix workflow is 256 site jobs. For larger lists, split into multiple runs.

## Resume

v3 uploads each site's whole `output/` as a per-site artifact. With the default
`max_pages=0` and `auto_resume=true`, an incomplete five-hour crawl automatically
starts another workflow run from that artifact/checkpoint. The chain stops only
when the site's discoverable URL queue is empty, or when the configurable
`max_continuations` safety limit is reached (24 runs by default).

Automatic continuation is intentionally disabled when `max_pages` is non-zero,
because a cumulative page cap is not a full-site crawl. To continue an older or
manually stopped run, supply its workflow run ID as `resume_run_id`.

## Benchmarking crawler changes

Use **Actions → Crawler benchmark → Run workflow** for acquisition benchmarks.
The default run crawls up to 1,000 pages from FXStreet with the production
HTTP-first scheduler, but skips DNS/RDAP, Google Sheets and Telegram so those
services do not distort crawler throughput.

Compare these fields from `stats.json` between runs:

- `attempted_pages_per_hour`
- `successful_pages_per_hour`
- `new_external_domains_per_hour`
- `unique_external_domains_seen`
- `browser_fallback_attempts`
- `rate_limit_events`
- `top_yield_sections`

The benchmark still uploads all link CSVs and the checkpoint as a GitHub
artifact. Run it from a clean start rather than resuming an older crawl when
comparing scheduler/downloader changes.
