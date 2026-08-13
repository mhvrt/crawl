from __future__ import annotations

import json
import os
import re
from urllib.parse import urlsplit


def slug(value: str) -> str:
    value = re.sub(r"^https?://", "", value.strip(), flags=re.I)
    value = re.sub(r"[^a-zA-Z0-9._-]+", "_", value)
    return value[:80].strip("_") or "site"


def normalize(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if not re.match(r"^https?://", value, re.I):
        value = "https://" + value
    parts = urlsplit(value)
    if not parts.hostname:
        return ""
    return f"{parts.scheme.lower()}://{parts.netloc.lower()}{parts.path or '/'}"


raw = os.getenv("DOMAINS_INPUT", "")
items = []
seen = set()
for token in re.split(r"[\n,;\s]+", raw):
    value = normalize(token)
    if not value or value in seen:
        continue
    seen.add(value)
    items.append({"domain": value, "slug": slug(value)})

if not items:
    raise SystemExit("No valid domains were supplied")
if len(items) > 256:
    raise SystemExit("GitHub Actions matrix supports at most 256 jobs per workflow run; split the list")

payload = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
print(payload)
with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
    fh.write(f"sites={payload}\n")
