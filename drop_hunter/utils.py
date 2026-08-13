from __future__ import annotations

import hashlib
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urldefrag, urlsplit, urlunsplit

try:
    import tldextract
except ImportError:  # local/static testing fallback
    tldextract = None

_TRACKING_PARAMS = {
    "fbclid", "gclid", "dclid", "msclkid", "yclid", "mc_cid", "mc_eid",
    "ref", "ref_src", "igshid", "phpsessid", "jsessionid", "sessionid",
    "sid", "sessid", "session_id", "cf_clearance",
}


def ensure_url(value: str) -> str:
    value = value.strip()
    if not re.match(r"^https?://", value, flags=re.I):
        value = "https://" + value
    return value


def canonicalize_url(url: str) -> str:
    """Normalize only obvious duplicates; preserve meaningful query params."""
    url, _fragment = urldefrag(url.strip())
    parts = urlsplit(url)
    scheme = (parts.scheme or "https").lower()
    host = (parts.hostname or "").lower().rstrip(".")
    port = parts.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    else:
        netloc = host

    query_pairs = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lk = key.lower()
        if lk.startswith("utm_") or lk in _TRACKING_PARAMS:
            continue
        query_pairs.append((key, value))
    query = urlencode(query_pairs, doseq=True)
    path = parts.path or "/"
    return urlunsplit((scheme, netloc, path, query, ""))


def hostname_from_url(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().rstrip(".")


def registrable_domain(host_or_url: str) -> str:
    host = hostname_from_url(host_or_url) if "://" in host_or_url else host_or_url.lower().rstrip(".")
    if not host:
        return ""
    if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", host) or ":" in host:
        return host
    if tldextract is not None:
        ext = tldextract.TLDExtract(suffix_list_urls=None)(host)
        return ext.top_domain_under_public_suffix or host
    # Fallback is intentionally conservative; production installs tldextract.
    labels = host.split(".")
    return ".".join(labels[-2:]) if len(labels) >= 2 else host


def safe_slug(value: str) -> str:
    value = re.sub(r"^https?://", "", value.strip(), flags=re.I)
    value = re.sub(r"[^a-zA-Z0-9._-]+", "_", value)
    return value[:100].strip("_") or "crawl"


def domain_key(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
