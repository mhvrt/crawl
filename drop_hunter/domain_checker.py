from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urljoin

import dns.exception
import dns.resolver
import httpx

IANA_RDAP_BOOTSTRAP = "https://data.iana.org/rdap/dns.json"


@dataclass
class DomainStatus:
    domain: str
    rdap_status: str
    dns_status: str
    candidate: bool
    confidence: str
    rdap_server: str = ""
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def classify_candidate(rdap_status: str, dns_status: str) -> tuple[bool, str]:
    if rdap_status == "NOT_FOUND" and dns_status == "NXDOMAIN":
        return True, "HIGH"
    if rdap_status == "NOT_FOUND" and dns_status in {"NO_NAMESERVERS", "NO_ANSWER", "UNKNOWN"}:
        return True, "MEDIUM"
    if rdap_status == "NOT_FOUND":
        return True, "LOW"
    return False, ""


async def _dns_status(domain: str) -> str:
    def query() -> str:
        resolver = dns.resolver.Resolver(configure=True)
        resolver.lifetime = 5.0
        try:
            resolver.resolve(domain, "NS")
            return "RESOLVES"
        except dns.resolver.NXDOMAIN:
            return "NXDOMAIN"
        except dns.resolver.NoAnswer:
            return "NO_ANSWER"
        except dns.resolver.NoNameservers:
            return "NO_NAMESERVERS"
        except dns.exception.Timeout:
            return "TIMEOUT"
        except Exception:
            return "UNKNOWN"
    return await asyncio.to_thread(query)


async def load_rdap_bootstrap(client: httpx.AsyncClient) -> dict[str, str]:
    try:
        r = await client.get(IANA_RDAP_BOOTSTRAP, timeout=30)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return {}

    mapping: dict[str, str] = {}
    for service in payload.get("services", []):
        if not isinstance(service, list) or len(service) < 2:
            continue
        tlds, urls = service[0], service[1]
        if not urls:
            continue
        base = next((u for u in urls if u.startswith("https://")), urls[0])
        if not base.endswith("/"):
            base += "/"
        for tld in tlds:
            mapping[str(tld).lower()] = base
    return mapping


async def _rdap_status(domain: str, client: httpx.AsyncClient, mapping: dict[str, str]) -> tuple[str, str, str]:
    tld = domain.rsplit(".", 1)[-1].lower() if "." in domain else ""
    base = mapping.get(tld, "")
    rdap_server = base or "https://rdap.org/"
    endpoint = urljoin(rdap_server, "domain/" + quote(domain, safe=".-"))
    detail = ""

    for attempt in range(4):
        try:
            r = await client.get(endpoint, timeout=20, follow_redirects=True)
            if r.status_code == 200:
                return "REGISTERED", rdap_server, detail
            if r.status_code == 404:
                return "NOT_FOUND", rdap_server, detail
            if r.status_code == 429:
                if attempt < 3:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                return "RATE_LIMITED", rdap_server, "429"
            if 500 <= r.status_code <= 599:
                if attempt < 3:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                return "SERVER_ERROR", rdap_server, str(r.status_code)
            return "UNKNOWN", rdap_server, str(r.status_code)
        except httpx.TimeoutException:
            if attempt < 3:
                await asyncio.sleep(min(2 ** attempt, 8))
                continue
            return "TIMEOUT", rdap_server, ""
        except Exception as exc:
            return "ERROR", rdap_server, type(exc).__name__
    return "UNKNOWN", rdap_server, detail


async def _check_one(domain: str, client: httpx.AsyncClient, mapping: dict[str, str], semaphore: asyncio.Semaphore) -> DomainStatus:
    async with semaphore:
        dns_status = await _dns_status(domain)

        # A domain answering authoritatively for NS is registered; skip RDAP to avoid
        # wasting rate limits on obviously-live targets.
        if dns_status == "RESOLVES":
            return DomainStatus(
                domain=domain,
                rdap_status="SKIPPED_DNS_RESOLVES",
                dns_status=dns_status,
                candidate=False,
                confidence="",
                detail="RDAP skipped because DNS NS resolves",
            )

        rdap_status, rdap_server, detail = await _rdap_status(domain, client, mapping)
        candidate, confidence = classify_candidate(rdap_status, dns_status)
        return DomainStatus(
            domain=domain,
            rdap_status=rdap_status,
            dns_status=dns_status,
            candidate=candidate,
            confidence=confidence,
            rdap_server=rdap_server,
            detail=detail,
        )


async def check_domains(domains: list[str], concurrency: int = 8) -> list[dict[str, Any]]:
    headers = {"User-Agent": "live-drop-hunter/0.3 (+GitHub Actions)"}
    limits = httpx.Limits(max_connections=max(4, concurrency), max_keepalive_connections=max(4, concurrency))
    async with httpx.AsyncClient(headers=headers, limits=limits) as client:
        mapping = await load_rdap_bootstrap(client)
        semaphore = asyncio.Semaphore(max(1, concurrency))
        tasks = [_check_one(d, client, mapping, semaphore) for d in sorted(set(domains))]
        results: list[dict[str, Any]] = []
        for fut in asyncio.as_completed(tasks):
            status = await fut
            results.append(status.as_dict())
        return results
