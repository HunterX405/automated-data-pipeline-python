from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
from re import search, IGNORECASE, Match
from hashlib import sha256
from json import dumps

def normalize_url(url: str) -> str:
    parts = urlsplit(url)
    normalized_query = urlencode(sorted(parse_qsl(parts.query)))
    return urlunsplit((
        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path,
        normalized_query,
        "",  # drop fragment
    ))

def relevant_headers(headers: dict[str] | None) -> dict:
    if not headers:
        return {}

    ALLOWED: set[str] = {
        "accept",
        "content-type",
    }

    return {
        k.lower(): v 
        for k, v in headers.items()
        if k.lower() in ALLOWED
    }

def build_cache_key(
        *,
        namespace: str,
        url: str,
        headers: dict | None = None,
        version: str = "v1",
    ) -> str:

    payload = {
        "url": normalize_url(url),
        "headers": relevant_headers(headers),
    }

    raw = dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = sha256(raw.encode()).hexdigest()

    return f"{namespace}:{version}:{digest}"

def get_max_age(cache_control: str) -> int:
        max_age: Match = search(r"max-age=(\d+)", cache_control, IGNORECASE)
        if max_age:
            return int(max_age.group(1))
        return 0
    
def calculate_ttl(cache_control: str, max_ttl: int) -> int:
    max_age = get_max_age(cache_control)
    stale_age: Match = search(r"stale-while-revalidate=(\d+)", cache_control, IGNORECASE)
    ttl = max_age
    if stale_age:
        ttl += int(stale_age.group(1))

    if ttl == 0 or ttl > max_ttl:
        ttl = max_ttl

    return ttl