from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
from json import dumps
from hashlib import sha256

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
