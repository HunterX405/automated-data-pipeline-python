from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit
from re import search, IGNORECASE, Match
from redis.asyncio import Redis
from json import loads, dumps
from typing import ClassVar
from httpx import Response
from hashlib import sha256
from os import getenv


class AsyncCache:
    """
    Singleton cache manager with all caching operations.
    Handles connection pooling and cache operations for all API clients.
    """

    _instance: ClassVar[None] = None
    _max_ttl: ClassVar[int] = 86_400  # Default 24 hours

    @classmethod
    def get_instance(cls):
        raise NotImplementedError

    @classmethod
    async def close(cls):
        raise NotImplementedError

    @classmethod
    def normalize_url(cls, url: str) -> str:
        parts = urlsplit(url)
        normalized_query = urlencode(sorted(parse_qsl(parts.query)))

        return urlunsplit((
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path,
            normalized_query,
            "",  # drop fragment
        ))

    @classmethod
    def relevant_headers(cls, headers: dict[str] | None) -> dict:
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

    @classmethod
    def build_cache_key(
            cls,
            namespace: str,
            url: str,
            headers: dict | None = None,
            version: str = "v1",
        ) -> str:

        payload = {
            "url": cls.normalize_url(url),
            "headers": cls.relevant_headers(headers),
        }

        raw = dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = sha256(raw.encode()).hexdigest()

        return f"{namespace}:{version}:{digest}"

    @classmethod
    def get_max_age(cls, cache_control: str) -> int:
        max_age: Match = search(r"max-age=(\d+)", cache_control, IGNORECASE)
        if max_age:
            return int(max_age.group(1))
        return 0
        
    @classmethod
    def calculate_ttl(cls, cache_control: str) -> int:
        max_age = cls.get_max_age(cache_control)
        stale_age: Match = search(r"stale-while-revalidate=(\d+)", cache_control, IGNORECASE)
        ttl = max_age
        if stale_age:
            ttl += int(stale_age.group(1))

        if ttl == 0 or ttl > cls._max_ttl:
            ttl = cls._max_ttl

        return ttl


class AsyncRedisCache(AsyncCache):
    """
    Singleton Redis cache manager with all caching operations.
    Handles connection pooling and cache operations for all API clients.
    """
    _instance: ClassVar[Redis | None] = None

    @classmethod
    def get_instance(cls, max_ttl: int | None = None) -> Redis:
        """Get or create the shared Redis instance"""
        if cls._instance is None:
            cls._instance = Redis.from_url(
                getenv("REDIS_URL", "redis://localhost:6379/0"),
                decode_responses=True,
                
            )
        
        if max_ttl is not None:
            cls._max_ttl = max_ttl

        return cls._instance
    
    @classmethod
    async def close(cls):
        """Close the shared connection"""
        if cls._instance is not None:
            await cls._instance.aclose()
            cls._instance = None

    @classmethod
    async def get_cache(cls, key: str) -> dict:
        """ Get cached entry from redis cache with key """
        redis = cls.get_instance()
        if (cached_json := await redis.get(key)) is not None:
            cached: dict = loads(cached_json)
            return cached
        return None
    
    @classmethod
    async def set_cache(cls, response: Response, key: str, update: bool = False) -> None:
        """ Cache the response to redis with hashed key """
        cache_control: str = response.headers.get('cache-control', "")
        
        ttl: int = cls.calculate_ttl(cache_control)

        redis = cls.get_instance()

        if update:
            await redis.expire(key, ttl)
            return

        payload = {
            "status_code": response.status_code,
            "url": str(response.url),
            "method": response.request.method,
            "http_version": response.http_version,
            "cache-control": cache_control,
            "etag": response.headers.get("etag", ""),
            "last-modified": response.headers.get("last-modified", ""),
            "max-age": cls.get_max_age(cache_control),
            "ttl": ttl,
            "body": response.json(),
        }
        
        await redis.set(key, dumps(payload), ex=ttl)
        

    @classmethod
    async def is_stale(cls, cached_response: dict, key :str):
        """ Check if the response is stale if the ttl exceeded max-age """
        redis = cls.get_instance()
        cache_ttl = await redis.ttl(key)
        max_age = cached_response['max-age']
        # Stale if we're in the stale-while-revalidate window
        # (remaining TTL is less than the SWR period)
        stale_window = cached_response['ttl'] - max_age
        return cache_ttl <= stale_window
