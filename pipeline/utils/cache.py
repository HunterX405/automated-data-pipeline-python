from httpx import AsyncHTTPTransport, Request, Response
from re import search, IGNORECASE, Match
from logging import Logger, getLogger
from redis.asyncio import Redis
from collections import Counter
from json import loads, dumps
from hashlib import sha256


class RedisCacheTransport(AsyncHTTPTransport):
    """
    Custom httpx transport that caches responses in Redis.
    Implements HTTP caching with Cache-Control, ETag, and conditional requests.
    
    Expects pre-normalized URLs to be passed in requests.
    """
    def __init__(
        self, 
        redis_url: str, 
        namespace: str = 'redis_cache', 
        max_ttl: int = 86_400,
        version: str = 'v1',
        stats: Counter = Counter(),
        **kwargs
    ) -> None:
        
        super().__init__(**kwargs)
        self._redis = Redis.from_url(redis_url, decode_responses=False)
        self._namespace = namespace
        self._max_ttl = max_ttl
        self._version = version
        self._log: Logger = getLogger('api')
        self._stats = stats

    async def handle_async_request(self, request: Request) -> Response:
        """Override to add caching layer"""

        # Only cache GET requests
        if request.method != "GET":
            return await super().handle_async_request(request)

        self._stats['responses'] += 1
        # URL is already normalized by CacheAPI
        cache_key = self._build_cache_key(request)
        cached = await self._get_cached(cache_key)

        # Cache miss - fetch from network
        if not cached:
            response = await super().handle_async_request(request)
            await response.aread()
            await self._cache_response(response, cache_key)
            self._log.debug(f"Cache: MISS - {request.url}")
            self._stats["network_requests"] += 1
            return response

        # Cache hit - check if revalidation needed
        if await self._needs_revalidation(cached, cache_key):
            response = await self._revalidate(request, cached, cache_key)
            if response:
                self._stats['responses'] += 1
                self._stats["network_requests"] += 1
                return response
        
        # Serve from cache
        self._stats["cache_hits"] += 1
        self._log.debug(f"Cache: HIT - {request.url}")
        return self._build_response_from_cache(cached)
    
    def _build_cache_key(self, request: Request) -> str:
        """
        Build cache key from request.
        Assumes URL is already normalized.
        """

        payload = {
            "url": str(request.url),  # Already normalized by CacheAPI
            "headers": self._relevant_headers(dict[str, str](request.headers)),
        }
        
        raw = dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = sha256(raw.encode()).hexdigest()
        
        return f"{self._namespace}:{self._version}:{digest}"

    @staticmethod
    def _relevant_headers(headers: dict[str, str]) -> dict[str, str]:
        """Extract only cache-relevant headers"""
        allowed: set = {"accept", "content-type"}
        return {
            k.lower(): v 
            for k, v in headers.items() 
            if k.lower() in allowed
        }
    
    async def _get_cached(self, key: str) -> dict | None:
        """Retrieve cached response from Redis"""
        if (cached := await self._redis.get(key)) is not None:
            return loads(cached)
        return None

    async def _cache_response(self, response: Response, key: str) -> None:
        """Store response in Redis cache"""
        cache_control = response.headers.get("cache-control", "")

        # Don't cache no-store responses
        if search(r"no-store", cache_control, IGNORECASE):
            return

        ttl = self._calculate_ttl(cache_control)
        headers = dict(response.headers)
        headers.pop("content-encoding", None)
        try:
            payload: dict[str] = {
                "status_code": response.status_code,
                "cache-control": cache_control,
                "headers": headers,
                "content": response.text,
                "max-age": self._get_max_age(cache_control),
                "ttl": ttl,
            }
            
            await self._redis.set(key, dumps(payload), ex=ttl)
        except Exception as e:
            self._log.error(f"Failed to store to cache. {e}")

    async def _needs_revalidation(
        self, 
        cached: dict, 
        key: str
    ) -> bool:
        """Check if cached response needs revalidation"""
        cache_control = cached.get("cache-control")
        # If there is no cache-control, store until redis default ttl expires
        if not cache_control:
            return False
        # Always revalidate if no-cache
        if search(r"no-cache", cache_control, IGNORECASE):
            return True
        
        # Check if must-revalidate
        if search(r"must-revalidate", cache_control, IGNORECASE):
            # Check if stale
            max_age: int = cached.get('max-age', 0)
            # If there is no max-age directive, store until redis ttl expires
            if max_age == 0:
                return False  # fresh until Redis automatically deletes it

            remaining_ttl: int = await self._redis.ttl(key)
            age: int = cached.get('ttl', 0) - remaining_ttl
            return age >= max_age
        
        return False

    async def _revalidate(
        self, 
        request: Request, 
        cached: dict, 
        key: str
    ) -> Response | None:
        """Perform conditional request for revalidation"""
        headers: dict[str, str] = cached.get("headers", {})
        
        # Add conditional headers from cached response
        if etag := headers.get("etag"):
            request.headers["if-none-match"] = etag
        elif last_modified := headers.get("last-modified"):
            request.headers["if-modified-since"] = last_modified
        else:
            # If no validation headers provided, get a fresh response
            self._log.debug(f"No validation headers provided, getting a fresh response. | {request.url}")
        
        response = await super().handle_async_request(request)

        if response.status_code == 200:
            # Modified - cache new response
            self._log.debug(f"<200> Resource has been updated | {request.url}")
            await self._cache_response(response, key)
            return response
        elif response.status_code == 304:
            # Not modified - update TTL and serve cached
            self._log.debug(f"<304> Not Modified | From-cache: True | {request.url}")
            cache_control = response.headers.get("cache-control", "")
            ttl = self._calculate_ttl(cache_control)
            await self._redis.expire(key, ttl)
        else:
            self._log.warning(f"Revalidation failed, using cached response. | Status Code: <{response.status_code}> ")
        return None

    def _build_response_from_cache(self, cached: dict[str, str]) -> Response:
        """Reconstruct Response object from cached data"""
        return Response(
            status_code=cached["status_code"],
            headers=cached['headers'],
            content=cached["content"],
        )

    def _calculate_ttl(self, cache_control: str) -> int:
        """Calculate TTL from Cache-Control header"""
        ttl = self._get_max_age(cache_control)
        
        stale_match: Match | None = search(
            r"stale-while-revalidate=(\d+)", 
            cache_control, 
            IGNORECASE
        )

        if stale_match:
            ttl += int(stale_match.group(1))
        
        return min(ttl, self._max_ttl) if ttl > 0 else self._max_ttl

    @staticmethod
    def _get_max_age(cache_control: str) -> int:
        """Extract max-age from Cache-Control header"""
        match = search(r"max-age=(\d+)", cache_control, IGNORECASE)
        return int(match.group(1)) if match else 0

    async def aclose(self) -> None:
        """Clean up resources"""
        await self._redis.aclose()
        await super().aclose()