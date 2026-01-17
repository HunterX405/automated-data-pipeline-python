from httpx import AsyncClient, Limits, HTTPStatusError, Response, RemoteProtocolError, Timeout, ReadTimeout
from asyncio import BoundedSemaphore, Task, create_task, CancelledError
from .cache import build_cache_key, get_max_age, calculate_ttl
from dataclasses import dataclass, field
from logging import getLogger, Logger
from redis.asyncio import Redis
from collections import Counter
from contextlib import suppress
from json import loads, dumps
from typing import ClassVar
from stamina import retry
from re import search
from os import getenv


@dataclass(slots = True, kw_only = True)
class CacheAPI:
    api_key: str | None = None
    namespace: str = "CacheAPI"
    max_concurrency: int = 10
    max_ttl: int = 86_400
    sleep_timeout = 300
    client: AsyncClient = field(init=False)
    stats: ClassVar[Counter] = Counter()
    _status_task: ClassVar[Task | None] = None
    _instances: ClassVar[list['CacheAPI']] = []
    _redis_cache: Redis = field(init=False)
    _semaphore: BoundedSemaphore = field(init=False)
    _log: Logger = field(init=False)

    def __post_init__(self):
        # Auto-register every instance for cleanup
        CacheAPI._instances.append(self)

        self._redis_cache = Redis.from_url(
            getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses = True,
        )
        self.client = AsyncClient(
            http2 = True,
            timeout = Timeout(
                connect = 5.0,
                read = 30.0,
                write = 10.0,
                pool = 5.0,
            ),
            limits = Limits(
                max_connections = 20,
                max_keepalive_connections = 10,  # Multiple connections to spread load
                keepalive_expiry = 30.0  # Refresh connections periodically
            )
        )
        self.client.event_hooks['response'] = [self._log_response]
        self.client.headers['content-type'] = "application/json"
        if self.api_key:
            self.client.headers['x-api-key'] = self.api_key
        
        self._log = getLogger(self.namespace)
        self._semaphore = BoundedSemaphore(self.max_concurrency)

        # Start status loop once on first instance created
        if CacheAPI._status_task is None:
            # Lazy import to prevent circular import
            from .status import status_loop
            CacheAPI._status_task = create_task(status_loop())

    async def close(self):
        """Close this instance's connections"""
        await self.client.aclose()
        await self._redis_cache.aclose()

    @classmethod
    async def cleanup_all(cls):
        """Cleanup all instances and stop status loop"""
        if cls._status_task:
            cls._status_task.cancel()
            with suppress(CancelledError):
                await cls._status_task
            cls._status_task = None
        
        for instance in list(cls._instances):
            await instance.close()

    async def _log_response(self, response: Response) -> None:
        """ Log event hook for responses from httpx """
        # All responses here will be from network as it is an event hook from httpx
        # response.is_error that is already being logged by stamina @retry event hook in logs.py
        if response.is_success:
            self._log.debug(
                f"'{response.http_version}' {response.request.method} <{response.status_code}> | "
                f"Cache-Control: {response.headers.get('cache-control', "")} | {response.url}"
            )
    
    async def _get_cached(self, key: str) -> dict:
        """ Get cached entry from redis cache with key """
        if (cached_json := await self._redis_cache.get(key)) is not None:
            cached: dict = loads(cached_json)
            self.stats['responses'] += 1
            self.stats["cache_hits"] += 1
            return cached
        return None
    
    async def _cache_response(self, response: Response, key: str, update: bool = False) -> None:
        """ Cache the response to redis with hashed key """
        cache_control: str = response.headers.get('cache-control', "")
        
        ttl: int = calculate_ttl(cache_control, self.max_ttl)

        if update:
            await self._redis_cache.expire(key, ttl)
            return

        payload = {
            "status_code": response.status_code,
            "url": str(response.url),
            "method": response.request.method,
            "http_version": response.http_version,
            "cache-control": cache_control,
            "etag": response.headers.get("etag", ""),
            "last-modified": response.headers.get("last-modified", ""),
            "max-age": get_max_age(cache_control),
            "ttl": ttl,
            "body": response.json(),
        }
        
        await self._redis_cache.set(key, dumps(payload), ex=ttl)
    
    async def _validate_response(self, url: str, headers: dict, key: str):
        """ Validate if the response with the server is still the same with cache and update cache if resource was updated """
        response: Response = await self.client.get(url, headers = headers)

        if response.status_code == 304:
            self._log.debug(f"'{response.http_version}' {response.request.method} <304> Not Modified | From-cache: True | {url}")
            
            # Update ttl based on the new cache-control headers
            await self._cache_response(response, key, update=True)

        elif response.status_code == 200:
            self._log.debug(f"{response.http_version}' {response.request.method} <200> Resource has been updated | {url}")
        return response
    
    async def _is_stale(self, cached_response: dict, key :str):
        """ Check if the response is stale if the ttl exceeded max-age """
        cache_ttl = await self._redis_cache.ttl(key)
        max_age = cached_response['max-age']
        # Stale if we're in the stale-while-revalidate window
        # (remaining TTL is less than the SWR period)
        stale_window = cached_response['ttl'] - max_age
        return cache_ttl <= stale_window

    @retry(on = (HTTPStatusError, RemoteProtocolError, ReadTimeout), wait_initial = 1, wait_max = sleep_timeout)
    async def _get_with_retry(self, url, params, headers, ext, key):
        """ Get response using httpx async client with retry if response was not successfull """
        response = await self.client.get(
            url=url,
            params=params,
            headers=headers,
            extensions=ext,
        )
        response.raise_for_status()

        cache_control: str = response.headers.get('cache-control', "")
        if not search(r"no-store", cache_control):
            await self._cache_response(response, cache_control, key)

        self.stats['responses'] += 1
        self.stats["network_requests"] += 1

        return response.json()
    
    async def get(self, url: str, params: dict | None = None, headers: dict | None = None, ext: dict | None = None):
        """ Get the response either from cache or server and validate the cached response if needed """
        async with self._semaphore:
            key = build_cache_key(namespace=self.namespace, url=url, headers=headers)
            cached_response = await self._get_cached(key)

            if not cached_response:
                return await self._get_with_retry(url, params, headers, ext, key)
            
            # There is a cached response
            cache_control = cached_response.get("cache-control", "")

            # Check if revalidation is needed according to Cache-Control directives
            needs_revalidation = (
                search(r"no-cache", cache_control) or
                (search(r"must-revalidate", cache_control) and await self._is_stale(cached_response, key))
            )

            if needs_revalidation:
                revalidation_headers: dict = {}
                if etag := cached_response.get("etag", ""):
                    revalidation_headers["if-none-match"] = etag
                elif last_modified := cached_response.get("last-modified", ""):
                    revalidation_headers["if-modified-since"] = last_modified

                if revalidation_headers:
                    response: Response = await self._validate_response(url, revalidation_headers, key)
                    self.stats['responses'] += 1

                    if response.status_code == 200:
                        self.stats["network_requests"] += 1
                        await self._cache_response(response, key)
                        return response.json()
            
            # Serve from cache 
            self._log.debug(
                f"'{cached_response['http_version']}' {cached_response['method']} "
                f"<{cached_response['status_code']}> | From-cache: True | {cached_response['url']}"
            )
            return cached_response['body']