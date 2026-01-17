from httpx import AsyncClient, Limits, HTTPStatusError, Response, RemoteProtocolError, Timeout, ReadTimeout
from dataclasses import dataclass, field
from redis.asyncio import Redis
from asyncio import BoundedSemaphore
from stamina import retry
from collections import Counter
from os import getenv
from json import loads, dumps
from logging import getLogger, Logger
from .cache import build_cache_key
from re import search, IGNORECASE, Match


@dataclass(slots = True, kw_only = True)
class CacheAPI:
    api_key: str | None = None
    max_concurrency: int = 10
    max_ttl: int = 86_400
    sleep_timeout = 300
    redis_cache: Redis = field(init=False)
    client: AsyncClient = field(init=False)
    semaphore: BoundedSemaphore = field(init=False)
    stats: Counter = field(default_factory=Counter)
    log: Logger = field(init=False)
    namespace: str = "CacheAPI"

    def __post_init__(self):
        self.redis_cache = Redis.from_url(
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
        self.log = getLogger(self.namespace)
        self.semaphore = BoundedSemaphore(self.max_concurrency)
        self.client.event_hooks['response'] = [self.log_response]
        self.client.headers['Content-Type'] = "application/json"
        if self.api_key:
            self.client.headers['x-api-key'] = self.api_key

    async def log_response(self, response: Response) -> None:
        # All responses here will be from network as it is an event hook from httpx
        # response.is_error that is already being logged by stamina @retry event hook in logs.py
        if response.is_success:
            self.log.debug(
                f"'{response.http_version}' {response.request.method} <{response.status_code}> | "
                f"Cache-Control: {response.headers.get('Cache-Control', "")} | {response.url}"
            )
    
    async def get_cached(self, key: str) -> dict:
        if (cached_json := await self.redis_cache.get(key)) is not None:
            cached: dict = loads(cached_json)
            self.stats['responses'] += 1
            self.stats["cache_hits"] += 1
            return cached
        return None
    
    def get_cache_max_age(self, cache_control: str) -> int:
        max_age: Match = search(r"max-age=(\d+)", cache_control, IGNORECASE)
        if max_age:
            return int(max_age.group(1))
        return 0
    
    async def cache_response(self, response: Response, cache_control: str, key: str):
        max_age = self.get_cache_max_age(cache_control)
        stale_age: Match = search(r"stale-while-revalidate=(\d+)", cache_control, IGNORECASE)
        ttl = max_age
        if stale_age:
            ttl += int(stale_age.group(1))
        else:
            ttl = self.max_ttl
        payload = {
            "status_code": response.status_code,
            "url": str(response.url),
            "method": response.request.method,
            "http_version": response.http_version,
            "cache-control": cache_control,
            "etag": response.headers.get("etag", ""),
            "last-modified": response.headers.get("last-modified", ""),
            "max-age": max_age,
            "ttl": ttl,
            "body": response.json(),
        }
        
        await self.redis_cache.set(key, dumps(payload), ex=ttl)
    
    async def validate_response(self, url: str, headers: dict):       
        response: Response = await self.client.get(url, headers = headers)
        if response.status_code == 304:
            self.log.debug(f"'{response.http_version}' {response.request.method} <304> Not Modified | From-cache: True | {url}")
        elif response.status_code == 200:
            self.log.debug(f"{response.http_version}' {response.request.method} <200> Resource has been updated | {url}")
        return response
    
    async def is_stale(self, cached_response: dict, key :str):
        cache_ttl = await self.redis_cache.ttl(key)
        max_age = cached_response['ttl'] - cached_response['max-age']
        return cache_ttl < max_age

    async def get(self, url: str, params: dict | None = None, headers: dict | None = None, ext: dict | None = None):
        async with self.semaphore:
            key = build_cache_key(
                namespace = self.namespace, 
                url = url,
                headers = headers
            )
            cached_response = await self.get_cached(key)
            if cached_response:
                cache_control = cached_response.get("cache-control", "")
                etag: str = cached_response.get("etag", "")
                last_modified: str = cached_response.get("last-modified", "")
                headers: dict | None = None

                if etag:
                    headers = {"If-None-Match": etag}    
                elif last_modified:
                    headers = {"If-Modified-Since": last_modified}

                is_no_cache = search(r"no-cache", cache_control)
                is_must_revalidate = search(r"must-revalidate", cache_control)
                is_stale = await self.is_stale(cached_response, key)

                should_validate = (headers and (is_no_cache or (is_must_revalidate and is_stale)))
                if should_validate:
                    response: Response = await self.validate_response(url, headers)
                    if response.status_code == 200:
                        self.cache_response(response, response.headers.get('Cache-Control', ""), key)
                        return response.json()
                else:
                    self.log.debug(
                        f"'{cached_response['http_version']}' {cached_response['method']} <{cached_response['status_code']}> | "
                        f"From-cache: True | {cached_response['url']}"
                    )
                return cached_response['body']

            return await self._get_with_retry(url, params, headers, ext, key)
    
    @retry(on = (HTTPStatusError, RemoteProtocolError, ReadTimeout), wait_initial = 1, wait_max = sleep_timeout)
    async def _get_with_retry(self, url, params, headers, ext, key):
        response = await self.client.get(
            url=url,
            params=params,
            headers=headers,
            extensions=ext,
        )
        response.raise_for_status()

        cache_control: str = response.headers.get('cache-control', "")
        if not search(r"no-store", cache_control):
            await self.cache_response(response, cache_control, key)

        self.stats['responses'] += 1
        self.stats["network_requests"] += 1

        return response.json()