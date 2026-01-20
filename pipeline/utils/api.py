from httpx import AsyncClient, Limits, HTTPStatusError, Response, RemoteProtocolError, Timeout, ReadTimeout
from asyncio import BoundedSemaphore, Task, CancelledError, create_task, sleep
from stamina.instrumentation import RetryDetails
from dataclasses import dataclass, field
from logging import getLogger, Logger
from .cache import AsyncRedisCache
from collections import Counter
from contextlib import suppress
from typing import ClassVar
from stamina import retry
from sys import stdout
from re import search, IGNORECASE


@dataclass(slots = True, kw_only = True)
class CacheAPI:
    api_key: str | None = None
    namespace: str = "api"  # For redis cache key
    max_concurrency: int = 10
    client: AsyncClient = field(init=False)
    cache: ClassVar[AsyncRedisCache] = AsyncRedisCache()
    stats: ClassVar[Counter] = Counter()
    _sleep_timeout: ClassVar[int] = 300
    _status_task: ClassVar[Task | None] = None
    _instances: ClassVar[list['CacheAPI']] = []
    _log: ClassVar[Logger] = getLogger('api')
    _semaphore: BoundedSemaphore = field(init=False)

    def __post_init__(self):
        # Auto-register every instance for cleanup
        CacheAPI._instances.append(self)

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
        
        # Limit asyncio concurrent calls
        self._semaphore = BoundedSemaphore(self.max_concurrency)

        # Start status loop once on first instance created
        if CacheAPI._status_task is None:
            CacheAPI._status_task = create_task(self._status_loop())

    async def close(self) -> None:
        """Close this instance's connections"""
        await self.client.aclose()

    @classmethod
    async def cleanup_all(cls) -> None:
        """Cleanup all instances and stop status loop"""
        if cls._status_task:
            cls._status_task.cancel()
            with suppress(CancelledError):
                await cls._status_task
            cls._status_task = None

        # Close async cache connection
        await cls.cache.close()
        
        for instance in list[CacheAPI](cls._instances):
            await instance.close()
        cls._instances.clear()

    @classmethod
    async def _status_loop(cls, interval: float = 1.0) -> None:
        """ Console display for API stats """
        is_interactive: bool = stdout.isatty()
        log_interval: float = interval if is_interactive else 30.0  # Log every 30s in Docker
        stats: Counter = cls.stats
        
        try:
            while True:
                line = (
                    f"API Requests: {stats['responses']} | "
                    f"Cached: {stats['cache_hits']} | "
                    f"Network: {stats['network_requests']} | "
                    f"Errors: {stats['errors']} | "
                    f"Retries: {stats['retries']} | "
                    f"Queue: {stats['queue']} | "
                    f"Elapsed: {(stats['elapsed'] / 60):.2f} minutes"
                )
                
                if is_interactive:
                    stdout.write("\r" + line)
                    stdout.flush()
                else:
                    print(line)

                await sleep(log_interval)
                stats['elapsed'] += log_interval
        except CancelledError:
            return
        
    # Stamina @retry log event hook
    @classmethod
    def log_retry_sleep(cls, rd: RetryDetails) -> None:
        cls.stats["retries"] += 1
        cls.stats["errors"] += 1
        error: Exception = rd.caused_by
        if isinstance(error, HTTPStatusError):
            response: Response = error.response
            cls._log.debug(
                f"<{response.status_code}> {response.reason_phrase} | "
                f"Retry #{rd.retry_num} | Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {response.url.host}"
            )
        else:
            url: str = error.request.url.host
            cls._log.debug(
                f"{error} | Retry #{rd.retry_num} | "
                f"Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {url}"
            )

    async def _log_response(self, response: Response) -> None:
        """ Log event hook for responses from httpx """
        # response.is_error that is already being logged by stamina @retry event hook in logs.py
        if response.is_success:
            self._log.debug(
                f"'{response.http_version}' {response.request.method} <{response.status_code}> | "
                f"Cache-Control: {response.headers.get('cache-control', 'None')} | {response.url}"
            )
    
    async def _validate_response(self, url: str, headers: dict, key: str) -> Response:
        """ Validate if the response with the server is still the same with cache and update cache if resource was updated """
        response: Response = await self.client.get(url, headers = headers)

        if response.status_code == 304:
            self._log.debug(f"'{response.http_version}' {response.request.method} <304> Not Modified | From-cache: True | {url}")
            
            # Update ttl based on the new cache-control headers
            await self.cache.set_cache(response, key, update=True)

        elif response.status_code == 200:
            self._log.debug(f"{response.http_version}' {response.request.method} <200> Resource has been updated | {url}")
        return response

    @retry(on = (HTTPStatusError, RemoteProtocolError, ReadTimeout), wait_initial = 1, wait_max = _sleep_timeout)
    async def _get_with_retry(self, url, params, headers, ext, key) -> dict[str]:
        """ Get response using httpx async client with retry if response was not successfull """
        response: Response = await self.client.get(
            url=url,
            params=params,
            headers=headers,
            extensions=ext,
        )
        response.raise_for_status()

        cache_control: str = response.headers.get('cache-control', "")
        if not search(r"no-store", cache_control, IGNORECASE):
            await self.cache.set_cache(response, key)

        self.stats['responses'] += 1
        self.stats["network_requests"] += 1

        return response.json()
    
    async def get(self, url: str, params: dict | None = None, headers: dict | None = None, ext: dict | None = None) -> dict[str]:
        """ Get the response either from cache or server and validate the cached response if needed """
        async with self._semaphore:
            key: str = self.cache.build_cache_key(namespace=self.namespace, url=url, headers=headers)
            cached_response: dict[str] = await self.cache.get_cache(key)

            if not cached_response:
                return await self._get_with_retry(url, params, headers, ext, key)
            
            # There is a cached response
            self.stats['responses'] += 1
            self.stats["cache_hits"] += 1
            cache_control: str = cached_response.get("cache-control", "")

            # Check if revalidation is needed according to Cache-Control headers and if resource is stale
            is_stale: bool = await self.cache.is_stale(cached_response, key)
            needs_revalidation: bool = (
                search(r"no-cache", cache_control, IGNORECASE) or
                (search(r"must-revalidate", cache_control, IGNORECASE) and is_stale) or 
                is_stale
            )

            if needs_revalidation:
                revalidation_headers: dict[str] = {}
                if etag := cached_response.get("etag", ""):
                    revalidation_headers["if-none-match"] = etag
                elif last_modified := cached_response.get("last-modified", ""):
                    revalidation_headers["if-modified-since"] = last_modified

                if revalidation_headers:
                    # If cache control has stale-while-revalidate, do background revalidation
                    if search(r"stale-while-revalidate", cache_control, IGNORECASE):
                        create_task(self._validate_response(url, revalidation_headers, key))
                    else:
                        response: Response = await self._validate_response(url, revalidation_headers, key)
                        self.stats['responses'] += 1

                        if response.status_code == 200:
                            self.stats["network_requests"] += 1
                            await self.cache.set_cache(response, key)
                            return response.json()
            
            # Serve from cache 
            self._log.debug(
                f"'{cached_response['http_version']}' {cached_response['method']} "
                f"<{cached_response['status_code']}> | From-cache: True | {cached_response['url']}"
            )
            return cached_response['body']