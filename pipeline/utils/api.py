from httpx import AsyncClient, Limits, HTTPStatusError, Response, RemoteProtocolError, Timeout, ReadTimeout
from asyncio import BoundedSemaphore, Task, CancelledError, create_task
from urllib.parse import SplitResult, urlsplit, parse_qsl, urlencode
from .logs import status_loop, log_response
from dataclasses import dataclass, field
from logging import getLogger, Logger
from .cache import RedisCacheTransport
from collections import Counter
from contextlib import suppress
from typing import ClassVar
from stamina import retry
from os import getenv


@dataclass(slots = True, kw_only = True)
class CacheAPI:
    api_key: str | None = None
    namespace: str = "api"  # For redis cache key
    max_concurrency: int = 10
    max_ttl: int = 86_400
    sleep_timeout: int = 300
    client: AsyncClient = field(init=False)
    stats: ClassVar[Counter] = Counter()
    _status_task: ClassVar[Task | None] = None
    _instances: ClassVar[list['CacheAPI']] = []
    _semaphore: BoundedSemaphore = field(init=False)
    _log: Logger = getLogger('api')

    def __post_init__(self):
        # Auto-register every instance for cleanup
        CacheAPI._instances.append(self)

        # Custom transport with redis cache
        transport = RedisCacheTransport(
            redis_url=getenv("REDIS_URL", "redis://localhost:6379/0"),
            namespace=self.namespace,
            max_ttl=self.max_ttl,
            stats=self.stats,
            http2=True,
            limits=Limits(  # Connection pooling (transport-level)
                max_connections = 20,
                max_keepalive_connections = 10,  # Multiple connections to spread load
                keepalive_expiry = 30.0  # Refresh connections periodically
            )
        )

        # Client: Handles request/response cycle and timeouts
        self.client = AsyncClient(
            transport=transport,
            timeout = Timeout(  # Time limits (client-level)
                connect = 5.0,
                read = 30.0,
                write = 10.0,
                pool = 5.0,
            )
        )
        
        self.client.event_hooks['response'] = [log_response]
        # set default headers
        self.client.headers['content-type'] = "application/json"
        if self.api_key:
            self.client.headers['x-api-key'] = self.api_key

        # Limit asyncio concurrent calls
        self._semaphore = BoundedSemaphore(self.max_concurrency)

        # Start status loop once on first instance created
        if CacheAPI._status_task is None:
            CacheAPI._status_task = create_task(status_loop(self.stats))

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
        
        for instance in list[CacheAPI](cls._instances):
            await instance.close()
        cls._instances.clear()

    @staticmethod
    def normalize_url(url: str, params: dict | None) -> str:
        """
        Normalize URL for consistent caching and requests.
        Handles special schemes like ipfs:// by converting to HTTP gateway URLs.
        Merges URL query params with additional params dict.
        
        Args:
            url: URL to normalize
            params: Additional query parameters to merge
            
        Returns:
            Normalized URL with merged and sorted query parameters
        """
        parsed_url: SplitResult = urlsplit(url, allow_fragments = False)
        scheme: str = parsed_url.scheme.lower()

        # If scheme is ipfs, change it to https and get from ipfs.io
        if scheme == 'ipfs':
            parsed_url = parsed_url._replace(
                scheme='https', 
                netloc='ipfs.io',
                path=f"ipfs/{parsed_url.netloc}{parsed_url.path}",
            )

        # Merge existing query params with additional params
        existing_params: dict = dict(parse_qsl(parsed_url.query))
        if params:
            existing_params |= params
        # Sort the queries so each query will be in the same format for caching
        normalized_query: str = urlencode(sorted(existing_params.items()))
        parsed_url = parsed_url._replace(query=normalized_query)

        return parsed_url.geturl()

    @retry(on = (HTTPStatusError, RemoteProtocolError, ReadTimeout), wait_initial = 1, wait_max = sleep_timeout)
    async def get(self, url: str, params: dict | None = None, headers: dict | None = None) -> dict[str]:
        """
        Get response with caching handled transparently by transport.
        
        Normalizes URLs to handle special schemes (ipfs://, etc.) and
        ensure consistent caching. Merges params into URL for normalization.

        Requests are retried for error responses
        
        Args:
            url: URL to fetch (can be http/https/ipfs/etc.)
            params(Optional): Query parameters (merged and sorted for cache consistency)
            headers(Optional): Request headers
        Returns:
            JSON response body
        """
        async with self._semaphore:
            normalized_url: str = self.normalize_url(url, params)
            response: Response = await self.client.get(url=normalized_url,headers=headers)
            response.raise_for_status()
            return response.json()
        
