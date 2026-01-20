from .api_clients import get_opensea_client, get_metadata_client
from asyncio import Task, Queue, create_task
from logging import getLogger, Logger
from ..utils.api import CacheAPI
from collections import Counter

log: Logger = getLogger('nft')

async def get_nft_collection_metadata(opensea_client: CacheAPI, nft_slug: str) -> dict[str]:
    """Fetch NFT collection metadata from opensea API"""
    url: str = f'https://api.opensea.io/api/v2/collections/{nft_slug}'
    response: dict[str] = await opensea_client.get(url)
    return response

async def get_nft_traits(metadata_client: CacheAPI, nft_metadata: dict) -> dict[str]:
    """Fetch NFTs traits from nft metadata url"""
    metadata_url: str = nft_metadata.get("metadata_url")
    if not metadata_url:
        log.warning(f"No metadata url found for {nft_metadata.get('collection')}")
        return nft_metadata
    response: dict[str] = await metadata_client.get(metadata_url)
    nft_metadata['traits'] = response.get("attributes")
    return nft_metadata

async def get_opensea_nfts(opensea_client: CacheAPI, chain: str, contract_address: str, queue: Queue) -> None:
    """Fetch NFTs from OpenSea and add to queue"""
    url: str = f"https://api.opensea.io/api/v2/chain/{chain}/contract/{contract_address}/nfts"
    params: dict[str, int | str] = {'limit': 200}
    next_cursor: str | None = None

    while True:
        if next_cursor:
            params['next'] = next_cursor

        response: dict[str] = await opensea_client.get(url,params=params)

        next_cursor = response.get("next", None)
        nfts: list[dict[str]] = response.get("nfts", [])
        for nft in nfts:
            await queue.put(nft)

        if not next_cursor:
            break

    log.debug(f"Finished fetching all nfts from {url}")

async def get_all_nfts(nft_slug: str) -> list[dict[str]]:
    """Collect NFTs with traits metadata using producer-consumer Queue pattern"""
    opensea_client: CacheAPI = get_opensea_client()
    collection_metadata: dict[str] = await get_nft_collection_metadata(opensea_client, nft_slug)
    
    contracts: list[dict[str]] = collection_metadata.get("contracts", [])
    if not contracts:
        log.error(f"No contract found for {nft_slug}")
        return
    
    contract: dict[str] = contracts[0]
    contract_address: str = contract["address"]
    chain: str = contract["chain"]
    metadata_client: CacheAPI = get_metadata_client()

    all_nfts: list[dict[str]] = []
    queue: Queue = Queue(maxsize=500)

    stats: Counter = CacheAPI.stats
    async def get_nft(metadata_client: CacheAPI, worker_id: int) -> None:
        """Get and process NFTs from queue"""
        while True:
            nft: dict[str] | None = await queue.get()
            try:
                if nft is None:
                    log.debug(f'Worker {worker_id} done fetching nfts.')
                    return
                nft_with_traits: dict[str] = await get_nft_traits(metadata_client, nft)
                all_nfts.append(nft_with_traits)
            except Exception as e:
                log.error(f"Worker {worker_id} encountered an error: {e}\nSkipping {nft}")
            finally:
                queue.task_done()
                stats['queue'] = queue.qsize()

    # No of concurrent tasks for asyncio.Queue
    worker_count: int = 15
    opensea_task: Task = create_task(get_opensea_nfts(opensea_client, chain, contract_address, queue), name="Get opensea nfts")
    for worker_id in range(1, worker_count + 1):
        create_task(get_nft(metadata_client, worker_id), name=f"Worker {worker_id}")

    # Wait to get all opensea nfts
    await opensea_task

    # Put None values to the queue to stop the workers
    for _ in range(1, worker_count + 1):
        await queue.put(None)

    # Wait for all workers to finish
    await queue.join()

    log.info(f"Successfully fetched all nfts from {nft_slug} | {len(all_nfts):,} nfts")
    return all_nfts