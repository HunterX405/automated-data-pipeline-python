from .api_clients import get_opensea_client, get_metadata_client
from asyncio import Queue, create_task
from logging import getLogger, Logger
from ..utils.api import CacheAPI
from json import dump

log: Logger = getLogger()

async def get_nft_collection_metadata(opensea_client: CacheAPI, nft_slug: str) -> dict:
    """Fetch NFT collection metadata from opensea API"""
    url = f'https://api.opensea.io/api/v2/collections/{nft_slug}'
    response = await opensea_client.get(url)
    return response

async def get_nft_traits(metadata_client: CacheAPI, nft_metadata: dict) -> dict:
    """Fetch NFTs traits from nft metadata url"""
    metadata_url = nft_metadata.get("metadata_url")
    if not metadata_url:
        log.warning(f"No metadata url found for {nft_metadata.get("collection")}")
        return nft_metadata
    response: dict = await metadata_client.get(metadata_url)
    nft_metadata['traits'] = response.get("attributes")
    return nft_metadata

async def get_opensea_nfts(opensea_client: CacheAPI, chain: str, contract_address: str, queue: Queue) -> None:
    """Fetch NFTs from OpenSea and add to queue"""
    url: str = f"https://api.opensea.io/api/v2/chain/{chain}/contract/{contract_address}/nfts"
    params: dict = {'limit': 200}
    next_cursor: str | None = None

    while True:
        if next_cursor:
            params['next'] = next_cursor

        response: dict = await opensea_client.get(url,params=params)

        next_cursor = response.get("next", None)
        nfts = response.get("nfts", [])
        for nft in nfts:
            await queue.put(nft)

        if not next_cursor:
            break

    log.debug(f"Finished fetching all nfts from {url}")

async def get_all_nfts(nft_slug: str) -> None:
    """Collect NFTs with traits metadata using producer-consumer Queue pattern"""
    try:
        opensea_client = get_opensea_client()
        collection_metadata: dict = await get_nft_collection_metadata(opensea_client, nft_slug)
        
        if "contracts" not in collection_metadata:
            log.error(f"No contract found for {nft_slug}")
            return
        
        contract = collection_metadata.get("contracts")[0]
        contract_address = contract["address"]
        chain = contract["chain"]
        metadata_client = get_metadata_client()

        all_nfts: list[dict] = []
        queue: Queue = Queue(maxsize=500)

        stats = CacheAPI.stats
        async def get_nft(metadata_client: CacheAPI, worker_id: int) -> None:
            """Get and process NFTs from queue"""
            while True:
                nft = await queue.get()
                try:
                    if nft is None:
                        log.debug(f'Worker {worker_id} done fetching nfts.')
                        return
                    nft_with_traits = await get_nft_traits(metadata_client, nft)
                    all_nfts.append(nft_with_traits)
                finally:
                    queue.task_done()
                    stats['queue'] = queue.qsize()

        # No of concurrent tasks for asyncio.Queue
        worker_count: int = 15
        opensea_task = create_task(get_opensea_nfts(opensea_client, chain, contract_address, queue))
        for worker_id in range(worker_count):
            create_task(get_nft(metadata_client, worker_id))

        # Wait to get all opensea nfts
        await opensea_task

        # Put None values to the queue to stop the workers
        for _ in range(worker_count):
            await queue.put(None)

        # Wait for all workers to finish
        await queue.join()

        with open(f'{nft_slug}.json', "w") as file_handle:
            dump(all_nfts, file_handle, indent=4)

        log.info(f"Successfully wrote data to {f'{nft_slug}.json'}")
    
    except IOError as e:
        log.error(f"Error writing to file: {e}")