from .api_clients import get_opensea_client, get_metadata_client
from asyncio import Task, Queue, create_task
from logging import getLogger, Logger
from ..utils.api import CacheAPI
from collections import Counter
from pydantic import BaseModel, Field, ConfigDict, ValidationError
from urllib.parse import SplitResult, urlsplit, parse_qsl, urlencode


# Using Pydantic Basemodel Class for Data Validation
class Trait(BaseModel):
    model_config: ConfigDict = ConfigDict(
        extra='ignore',
        str_strip_whitespace=True
    )

    trait_type: str
    value: str

class Nft(BaseModel):
    model_config: ConfigDict = ConfigDict(
        extra='ignore',
        str_strip_whitespace=True
    )

    identifier: int
    collection: str
    contract: str
    token_standard: str
    name: str | None = None
    metadata_url: str | None = None
    traits: list[Trait] = Field(default_factory=list)

class OpenseaGetNfts(BaseModel):
    nfts: list[Nft] = Field(default_factory=list)
    next: str | None = None

class Contract(BaseModel):
    address: str
    chain: str

class Collection(BaseModel):
    model_config: ConfigDict = ConfigDict(
        extra='ignore',
        str_strip_whitespace=True
    )

    collection: str
    name: str
    contracts: list[Contract]
    total_supply: int

    @property
    def first_contract(self) -> Contract | None:
        return self.contracts[0] if self.contracts else None

log: Logger = getLogger('nft')

async def get_nft_collection_metadata(opensea_client: CacheAPI, nft_slug: str) -> Collection:
    """Fetch NFT collection metadata from opensea API"""
    url: str = f'https://api.opensea.io/api/v2/collections/{nft_slug}'
    response: dict[str] = await opensea_client.get(url)
    try:
        collection = Collection.model_validate(response)
        return collection
    except ValidationError:
        log.error(f"Failed to validate {nft_slug} collection metadata.")
        return None

async def get_nft_traits(metadata_client: CacheAPI, nft: Nft) -> list[Trait]:
    """Fetch NFTs traits from nft metadata url"""
    if not nft.metadata_url:
        log.warning(f"No metadata url found for {nft.collection}")
        return []
    url: str = normalize_url(nft.metadata_url)
    response: dict[str] = await metadata_client.get(url)
    try:
        traits: list[Trait] = [Trait.model_validate(trait) for trait in response.get("attributes", [])]
        return traits
    except ValidationError as e:
        log.error(f"Failed to validate {nft.name} traits | {e}")
        return []


def normalize_url(url: str) -> str:
    parsed_url: SplitResult = urlsplit(url, allow_fragments = False)

    # If scheme is ipfs, change it to https and get from ipfs.io
    if parsed_url.scheme == 'ipfs':
        parsed_url = parsed_url._replace(
            scheme='https', 
            netloc='ipfs.io',
            path=f"ipfs/{parsed_url.netloc}{parsed_url.path}",
        )

    # Sort the queries so each query will be in the same format for caching
    normalized_query: str = urlencode(sorted(parse_qsl(parsed_url.query)))
    parsed_url = parsed_url._replace(query=normalized_query)

    return parsed_url.geturl()

async def get_opensea_nfts(opensea_client: CacheAPI, chain: str, contract_address: str, queue: Queue) -> None:
    """Fetch NFTs from OpenSea and add to queue"""
    url: str = normalize_url(f"https://api.opensea.io/api/v2/chain/{chain}/contract/{contract_address}/nfts")

    params: dict[str, int | str] = {'limit': 200}
    next_cursor: str | None = None

    try:
        while True:
            if next_cursor:
                params['next'] = next_cursor

            response: dict[str] = await opensea_client.get(url,params=params)
            opensea = OpenseaGetNfts.model_validate(response)

            next_cursor = opensea.next
            nfts: list[Nft] = opensea.nfts

            for nft in nfts:
                await queue.put(Nft.model_validate(nft))
                
            if not next_cursor:
                break
    except ValidationError as e:
        log.error(f"Failed to validate NFT | {e}")

    log.debug(f"Finished fetching all nfts from {url}")

async def get_all_nfts(nft_slug: str) -> list[dict[str]]:
    """Collect NFTs with traits metadata using producer-consumer Queue pattern"""
    opensea_client: CacheAPI = get_opensea_client()
    collection_metadata: Collection = await get_nft_collection_metadata(opensea_client, nft_slug)

    # Get first contract instance from collection
    contract: Contract | None = collection_metadata.first_contract

    if not contract:
        log.error(f"No contract data found for {nft_slug}")
        return

    contract_address: str = contract.address
    chain: str = contract.chain
    metadata_client: CacheAPI = get_metadata_client()
    all_nfts: list[Nft] = []
    queue: Queue = Queue[Nft](maxsize=500)

    stats: Counter = CacheAPI.stats
    async def get_nft(metadata_client: CacheAPI, worker_id: int) -> None:
        """Get and process NFTs from queue"""
        while True:
            nft: Nft | None = await queue.get()
            try:
                if nft is None:
                    log.debug(f'Worker {worker_id} done fetching nfts.')
                    return
                nft.traits = await get_nft_traits(metadata_client, nft)
                all_nfts.append(nft)
            except Exception as e:
                log.error(f"Worker {worker_id} encountered an error: {e}")
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