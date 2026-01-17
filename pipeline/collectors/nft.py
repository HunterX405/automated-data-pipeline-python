from .api_clients import get_opensea_client, get_metadata_client
from logging import getLogger, Logger
from ..utils.api import CacheAPI
from asyncio import TaskGroup
from json import dump

log: Logger = getLogger('NFT')

async def get_nft_collection_metadata(opensea_client: CacheAPI, nft_slug: str) -> dict:
    url = f'https://api.opensea.io/api/v2/collections/{nft_slug}'
    response = await opensea_client.get(url)
    return response

async def get_nft_traits(metadata_client: CacheAPI, nft_metadata: dict) -> dict:
    metadata_url = nft_metadata.get("metadata_url")
    if not metadata_url:
        log.warning(f"No metadata url found for {nft_metadata.get("collection")}")
        return nft_metadata
    response = await metadata_client.get(metadata_url)
    response_json: dict = response
    nft_metadata['traits'] = response_json.get("attributes")
    return nft_metadata

async def get_all_nfts(opensea_client: CacheAPI, metadata_client: CacheAPI, contract_address: str, chain: str) -> list[dict]:
    all_nfts: list = []
    url: str = f"https://api.opensea.io/api/v2/chain/{chain}/contract/{contract_address}/nfts"
    params: dict = {'limit': 200}
    next_cursor: str | None = None

    while True:
        if next_cursor:
            params['next'] = next_cursor

        response = await opensea_client.get(url,params=params)
        response_json: dict = response

        next_cursor = response_json.get("next")
        nfts = response_json.get("nfts", [])

        async with TaskGroup() as tg:
            tasks = [tg.create_task(get_nft_traits(metadata_client, nft)) for nft in nfts]

        all_nfts.extend(task.result() for task in tasks)

        if not next_cursor:
            break

    return all_nfts

async def get_nfts(nft_slug: str):
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
        nfts = await get_all_nfts(opensea_client, metadata_client, contract_address, chain)

        with open(f'{nft_slug}.json', "w") as file_handle:
            dump(nfts, file_handle, indent=4)

        log.info(f"Successfully wrote data to {f'{nft_slug}.json'}")
    
    except IOError as e:
        log.error(f"Error writing to file: {e}")