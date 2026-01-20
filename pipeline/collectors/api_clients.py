from ..utils.api import CacheAPI
from os import getenv

def get_opensea_client() -> CacheAPI:
    """Get OpenSea API client"""
    OPENSEA_API_KEY: str | None = getenv("OPENSEA_API_KEY")
    return CacheAPI(
        api_key = OPENSEA_API_KEY, 
        namespace = "opensea",
        max_concurrency = 2
    )

def get_metadata_client() -> CacheAPI:
    """Get nft metadata API client"""
    return CacheAPI(
        namespace = "metadata",
        max_concurrency = 15
    )