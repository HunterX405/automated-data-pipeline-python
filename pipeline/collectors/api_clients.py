from os import getenv
from ..utils.api import CacheAPI
from collections import Counter

# Console api stats
_stats = Counter()

def get_stats():
    return _stats

def get_opensea_client():
    OPENSEA_API_KEY = getenv("OPENSEA_API_KEY")
    return CacheAPI(
        api_key = OPENSEA_API_KEY, 
        max_concurrency = 2, 
        stats = _stats,
        namespace = "opensea"
    )

def get_metadata_client():
    return CacheAPI(
    max_concurrency = 10, 
    stats = _stats,
    namespace = "metadata"
)