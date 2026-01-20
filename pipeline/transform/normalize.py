import pyarrow as pa
import pyarrow.parquet as pq
from logging import getLogger, Logger

log: Logger = getLogger('transform')

def normalize_nfts(nft_slug: str, all_nfts: list[dict[str]], schema: pa.Schema | None = None) -> dict[str, pa.Table]:
    """Normalize NFTs data into parquet files"""
    all_nfts_table: pa.Table = pa.Table.from_pylist(all_nfts, schema=schema)
    
    # Convert the identifier from string to int
    cast_identifier = all_nfts_table.column('identifier').cast(pa.int32())
    all_nfts_table = all_nfts_table.set_column(
        all_nfts_table.schema.get_field_index('identifier'), 
        'identifier', 
        cast_identifier
    )

    nfts_table: pa.Table = all_nfts_table.select([
        'identifier', 
        'collection', 
        'contract', 
        'token_standard', 
        'name', 
        'metadata_url'
    ])
    nft_traits_table: pa.Table = all_nfts_table.select([
        'identifier', 
        'traits'
    ]).flatten()

    log.info(f"Successfully normalized nft data to {nft_slug}_nfts and {nft_slug}_traits")
    
    return {'nfts': nfts_table, 'traits': nft_traits_table}
