import pyarrow as pa
import pandas as pd
from logging import getLogger, Logger
from ..collectors.nft import Nft
from pathlib import Path

log: Logger = getLogger('nft')

def normalize_nfts(
    nft_slug: str, 
    all_nfts: list[Nft] | None = None, 
    data_path: Path | None = None
    ) -> dict[str, pa.Table]:
    """Normalize NFTs data into parquet files"""
    
    if not data_path.is_file():
        # Convert pydantic BaseModel class to dict
        rows: list[dict[str]]= [m.model_dump(mode="python", by_alias=True) for m in all_nfts]

        # Convert to pandas dataframe
        nfts_data: pd.DataFrame = pd.DataFrame(rows)

        # Store raw data to parquet file
        nfts_data.to_parquet(data_path)
    else:
        nfts_data: pd.DataFrame = pd.read_parquet(data_path)
    
    # Explode list of traits
    df_traits = nfts_data[["identifier", "traits"]].explode("traits", ignore_index=True)
    traits_expanded = pd.json_normalize(df_traits["traits"])
    df_traits = pd.concat(
        [df_traits[["identifier"]].reset_index(drop=True), traits_expanded.reset_index(drop=True)],
        axis=1,
    )

    nfts_data = nfts_data.drop(columns='traits')
    traits = df_traits.groupby("trait_type").count()
    print(traits)
    
    all_traits: pd.DataFrame = df_traits.groupby("trait_type")['value'].value_counts().reset_index()

    log.info(f"Successfully normalized {nft_slug} nfts data")
    print("NFT")
    print(f"Columns: {list[str](nfts_data)}")
    print(f"Items: {len(nfts_data):,}")
    print("NFT traits")
    print(f"Columns: {list[str](df_traits)}")
    print(f"NFT with traits: {len(df_traits):,}")

    processed_data_dir = data_path.parent.parent / 'processed'
    processed_data_dir.mkdir(parents=True, exist_ok=True)
    all_traits.to_parquet(processed_data_dir / f"{nft_slug}_traits.parquet")
    
    # return {'nfts': nfts_table}
