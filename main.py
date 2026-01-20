from stamina.instrumentation import set_on_retry_hooks
from pipeline.collectors.nft import get_all_nfts
from argparse import ArgumentParser, Namespace
from pipeline.utils.logs import set_logger
from pipeline.utils.api import CacheAPI
from logging import getLogger, Logger
from dotenv import load_dotenv
from time import perf_counter
from asyncio import run
import pyarrow as pa
from pipeline.transform.normalize import normalize_nfts
from pipeline.load.store import to_parquet_file

async def main():
    # load environment variables
    load_dotenv()

    # Optional console arguments
    parser: ArgumentParser = ArgumentParser()
    # --logfile flag to output logs to a project.log file
    parser.add_argument(
        "-l", "--logfile",
        action="store_true",
        help="Enable file logging (project.log)",
    )
    args: Namespace = parser.parse_args()

    # logs
    set_logger(args.logfile)
    log: Logger = getLogger()

    # CacheAPI retry logs event hook
    set_on_retry_hooks([CacheAPI.log_retry_sleep])

    # NFT collection name to extract nfts from
    nft_slug: str = 'dxterminal'

    # COLLECT phase

    # track program runtime
    start_time: float = perf_counter()
    try:
        all_nfts: list[dict[str]] = await get_all_nfts(nft_slug)
    finally:
        # Cleanup and close api connections and async functions
        await CacheAPI.cleanup_all()
    end_time: float = perf_counter()
    elapsed_time_min: float = (end_time - start_time) / 60
    log.info(f"The collect operation took {elapsed_time_min:.2f} minutes.")

    # TRANSFORM phase

    # PyArrow table schema for nfts
    schema: pa.Schema = pa.schema([
        pa.field('identifier', pa.string()),
        pa.field('collection', pa.string()),
        pa.field('contract', pa.string()),
        pa.field('token_standard', pa.string()),
        pa.field('name', pa.string()),
        pa.field('metadata_url', pa.string()),
        pa.field('traits', pa.list_(pa.struct([
            pa.field('trait_type', pa.string()),
            pa.field('value', pa.string()),
        ]))),
    ])

    nft_tables: dict[str, pa.Table] = normalize_nfts(nft_slug, all_nfts, schema)

    # LOAD phase

    data_directory: str = 'output'
    to_parquet_file(nft_tables.get('nfts'), f'{nft_slug}_nfts', data_directory)
    to_parquet_file(nft_tables.get('traits'), f'{nft_slug}_traits', data_directory)

if __name__ == "__main__":
    run(main())
