from pipeline.utils.logs import set_logger, log_retry_sleep
from pipeline.transform.normalize import normalize_nfts
from stamina.instrumentation import set_on_retry_hooks
from pipeline.collectors.nft import get_all_nfts, Nft
from argparse import ArgumentParser, Namespace
from pipeline.utils.api import CacheAPI
from logging import getLogger, Logger
from dotenv import load_dotenv
from time import perf_counter
from pathlib import Path
from asyncio import run


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
    log: Logger = getLogger('main')

    ## Track collect phase runtime
    start_time: float = perf_counter()

    # COLLECT PHASE

    ## NFT collection name to extract nfts from
    nft_slug: str = 'baseprimates'

    # Raw data directory
    raw_data_dir: Path = Path('data') / 'raw'
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    filepath: Path = raw_data_dir / f"{nft_slug}.parquet"
    all_nfts: list[Nft] | None = None
    if not filepath.is_file():
        ## CacheAPI retry logs event hook
        set_on_retry_hooks([log_retry_sleep])

        try:
            all_nfts: list[Nft] = await get_all_nfts(nft_slug)
        finally:
            # Cleanup and close api connections and async functions
            await CacheAPI.cleanup_all()

        print(all_nfts[:1])

    # TRANSFORM phase
    normalize_nfts(nft_slug, all_nfts, data_path=filepath)

    # LOAD phase

    # data_directory: str = 'data'
    # to_parquet_file(nft_tables.get('nfts'), f'{nft_slug}_nfts', data_directory)
    # to_parquet_file(nft_tables.get('traits'), f'{nft_slug}_traits', data_directory)

    end_time: float = perf_counter()
    elapsed: float = end_time - start_time
    log.info(
        f"The collect operation took "
        f"{elapsed / 60 if elapsed > 60 else elapsed:.2f} "
        f"{'minutes' if elapsed > 60 else 'seconds'}"
    )

if __name__ == "__main__":
    run(main())
