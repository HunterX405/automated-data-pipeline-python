from stamina.instrumentation import set_on_retry_hooks
from argparse import ArgumentParser, Namespace
from pipeline.collectors.nft import get_all_nfts
from pipeline.utils.logs import set_logger
from pipeline.utils.api import CacheAPI
from logging import getLogger, Logger
from dotenv import load_dotenv
from time import perf_counter
from asyncio import run


async def main():
    # load environment variables
    load_dotenv()

    # Optional console arguments
    parser = ArgumentParser()
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

    # track program runtime
    start_time: float = perf_counter()
    try:
        await get_all_nfts(nft_slug)
    finally:
        # Cleanup and close api connections and async functions
        await CacheAPI.cleanup_all()
    end_time: float = perf_counter()

    elapsed_time_min: float = (end_time - start_time) / 60
    log.info(f"The operation took {elapsed_time_min:.2f} minutes.")


if __name__ == "__main__":
    run(main())
