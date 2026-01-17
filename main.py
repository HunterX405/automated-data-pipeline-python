from pipeline.utils.logs import init_logs
from pipeline.collectors.nft import get_nfts
from asyncio import run
from time import perf_counter
from logging import getLogger, Logger
from dotenv import load_dotenv


async def main():
    # load environment variables
    load_dotenv()

    # logs
    init_logs()
    log: Logger = getLogger('main')

    # NFT collection name to extract nfts from
    nft_slug = 'dxterminal'

    # track program runtime
    start_time: float = perf_counter()
    await get_nfts(nft_slug)
    end_time: float = perf_counter()

    elapsed_time_min: float = (end_time - start_time) / 60
    log.info(f"The operation took {elapsed_time_min:.2f} minutes.")


if __name__ == "__main__":
    run(main())
