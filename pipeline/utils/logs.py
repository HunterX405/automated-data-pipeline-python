from logging import getLogger, Logger, StreamHandler, Formatter, DEBUG, WARNING, ERROR, INFO
from logging.handlers import RotatingFileHandler
from stamina.instrumentation import RetryDetails
from httpx import HTTPStatusError, Response
from asyncio import sleep, CancelledError
from collections import Counter
from pathlib import Path
from sys import stdout


class StatusAwareStreamHandler(StreamHandler):
    def emit(self, record):
        # Move below the status line
        stdout.write("\n")
        stdout.flush()
        super().emit(record)

def set_logger(log_to_file: bool = False):
    # Create log handler
    root_logger: Logger = getLogger()
    root_logger.setLevel(DEBUG)

    # Prevent duplicate handlers
    if root_logger.handlers:
        return

    # supress httpx INFO and DEBUG logs
    getLogger("httpx").setLevel(WARNING)
    getLogger("httpcore").setLevel(WARNING)
    getLogger("hpack").setLevel(ERROR)
    getLogger("h2").setLevel(ERROR)

    # All Custom loggers
    getLogger('main').setLevel(DEBUG)
    getLogger('status').setLevel(DEBUG)
    getLogger('retry').setLevel(DEBUG)
    getLogger('api').setLevel(WARNING)
    getLogger('nft').setLevel(DEBUG)

    formatter: Formatter = Formatter(
        '%(asctime)s %(levelname)s: %(name)s.%(funcName)s() - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler: StatusAwareStreamHandler = StatusAwareStreamHandler()
    console_handler.setLevel(INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_to_file:
        # Create the logs directory if there isn't one
        Path("./logs").mkdir(parents=True, exist_ok=True)

        # Always start the project.log file fresh every run
        Path("./logs/project.log").unlink(missing_ok=True)

        file_handler: RotatingFileHandler = RotatingFileHandler(
            "./logs/project.log",
            maxBytes=5_000_000,
            backupCount=2,
        )
        file_handler.setLevel(DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

# CacheAPI console status display
async def status_loop(stats: Counter, interval: float = 1.0) -> None:
    """ Console display for API stats """
    log: Logger = getLogger('status')
    is_interactive: bool = stdout.isatty()
    log_interval: float = interval if is_interactive else 30.0  # Log every 30s in Docker
    
    try:
        while True:
            line = (
                f"API Requests: {stats['responses']} | "
                f"Cached: {stats['cache_hits']} | "
                f"Network: {stats['network_requests']} | "
                f"Errors: {stats['errors']} | "
                f"Retries: {stats['retries']} | "
                f"Queue: {stats['queue']} | "
                f"Elapsed: {(stats['elapsed'] / 60):.2f} min "
            )
            
            if is_interactive:
                stdout.write("\r" + str(stats))
                stdout.flush()
            else:
                print(stats)

            log.debug(stats)

            await sleep(log_interval)
            stats['elapsed'] += log_interval
    except CancelledError:
        return

# Stamina @retry event hook
def log_retry_sleep(rd: RetryDetails) -> None:
        from .api import CacheAPI
        stats = CacheAPI.stats
        log: Logger = getLogger('retry')
        stats["retries"] += 1
        stats["errors"] += 1
        error: Exception = rd.caused_by
        if isinstance(error, HTTPStatusError):
            response: Response = error.response
            log.debug(
                f"<{response.status_code}> {response.reason_phrase} | "
                f"Retry #{rd.retry_num} | Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {response.url.host}"
            )
        else:
            url: str = error.request.url.host
            log.debug(
                f"{error} | Retry #{rd.retry_num} | "
                f"Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {url}"
            )

# httpx response event hook
async def log_response(response: Response) -> None:
    """ Log event hook for responses from httpx """
    log: Logger = getLogger('api')
    # response.is_error is already being logged by log_retry_sleep
    if response.is_success:
        log.debug(
            f"'{response.http_version}' {response.request.method} <{response.status_code}> | "
            f"Cache-Control: {response.headers.get('cache-control', 'None')} | {response.url}"
        )
