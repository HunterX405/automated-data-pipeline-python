from logging import getLogger, StreamHandler, Formatter, DEBUG, WARNING, ERROR, INFO
from stamina.instrumentation import RetryDetails, set_on_retry_hooks
from .api import CacheAPI
from logging.handlers import RotatingFileHandler
from argparse import ArgumentParser, Namespace
from httpx import HTTPStatusError, Response
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
    root_logger = getLogger()
    root_logger.setLevel(DEBUG)

    # Prevent duplicate handlers
    if root_logger.handlers:
        return

    # supress httpx INFO and DEBUG logs
    getLogger("httpx").setLevel(WARNING)
    getLogger("httpcore").setLevel(WARNING)
    getLogger("hpack").setLevel(ERROR)
    getLogger("h2").setLevel(ERROR)

    # Stamina @retry logger
    def log_retry_sleep(rd: RetryDetails):
        stats = CacheAPI.stats
        stats["retries"] += 1
        stats["errors"] += 1
        error = rd.caused_by
        if error.__class__ == HTTPStatusError:
            response: Response = error.response
            root_logger.debug(
                f"<{response.status_code}> {response.reason_phrase} | "
                f"Retry #{rd.retry_num} | Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {response.url.host}"
            )
        else:
            root_logger.debug(
                f"{error} | Retry #{rd.retry_num} | "
                f"Sleep {rd.wait_for:.2f}s | "
                f"Elapsed: {rd.waited_so_far:.2f}s | {error.request.url.host}"
            )
    
    set_on_retry_hooks([log_retry_sleep])

    formatter = Formatter(
        '%(asctime)s %(levelname)s: %(name)s.%(funcName)s() - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = StatusAwareStreamHandler()
    console_handler.setLevel(INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_to_file:
        # Create the logs directory if there isn't one
        Path("./logs").mkdir(parents=True, exist_ok=True)

        # Always start the project.log file fresh every run
        Path("./logs/project.log").unlink(missing_ok=True)

        file_handler = RotatingFileHandler(
            "./logs/project.log",
            maxBytes=5_000_000,
            backupCount=2,
        )
        file_handler.setLevel(DEBUG)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        "--logfile",
        action="store_true",
        help="Enable file logging (project.log)",
    )
    return parser.parse_args()

def init_logs():
    args: Namespace = parse_args()
    set_logger(log_to_file = args.logfile)
