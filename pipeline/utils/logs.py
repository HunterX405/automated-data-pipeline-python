from logging import getLogger, Logger, StreamHandler, Formatter, DEBUG, WARNING, ERROR, INFO
from logging.handlers import RotatingFileHandler
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
