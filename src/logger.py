import logging
from pathlib import Path
from typing import Optional, Union
from config import Config


def setup_logging(
    log_file: Optional[Union[str, Path]] = None,
    level: int = logging.INFO
) -> None:
    """
    Sets up centralized logging configuration.

    Args:
        log_file (str | Path | None): Path to log file. Defaults to Config.LOG_FILE.
        level (int): Logging level. Defaults to logging.INFO.
    """
    log_file = log_file or Config.LOG_FILE
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return  # Prevent re-configuring if already set up

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ],
        force=True  # optional: allows reset in dev if needed
    )
