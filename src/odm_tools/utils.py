import logging
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

import structlog


def setup_logging(
    log_level: str,
    format_type: Literal["json", "stdout"] = "stdout",
    suppress: Iterable[str] = ("urllib3", "requests"),
) -> None:
    """
    Set up production-ready console logging for containerized applications.

    Args:
        level: Log level (debug, info, warning, error, critical)
        format_type: Either 'json' for structured logging or 'standard' for human-readable

    Returns:
        Configured root logger
    """
    logging.basicConfig(level=log_level.upper())
    # suppress noisy third-party loggers
    for logger in suppress:
        logging.getLogger(logger).setLevel(logging.WARNING)
    # configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]
    if sys.stderr.isatty():
        # pretty printing when we run in a terminal session.
        processors.extend([structlog.dev.ConsoleRenderer()])
    else:
        # print JSON when we run, e.g., in a Docker container.
        processors.extend(
            [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ]
        )
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def validate_request_structure(request_path: Path) -> None:
    """
    Validate that request directory has proper structure.
    The expected structure is as follows:
    data/
    ├── request_id_1/
    │   ├── rgb - folder containing images to orthorectify (rgb)
    │   ├── thermal - folder containing images to orthorectify (swir)
    │   └── request.json - file containing request metadata
    ├── request_id_2/
    │   ├── rgb/
    │   ├── ...
    │   └── request.json
    └── ...
    This function checks the contents of one of these request folders.

    Args:
        request_path: path to a mission directory.

    Raises:
        ValueError: when the structure is not the expected one
    """
    if not request_path.is_dir():
        raise ValueError(f"Path '{request_path}' is not a valid directory")
    # Check for request.json
    request_files = list(request_path.glob("*.json"))
    if not request_files:
        raise ValueError(f"No json files found in {request_path}")
    request_file = request_files[0]
    if not request_file.exists():
        raise ValueError(f"request.json file missing in '{request_path}'")
    # Check for at least one datatype directory
    datatype_dirs = [d for d in request_path.iterdir() if d.is_dir()]
    if not datatype_dirs:
        raise ValueError("At least rgb data type subdirectory is required.")
    if len(datatype_dirs) == 1:
        if datatype_dirs[0].name == "thermal":
            raise ValueError("Cannot orthorectify just thermal")


def find_images(root_path: Path, suffix: str | None = None, extension: str = ".jpg") -> list[Path]:
    """Find all image files in a directory."""
    search_pattern = f"*{suffix or ''}{extension}"
    return sorted(root_path.glob(search_pattern))
