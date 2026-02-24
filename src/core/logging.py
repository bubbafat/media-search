"""Structured logging setup."""

import logging
import sys

from src.core.config import get_config


def setup_logging() -> None:
    """Configure root logger (level, format, handlers)."""
    cfg = get_config()
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
