"""Workers: inherit from BaseWorker and implement handle_signal(command)."""

from src.workers.base import BaseWorker
from src.workers.scanner import ScannerWorker

__all__ = ["BaseWorker", "ScannerWorker"]
