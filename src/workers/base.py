"""Base worker: all workers must inherit and implement handle_signal(command)."""

from abc import ABC, abstractmethod
from typing import Any


class BaseWorker(ABC):
    """Base for all workers. Subclasses implement handle_signal(command)."""

    @abstractmethod
    def handle_signal(self, command: Any) -> None:
        """Process one command/signal. Override in subclasses."""
        ...
