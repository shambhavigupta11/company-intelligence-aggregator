"""Base classes shared across scrapers."""

from abc import ABC, abstractmethod
from typing import Any


class BaseScraper(ABC):
    """Abstract base for all data acquisition sources."""

    source_name: str = "base"

    @abstractmethod
    def fetch(self, **kwargs: Any) -> list[dict]:
        """Fetch raw records from the source. Returns list of dicts."""
