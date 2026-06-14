"""Base provider with shared utilities."""

from typing import Any, Dict
from ..base import LLMConfig


class BaseProvider:
    """Shared utilities for all providers."""

    def __init__(self, config: LLMConfig):
        """Initialize with config."""
        self.config = config

    def validate_config(self) -> bool:
        """Validate provider configuration."""
        if not self.config.model_name:
            return False
        if self.config.temperature < 0 or self.config.temperature > 2:
            return False
        if self.config.max_tokens < 1:
            return False
        return True
