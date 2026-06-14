"""Base classes for LLM provider abstraction."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class LLMProviderType(Enum):
    """Supported LLM provider types."""
    OLLAMA = "ollama"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    LOCAL = "local"


@dataclass
class LLMConfig:
    """Unified configuration for all LLM providers."""
    provider_type: LLMProviderType
    model_name: str
    temperature: float = 0.7
    max_tokens: int = 1000
    top_p: float = 1.0

    # Optional: provider-specific fields
    base_url: Optional[str] = None          # Ollama endpoint
    api_key: Optional[str] = None           # OpenAI, Anthropic, Google API keys
    organization: Optional[str] = None      # OpenAI organization
    timeout: int = 30

    # Additional settings
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Ensure metadata is initialized."""
        if not isinstance(self.metadata, dict):
            self.metadata = {}


class LLMProvider(ABC):
    """
    Abstract base class for all LLM providers.

    Subclasses must implement health_check() and generate().
    Optional methods can be overridden for provider-specific features.
    """

    def __init__(self, config: LLMConfig):
        """Initialize provider with configuration."""
        self.config = config
        self.logger = logger

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if provider is available and healthy.

        Returns:
            True if healthy, False otherwise
        """
        pass

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """
        Generate text from prompt.

        Args:
            prompt: Input prompt text

        Returns:
            Generated text response
        """
        pass

    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text (provider-specific).

        Default: rough estimate. Override for accuracy.

        Args:
            text: Text to count

        Returns:
            Token count estimate
        """
        # Rough estimate: 1 token ≈ 4 characters
        return len(text) // 4

    def get_cost(self, input_tokens: int, output_tokens: int) -> float:
        """
        Calculate cost for a request.

        Default: no cost. Override for API-based providers.

        Args:
            input_tokens: Input token count
            output_tokens: Output token count

        Returns:
            Cost in USD (or 0.0 for free providers)
        """
        return 0.0

    def get_model_info(self) -> Dict[str, Any]:
        """
        Get model information (context size, features, etc).

        Default: minimal info. Override for details.

        Returns:
            Dictionary with model metadata
        """
        return {
            'name': self.config.model_name,
            'provider': self.config.provider_type.value,
            'context_size': 'unknown',
            'supports_vision': False,
            'supports_functions': False,
        }

    def validate_config(self) -> bool:
        """Validate configuration is correct."""
        if not self.config.model_name:
            self.logger.error("Model name is required")
            return False
        if self.config.temperature < 0 or self.config.temperature > 2:
            self.logger.error("Temperature must be between 0 and 2")
            return False
        if self.config.max_tokens < 1:
            self.logger.error("Max tokens must be >= 1")
            return False
        return True
