"""Factory for creating LLM provider instances."""

from typing import Union
import logging
from .base import LLMConfig, LLMProviderType, LLMProvider
from .providers import OllamaProvider, OpenAIProvider, AnthropicProvider, GoogleProvider

logger = logging.getLogger(__name__)


def get_llm_provider(config: LLMConfig) -> LLMProvider:
    """
    Factory function to create LLM provider instance.

    Args:
        config: LLMConfig with provider type and settings

    Returns:
        LLMProvider subclass instance

    Raises:
        ValueError: If provider type is unsupported
    """
    if config.provider_type == LLMProviderType.OLLAMA:
        return OllamaProvider(config)
    elif config.provider_type == LLMProviderType.OPENAI:
        return OpenAIProvider(config)
    elif config.provider_type == LLMProviderType.ANTHROPIC:
        return AnthropicProvider(config)
    elif config.provider_type == LLMProviderType.GOOGLE:
        return GoogleProvider(config)
    else:
        raise ValueError(f"Unsupported provider: {config.provider_type}")
