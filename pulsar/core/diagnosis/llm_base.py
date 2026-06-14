# pulsar/core/diagnosis/llm_base.py
"""
DEPRECATED: Use pulsar.core.llm_connectors instead.

This module re-exports from the new llm_connectors layer for backward compatibility.
New code should import from pulsar.core.llm_connectors.
"""

# Re-export for backward compatibility
from pulsar.core.llm_connectors import (
    LLMProvider,
    LLMConfig,
    LLMProviderType,
    get_llm_provider,
    CostTracker,
)
from pulsar.core.llm_connectors.providers import OllamaProvider

__all__ = [
    'LLMProvider',
    'LLMConfig',
    'LLMProviderType',
    'get_llm_provider',
    'CostTracker',
    'OllamaProvider',
]
