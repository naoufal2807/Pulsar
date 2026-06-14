"""Anthropic LLM provider implementation."""

import os
import logging
from .base_provider import BaseProvider
from ..base import LLMConfig, LLMProvider

logger = logging.getLogger(__name__)


class AnthropicProvider(BaseProvider, LLMProvider):
    """
    Anthropic provider for Claude models.

    Supports Claude 3 family with 200K context window.
    Requires ANTHROPIC_API_KEY environment variable.
    """

    # Pricing per 1M tokens (as of Feb 2025)
    PRICING = {
        'claude-3-opus': {'input': 0.015, 'output': 0.075},
        'claude-3-sonnet': {'input': 0.003, 'output': 0.015},
        'claude-3-haiku': {'input': 0.00025, 'output': 0.00125},
    }

    def __init__(self, config: LLMConfig):
        """Initialize Anthropic provider."""
        BaseProvider.__init__(self, config)
        LLMProvider.__init__(self, config)

        # Get API key from environment
        api_key = config.api_key or os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable not set")

        self.config.api_key = api_key
        self.logger.debug(f"Anthropic provider initialized: {config.model_name}")

    def health_check(self) -> bool:
        """Check if Anthropic API is accessible."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.config.api_key)
            # Try a minimal request to verify credentials
            _ = client.messages.create(
                model=self.config.model_name,
                max_tokens=10,
                messages=[{"role": "user", "content": "hi"}]
            )
            return True
        except Exception as e:
            self.logger.error(f"Anthropic health check failed: {e}")
            return False

    def generate(self, prompt: str) -> str:
        """Generate text using Anthropic."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.config.api_key)

            message = client.messages.create(
                model=self.config.model_name,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                messages=[{"role": "user", "content": prompt}]
            )

            return message.content[0].text

        except Exception as e:
            self.logger.error(f"Generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

    def count_tokens(self, text: str) -> int:
        """Count tokens using Anthropic's API."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self.config.api_key)
            response = client.messages.count_tokens(
                model=self.config.model_name,
                messages=[{"role": "user", "content": text}]
            )
            return response.input_tokens
        except Exception:
            # Fallback to character-based estimate
            return super().count_tokens(text)

    def get_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate request cost."""
        # Find matching pricing (support partial model names)
        pricing = None
        for model_key, price in self.PRICING.items():
            if model_key in self.config.model_name.lower():
                pricing = price
                break

        if not pricing:
            return 0.0

        # Note: Anthropic pricing is per 1M tokens
        input_cost = (input_tokens / 1_000_000) * pricing['input']
        output_cost = (output_tokens / 1_000_000) * pricing['output']
        return input_cost + output_cost

    def get_model_info(self) -> dict:
        """Get Claude model information."""
        pricing = None
        for model_key, price in self.PRICING.items():
            if model_key in self.config.model_name.lower():
                pricing = price
                break

        return {
            'name': self.config.model_name,
            'provider': 'anthropic',
            'context_size': 200000,
            'supports_vision': True,
            'supports_functions': True,
            'cost_per_1m_input': pricing['input'] if pricing else 0.0,
            'cost_per_1m_output': pricing['output'] if pricing else 0.0,
        }
