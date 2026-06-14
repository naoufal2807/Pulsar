"""OpenAI LLM provider implementation."""

import os
import logging
from .base_provider import BaseProvider
from ..base import LLMConfig, LLMProvider

logger = logging.getLogger(__name__)


class OpenAIProvider(BaseProvider, LLMProvider):
    """
    OpenAI provider for GPT models.

    Supports GPT-4, GPT-3.5-turbo with token counting and cost tracking.
    Requires OPENAI_API_KEY environment variable.
    """

    # Pricing per 1K tokens (as of Feb 2025)
    PRICING = {
        'gpt-4': {'input': 0.03, 'output': 0.06},
        'gpt-4-turbo': {'input': 0.01, 'output': 0.03},
        'gpt-3.5-turbo': {'input': 0.0005, 'output': 0.0015},
    }

    # Context window sizes
    CONTEXT_WINDOWS = {
        'gpt-4': 8192,
        'gpt-4-32k': 32768,
        'gpt-4-turbo': 128000,
        'gpt-3.5-turbo': 4096,
    }

    def __init__(self, config: LLMConfig):
        """Initialize OpenAI provider."""
        BaseProvider.__init__(self, config)
        LLMProvider.__init__(self, config)

        # Get API key from environment
        api_key = config.api_key or os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set")

        self.config.api_key = api_key
        self.logger.debug(f"OpenAI provider initialized: {config.model_name}")

    def health_check(self) -> bool:
        """Check if OpenAI API is accessible."""
        try:
            import openai
            openai.api_key = self.config.api_key
            # Try a minimal request to verify credentials
            models = openai.Model.list()
            return True
        except Exception as e:
            self.logger.error(f"OpenAI health check failed: {e}")
            return False

    def generate(self, prompt: str) -> str:
        """Generate text using OpenAI."""
        try:
            import openai
            openai.api_key = self.config.api_key

            response = openai.ChatCompletion.create(
                model=self.config.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                top_p=self.config.top_p,
            )

            return response.choices[0].message.content

        except Exception as e:
            self.logger.error(f"Generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

    def count_tokens(self, text: str) -> int:
        """Count tokens using tiktoken."""
        try:
            import tiktoken
            encoding = tiktoken.encoding_for_model(self.config.model_name)
            return len(encoding.encode(text))
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

        input_cost = (input_tokens / 1000) * pricing['input']
        output_cost = (output_tokens / 1000) * pricing['output']
        return input_cost + output_cost

    def get_model_info(self) -> dict:
        """Get OpenAI model information."""
        context_size = 'unknown'
        for model_key, size in self.CONTEXT_WINDOWS.items():
            if model_key in self.config.model_name.lower():
                context_size = size
                break

        pricing = None
        for model_key, price in self.PRICING.items():
            if model_key in self.config.model_name.lower():
                pricing = price
                break

        return {
            'name': self.config.model_name,
            'provider': 'openai',
            'context_size': context_size,
            'supports_vision': 'vision' in self.config.model_name.lower(),
            'supports_functions': True,
            'cost_per_1k_input': pricing['input'] if pricing else 0.0,
            'cost_per_1k_output': pricing['output'] if pricing else 0.0,
        }
