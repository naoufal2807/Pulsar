"""Google Gemini LLM provider implementation."""

import os
import logging
from .base_provider import BaseProvider
from ..base import LLMConfig, LLMProvider

logger = logging.getLogger(__name__)


class GoogleProvider(BaseProvider, LLMProvider):
    """
    Google Gemini provider.

    Supports Gemini models with free tier available.
    Requires GOOGLE_API_KEY environment variable.
    """

    # Pricing (free tier available)
    PRICING = {
        'gemini-pro': {'input': 0.0, 'output': 0.0},  # Free
        'gemini-1.5-pro': {'input': 0.0075, 'output': 0.03},
        'gemini-1.5-flash': {'input': 0.00075, 'output': 0.003},
    }

    def __init__(self, config: LLMConfig):
        """Initialize Google provider."""
        BaseProvider.__init__(self, config)
        LLMProvider.__init__(self, config)

        # Get API key from environment
        api_key = config.api_key or os.getenv('GOOGLE_API_KEY')
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable not set")

        self.config.api_key = api_key
        self.logger.debug(f"Google Gemini provider initialized: {config.model_name}")

    def health_check(self) -> bool:
        """Check if Google API is accessible."""
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.config.api_key)
            model = genai.GenerativeModel(self.config.model_name)
            # Try a minimal request to verify credentials
            _ = model.generate_content("hi", stream=False)
            return True
        except Exception as e:
            self.logger.error(f"Google health check failed: {e}")
            return False

    def generate(self, prompt: str) -> str:
        """Generate text using Google Gemini."""
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.config.api_key)

            model = genai.GenerativeModel(self.config.model_name)
            response = model.generate_content(
                prompt,
                generation_config={
                    'temperature': self.config.temperature,
                    'max_output_tokens': self.config.max_tokens,
                    'top_p': self.config.top_p,
                },
                stream=False
            )

            return response.text

        except Exception as e:
            self.logger.error(f"Generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

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
        """Get Gemini model information."""
        pricing = None
        for model_key, price in self.PRICING.items():
            if model_key in self.config.model_name.lower():
                pricing = price
                break

        return {
            'name': self.config.model_name,
            'provider': 'google',
            'context_size': 32000,
            'supports_vision': True,
            'supports_functions': False,
            'cost_per_1k_input': pricing['input'] if pricing else 0.0,
            'cost_per_1k_output': pricing['output'] if pricing else 0.0,
            'free_tier': True,
        }
