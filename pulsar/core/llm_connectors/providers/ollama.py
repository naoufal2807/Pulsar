"""Ollama LLM provider implementation."""

from typing import Optional
import logging
from .base_provider import BaseProvider
from ..base import LLMConfig, LLMProvider

logger = logging.getLogger(__name__)


class OllamaProvider(BaseProvider, LLMProvider):
    """
    Ollama provider for local models (Gemma, Llama, Mistral, etc).

    Free to run locally, no API key required.
    Default endpoint: http://localhost:11434
    """

    def __init__(self, config: LLMConfig):
        """Initialize Ollama provider."""
        BaseProvider.__init__(self, config)
        LLMProvider.__init__(self, config)

        # Set default base_url if not provided
        if not self.config.base_url:
            self.config.base_url = "http://localhost:11434"

        self.logger.debug(f"Ollama provider initialized: {config.model_name} at {self.config.base_url}")

    def health_check(self) -> bool:
        """Check if Ollama service is running and model is available."""
        try:
            import requests

            response = requests.get(
                f"{self.config.base_url}/api/tags",
                timeout=self.config.timeout
            )

            if response.status_code != 200:
                self.logger.error(f"Ollama health check failed: {response.status_code}")
                return False

            models = response.json().get('models', [])
            model_names = [m.get('name', '') for m in models]

            # Check if our model is available (can be partial match)
            model_found = any(self.config.model_name in name for name in model_names)

            if not model_found:
                self.logger.warning(
                    f"Model '{self.config.model_name}' not found. "
                    f"Available: {model_names}"
                )
                return False

            self.logger.debug(f"Ollama health check passed: {self.config.model_name}")
            return True

        except Exception as e:
            self.logger.error(f"Ollama health check failed: {e}")
            return False

    def generate(self, prompt: str) -> str:
        """Generate text using Ollama."""
        try:
            import requests

            response = requests.post(
                f"{self.config.base_url}/api/generate",
                json={
                    "model": self.config.model_name,
                    "prompt": prompt,
                    "temperature": self.config.temperature,
                    "stream": False,
                },
                timeout=self.config.timeout,
            )

            if response.status_code != 200:
                raise RuntimeError(f"Ollama request failed: {response.status_code}")

            result = response.json()
            return result.get('response', '')

        except Exception as e:
            self.logger.error(f"Generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

    def get_model_info(self) -> dict:
        """Get Ollama model information."""
        return {
            'name': self.config.model_name,
            'provider': 'ollama',
            'context_size': '4K-32K (model-dependent)',
            'supports_vision': False,
            'supports_functions': False,
            'cost_per_1k_tokens': 0.0,
            'base_url': self.config.base_url,
        }
