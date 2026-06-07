# pulsar/core/diagnosis/llm_base.py
"""
Base LLM abstraction for pluggable language model providers.

This module defines the base interface for LLM providers, allowing
easy swapping between different providers (Ollama, OpenAI, Anthropic, etc.)
without changing application code.

Architecture:
- LLMProvider: Abstract base class
- OllamaProvider: Ollama implementation (Gemma, Llama, etc.)
- Future: OpenAIProvider, AnthropicProvider, LocalModelProvider, etc.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class LLMProviderType(Enum):
    """Supported LLM provider types."""
    OLLAMA = "ollama"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    LOCAL = "local"
    CUSTOM = "custom"


@dataclass
class LLMConfig:
    """Configuration for LLM provider."""
    provider_type: LLMProviderType
    model_name: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    temperature: float = 0.7
    max_tokens: int = 1000
    timeout: int = 30
    retry_count: int = 3
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        """Initialize metadata if not provided."""
        if self.metadata is None:
            self.metadata = {}


class LLMProvider(ABC):
    """
    Abstract base class for LLM providers.

    All LLM providers must implement this interface to be used
    throughout the system.
    """

    def __init__(self, config: LLMConfig):
        """
        Initialize LLM provider.

        Args:
            config: LLMConfig with provider settings
        """
        self.config = config
        self.logger = logger

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if provider is available and healthy.

        Returns:
            True if provider is healthy, False otherwise
        """
        pass

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """
        Generate text from prompt.

        Args:
            prompt: Input prompt

        Returns:
            Generated text
        """
        pass

    @abstractmethod
    def generate_streaming(self, prompt: str):
        """
        Generate text with streaming output.

        Args:
            prompt: Input prompt

        Yields:
            Text chunks as they're generated
        """
        pass

    @abstractmethod
    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        Generate chat completion.

        Args:
            messages: List of messages in conversation
            system_prompt: Optional system instruction

        Returns:
            Generated response
        """
        pass

    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """
        Generate embeddings for text (if supported).

        Args:
            text: Input text

        Returns:
            Embedding vector

        Raises:
            NotImplementedError: If provider doesn't support embeddings
        """
        pass

    def validate_config(self) -> bool:
        """
        Validate provider configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
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


class OllamaProvider(LLMProvider):
    """
    Ollama LLM provider implementation.

    Supports local models running via Ollama (Gemma, Llama, Mistral, etc.)
    """

    def __init__(self, config: LLMConfig):
        """
        Initialize Ollama provider.

        Args:
            config: LLMConfig with Ollama settings
        """
        super().__init__(config)

        # Set defaults for Ollama
        if not config.base_url:
            self.config.base_url = "http://localhost:11434"

        self.logger.debug(f"Initialized Ollama provider: {config.model_name} at {self.config.base_url}")

    def health_check(self) -> bool:
        """
        Check if Ollama service is available.

        Returns:
            True if Ollama is running and model is available
        """
        try:
            import requests

            # Check if Ollama service is running
            response = requests.get(
                f"{self.config.base_url}/api/tags",
                timeout=self.config.timeout
            )

            if response.status_code != 200:
                self.logger.error(f"Ollama health check failed: {response.status_code}")
                return False

            # Check if model exists
            models = response.json().get('models', [])
            model_names = [m.get('name', '') for m in models]

            model_found = any(self.config.model_name in name for name in model_names)

            if not model_found:
                self.logger.warning(
                    f"Model '{self.config.model_name}' not found in Ollama. "
                    f"Available: {model_names}"
                )
                return False

            self.logger.debug(f"Ollama health check passed. Model: {self.config.model_name}")
            return True

        except Exception as e:
            self.logger.error(f"Ollama health check failed: {e}")
            return False

    def generate(self, prompt: str) -> str:
        """
        Generate text using Ollama.

        Args:
            prompt: Input prompt

        Returns:
            Generated text

        Raises:
            RuntimeError: If generation fails
        """
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
            generated_text = result.get('response', '')

            self.logger.debug(f"Generated {len(generated_text)} characters")
            return generated_text

        except Exception as e:
            self.logger.error(f"Generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

    def generate_streaming(self, prompt: str):
        """
        Generate text with streaming from Ollama.

        Args:
            prompt: Input prompt

        Yields:
            Text chunks as they're generated

        Raises:
            RuntimeError: If streaming fails
        """
        try:
            import requests

            response = requests.post(
                f"{self.config.base_url}/api/generate",
                json={
                    "model": self.config.model_name,
                    "prompt": prompt,
                    "temperature": self.config.temperature,
                    "stream": True,
                },
                timeout=self.config.timeout,
                stream=True,
            )

            if response.status_code != 200:
                raise RuntimeError(f"Ollama request failed: {response.status_code}")

            for line in response.iter_lines():
                if line:
                    try:
                        chunk = line.decode('utf-8')
                        import json
                        data = json.loads(chunk)
                        if 'response' in data:
                            yield data['response']
                    except Exception as e:
                        self.logger.debug(f"Error parsing stream chunk: {e}")
                        continue

        except Exception as e:
            self.logger.error(f"Streaming failed: {e}")
            raise RuntimeError(f"LLM streaming failed: {e}")

    def chat_completion(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        Generate chat completion using Ollama.

        Args:
            messages: List of messages [{"role": "user", "content": "..."}, ...]
            system_prompt: Optional system instruction

        Returns:
            Generated response

        Raises:
            RuntimeError: If request fails
        """
        try:
            import requests

            # Format messages for Ollama
            formatted_messages = []
            if system_prompt:
                formatted_messages.append({"role": "system", "content": system_prompt})
            formatted_messages.extend(messages)

            # Build prompt from messages (Ollama doesn't have native chat API in older versions)
            # For newer Ollama with chat API, adjust as needed
            prompt = "\n".join([
                f"{msg['role']}: {msg['content']}"
                for msg in formatted_messages
            ])

            return self.generate(prompt)

        except Exception as e:
            self.logger.error(f"Chat completion failed: {e}")
            raise RuntimeError(f"Chat completion failed: {e}")

    def embed(self, text: str) -> List[float]:
        """
        Generate embeddings (not commonly supported in Ollama).

        Args:
            text: Input text

        Returns:
            Embedding vector

        Raises:
            NotImplementedError: Embedding not supported by default
        """
        try:
            import requests

            response = requests.post(
                f"{self.config.base_url}/api/embed",
                json={
                    "model": self.config.model_name,
                    "input": text,
                },
                timeout=self.config.timeout,
            )

            if response.status_code != 200:
                raise NotImplementedError(f"Embeddings not supported: {response.status_code}")

            result = response.json()
            return result.get('embeddings', [[]])[0]

        except Exception as e:
            self.logger.warning(f"Embeddings not available: {e}")
            raise NotImplementedError(f"Embeddings not supported: {e}")


def get_llm_provider(config: LLMConfig) -> LLMProvider:
    """
    Factory function to get LLM provider instance.

    Args:
        config: LLMConfig with provider type and settings

    Returns:
        LLMProvider instance

    Raises:
        ValueError: If provider type is not supported
    """
    if config.provider_type == LLMProviderType.OLLAMA:
        return OllamaProvider(config)
    else:
        raise ValueError(f"Unsupported provider type: {config.provider_type}")
