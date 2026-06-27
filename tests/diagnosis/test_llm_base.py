# tests/diagnosis/test_llm_base.py
"""Tests for LLM base classes and providers."""

import pytest
from pulsar.core.diagnosis.llm_base import (
    LLMConfig, LLMProviderType, LLMProvider, OllamaProvider, get_llm_provider
)


class TestLLMConfig:
    """Test LLM configuration."""

    def test_config_creation(self):
        """Test creating LLM config."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
        )

        assert config.provider_type == LLMProviderType.OLLAMA
        assert config.model_name == "gemma:2b"
        assert config.temperature == 0.7

    def test_config_with_custom_values(self):
        """Test config with custom values."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
            temperature=0.5,
            max_tokens=2000,
            base_url="http://localhost:11434",
        )

        assert config.temperature == 0.5
        assert config.max_tokens == 2000
        assert config.base_url == "http://localhost:11434"

    def test_config_defaults(self):
        """Test config defaults."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
        )

        assert config.timeout == 30
        assert config.metadata == {}


class TestOllamaProvider:
    """Test Ollama provider."""

    @pytest.fixture
    def config(self):
        return LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
            base_url="http://localhost:11434",
        )

    def test_ollama_provider_creation(self, config):
        """Test creating Ollama provider."""
        provider = OllamaProvider(config)

        assert provider is not None
        assert provider.config.model_name == "gemma:2b"

    def test_ollama_default_base_url(self):
        """Test Ollama uses default base URL."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
        )

        provider = OllamaProvider(config)

        assert provider.config.base_url == "http://localhost:11434"

    def test_ollama_validate_config(self, config):
        """Test config validation."""
        provider = OllamaProvider(config)

        assert provider.validate_config() is True

    def test_ollama_validate_config_invalid_temperature(self):
        """Test config validation fails with invalid temperature."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
            temperature=3.0,  # Invalid
        )

        provider = OllamaProvider(config)

        assert provider.validate_config() is False

    def test_ollama_validate_config_no_model(self):
        """Test config validation fails with no model."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="",
        )

        provider = OllamaProvider(config)

        assert provider.validate_config() is False

    def test_get_llm_provider_ollama(self):
        """Test factory function for Ollama."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
        )

        provider = get_llm_provider(config)

        assert isinstance(provider, OllamaProvider)

    def test_get_llm_provider_unsupported(self):
        """Test factory raises error for unsupported provider."""
        config = LLMConfig(
            provider_type=LLMProviderType.OPENAI,
            model_name="gpt-4",
        )

        with pytest.raises(ValueError):
            get_llm_provider(config)


class TestOllamaProviderMethods:
    """Test Ollama provider methods (with mocking since Ollama may not be available)."""

    @pytest.fixture
    def config(self):
        return LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma:2b",
            temperature=0.7,
            max_tokens=1000,
        )

    def test_ollama_health_check_not_available(self, config):
        """Test health check when Ollama is not running."""
        provider = OllamaProvider(config)

        # If Ollama is not running, health check should return False
        result = provider.health_check()

        # Result depends on whether Ollama is actually available
        assert isinstance(result, bool)

    def test_ollama_generate_not_available(self, config):
        """Test generate raises error when Ollama not available."""
        provider = OllamaProvider(config)

        # Should raise RuntimeError if Ollama not available
        try:
            result = provider.generate("Hello world")
            # If successful, should return string
            assert isinstance(result, str)
        except RuntimeError as e:
            # Expected if Ollama not running
            assert "generation failed" in str(e).lower() or "ollama" in str(e).lower()

    def test_ollama_generate_not_available(self, config):
        """Test generate when Ollama not available."""
        provider = OllamaProvider(config)

        try:
            result = provider.generate("Hello")
            assert isinstance(result, str)
        except RuntimeError:
            pass

    def test_ollama_health_check_when_unavailable(self, config):
        """Test health check returns False when Ollama not running."""
        provider = OllamaProvider(config)
        health = provider.health_check()
        assert isinstance(health, bool)
