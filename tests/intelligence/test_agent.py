# tests/intelligence/test_agent.py
"""Tests for the Agent class."""

import pytest
from datetime import datetime

from pulsar.core.intelligence.agent import Agent, Message
from pulsar.core.diagnosis.llm_base import LLMConfig, LLMProviderType


class TestAgent:
    """Test Agent initialization and functionality."""

    def test_agent_initialization_default(self):
        """Test agent initializes with default Ollama config."""
        agent = Agent()

        assert agent.llm_config is not None
        assert agent.llm_config.model_name == "minimax-m3:cloud"
        assert agent.llm_config.provider_type == LLMProviderType.OLLAMA
        assert len(agent.memory) == 0

    def test_agent_initialization_custom_config(self):
        """Test agent initializes with custom config."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma3:270m",
            temperature=0.5,
        )

        agent = Agent(llm_config=config)

        assert agent.llm_config.temperature == 0.5
        assert agent.llm_config.model_name == "gemma3:270m"

    def test_agent_custom_system_prompt(self):
        """Test agent accepts custom system prompt."""
        custom_prompt = "You are a data quality expert."
        agent = Agent(system_prompt=custom_prompt)

        assert agent.system_prompt == custom_prompt

    def test_agent_memory_storage(self):
        """Test agent stores messages in memory."""
        agent = Agent()

        agent._add_to_memory("user", "What is concentration?")
        agent._add_to_memory("assistant", "Concentration measures market power.")

        assert len(agent.memory) == 2
        assert agent.memory[0].role == "user"
        assert agent.memory[1].role == "assistant"

    def test_agent_memory_with_metadata(self):
        """Test agent stores metadata with messages."""
        agent = Agent()

        metadata = {'context': 'market_analysis', 'importance': 'high'}
        agent._add_to_memory("user", "Analyze concentration", metadata)

        assert agent.memory[0].metadata == metadata

    def test_agent_get_memory(self):
        """Test exporting memory."""
        agent = Agent()

        agent._add_to_memory("user", "Question 1")
        agent._add_to_memory("assistant", "Answer 1")

        memory = agent.get_memory()

        assert len(memory) == 2
        assert memory[0]['role'] == "user"
        assert memory[0]['content'] == "Question 1"
        assert 'timestamp' in memory[0]

    def test_agent_clear_memory(self):
        """Test clearing conversation memory."""
        agent = Agent()

        agent._add_to_memory("user", "Question 1")
        agent._add_to_memory("assistant", "Answer 1")

        summary = agent.clear_memory()

        assert summary['total_messages'] == 2
        assert len(agent.memory) == 0

    def test_agent_think_fallback(self):
        """Test think method uses fallback when LLM unavailable."""
        agent = Agent()
        agent.provider_available = False

        response = agent.think("What is data quality?")

        assert "unavailable" in response.lower()
        assert len(agent.memory) == 2  # User message + assistant response

    def test_agent_analyze(self):
        """Test analyze method."""
        agent = Agent()
        agent.provider_available = False

        response = agent.analyze(
            "36 countries, top 10 have 76% of medals",
            "concentration",
        )

        assert len(agent.memory) > 0

    def test_agent_reason_about(self):
        """Test reason_about method with context."""
        agent = Agent()
        agent.provider_available = False

        context = {
            'concentration': '76.8%',
            'top_10_countries': 10,
            'total_countries': 36,
        }

        response = agent.reason_about(
            "Top 10 countries dominate medals",
            context,
        )

        assert len(agent.memory) > 0

    def test_agent_health_check(self):
        """Test health check functionality."""
        agent = Agent()

        health = agent.health_check()

        assert health['agent_status'] == 'healthy'
        assert 'llm_model' in health
        assert 'provider_available' in health
        assert 'memory_size' in health

    def test_agent_export_session(self):
        """Test exporting full session."""
        agent = Agent()

        agent._add_to_memory("user", "Test message")
        agent._add_to_memory("assistant", "Test response")

        session = agent.export_session()

        assert session['model'] == "minimax-m3:cloud"
        assert session['provider_type'] == "ollama"
        assert session['total_messages'] == 2
        assert 'messages' in session

    def test_agent_memory_conversation_flow(self):
        """Test multi-turn conversation memory."""
        agent = Agent()

        # Simulate conversation
        agent._add_to_memory("user", "What is concentration?")
        agent._add_to_memory("assistant", "Concentration measures market power.")
        agent._add_to_memory("user", "How do we reduce it?")
        agent._add_to_memory("assistant", "Through diversification and entry barriers.")

        assert len(agent.memory) == 4
        assert agent.memory[0].content == "What is concentration?"
        assert agent.memory[2].content == "How do we reduce it?"

    def test_message_dataclass(self):
        """Test Message dataclass."""
        now = datetime.now()
        msg = Message(
            role="user",
            content="Test",
            timestamp=now,
            metadata={'test': True},
        )

        assert msg.role == "user"
        assert msg.content == "Test"
        assert msg.metadata == {'test': True}

    def test_agent_llm_provider_integration(self):
        """Test agent integrates with LLM provider."""
        agent = Agent()

        # Should have initialized an LLM provider
        assert agent.llm_provider is not None
        assert hasattr(agent.llm_provider, 'health_check')
        assert hasattr(agent.llm_provider, 'generate')


class TestAgentIntegration:
    """Integration tests for Agent."""

    def test_full_conversation_flow(self):
        """Test full conversation flow with memory."""
        agent = Agent()
        agent.provider_available = False  # Use fallback

        # Turn 1: Ask question
        response1 = agent.think("What is market concentration?")
        assert len(agent.memory) == 2

        # Turn 2: Follow-up question (should have memory context)
        response2 = agent.think("How does it affect strategy?")
        assert len(agent.memory) == 4

        # Verify memory order
        assert agent.memory[0].role == "user"
        assert agent.memory[1].role == "assistant"
        assert agent.memory[2].role == "user"
        assert agent.memory[3].role == "assistant"

    def test_session_export_and_clear(self):
        """Test exporting session before clearing."""
        agent = Agent()

        agent._add_to_memory("user", "Question")
        agent._add_to_memory("assistant", "Answer")

        # Export before clear
        session = agent.export_session()
        assert session['total_messages'] == 2

        # Clear memory
        summary = agent.clear_memory()
        assert summary['total_messages'] == 2
        assert len(agent.memory) == 0

        # New memory should be empty
        memory = agent.get_memory()
        assert len(memory) == 0
