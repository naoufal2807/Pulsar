"""Agent registry and factory for creating agent instances."""

from typing import Optional, Type, Dict, Any
import logging

import polars as pl

from pulsar.core.intelligence.agent_base import Agent
from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class AgentRegistry:
    """Factory for creating agent instances."""

    _agents: Dict[str, Type[Agent]] = {
        'reasoning': ReasoningAgent,
        'default': ReasoningAgent,  # Default agent type
    }

    @classmethod
    def register(cls, agent_type: str, agent_class: Type[Agent]) -> None:
        """
        Register a new agent type.

        Args:
            agent_type: Name of agent type (e.g., 'reasoning')
            agent_class: Agent class (must be subclass of Agent)
        """
        if not issubclass(agent_class, Agent):
            raise TypeError(f"{agent_class} must be subclass of Agent")
        cls._agents[agent_type] = agent_class
        logger.info(f"Registered agent type: {agent_type}")

    @classmethod
    def get_agent(
        cls,
        agent_type: str = 'reasoning',
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        tools_enabled: bool = True,
        **kwargs
    ) -> Agent:
        """
        Create an agent instance.

        Args:
            agent_type: Type of agent ('reasoning', etc)
            llm_config: LLM configuration
            df: DataFrame for tool operations
            tools_enabled: Enable tool calling
            **kwargs: Additional arguments passed to agent constructor

        Returns:
            Agent instance

        Raises:
            ValueError: If agent_type not found
        """
        if agent_type not in cls._agents:
            raise ValueError(f"Unknown agent type: {agent_type}")

        agent_class = cls._agents[agent_type]
        return agent_class(
            llm_config=llm_config,
            df=df,
            tools_enabled=tools_enabled,
            **kwargs
        )

    @classmethod
    def list_agents(cls) -> list:
        """Get list of available agent types."""
        return list(cls._agents.keys())
