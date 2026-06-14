"""Abstract agent base class architecture."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

import polars as pl

from pulsar.core.llm_connectors import LLMProvider, LLMConfig, get_llm_provider

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A message in the conversation."""
    role: str                           # 'user' or 'assistant'
    content: str                        # Message content
    timestamp: datetime
    metadata: Dict[str, Any] = None    # Optional metadata

    def __post_init__(self):
        """Ensure metadata is initialized."""
        if self.metadata is None:
            self.metadata = {}


class Agent(ABC):
    """
    Abstract base class for all agent types.

    Subclasses choose their own memory strategy via _init_memory().
    All agents have:
    - LLM provider (pluggable)
    - Tool registry (optional)
    - System prompt (customizable)
    """

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        system_prompt: Optional[str] = None,
        df: Optional[pl.DataFrame] = None,
        tools_enabled: bool = True,
    ):
        """
        Initialize agent.

        Args:
            llm_config: LLM configuration (defaults to Ollama/Gemma)
            system_prompt: System-level instructions
            df: DataFrame for tool operations
            tools_enabled: Enable tool calling
        """
        # Initialize LLM provider
        if llm_config is None:
            from pulsar.core.llm_connectors import LLMProviderType
            llm_config = LLMConfig(
                provider_type=LLMProviderType.OLLAMA,
                model_name="gemma3:270m",
                base_url="http://localhost:11434",
                temperature=0.7,
                max_tokens=1000,
            )

        try:
            self.llm_provider = get_llm_provider(llm_config)
            self.provider_available = self.llm_provider.health_check()
        except Exception as e:
            logger.error(f"Failed to initialize LLM provider: {e}")
            self.llm_provider = None
            self.provider_available = False

            # Critical: Flag provider failure explicitly
            raise RuntimeError(
                f"LLM provider unavailable: {e}. "
                f"If using Ollama, verify it's running at {llm_config.base_url}"
            )

        self.llm_config = llm_config
        self.df = df
        self.tools_enabled = tools_enabled

        # Initialize tool registry if enabled
        self.tool_registry: Optional[Any] = None
        if tools_enabled and df is not None:
            try:
                from pulsar.core.intelligence.tools import create_default_registry
                self.tool_registry = create_default_registry(df)
                logger.info(f"Tool registry initialized with {len(self.tool_registry.tools)} tools")
            except Exception as e:
                logger.warning(f"Failed to initialize tool registry: {e}")

        # System prompt
        self.system_prompt = system_prompt or self._default_system_prompt()

        # Memory - subclass decides strategy
        self._init_memory()

        # Session tracking
        self.session_start = datetime.now()

        logger.info(
            f"{self.__class__.__name__} initialized: {llm_config.model_name} "
            f"(Provider: {self.provider_available}, Tools: {tools_enabled})"
        )

    @abstractmethod
    def _init_memory(self) -> None:
        """
        Initialize memory strategy (subclass chooses).

        Options:
        - Full conversation history: self.memory = []
        - Stateless: self.memory = None
        - Stage-aware: self.stage_memory = {}
        """
        pass

    def _default_system_prompt(self) -> str:
        """Default system prompt for the agent."""
        return (
            "You are an intelligent data analysis agent. "
            "Your role is to analyze data patterns, understand quality issues, "
            "and provide actionable insights. "
            "Be concise, specific, and focus on business implications. "
            "Provide reasoning for your analysis."
        )

    @abstractmethod
    def think(self, question: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Think through a question.

        Args:
            question: The question to analyze
            context: Optional context dictionary

        Returns:
            Agent's response/reasoning
        """
        pass

    def analyze(self, data_summary: str, analysis_type: str) -> str:
        """
        Analyze data using the agent.

        Args:
            data_summary: Summary of the data
            analysis_type: Type of analysis

        Returns:
            Analysis result
        """
        question = f"Please analyze the following {analysis_type}:\n{data_summary}"
        return self.think(question)

    def health_check(self) -> Dict[str, Any]:
        """Check agent and LLM provider health."""
        return {
            'agent_type': self.__class__.__name__,
            'agent_status': 'healthy',
            'llm_model': self.llm_config.model_name,
            'provider_available': self.provider_available,
            'tools_enabled': self.tools_enabled,
            'tools_available': len(self.tool_registry.tools) if self.tool_registry else 0,
            'session_duration': (datetime.now() - self.session_start).total_seconds(),
        }
