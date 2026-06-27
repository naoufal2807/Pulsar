# pulsar/core/intelligence/__init__.py
"""Intelligence layer for data understanding."""

from .agent import Agent, Message
from .tools import ToolRegistry, ToolDefinition, ToolParameter, create_default_registry

# Domain agents (Approach B — journey specialists)
from .schema_agent import SchemaAgent
from .quality_agent import QualityAgent
from .stats_agent import StatsAgent
from .narrator_agent import NarratorAgent

# Pipeline wiring
from .agent_registry import AgentRegistry
from .question_router import get_tiers, list_modes

__all__ = [
    # Core
    'Agent',
    'Message',
    # Tool infrastructure
    'ToolRegistry',
    'ToolDefinition',
    'ToolParameter',
    'create_default_registry',  # kept for journey.py backward compat
    # Domain agents
    'SchemaAgent',
    'QualityAgent',
    'StatsAgent',
    'NarratorAgent',
    # Pipeline
    'AgentRegistry',
    'get_tiers',
    'list_modes',
]
