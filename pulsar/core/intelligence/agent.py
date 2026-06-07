# pulsar/core/intelligence/agent.py
"""
Agent: LLM-powered intelligence backbone for data analysis.

Architecture:
- Uses pluggable LLM providers (base class: LLMProvider)
- Maintains conversation memory throughout session
- Clears memory at end of session
- First implementation: Ollama with Gemma3:270m

Memory Structure:
- Stores all messages in conversation
- Uses memory for context in subsequent calls
- Each message: {role, content, timestamp}
- Can be exported and cleared
"""

from typing import Any, Dict, List, Optional
import logging
from datetime import datetime
from dataclasses import dataclass
import json
import re

import polars as pl

from pulsar.core.diagnosis.llm_base import LLMProvider, LLMConfig, LLMProviderType, get_llm_provider
from pulsar.core.intelligence.tools import ToolRegistry, create_default_registry

logger = logging.getLogger(__name__)


@dataclass
class Message:
    """A message in the conversation."""
    role: str                           # 'user' or 'assistant'
    content: str                        # Message content
    timestamp: datetime
    metadata: Dict[str, Any] = None    # Optional metadata


class Agent:
    """
    LLM-powered agent for data intelligence.

    Uses pluggable LLM providers for reasoning.
    Maintains conversation memory throughout session.
    Memory is cleared at end of session.

    Architecture:
    - LLM Provider base class for all implementations
    - First provider: Ollama with Gemma3:270m
    - Future providers: OpenAI, Anthropic, Local models, etc.
    """

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        system_prompt: Optional[str] = None,
        df: Optional[pl.DataFrame] = None,
        tools_enabled: bool = True,
    ):
        """
        Initialize agent with LLM provider and optional tools.

        Args:
            llm_config: LLM configuration (defaults to Ollama/Gemma3:270m)
            system_prompt: System-level instructions for the agent
            df: DataFrame for tool operations
            tools_enabled: Enable tool calling capabilities
        """
        # Initialize LLM provider
        if llm_config is None:
            # Default to Ollama with Gemma3:270m
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

        self.llm_config = llm_config

        # Tool support (must be set before _default_system_prompt)
        self.df = df
        self.tools_enabled = tools_enabled
        self.tool_registry: Optional[ToolRegistry] = None
        if tools_enabled and df is not None:
            self.tool_registry = create_default_registry(df)
            logger.info(f"Tool registry initialized with {len(self.tool_registry.tools)} tools")

        self.system_prompt = system_prompt or self._default_system_prompt()

        # Conversation memory
        self.memory: List[Message] = []
        self.session_start = datetime.now()

        logger.info(
            f"Agent initialized: {llm_config.model_name} "
            f"(Provider available: {self.provider_available}, Tools: {tools_enabled})"
        )

    def _default_system_prompt(self) -> str:
        """Default system prompt for the agent."""
        prompt = (
            "You are an intelligent data analysis agent. "
            "Your role is to analyze data patterns, understand quality issues, "
            "and provide actionable insights. "
            "Be concise, specific, and focus on business implications. "
            "Provide reasoning for your analysis."
        )

        # Add tool calling instructions if tools enabled
        if self.tools_enabled and self.tool_registry:
            prompt += "\n\nYou have access to the following tools:"
            for tool_schema in self.tool_registry.get_schemas():
                prompt += f"\n- {tool_schema['name']}: {tool_schema['description']}"
            prompt += "\n\nWhen you need to analyze data, call tools like: [TOOL: tool_name(param1=value1, param2=value2)]"
            prompt += "\nAfter calling a tool, you will see the result. Use that result in your analysis."

        return prompt

    def think(self, question: str, context: Optional[Dict[str, Any]] = None, max_iterations: int = 3) -> str:
        """
        Think through a question using the LLM with conversation memory and tool calling.

        Args:
            question: The question to analyze
            context: Optional context dictionary for the query
            max_iterations: Max times to call tools in one think() session

        Returns:
            Agent's response/reasoning
        """
        if not self.provider_available:
            return self._fallback_response(question, context)

        try:
            # Store user question in memory
            self._add_to_memory("user", question)

            # Iteratively call LLM and tools
            current_question = question
            iteration = 0
            final_response = ""

            while iteration < max_iterations:
                # Build prompt with memory context
                prompt = self._build_prompt(current_question, context)

                # Get response from LLM
                response = self.llm_provider.generate(prompt)

                # Check if response contains tool calls
                if "[TOOL:" in response and self.tools_enabled:
                    # Execute tool calls
                    tool_results = self._execute_tool_calls(response)

                    if tool_results:
                        # Ask LLM to use tool results
                        current_question = f"Tool results:\n{json.dumps(tool_results, default=str)}\n\nBased on these results, provide your analysis and insights."
                        iteration += 1
                        continue

                # No tool calls or tool execution failed - this is the final response
                final_response = response
                break

            # Store final response in memory
            self._add_to_memory("assistant", final_response)

            return final_response

        except Exception as e:
            logger.error(f"Agent thinking failed: {e}")
            return self._fallback_response(question, context)

    def analyze(self, data_summary: str, analysis_type: str) -> str:
        """
        Analyze data using the agent.

        Args:
            data_summary: Summary of the data
            analysis_type: Type of analysis (concentration, distribution, risk, etc.)

        Returns:
            Analysis result
        """
        question = f"Please analyze the following {analysis_type}:\n{data_summary}"
        return self.think(question)

    def reason_about(self, observation: str, context: Dict[str, Any]) -> str:
        """
        Reason about an observation with context.

        Args:
            observation: What was observed
            context: Context for reasoning

        Returns:
            Reasoning and implications
        """
        context_str = "\n".join([f"- {k}: {v}" for k, v in context.items()])
        question = f"Observation: {observation}\n\nContext:\n{context_str}\n\nWhat does this mean and what are the implications?"
        return self.think(question, context)

    def _build_prompt(self, question: str, context: Optional[Dict[str, Any]]) -> str:
        """Build prompt with memory context."""
        prompt_parts = [self.system_prompt]

        # Add recent memory (last 5 messages for context)
        if self.memory:
            prompt_parts.append("\nRecent conversation:")
            for msg in self.memory[-5:]:
                prompt_parts.append(f"{msg.role.upper()}: {msg.content}")

        # Add context if provided
        if context:
            prompt_parts.append("\nContext:")
            for key, value in context.items():
                prompt_parts.append(f"- {key}: {value}")

        # Add current question
        prompt_parts.append(f"\nYour turn to respond to: {question}")

        return "\n".join(prompt_parts)

    def _add_to_memory(self, role: str, content: str, metadata: Optional[Dict] = None):
        """Add message to conversation memory."""
        message = Message(
            role=role,
            content=content,
            timestamp=datetime.now(),
            metadata=metadata or {},
        )
        self.memory.append(message)

    def _fallback_response(self, question: str, context: Optional[Dict]) -> str:
        """Fallback response when LLM unavailable."""
        logger.warning("LLM provider unavailable, using fallback response")
        self._add_to_memory("user", question)

        fallback = (
            "LLM provider is currently unavailable. "
            "Using heuristic analysis instead. "
            "Please ensure Ollama is running with gemma3:270m model available."
        )

        self._add_to_memory("assistant", fallback)
        return fallback

    def get_memory(self) -> List[Dict[str, Any]]:
        """
        Export conversation memory.

        Returns:
            List of messages in conversation
        """
        return [
            {
                'role': msg.role,
                'content': msg.content,
                'timestamp': msg.timestamp.isoformat(),
                'metadata': msg.metadata,
            }
            for msg in self.memory
        ]

    def clear_memory(self) -> Dict[str, Any]:
        """
        Clear conversation memory at end of session.

        Returns:
            Summary of cleared memory
        """
        summary = {
            'total_messages': len(self.memory),
            'session_duration': (datetime.now() - self.session_start).total_seconds(),
            'messages': self.get_memory(),
        }

        self.memory = []
        logger.info("Agent memory cleared")

        return summary

    def export_session(self) -> Dict[str, Any]:
        """
        Export full session before clearing.

        Returns:
            Complete session data
        """
        return {
            'model': self.llm_config.model_name,
            'provider_type': self.llm_config.provider_type.value,
            'session_start': self.session_start.isoformat(),
            'session_end': datetime.now().isoformat(),
            'total_messages': len(self.memory),
            'messages': self.get_memory(),
        }

    def health_check(self) -> Dict[str, Any]:
        """
        Check agent and LLM provider health.

        Returns:
            Health status
        """
        return {
            'agent_status': 'healthy',
            'llm_provider': self.llm_config.model_name,
            'provider_available': self.provider_available,
            'tools_enabled': self.tools_enabled,
            'tools_available': len(self.tool_registry.tools) if self.tool_registry else 0,
            'memory_size': len(self.memory),
            'session_duration': (datetime.now() - self.session_start).total_seconds(),
        }

    def _execute_tool_calls(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Parse and execute tool calls from LLM response.

        Tool call format: [TOOL: tool_name(param1=value1, param2=value2)]

        Args:
            response: LLM response potentially containing tool calls

        Returns:
            Dictionary with tool results
        """
        if not self.tool_registry:
            return None

        # Pattern to find tool calls: [TOOL: tool_name(args)]
        pattern = r'\[TOOL:\s*(\w+)\((.*?)\)\]'
        matches = re.findall(pattern, response)

        if not matches:
            return None

        results = {}

        for tool_name, args_str in matches:
            try:
                # Parse arguments
                args = self._parse_tool_args(args_str)
                logger.debug(f"Executing tool: {tool_name} with args: {args}")

                # Call tool
                result = self.tool_registry.call(tool_name, **args)
                results[tool_name] = result

                logger.info(f"Tool executed: {tool_name}")

            except Exception as e:
                logger.error(f"Tool execution failed ({tool_name}): {e}")
                results[tool_name] = {'error': str(e)}

        return results if results else None

    def _parse_tool_args(self, args_str: str) -> Dict[str, Any]:
        """
        Parse tool arguments from string format.

        Handles: param1=value1, param2="string value", param3=123

        Args:
            args_str: Arguments string

        Returns:
            Dictionary of parsed arguments
        """
        args = {}

        # Split by comma, but be careful with quoted strings
        parts = []
        current = ""
        in_quotes = False

        for char in args_str:
            if char == '"':
                in_quotes = not in_quotes
            if char == ',' and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += char

        if current:
            parts.append(current.strip())

        # Parse each part as key=value
        for part in parts:
            if '=' not in part:
                continue

            key, value = part.split('=', 1)
            key = key.strip()
            value = value.strip()

            # Try to parse value
            if value.startswith('"') and value.endswith('"'):
                # String
                args[key] = value[1:-1]
            elif value.lower() in ('true', 'false'):
                # Boolean
                args[key] = value.lower() == 'true'
            else:
                # Try number
                try:
                    if '.' in value:
                        args[key] = float(value)
                    else:
                        args[key] = int(value)
                except ValueError:
                    # Keep as string
                    args[key] = value

        return args
