# pulsar/core/intelligence/agent.py
"""
Agent implementations: ReasoningAgent and backward-compatible Agent alias.

Exports:
- Agent: Backward-compatible alias for ReasoningAgent (for existing code)
- ReasoningAgent: Full-capability agent with conversation memory
"""

from typing import Any, Dict, List, Optional
import logging
from datetime import datetime
import json

import polars as pl

from pulsar.core.intelligence.agent_base import Agent as AgentBase, Message
from pulsar.core.intelligence.conversation_buffer import ConversationBuffer
from pulsar.core.intelligence.conversation_store import ConversationStore
from pulsar.core.intelligence.checkpoint_manager import CheckpointManager
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.request_context import (
    set_request_id, get_request_id, set_conversation_id, set_stage
)
from pulsar.core.llm_connectors import LLMConfig
from pulsar.core.intelligence.function_calls import FunctionCallParser, create_function_definitions

logger = logging.getLogger(__name__)

# Re-export base class for convenience
__all__ = ['Agent', 'ReasoningAgent', 'Message']


class ReasoningAgent(AgentBase):
    """
    Full-capability agent with conversation memory.

    Maintains full conversation history and iterates with tools.
    Memory strategy: Full conversation history (stores all messages).
    """

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        system_prompt: Optional[str] = None,
        df: Optional[pl.DataFrame] = None,
        tools_enabled: bool = True,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        stage_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        """
        Initialize reasoning agent with full conversation memory (Layer 1-5.5).

        Args:
            conversation_id: Optional ID for resuming conversations (Layer 2)
            dataset_name: Optional dataset name for audit trail (Layer 2)
            stage_name: Optional stage name for journey execution (Layer 5.5)
            shared_state: Optional shared state store for agent communication (Layer 5.5)
        """
        super().__init__(llm_config, system_prompt, df, tools_enabled)

        # Layer 2: Persistent storage
        self.store = ConversationStore()
        self.conversation_id = conversation_id
        self.dataset_name = dataset_name

        # Layer 4: Pre-tool checkpoints for recovery
        self.checkpoint_manager = CheckpointManager()

        # Layer 5.5: Shared state for agent communication
        self.stage_name = stage_name
        self.shared_state = shared_state or SharedStateStore()

    def _init_memory(self) -> None:
        """Initialize full conversation memory with ConversationBuffer (Layer 1)."""
        # Use ConversationBuffer for token-efficient memory management
        # This provides 4x token savings after turn 5 through automatic summarization
        self.buffer = ConversationBuffer(max_tokens=6000, window_size=3)

        # Keep backward compatibility - export buffer as memory list
        self.memory: List[Message] = []

    def _default_system_prompt(self) -> str:
        """Default system prompt with tool definitions if enabled."""
        prompt = super()._default_system_prompt()

        # Add function calling instructions if tools enabled
        if self.tools_enabled and self.tool_registry:
            prompt += "\n\n" + create_function_definitions()

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
            # Layer 5: Set request context for end-to-end tracing
            if not get_request_id():
                set_request_id(self.conversation_id or "no-conv")
            if self.conversation_id:
                set_conversation_id(self.conversation_id)
            if self.stage_name:
                set_stage(self.stage_name)

            # Layer 5.5: Load context from shared state
            shared_context = self._load_from_shared_state()
            full_context = {**(context or {}), **shared_context}

            # Store user question in memory
            self._add_to_memory("user", question)

            # Iteratively call LLM and tools
            current_question = question
            iteration = 0
            final_response = ""

            while iteration < max_iterations:
                # Build prompt with memory context (use full_context from shared state)
                prompt = self._build_prompt(current_question, full_context)

                # Get response from LLM
                response = self.llm_provider.generate(prompt)

                # Check if response contains function calls (JSON format)
                logger.debug(f"LLM Response (first 200 chars): {response[:200]}")
                has_calls = FunctionCallParser.response_has_function_calls(response)
                logger.debug(f"Function calls detected: {has_calls}")

                if has_calls and self.tools_enabled:
                    logger.info("Function calls detected - parsing and executing")
                    # Parse and execute function calls
                    tool_results = self._execute_function_calls(response)

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

            # Layer 5.5: Extract and store findings in shared state
            self._extract_and_store_findings(final_response)

            # Layer 2: Persist conversation to store
            self._persist_conversation()

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
        """
        Build prompt with token-efficient memory context (Layer 1).

        Uses ConversationBuffer.get_context_window() to get optimized messages
        that maintain token efficiency through sliding window + summarization.
        """
        prompt_parts = [self.system_prompt]

        # Add dataset summary if DataFrame is available
        if self.df is not None:
            prompt_parts.append(self._get_data_summary())

        # Get token-optimized conversation window (Layer 1: ConversationBuffer)
        # This returns: system + summary of old messages + recent messages
        # Savings: 75-90% after turn 5
        if hasattr(self, 'buffer') and self.buffer.messages:
            prompt_parts.append("\nConversation history:")
            window = self.buffer.get_context_window()

            # Skip system message (already in system_prompt)
            for msg in window[1:]:
                content = msg.get("content", "")
                role = msg.get("role", "user").upper()
                prompt_parts.append(f"{role}: {content}")

            # Log token efficiency
            stats = self.buffer.get_context_window_stats()
            if stats['savings_percent'] > 10:
                logger.debug(
                    f"Buffer optimization: {stats['original_tokens']} → "
                    f"{stats['optimized_tokens']} tokens ({stats['savings_percent']:.0f}% saved, "
                    f"{stats['savings_ratio']} compression ratio)"
                )

        # Add context if provided
        if context:
            prompt_parts.append("\nContext:")
            for key, value in context.items():
                prompt_parts.append(f"- {key}: {value}")

        # Add current question
        prompt_parts.append(f"\nYour turn to respond to: {question}")

        return "\n".join(prompt_parts)

    def _get_data_summary(self) -> str:
        """Generate a summary of the dataset for inclusion in prompts."""
        if self.df is None:
            return ""

        try:
            summary_parts = ["\nDATASET SUMMARY:"]
            summary_parts.append(f"Shape: {self.df.shape[0]} rows, {self.df.shape[1]} columns")

            # Column info
            summary_parts.append("Columns:")
            for col in self.df.columns[:20]:  # Limit to first 20 columns
                dtype = str(self.df[col].dtype)
                null_count = self.df[col].null_count()
                null_pct = (null_count / self.df.shape[0] * 100) if self.df.shape[0] > 0 else 0
                summary_parts.append(f"  - {col}: {dtype} (null: {null_pct:.1f}%)")

            # Sample data
            summary_parts.append("\nFirst few rows:")
            sample = self.df.head(3).to_dicts()
            for i, row in enumerate(sample, 1):
                summary_parts.append(f"  Row {i}: {row}")

            summary_parts.append("")
            return "\n".join(summary_parts)
        except Exception as e:
            logger.error(f"Failed to generate data summary: {e}")
            return "\nDATASET: (Unable to generate summary)"

    def _add_to_memory(self, role: str, content: str, metadata: Optional[Dict] = None):
        """Add message to conversation memory (Layer 1: ConversationBuffer)."""
        message = Message(
            role=role,
            content=content,
            timestamp=datetime.now(),
            metadata=metadata or {},
        )

        # Add to buffer (Layer 1 - token efficient)
        if hasattr(self, 'buffer'):
            self.buffer.add_message(role, content, metadata)

        # Keep in memory list for backward compatibility
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

    def _load_from_shared_state(self) -> Dict[str, Any]:
        """
        Load required keys from shared state (Layer 5.5).

        Returns:
            Dictionary of context from shared state
        """
        if not self.stage_name or not self.shared_state:
            return {}

        required_keys = SharedStateStore.get_required_keys(self.stage_name)
        context = {}

        for key in required_keys:
            value = self.shared_state.get(key, stage=self.stage_name)
            if value:
                context[key] = value
                logger.debug(f"Loaded from shared state: {key}")
            else:
                logger.debug(f"Required key not found: {key}")

        return context

    def _extract_and_store_findings(self, response: str) -> None:
        """
        Extract key findings from response and store in shared state (Layer 5.5).

        Args:
            response: LLM response text
        """
        if not self.stage_name or not self.shared_state:
            return

        findings = {
            f"{self.stage_name}.response_summary": response[:500],
            f"{self.stage_name}.findings": self._parse_findings(response),
            f"{self.stage_name}.issues": self._parse_issues(response),
        }

        for key, value in findings.items():
            if value:  # Only store non-empty values
                self.shared_state.set(key, value, stage=self.stage_name)
                logger.info(f"Stored to shared state: {key}")

    def _parse_findings(self, response: str) -> List[str]:
        """Extract key findings from LLM response."""
        findings = []
        for line in response.split('\n'):
            s = line.strip()
            if not s:
                continue
            if any(s.startswith(f"{i}.") for i in range(1, 10)):
                findings.append(s)
            elif s.startswith(('- ', '* ', '• ', '· ')):
                findings.append(s.lstrip('-*•· ').strip())
        return findings[:5]

    def _parse_issues(self, response: str) -> List[str]:
        """Extract issues/problems from LLM response."""
        issues = []
        lines = response.split('\n')
        for line in lines:
            if any(keyword in line.lower() for keyword in ['issue', 'problem', 'error', 'warning', 'critical', 'blocking']):
                issues.append(line.strip())
        return issues[:3]  # Top 3 issues

    def _save_pre_tool_checkpoint(self, tool_name: str, parameters: Dict[str, Any]) -> Optional[str]:
        """
        Save agent state before tool execution (Layer 4).

        Creates a checkpoint that can be used to recover if tool times out.

        Args:
            tool_name: Name of tool about to execute
            parameters: Tool parameters

        Returns:
            checkpoint_id for recovery, or None if checkpointing failed
        """
        if not self.conversation_id:
            return None

        try:
            agent_state = {
                "conversation_id": self.conversation_id,
                "dataset_name": self.dataset_name,
                "buffer": self.buffer.export() if hasattr(self, 'buffer') else [],
                "memory": self.get_memory(),
                "llm_provider": self.llm_provider.__class__.__name__ if self.llm_provider else None,
            }

            checkpoint_id = self.checkpoint_manager.save_pre_tool_checkpoint(
                agent_state=agent_state,
                tool_name=tool_name,
                parameters=parameters,
                conversation_id=self.conversation_id,
                dataset_name=self.dataset_name,
                description=f"Before executing tool: {tool_name}",
            )

            logger.debug(f"Pre-tool checkpoint saved: {checkpoint_id} for {tool_name}")
            return checkpoint_id

        except Exception as e:
            logger.error(f"Failed to save pre-tool checkpoint: {e}")
            return None

    def restore_from_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Restore agent state from a checkpoint (Layer 4).

        Used for recovery after tool timeout/failure.

        Args:
            checkpoint_id: Checkpoint ID to restore from

        Returns:
            True if restored, False if checkpoint not found/invalid
        """
        try:
            agent_state = self.checkpoint_manager.restore_from_checkpoint(checkpoint_id)
            if not agent_state:
                logger.warning(f"Failed to restore from checkpoint: {checkpoint_id}")
                return False

            # Restore buffer
            if hasattr(self, 'buffer') and agent_state.get('buffer'):
                self.buffer.import_messages(agent_state['buffer'])

            # Restore memory
            if agent_state.get('memory'):
                self.memory = []
                for msg_dict in agent_state['memory']:
                    msg = Message(
                        role=msg_dict['role'],
                        content=msg_dict['content'],
                        timestamp=datetime.fromisoformat(msg_dict.get('timestamp', datetime.now().isoformat())),
                        metadata=msg_dict.get('metadata', {}),
                    )
                    self.memory.append(msg)

            logger.info(f"Restored from checkpoint: {checkpoint_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to restore from checkpoint: {e}")
            return False

    def _persist_conversation(self) -> None:
        """
        Persist conversation to store (Layer 2).

        Saves full conversation history to SQLite for resume/audit/debugging.
        """
        if not self.conversation_id or not hasattr(self, 'store'):
            return

        try:
            self.store.save(
                conversation_id=self.conversation_id,
                agent_id=self.__class__.__name__,
                messages=self.buffer.export() if hasattr(self, 'buffer') else self.memory,
                dataset_name=self.dataset_name,
                status="active",
            )
            logger.debug(f"Conversation persisted: {self.conversation_id}")
        except Exception as e:
            logger.error(f"Failed to persist conversation: {e}")

    def load_conversation(self, conversation_id: str) -> bool:
        """
        Load conversation from store (Layer 2).

        Args:
            conversation_id: Conversation ID to resume

        Returns:
            True if loaded, False otherwise
        """
        try:
            data = self.store.load(conversation_id)
            if not data:
                logger.warning(f"Conversation not found: {conversation_id}")
                return False

            self.conversation_id = conversation_id
            messages = data["messages"]

            # Restore to buffer
            if hasattr(self, 'buffer'):
                self.buffer.import_messages(messages)

            logger.info(f"Loaded conversation: {conversation_id} ({len(messages)} messages)")
            return True

        except Exception as e:
            logger.error(f"Failed to load conversation: {e}")
            return False

    def health_check(self) -> Dict[str, Any]:
        """Check agent and LLM provider health with buffer + store stats (Layer 1-2)."""
        base_health = super().health_check()

        buffer_stats = {}
        if hasattr(self, 'buffer'):
            buffer_stats = self.buffer.get_usage_stats()

        base_health.update({
            'memory_size': len(self.memory),
            'buffer_stats': buffer_stats,
            'conversation_id': self.conversation_id if hasattr(self, 'conversation_id') else None,
        })
        return base_health

    def _execute_function_calls(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Parse and execute function calls from LLM response.

        Function call format (JSON):
        {
          "function": "compute_statistics",
          "parameters": {"column": "sales"}
        }

        Args:
            response: LLM response potentially containing function calls

        Returns:
            Dictionary with function results
        """
        if not self.tool_registry:
            return None

        try:
            # Parse function calls from response
            function_calls = FunctionCallParser.extract_function_calls(response)

            if not function_calls:
                logger.debug("No function calls found in response")
                return None

            logger.info(f"Found {len(function_calls)} function call(s)")
            results = {}

            for call in function_calls:
                try:
                    logger.debug(f"Executing function: {call.function} with params: {call.parameters}")

                    # Layer 4: Save checkpoint before tool call for recovery
                    checkpoint_id = self._save_pre_tool_checkpoint(call.function, call.parameters)

                    # Call function via tool registry
                    result = self.tool_registry.call(call.function, **call.parameters)
                    results[call.function] = result

                    # Mark checkpoint as successful
                    if checkpoint_id:
                        self.checkpoint_manager.mark_recovery_successful(checkpoint_id)

                    logger.info(f"Function executed successfully: {call.function}")

                except Exception as e:
                    logger.error(f"Function execution failed ({call.function}): {e}")
                    results[call.function] = {'error': str(e)}
                    # Note: checkpoint remains in pending_recovery state for potential retry

            return results if results else None

        except Exception as e:
            logger.error(f"Error parsing function calls: {e}")
            return None


# Backward-compatible alias: Agent now refers to ReasoningAgent
Agent = ReasoningAgent
