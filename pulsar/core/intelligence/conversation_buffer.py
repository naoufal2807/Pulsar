"""
Layer 1: ConversationBuffer - Token-efficient conversation management.

Manages conversation with sliding window + automatic summarization to maintain
token efficiency across 20+ turn conversations.

Problem solved:
- Full history re-sent to API each turn wastes tokens exponentially
- Without management, agents hit 8K context limit after 5-8 turns

Solution:
- Keep system prompt (always)
- Summarize messages beyond window (2-3 sentences each)
- Keep last 3-4 recent turns (most relevant)
- Estimate tokens before API call
- Alert on overflow

Expected savings: 75-90% token reduction after turn 5
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ConversationBuffer:
    """Token-aware conversation manager with sliding window summarization."""

    def __init__(self, max_tokens: int = 6000, window_size: int = 3):
        """
        Initialize conversation buffer.

        Args:
            max_tokens: Maximum tokens allowed in context (default: 6000/8K window)
            window_size: Number of recent turns to keep (default: 3)
        """
        self.messages: List[Dict[str, str]] = []
        self.max_tokens = max_tokens
        self.window_size = window_size
        self.token_count = 0
        self.summarization_count = 0  # Track how many times we've summarized

        logger.info(
            f"ConversationBuffer initialized: max_tokens={max_tokens}, window_size={window_size}"
        )

    def add_message(self, role: str, content: str, metadata: Optional[Dict] = None) -> None:
        """
        Add message to conversation and update token count.

        Args:
            role: "user", "assistant", "tool_result", etc.
            content: Message content
            metadata: Optional metadata (e.g., tool_name, duration)
        """
        message = {
            "role": role,
            "content": content,
        }

        if metadata:
            message["metadata"] = metadata

        self.messages.append(message)

        # Recalculate token count
        self.token_count = sum(
            self.estimate_tokens(m.get("content", "")) for m in self.messages
        )

        logger.debug(
            f"Added message: role={role}, content_len={len(content)}, "
            f"total_tokens={self.token_count}, message_count={len(self.messages)}"
        )

    @staticmethod
    def estimate_tokens(text: str) -> int:
        """
        Estimate token count for text.

        Rule of thumb: 1 token ≈ 4 characters (Claude tokenization)
        More accurate would be tiktoken, but this is fast and close enough.

        Args:
            text: Text to estimate

        Returns:
            Estimated token count
        """
        return max(1, len(text) // 4)

    def is_context_overflow(self) -> bool:
        """
        Check if token count exceeds maximum.

        Returns:
            True if overflow, False otherwise
        """
        return self.token_count > self.max_tokens

    def get_context_window(self) -> List[Dict[str, str]]:
        """
        Get optimized messages for Claude API call.

        Logic:
        - If few messages: return all (no optimization needed)
        - If many messages: keep system + recent, summarize middle

        Returns:
            List of messages optimized for token efficiency
        """
        # If under window size, return all messages (no summarization needed)
        if len(self.messages) <= self.window_size:
            return self.messages

        # Otherwise: keep system + recent, summarize middle
        system_messages = []  # Usually just one system message at start
        middle_messages = []
        recent_messages = []

        # Separate into phases
        for i, msg in enumerate(self.messages):
            if i < 1:  # First message (usually system prompt)
                system_messages.append(msg)
            elif i < len(self.messages) - self.window_size:  # Middle (to summarize)
                middle_messages.append(msg)
            else:  # Recent (keep as-is)
                recent_messages.append(msg)

        # Build output
        result = system_messages.copy()

        # Add summary if there are middle messages
        if middle_messages:
            summary = self._summarize_messages(middle_messages)
            result.append(summary)

        # Add recent messages
        result.extend(recent_messages)

        logger.debug(
            f"Context window: {len(self.messages)} messages → {len(result)} optimized "
            f"({len(system_messages)} system + 1 summary + {len(recent_messages)} recent)"
        )

        return result

    def _summarize_messages(self, messages: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Condense a list of messages into a single summary message.

        Strategy:
        - Extract key facts from each message
        - Truncate to 80 chars per message
        - Return as "user" role to maintain conversation flow

        Args:
            messages: Messages to summarize

        Returns:
            Summary as a message dict
        """
        summary_lines = ["[CONVERSATION SUMMARY - Previous turns summarized below]"]

        for msg in messages:
            role = msg.get("role", "unknown").upper()
            content = msg.get("content", "")

            # Truncate long content
            if len(content) > 100:
                content_snippet = content[:100] + "..."
            else:
                content_snippet = content

            # Format based on role
            if role == "USER":
                summary_lines.append(f"- User question: {content_snippet}")
            elif role == "ASSISTANT":
                summary_lines.append(f"- Agent response: {content_snippet}")
            elif role == "TOOL_RESULT":
                summary_lines.append(f"- Tool result: {content_snippet}")
            else:
                summary_lines.append(f"- {role}: {content_snippet}")

        self.summarization_count += 1

        summary_content = "\n".join(summary_lines)

        logger.info(
            f"Summarized {len(messages)} messages into summary "
            f"(total summaries created: {self.summarization_count})"
        )

        return {
            "role": "user",
            "content": summary_content,
            "metadata": {
                "type": "summary",
                "summarized_count": len(messages),
                "timestamp": datetime.now().isoformat(),
            },
        }

    def get_usage_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics for logging/monitoring.

        Returns:
            Dict with usage info
        """
        overflow = self.is_context_overflow()
        overhead_percent = (self.token_count / self.max_tokens * 100) if self.max_tokens else 0

        return {
            "message_count": len(self.messages),
            "estimated_tokens": self.token_count,
            "token_budget": self.max_tokens,
            "utilization_percent": overhead_percent,
            "overflow": overflow,
            "window_size": self.window_size,
            "summarization_count": self.summarization_count,
        }

    def get_context_window_stats(self) -> Dict[str, Any]:
        """
        Get stats about the optimized context window.

        Returns:
            Dict with window statistics
        """
        window = self.get_context_window()
        window_tokens = sum(self.estimate_tokens(m.get("content", "")) for m in window)

        original_tokens = self.token_count
        savings_percent = ((original_tokens - window_tokens) / original_tokens * 100) if original_tokens else 0

        return {
            "original_messages": len(self.messages),
            "optimized_messages": len(window),
            "original_tokens": original_tokens,
            "optimized_tokens": window_tokens,
            "savings_percent": savings_percent,
            "savings_ratio": f"{original_tokens / max(1, window_tokens):.1f}x",
        }

    def clear(self) -> None:
        """Clear all messages (start fresh conversation)."""
        self.messages = []
        self.token_count = 0
        logger.info("ConversationBuffer cleared")

    def export(self) -> List[Dict[str, str]]:
        """
        Export full conversation (for persistence).

        Returns:
            All messages including those that would be summarized
        """
        return self.messages.copy()

    def import_messages(self, messages: List[Dict[str, str]]) -> None:
        """
        Import messages (for resuming from checkpoint).

        Args:
            messages: Messages to import
        """
        self.messages = messages.copy()
        self.token_count = sum(
            self.estimate_tokens(m.get("content", "")) for m in self.messages
        )
        logger.info(f"Imported {len(messages)} messages into buffer")
