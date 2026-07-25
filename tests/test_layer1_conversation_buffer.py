"""
Test Layer 1: ConversationBuffer - Token efficiency verification.

Verify that ConversationBuffer provides token savings through:
- Sliding window management
- Message summarization
- Token estimation
- Overflow detection
"""

import pytest
from pulsar.core.intelligence.conversation_buffer import ConversationBuffer


def test_conversation_buffer_creation():
    """Test buffer initialization."""
    buffer = ConversationBuffer(max_tokens=1000, window_size=2)

    assert buffer.max_tokens == 1000
    assert buffer.window_size == 2
    assert len(buffer.messages) == 0
    assert buffer.token_count == 0


def test_add_messages():
    """Test adding messages and token tracking."""
    buffer = ConversationBuffer(max_tokens=1000, window_size=2)

    buffer.add_message("user", "Hello, this is a test message.")
    assert len(buffer.messages) == 1
    assert buffer.token_count > 0

    buffer.add_message("assistant", "This is a response from the assistant.")
    assert len(buffer.messages) == 2
    assert buffer.token_count > 0


def test_token_estimation():
    """Test token estimation accuracy."""
    # Rule: 1 token ≈ 4 characters
    assert ConversationBuffer.estimate_tokens("hello") >= 1  # 5 chars = 1.25 tokens
    assert ConversationBuffer.estimate_tokens("x" * 400) == 100  # 400 chars = 100 tokens


def test_context_overflow_detection():
    """Test overflow detection."""
    buffer = ConversationBuffer(max_tokens=100, window_size=2)

    # Under limit
    buffer.add_message("user", "short message")
    assert not buffer.is_context_overflow()

    # Over limit
    buffer.add_message("assistant", "x" * 500)  # ~125 tokens
    assert buffer.is_context_overflow()


def test_context_window_no_summarization():
    """Test get_context_window with few messages (no summarization)."""
    buffer = ConversationBuffer(max_tokens=1000, window_size=3)

    buffer.add_message("user", "Question 1")
    buffer.add_message("assistant", "Answer 1")
    buffer.add_message("user", "Question 2")

    window = buffer.get_context_window()

    # Should return all messages (under window size)
    assert len(window) == len(buffer.messages)


def test_context_window_with_summarization():
    """Test get_context_window with many messages (triggers summarization)."""
    buffer = ConversationBuffer(max_tokens=6000, window_size=2)

    # Add 10 messages (more than window size)
    for i in range(10):
        buffer.add_message("user", f"Question {i}: " + "x" * 50)
        buffer.add_message("assistant", f"Answer {i}: " + "x" * 50)

    window = buffer.get_context_window()

    # Should have: system(1) + summary(1) + recent(2*window_size)
    # But we don't have a system message, so: summary(1) + recent(2*window_size)
    assert len(window) <= len(buffer.messages)
    assert len(window) < 10  # Should be compressed

    # Should have a summary message
    summary_found = any(
        msg.get("metadata", {}).get("type") == "summary" for msg in window
    )
    assert summary_found, "Summary message should be in window"


def test_token_savings():
    """Test message count reduction from summarization (primary Layer 1 benefit)."""
    buffer = ConversationBuffer(max_tokens=6000, window_size=2)

    # Add many messages with substantial content (to trigger summarization)
    for i in range(15):
        buffer.add_message("user", f"Question {i}: " + "x" * 100)
        buffer.add_message("assistant", f"Answer {i}: " + "y" * 100)

    # Get stats
    stats = buffer.get_context_window_stats()

    # Primary benefit: message count reduction (30 messages → ~5)
    original_messages = stats["original_messages"]
    optimized_messages = stats["optimized_messages"]

    # Should see significant message reduction
    assert optimized_messages < original_messages, \
        f"Expected {optimized_messages} messages < {original_messages}"

    compression_ratio = original_messages / optimized_messages
    assert compression_ratio > 3, \
        f"Expected compression ratio > 3x, got {compression_ratio:.1f}x"

    print(f"✓ Message compression: {original_messages} → {optimized_messages} messages "
          f"({stats['savings_ratio']} compression ratio)")


def test_usage_stats():
    """Test usage statistics reporting."""
    buffer = ConversationBuffer(max_tokens=1000, window_size=2)

    buffer.add_message("user", "test")
    buffer.add_message("assistant", "response")

    stats = buffer.get_usage_stats()

    assert "message_count" in stats
    assert "estimated_tokens" in stats
    assert "token_budget" in stats
    assert "overflow" in stats
    assert "window_size" in stats
    assert "summarization_count" in stats


def test_clear():
    """Test clearing buffer."""
    buffer = ConversationBuffer()

    buffer.add_message("user", "test")
    assert len(buffer.messages) > 0

    buffer.clear()
    assert len(buffer.messages) == 0
    assert buffer.token_count == 0


def test_export_import():
    """Test exporting and importing messages."""
    buffer1 = ConversationBuffer()

    buffer1.add_message("user", "Question 1")
    buffer1.add_message("assistant", "Answer 1")

    # Export
    messages = buffer1.export()
    assert len(messages) == 2

    # Import to new buffer
    buffer2 = ConversationBuffer()
    buffer2.import_messages(messages)

    assert len(buffer2.messages) == 2
    assert buffer2.token_count == buffer1.token_count


def test_summarization_count():
    """Test that summarization count increases."""
    buffer = ConversationBuffer(max_tokens=6000, window_size=2)

    initial_count = buffer.summarization_count

    # Add many messages to trigger summarization
    for i in range(10):
        buffer.add_message("user", f"Q{i}: " + "x" * 50)
        buffer.add_message("assistant", f"A{i}: " + "x" * 50)

    # Get context window (triggers summarization)
    buffer.get_context_window()

    # Summarization count should increase
    assert buffer.summarization_count > initial_count


if __name__ == "__main__":
    # Run tests
    test_conversation_buffer_creation()
    print("✓ Creation test passed")

    test_add_messages()
    print("✓ Add messages test passed")

    test_token_estimation()
    print("✓ Token estimation test passed")

    test_context_overflow_detection()
    print("✓ Overflow detection test passed")

    test_context_window_no_summarization()
    print("✓ Context window (no summarization) test passed")

    test_context_window_with_summarization()
    print("✓ Context window (with summarization) test passed")

    test_token_savings()
    print("✓ Token savings test passed")

    test_usage_stats()
    print("✓ Usage stats test passed")

    test_clear()
    print("✓ Clear test passed")

    test_export_import()
    print("✓ Export/import test passed")

    test_summarization_count()
    print("✓ Summarization count test passed")

    print("\n✅ All Layer 1 tests passed!")
