"""
Quick tests for Layer 2 & 3.
"""

import json
import pytest
import tempfile
from pathlib import Path
from pulsar.core.intelligence.conversation_store import ConversationStore
from pulsar.core.intelligence.tool_result_cache import ToolResultCache


class TestConversationStore:
    """Layer 2: ConversationStore tests."""

    def test_save_and_load(self):
        """Test saving and loading conversations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ConversationStore(f"{tmpdir}/test.db")

            # Save conversation
            messages = [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ]
            store.save("conv1", "agent-v1", messages, dataset_name="test_data")

            # Load conversation
            loaded = store.load("conv1")
            assert loaded is not None
            assert len(loaded["messages"]) == 2
            assert loaded["status"] == "active"

    def test_list_by_agent(self):
        """Test listing conversations by agent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ConversationStore(f"{tmpdir}/test.db")

            # Save multiple conversations
            for i in range(3):
                messages = [{"role": "user", "content": f"Message {i}"}]
                store.save(f"conv{i}", "agent-v1", messages)

            # List conversations
            convs = store.list_by_agent("agent-v1")
            assert len(convs) == 3

    def test_delete_conversation(self):
        """Test archiving conversations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ConversationStore(f"{tmpdir}/test.db")

            messages = [{"role": "user", "content": "Test"}]
            store.save("conv1", "agent-v1", messages)

            # Delete (archive)
            store.delete("conv1")

            # Should not appear in active list
            convs = store.list_by_agent("agent-v1", status="active")
            assert len(convs) == 0


class TestToolResultCache:
    """Layer 3: ToolResultCache tests."""

    def test_store_and_retrieve(self):
        """Test caching and retrieving results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ToolResultCache(tmpdir)

            # Store result
            result = {"rows": [{"id": 1, "value": "test"}] * 100}
            result_id, summary = cache.store_result(
                "query_db",
                {"query": "SELECT * FROM table"},
                result
            )

            # Summary should be short
            assert len(summary) < 200
            assert "100" in summary or "rows" in summary.lower()

            # Retrieve full result
            full = cache.get_full_result(result_id)
            assert full is not None
            assert len(full["result"]["rows"]) == 100

    def test_compression(self):
        """Test result compression."""
        cache = ToolResultCache()

        # Large result
        large_result = {"data": "x" * 50000}
        result_id, summary = cache.store_result("big_tool", {}, large_result)

        # Summary should be much shorter
        assert len(summary) < len(json.dumps(large_result))

    def test_cache_stats(self):
        """Test cache statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = ToolResultCache(tmpdir)

            result = {"data": [1, 2, 3]}
            cache.store_result("tool1", {}, result)

            stats = cache.get_stats()
            assert stats["cached_results"] == 1
            assert stats["total_size_bytes"] > 0


if __name__ == "__main__":
    import json

    # Run tests
    test = TestConversationStore()
    test.test_save_and_load()
    print("[OK] ConversationStore: save and load")

    test.test_list_by_agent()
    print("[OK] ConversationStore: list by agent")

    test.test_delete_conversation()
    print("[OK] ConversationStore: delete")

    test2 = TestToolResultCache()
    test2.test_store_and_retrieve()
    print("[OK] ToolResultCache: store and retrieve")

    test2.test_compression()
    print("[OK] ToolResultCache: compression")

    test2.test_cache_stats()
    print("[OK] ToolResultCache: stats")

    print("\n[OK] All Layer 2 & 3 tests passed!")
