#!/usr/bin/env python
"""
Integration test: Layer 4 with ReasoningAgent.

Tests that pre-tool checkpoints are saved and can be used for recovery.
"""

import tempfile
import uuid
from pulsar.core.intelligence.agent import ReasoningAgent

print("[OK] Layer 4: ReasoningAgent Integration")
print("=" * 60)

# Test 1: Agent saves checkpoints before tool calls
print("\n[TEST 1] Agent saves pre-tool checkpoints")
with tempfile.TemporaryDirectory() as tmpdir:
    conv_id = str(uuid.uuid4())
    agent = ReasoningAgent(
        conversation_id=conv_id,
        dataset_name="test_data",
    )

    # Add some messages to memory
    agent._add_to_memory("user", "What is the capital of France?")
    agent._add_to_memory("assistant", "Paris")

    # Save a pre-tool checkpoint
    checkpoint_id = agent._save_pre_tool_checkpoint(
        tool_name="query_db",
        parameters={"query": "SELECT * FROM cities"}
    )

    assert checkpoint_id is not None
    print(f"    Saved checkpoint: {checkpoint_id}")

    # Verify checkpoint was saved
    metadata = agent.checkpoint_manager.get_checkpoint_metadata(checkpoint_id)
    assert metadata is not None
    assert metadata.tool_attempt.tool_name == "query_db"
    assert metadata.status == "pending_recovery"
    print(f"    Tool: {metadata.tool_attempt.tool_name}")
    print(f"    Status: {metadata.status}")
    print("[PASS] Checkpoint saved")

# Test 2: Agent can restore from checkpoint
print("\n[TEST 2] Agent restores from checkpoint")
with tempfile.TemporaryDirectory() as tmpdir:
    conv_id = str(uuid.uuid4())
    agent = ReasoningAgent(
        conversation_id=conv_id,
        dataset_name="test_data",
    )

    # Add messages
    agent._add_to_memory("user", "Question 1")
    agent._add_to_memory("assistant", "Answer 1")
    agent._add_to_memory("user", "Question 2")

    initial_count = len(agent.memory)

    # Save checkpoint
    checkpoint_id = agent._save_pre_tool_checkpoint(
        tool_name="analyze_data",
        parameters={"column": "revenue"}
    )

    # Clear memory to simulate fresh agent
    agent.memory = []

    # Restore
    restored = agent.restore_from_checkpoint(checkpoint_id)
    assert restored is True
    assert len(agent.memory) == initial_count
    print(f"    Before: {initial_count} messages")
    print(f"    After restore: {len(agent.memory)} messages")
    print("[PASS] Restore successful")

# Test 3: Mark checkpoint as recovered after successful tool execution
print("\n[TEST 3] Mark checkpoint as recovered")
with tempfile.TemporaryDirectory() as tmpdir:
    conv_id = str(uuid.uuid4())
    agent = ReasoningAgent(
        conversation_id=conv_id,
        dataset_name="test_data",
    )

    agent._add_to_memory("user", "Test question")

    # Save checkpoint
    checkpoint_id = agent._save_pre_tool_checkpoint(
        tool_name="compute_stats",
        parameters={"column": "profit"}
    )

    # Mark as recovered (what happens when tool succeeds)
    agent.checkpoint_manager.mark_recovery_successful(checkpoint_id)

    metadata = agent.checkpoint_manager.get_checkpoint_metadata(checkpoint_id)
    assert metadata.status == "recovered"
    print(f"    Checkpoint status: {metadata.status}")
    print("[PASS] Mark as recovered")

# Test 4: Get recovery candidates for a conversation
print("\n[TEST 4] Get recovery candidates")
with tempfile.TemporaryDirectory() as tmpdir:
    conv_id = str(uuid.uuid4())
    agent = ReasoningAgent(
        conversation_id=conv_id,
        dataset_name="test_data",
    )

    # Create multiple checkpoints
    checkpoint_ids = []
    for i in range(3):
        agent._add_to_memory("user", f"Question {i}")
        checkpoint_id = agent._save_pre_tool_checkpoint(
            tool_name=f"tool_{i}",
            parameters={"index": i}
        )
        checkpoint_ids.append(checkpoint_id)

    # Mark first and last as recovered
    agent.checkpoint_manager.mark_recovery_successful(checkpoint_ids[0])
    agent.checkpoint_manager.mark_recovery_successful(checkpoint_ids[2])

    # Get candidates (should only be the middle one)
    candidates = agent.checkpoint_manager.get_recovery_candidates(conv_id)
    assert len(candidates) == 1
    assert candidates[0].checkpoint_id == checkpoint_ids[1]
    print(f"    Created: 3 checkpoints")
    print(f"    Recovered: 2")
    print(f"    Pending candidates: {len(candidates)}")
    print("[PASS] Recovery candidates")

# Test 5: Checkpoint contains full agent state
print("\n[TEST 5] Checkpoint contains full agent state")
with tempfile.TemporaryDirectory() as tmpdir:
    conv_id = str(uuid.uuid4())
    agent = ReasoningAgent(
        conversation_id=conv_id,
        dataset_name="test_data",
    )

    # Add several messages
    agent._add_to_memory("user", "First question")
    agent._add_to_memory("assistant", "First answer")
    agent._add_to_memory("user", "Second question")

    checkpoint_id = agent._save_pre_tool_checkpoint(
        tool_name="query_data",
        parameters={"query": "SELECT column FROM table"}
    )

    # Get checkpoint metadata
    metadata = agent.checkpoint_manager.get_checkpoint_metadata(checkpoint_id)

    # Restore and verify
    restored_state = agent.checkpoint_manager.restore_from_checkpoint(checkpoint_id)
    assert restored_state is not None
    assert restored_state["conversation_id"] == conv_id
    assert restored_state["dataset_name"] == "test_data"
    assert len(restored_state["memory"]) == 3
    print(f"    Conversation ID: {restored_state['conversation_id']}")
    print(f"    Dataset: {restored_state['dataset_name']}")
    print(f"    Messages in state: {len(restored_state['memory'])}")
    print(f"    State hash: {metadata.agent_state_hash[:16]}...")
    print("[PASS] Full state saved")

print("\n" + "=" * 60)
print("[OK] ALL LAYER 4 INTEGRATION TESTS PASSED (5/5)")
print("=" * 60)
print("\nLayer 4 Integration Status:")
print("  - Pre-tool checkpoints       [WORKING]")
print("  - State restoration          [WORKING]")
print("  - Recovery tracking          [WORKING]")
print("  - Candidate queries          [WORKING]")
print("  - Full state persistence     [WORKING]")
print("\nReady for Layer 5: Request ID Threading")
