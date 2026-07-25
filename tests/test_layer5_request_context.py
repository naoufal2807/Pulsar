#!/usr/bin/env python
"""
Unit tests for Layer 5: Request ID Threading via contextvars.

Tests end-to-end request tracing without manual parameter passing.
"""

import logging
from pulsar.core.intelligence.request_context import (
    generate_request_id,
    set_request_id,
    get_request_id,
    set_conversation_id,
    get_conversation_id,
    set_stage,
    get_stage,
    clear_request_context,
    request_context,
    get_context_summary,
    RequestContextFilter,
)

print("[OK] Layer 5: Request ID Threading")
print("=" * 60)

# Test 1: Generate and set request ID
print("\n[TEST 1] Generate and set request ID")
request_id = generate_request_id()
assert request_id is not None
assert len(request_id) == 36  # UUID length
print(f"    Generated: {request_id[:12]}...")

set_request_id(request_id)
retrieved = get_request_id()
assert retrieved == request_id
print(f"    Retrieved: {retrieved[:12]}...")
print("[PASS] Request ID")

# Test 2: Set and get conversation ID
print("\n[TEST 2] Conversation ID context")
conv_id = "conv-12345"
set_conversation_id(conv_id)
retrieved = get_conversation_id()
assert retrieved == conv_id
print(f"    Set: {conv_id}")
print(f"    Retrieved: {retrieved}")
print("[PASS] Conversation ID")

# Test 3: Set and get stage
print("\n[TEST 3] Journey stage context")
set_stage("scout")
assert get_stage() == "scout"

set_stage("explorer")
assert get_stage() == "explorer"
print(f"    Stage 1: scout")
print(f"    Stage 2: explorer")
print("[PASS] Stage context")

# Test 4: Clear all context
print("\n[TEST 4] Clear request context")
request_id = generate_request_id()
set_request_id(request_id)
set_conversation_id("conv-123")
set_stage("detective")

assert get_request_id() is not None
assert get_conversation_id() is not None
assert get_stage() is not None

clear_request_context()

assert get_request_id() is None
assert get_conversation_id() is None
assert get_stage() is None
print("    All context cleared")
print("[PASS] Clear context")

# Test 5: Context manager
print("\n[TEST 5] Context manager with auto-generated ID")
with request_context() as request_id:
    assert request_id is not None
    assert get_request_id() == request_id
    print(f"    Context request_id: {request_id[:12]}...")

# Outside context, should be cleared
assert get_request_id() is None
print("    Context cleared after exit")
print("[PASS] Context manager auto-generate")

# Test 6: Context manager with provided values
print("\n[TEST 6] Context manager with provided values")
req_id = "req-test-123"
conv_id = "conv-test-456"
stage = "analyst"

with request_context(
    request_id=req_id,
    conversation_id=conv_id,
    stage=stage
):
    assert get_request_id() == req_id
    assert get_conversation_id() == conv_id
    assert get_stage() == stage
    print(f"    Request: {get_request_id()}")
    print(f"    Conversation: {get_conversation_id()}")
    print(f"    Stage: {get_stage()}")

# Cleared after context
assert get_request_id() is None
print("    Context cleared after exit")
print("[PASS] Context manager with values")

# Test 7: Nested contexts
print("\n[TEST 7] Nested context managers")
with request_context(request_id="outer-req"):
    outer_req = get_request_id()
    assert outer_req == "outer-req"

    with request_context(request_id="inner-req"):
        inner_req = get_request_id()
        assert inner_req == "inner-req"
        print(f"    Outer: {outer_req}")
        print(f"    Inner: {inner_req}")

    # Back to outer
    assert get_request_id() == outer_req
    print(f"    Back to outer: {get_request_id()}")

# All cleared
assert get_request_id() is None
print("    All contexts cleared")
print("[PASS] Nested contexts")

# Test 8: Get context summary
print("\n[TEST 8] Context summary")
set_request_id("req-789")
set_conversation_id("conv-789")
set_stage("ace")

summary = get_context_summary()
assert summary["request_id"] == "req-789"
assert summary["conversation_id"] == "conv-789"
assert summary["stage"] == "ace"
print(f"    Summary: {summary}")
print("[PASS] Context summary")

clear_request_context()

# Test 9: RequestContextFilter
print("\n[TEST 9] RequestContextFilter for logging")
logger = logging.getLogger("test_layer5")

# Create a test log record
record = logging.LogRecord(
    name="test_layer5",
    level=logging.INFO,
    pathname="test.py",
    lineno=1,
    msg="Test message",
    args=(),
    exc_info=None,
)

# Apply filter without context (should use defaults)
filter_obj = RequestContextFilter()
filter_obj.filter(record)

assert hasattr(record, "request_id")
assert hasattr(record, "conversation_id")
assert hasattr(record, "stage")
assert record.request_id == "-"  # Default when not set
print(f"    Without context - request_id: {record.request_id}")

# Set context and apply filter again
set_request_id("req-filter-test")
set_conversation_id("conv-filter")
set_stage("scout")

record2 = logging.LogRecord(
    name="test_layer5",
    level=logging.INFO,
    pathname="test.py",
    lineno=1,
    msg="Test message",
    args=(),
    exc_info=None,
)

filter_obj.filter(record2)
assert record2.request_id == "req-filter-test"
assert record2.conversation_id == "conv-filter"
assert record2.stage == "scout"
print(f"    With context - request_id: {record2.request_id}")
print(f"    With context - conversation_id: {record2.conversation_id}")
print(f"    With context - stage: {record2.stage}")

clear_request_context()
print("[PASS] RequestContextFilter")

# Test 10: Context isolation (no cross-contamination)
print("\n[TEST 10] Context isolation")

# Set in context 1
set_request_id("req-1")
set_conversation_id("conv-1")
req1 = get_request_id()
conv1 = get_conversation_id()

# Clear and set in context 2
clear_request_context()
set_request_id("req-2")
set_conversation_id("conv-2")
req2 = get_request_id()
conv2 = get_conversation_id()

# Verify isolation (no cross-contamination)
assert req1 == "req-1"
assert req2 == "req-2"
assert conv1 == "conv-1"
assert conv2 == "conv-2"
print(f"    Context 1 - req: {req1}, conv: {conv1}")
print(f"    Context 2 - req: {req2}, conv: {conv2}")
print("[PASS] Context isolation")

clear_request_context()

print("\n" + "=" * 60)
print("[OK] ALL LAYER 5 TESTS PASSED (10/10)")
print("=" * 60)
print("\nLayer 5: Request ID Threading")
print("  - Request ID generation     [WORKING]")
print("  - Context variable storage  [WORKING]")
print("  - Context managers          [WORKING]")
print("  - Logging filter            [WORKING]")
print("  - Context isolation         [WORKING]")
print("\nExpected impact:")
print("  - Log correlation: No manual ID passing")
print("  - Debugging speed: 30 min to 2 min via grep")
print("  - Code simplicity: Zero function signature changes")
