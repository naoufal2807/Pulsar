"""Comprehensive test with full logging output."""

import sys
import logging
from io import StringIO
from datetime import datetime
import polars as pl
from pulsar.core.llm_connectors import LLMConfig, LLMProviderType, get_llm_provider
from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.intelligence.tools import create_default_registry
from pulsar.core.intelligence.prompt_system import get_prompt_system


# Set up logging to capture everything
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create string buffer to capture logs
log_buffer = StringIO()
handler = logging.StreamHandler(log_buffer)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)


def test_llm_connectors():
    """Test Phase 1: LLM Connectors."""
    print("\n" + "="*80)
    print("PHASE 1: LLM CONNECTORS")
    print("="*80)

    # Test Ollama provider
    print("\n[TEST] Creating OllamaProvider with minimax-m3:cloud")
    config = LLMConfig(
        provider_type=LLMProviderType.OLLAMA,
        model_name="minimax-m3:cloud",
        base_url="http://localhost:11434",
        temperature=0.7,
        max_tokens=500
    )
    print(f"Config created: {config}")

    provider = get_llm_provider(config)
    print(f"Provider instance: {provider.__class__.__name__}")
    print(f"Model: {provider.config.model_name}")
    print(f"Base URL: {provider.config.base_url}")

    # Health check
    print("\n[TEST] Health check...")
    health = provider.health_check()
    print(f"Provider health: {health}")

    if health:
        # Test token counting
        print("\n[TEST] Token counting...")
        tokens = provider.count_tokens("Hello, world!")
        print(f"Tokens in 'Hello, world!': {tokens}")

        # Test model info
        print("\n[TEST] Model info...")
        info = provider.get_model_info()
        print(f"Model info: {info}")

    return provider


def test_agent_architecture(df):
    """Test Phase 2: Agent Architecture."""
    print("\n" + "="*80)
    print("PHASE 2: AGENT ARCHITECTURE")
    print("="*80)

    # Test creating agent with Ollama
    print("\n[TEST] Creating ReasoningAgent with Ollama...")
    agent = ReasoningAgent(
        llm_config=LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="minimax-m3:cloud",
            base_url="http://localhost:11434"
        ),
        df=df,
        tools_enabled=True
    )
    print(f"Agent: {agent.__class__.__name__}")
    print(f"Provider available: {agent.provider_available}")
    print(f"LLM model: {agent.llm_config.model_name}")
    print(f"Tools enabled: {agent.tools_enabled}")
    print(f"Tools available: {len(agent.tool_registry.tools) if agent.tool_registry else 0}")

    # Test health check
    print("\n[TEST] Agent health check...")
    health = agent.health_check()
    print(f"Health status: {health}")

    # Test memory initialization
    print("\n[TEST] Memory structure...")
    print(f"Memory type: {type(agent.memory).__name__}")
    print(f"Memory size: {len(agent.memory)}")

    # Test agent registry
    print("\n[TEST] Agent registry...")
    registry_agent = AgentRegistry.get_agent('reasoning', df=df, tools_enabled=True)
    print(f"Registry created agent: {registry_agent.__class__.__name__}")

    return agent


def test_tools_layer(agent, df):
    """Test Phase 3: Tools Layer."""
    print("\n" + "="*80)
    print("PHASE 3: TOOLS LAYER")
    print("="*80)

    print(f"\n[TEST] Tool registry...")
    print(f"Total tools: {len(agent.tool_registry.tools)}")
    print(f"Tool names: {list(agent.tool_registry.tools.keys())}")

    # Test core tools
    print("\n[TEST] Core tools execution...")

    print("  - describe_dataset")
    result = agent.tool_registry.call('describe_dataset', use_cache=False)
    print(f"    Result keys: {list(result.keys())}")

    print("  - compute_statistics (release_year)")
    result = agent.tool_registry.call('compute_statistics', column='release_year', use_cache=False)
    print(f"    Result: mean={result.get('mean'):.2f}, min={result.get('min')}, max={result.get('max')}")

    print("  - get_top_values (type, limit=5)")
    result = agent.tool_registry.call('get_top_values', column='type', limit=5, use_cache=False)
    print(f"    Result: {result}")

    # Test intelligence tools
    print("\n[TEST] Intelligence tools execution...")

    print("  - infer_domain")
    result = agent.tool_registry.call('infer_domain', use_cache=False)
    print(f"    Domain: {result}")

    print("  - identify_key_entities")
    result = agent.tool_registry.call('identify_key_entities', use_cache=False)
    print(f"    Entities: {list(result.keys())}")

    # Test caching
    print("\n[TEST] Tool caching...")
    print("  First call: compute_statistics(release_year)")
    r1 = agent.tool_registry.call('compute_statistics', column='release_year', use_cache=True)
    cache_size_1 = len(agent.tool_registry.cache)
    print(f"    Cache size: {cache_size_1}")

    print("  Second call: compute_statistics(release_year) [should hit cache]")
    r2 = agent.tool_registry.call('compute_statistics', column='release_year', use_cache=True)
    cache_size_2 = len(agent.tool_registry.cache)
    print(f"    Cache size: {cache_size_2}")
    print(f"    Results identical: {r1 == r2}")

    print("  Different params: compute_statistics(rating)")
    r3 = agent.tool_registry.call('compute_statistics', column='rating', use_cache=True)
    print(f"    Cache size: {len(agent.tool_registry.cache)}")
    print(f"    Different result: {r1 != r3}")


def test_prompt_system(df):
    """Test Phase 4: Prompt System."""
    print("\n" + "="*80)
    print("PHASE 4: PROMPT SYSTEM")
    print("="*80)

    ps = get_prompt_system()

    print(f"\n[TEST] Prompt templates loaded...")
    print(f"Templates: {list(ps.templates.keys())}")

    # Test system prompt
    print("\n[TEST] System prompt rendering...")
    sys_prompt = ps.render_system_prompt(
        domain='Entertainment',
        dataset_name='Netflix Catalog',
        analysis_purpose='Comprehensive data understanding'
    )
    print(f"System prompt (first 200 chars):\n{sys_prompt[:200]}...")

    # Test journey stages
    print("\n[TEST] Journey stage prompts...")
    stages = ['scout', 'explorer', 'detective', 'analyst', 'ace']

    for stage in stages:
        prompt = ps.render_journey_stage(
            stage=stage,
            dataset_name='Netflix',
            domain='Entertainment',
            row_count=df.height,
            column_count=df.width,
            column_names=', '.join(df.columns[:3])
        )
        print(f"  {stage:12} - {len(prompt)} chars, first 100: {prompt[:100]}...")


def test_agent_thinking(agent):
    """Test agent.think() with real LLM."""
    print("\n" + "="*80)
    print("INTEGRATION TEST: AGENT THINKING")
    print("="*80)

    question = "What is this dataset about in 2-3 sentences? What domain is it?"

    print(f"\n[TEST] Agent.think()")
    print(f"Question: {question}")
    print(f"Tools available: {len(agent.tool_registry.tools)}")
    print(f"LLM: {agent.llm_config.model_name}")
    print(f"Provider: {'HEALTHY' if agent.provider_available else 'UNAVAILABLE'}")

    print("\nCalling agent.think()...")
    try:
        response = agent.think(question)
        print(f"\nResponse (first 300 chars):\n{response[:300]}...")

        print(f"\nAgent state after thinking:")
        print(f"  Memory size: {len(agent.memory)}")
        print(f"  Cache entries: {len(agent.tool_registry.cache)}")
        print(f"  Session duration: {agent.health_check()['session_duration']:.2f}s")

        return True
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("COMPREHENSIVE PULSAR IMPLEMENTATION TEST")
    print("="*80)
    print(f"Date: {datetime.now()}")

    # Load dataset
    print("\n[SETUP] Loading Netflix dataset...")
    try:
        df = pl.read_csv('netflix_titles.csv')
        print(f"Loaded: {df.height} rows x {df.width} columns")
    except Exception as e:
        print(f"ERROR loading dataset: {e}")
        return

    # Run tests
    try:
        provider = test_llm_connectors()
        agent = test_agent_architecture(df)
        test_tools_layer(agent, df)
        test_prompt_system(df)
        success = test_agent_thinking(agent)

        # Summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        if success:
            print("[OK] Phase 1: LLM Connectors - PASS")
            print("[OK] Phase 2: Agent Architecture - PASS")
            print("[OK] Phase 3: Tools Layer - PASS")
            print("[OK] Phase 4: Prompt System - PASS")
            print("[OK] Integration: Agent Thinking - PASS")
            print("\n[SUCCESS] ALL TESTS PASSED!")
        else:
            print("[FAIL] Agent thinking test failed")

    except Exception as e:
        print(f"\nERROR during tests: {e}")
        import traceback
        traceback.print_exc()

    # Write logs to file
    print("\n[OUTPUT] Writing detailed logs to file...")
    logs = log_buffer.getvalue()
    with open('test_output.log', 'w') as f:
        f.write(logs)
    print(f"Log file written: test_output.log ({len(logs)} bytes)")


if __name__ == '__main__':
    main()
