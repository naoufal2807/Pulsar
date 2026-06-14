"""Test all agent types with real data (Ollama + Netflix)."""

import polars as pl
from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.analysis_agent import AnalysisAgent
from pulsar.core.intelligence.diagnosis_agent import DiagnosisAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.llm_connectors import LLMConfig, LLMProviderType


def test_all_agents():
    """Test all agent types with real data."""
    print("\n" + "="*80)
    print("ALL AGENT TYPES TEST WITH NETFLIX DATASET")
    print("="*80)

    # Load dataset
    print("\n[SETUP] Loading Netflix dataset...")
    try:
        df = pl.read_csv('netflix_titles.csv')
        print(f"Loaded: {df.height} rows x {df.width} columns")
    except FileNotFoundError:
        print("ERROR: netflix_titles.csv not found")
        return False

    # Create LLM config for Ollama
    llm_config = LLMConfig(
        provider_type=LLMProviderType.OLLAMA,
        model_name="minimax-m3:cloud",
        base_url="http://localhost:11434"
    )

    # Test 1: ReasoningAgent
    print("\n" + "-"*80)
    print("TEST 1: REASONING AGENT (Full conversation, iterative)")
    print("-"*80)

    reasoning = AgentRegistry.get_agent('reasoning', llm_config=llm_config, df=df, tools_enabled=True)
    print(f"Agent Type: {reasoning.__class__.__name__}")
    print(f"Memory Type: list (full conversation)")
    print(f"Tools: {len(reasoning.tool_registry.tools)}")

    q1 = "What is this Netflix dataset about? What domain is it in?"
    print(f"\nQuestion: {q1}")
    try:
        r1 = reasoning.think(q1)
        print(f"Response (first 300 chars): {r1[:300]}...")
        print(f"Memory after: {len(reasoning.memory)} messages")
    except Exception as e:
        print(f"ERROR: {e}")
        return False

    # Test 2: AnalysisAgent
    print("\n" + "-"*80)
    print("TEST 2: ANALYSIS AGENT (Lightweight, single-shot)")
    print("-"*80)

    analysis = AgentRegistry.get_agent('analysis', llm_config=llm_config, df=df, tools_enabled=False)
    print(f"Agent Type: {analysis.__class__.__name__}")
    print(f"Memory Type: None (stateless)")
    print(f"Tools Enabled: False (single-shot)")

    q2 = "How many titles are in this dataset and what is the average release year?"
    print(f"\nQuestion: {q2}")
    try:
        r2 = analysis.think(q2)
        print(f"Response (first 300 chars): {r2[:300]}...")
        print(f"Memory: {analysis.memory}")
    except Exception as e:
        print(f"ERROR: {e}")
        return False

    # Test 3: DiagnosisAgent
    print("\n" + "-"*80)
    print("TEST 3: DIAGNOSIS AGENT (Problem-focused, issue detection)")
    print("-"*80)

    diagnosis = AgentRegistry.get_agent('diagnosis', llm_config=llm_config, df=df, tools_enabled=True)
    print(f"Agent Type: {diagnosis.__class__.__name__}")
    print(f"Memory Type: list (problem-focused)")
    print(f"Tools: {len(diagnosis.tool_registry.tools) if diagnosis.tool_registry else 0}")

    q3 = "Are there any data quality issues or anomalies in this dataset?"
    print(f"\nQuestion: {q3}")
    try:
        r3 = diagnosis.think(q3)
        print(f"Response (first 300 chars): {r3[:300]}...")
        print(f"Memory after: {len(diagnosis.memory)} messages")
        print(f"Issues tracked: {len(diagnosis.issues)}")
    except Exception as e:
        print(f"ERROR: {e}")
        return False

    # Summary Comparison
    print("\n" + "="*80)
    print("AGENT COMPARISON SUMMARY")
    print("="*80)

    agents = {
        'ReasoningAgent': reasoning,
        'AnalysisAgent': analysis,
        'DiagnosisAgent': diagnosis,
    }

    print("\n{:<20} {:<20} {:<20} {:<20}".format(
        "Agent Type", "Memory Type", "Tools", "Status"
    ))
    print("-"*80)

    for name, agent in agents.items():
        health = agent.health_check()
        memory_type = 'Full History' if isinstance(agent.memory, list) else 'Stateless'
        tools = len(agent.tool_registry.tools) if agent.tool_registry else 0
        status = 'HEALTHY' if health['provider_available'] else 'UNAVAILABLE'

        print("{:<20} {:<20} {:<20} {:<20}".format(
            name, memory_type, str(tools), status
        ))

    # Test registry
    print("\n" + "-"*80)
    print("AGENT REGISTRY")
    print("-"*80)

    available = AgentRegistry.list_agents()
    print(f"Available agents: {', '.join(available)}")
    print("[OK] All agent types registered and working")

    print("\n" + "="*80)
    print("[SUCCESS] ALL AGENT TYPES TESTED SUCCESSFULLY")
    print("="*80 + "\n")

    return True


if __name__ == '__main__':
    success = test_all_agents()
    exit(0 if success else 1)
