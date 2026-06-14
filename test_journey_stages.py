"""Test full journey through all stages with Ollama."""

import polars as pl
from pulsar.core.llm_connectors import LLMConfig, LLMProviderType
from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.prompt_system import get_prompt_system


def test_journey_stage(stage_name, df, agent):
    """Test a single journey stage."""
    print(f"\n{'=' * 70}")
    print(f"STAGE: {stage_name.upper()}")
    print('=' * 70)

    ps = get_prompt_system()

    # Get stage prompt
    stage_prompt = ps.render_journey_stage(
        stage=stage_name,
        dataset_name='Netflix Catalog',
        domain='Entertainment/Media',
        row_count=df.height,
        column_count=df.width,
        column_names=', '.join(df.columns[:5]) + '...'
    )

    print(f"\nPrompt (first 400 chars):\n{stage_prompt[:400]}...\n")

    # Define stage-specific questions
    questions = {
        'scout': "Analyze this Netflix dataset. What IS it? What domain? What can we learn?",
        'explorer': "Identify the key patterns, entities, and leaders in this entertainment data.",
        'detective': "Find any anomalies, data quality issues, or unusual patterns.",
        'analyst': "Analyze the distribution, variability, and relationships in this data.",
        'ace': "Synthesize all findings into executive summary with key insights and recommendations."
    }

    question = questions.get(stage_name, "Analyze this data")

    # Run agent thinking
    print(f"Question: {question}\n")
    print("Agent Analysis:")
    print("-" * 70)

    try:
        response = agent.think(question)
        # Show first 800 chars
        print(response[:800] + "...\n")
    except Exception as e:
        print(f"ERROR: {e}\n")


def main():
    """Run journey test through all stages."""
    print("\n" + "=" * 70)
    print("NETFLIX DATASET JOURNEY TEST")
    print("Using: Ollama minimax-m3:cloud")
    print("=" * 70)

    # Load dataset
    print("\n[LOADING] Netflix catalog...")
    try:
        df = pl.read_csv('netflix_titles.csv')
        print(f"Loaded: {df.height} rows, {df.width} columns")
    except FileNotFoundError:
        print("ERROR: netflix_titles.csv not found")
        return

    # Create agent with Ollama
    print("\n[CREATING] ReasoningAgent with minimax-m3...")
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
    print(f"Tools: {len(agent.tool_registry.tools)}")
    print(f"LLM Status: {'HEALTHY' if agent.provider_available else 'UNAVAILABLE'}")

    # Test each stage
    stages = ['scout', 'explorer', 'detective', 'analyst', 'ace']

    for stage in stages:
        try:
            test_journey_stage(stage, df, agent)
        except Exception as e:
            print(f"ERROR in {stage}: {e}")

    # Final summary
    print("\n" + "=" * 70)
    print("JOURNEY COMPLETE")
    print("=" * 70)
    print(f"Total conversation messages: {len(agent.memory)}")
    print(f"Stages tested: {len(stages)}")
    print(f"Tools used: {len(agent.tool_registry.tools)}")
    print(f"Cache entries: {len(agent.tool_registry.cache)}")
    print(f"Session duration: {agent.health_check()['session_duration']:.2f}s")
    print("\n[SUCCESS] Journey test completed!\n")


if __name__ == '__main__':
    main()
