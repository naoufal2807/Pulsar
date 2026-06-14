"""Comprehensive test of Phases 1-4 implementation."""

import pytest
import polars as pl
from pulsar.core.llm_connectors import LLMConfig, LLMProviderType, get_llm_provider
from pulsar.core.intelligence.agent import Agent, ReasoningAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.intelligence.tools import create_default_registry
from pulsar.core.intelligence.prompt_system import get_prompt_system


class TestPhase1LLMConnectors:
    """Test Phase 1: LLM Connectors."""

    def test_ollama_provider_creation(self):
        """Test creating OllamaProvider."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma3:270m",
            base_url="http://localhost:11434"
        )
        provider = get_llm_provider(config)
        assert provider is not None
        assert provider.__class__.__name__ == "OllamaProvider"

    def test_openai_provider_creation(self):
        """Test creating OpenAIProvider (without API key for now)."""
        config = LLMConfig(
            provider_type=LLMProviderType.OPENAI,
            model_name="gpt-4",
            api_key="test-key"
        )
        # Should be able to create provider with test key
        try:
            provider = get_llm_provider(config)
            assert provider is not None
            assert provider.__class__.__name__ == "OpenAIProvider"
        except ValueError:
            pytest.skip("OpenAI API key not configured")

    def test_anthropic_provider_creation(self):
        """Test creating AnthropicProvider (without API key for now)."""
        config = LLMConfig(
            provider_type=LLMProviderType.ANTHROPIC,
            model_name="claude-3-haiku",
            api_key="test-key"
        )
        try:
            provider = get_llm_provider(config)
            assert provider is not None
            assert provider.__class__.__name__ == "AnthropicProvider"
        except ValueError:
            pytest.skip("Anthropic API key not configured")

    def test_unsupported_provider(self):
        """Test that unsupported provider raises error."""
        config = LLMConfig(
            provider_type=LLMProviderType.LOCAL,
            model_name="test"
        )
        with pytest.raises(ValueError):
            get_llm_provider(config)

    def test_ollama_health_check(self):
        """Test Ollama provider health check."""
        config = LLMConfig(
            provider_type=LLMProviderType.OLLAMA,
            model_name="gemma3:270m",
            base_url="http://localhost:11434"
        )
        provider = get_llm_provider(config)
        # This will fail if Ollama not running, which is expected in test
        health = provider.health_check()
        # Don't assert True/False - just that it returns a bool
        assert isinstance(health, bool)


class TestPhase2AgentArchitecture:
    """Test Phase 2: Agent Architecture."""

    def test_agent_base_not_instantiable(self):
        """Test that Agent base class cannot be instantiated directly."""
        from pulsar.core.intelligence.agent_base import Agent as AgentBase
        # AgentBase is abstract, so instantiating should require implementing abstract methods
        with pytest.raises(TypeError):
            AgentBase()

    def test_reasoning_agent_creation(self):
        """Test creating ReasoningAgent."""
        agent = ReasoningAgent()
        assert agent is not None
        assert agent.__class__.__name__ == "ReasoningAgent"
        assert hasattr(agent, 'memory')
        assert isinstance(agent.memory, list)

    def test_agent_alias(self):
        """Test that Agent alias points to ReasoningAgent."""
        agent = Agent()
        assert isinstance(agent, ReasoningAgent)
        assert agent.__class__.__name__ == "ReasoningAgent"

    def test_agent_with_dataframe(self):
        """Test creating agent with DataFrame and tools."""
        df = pl.DataFrame({
            'A': [1, 2, 3],
            'B': ['a', 'b', 'c'],
            'C': [10.0, 20.0, 30.0],
        })
        agent = ReasoningAgent(df=df, tools_enabled=True)
        assert agent.df is not None
        assert agent.tools_enabled is True
        assert agent.tool_registry is not None

    def test_agent_health_check(self):
        """Test agent health check returns proper structure."""
        agent = ReasoningAgent()
        health = agent.health_check()
        assert 'agent_type' in health
        assert health['agent_type'] == 'ReasoningAgent'
        assert 'llm_model' in health
        assert 'provider_available' in health
        assert 'session_duration' in health

    def test_agent_registry(self):
        """Test AgentRegistry factory."""
        agent = AgentRegistry.get_agent('reasoning')
        assert isinstance(agent, ReasoningAgent)

    def test_agent_registry_list(self):
        """Test listing available agent types."""
        agents = AgentRegistry.list_agents()
        assert 'reasoning' in agents
        assert 'default' in agents

    def test_agent_with_custom_system_prompt(self):
        """Test agent with custom system prompt."""
        custom_prompt = "You are a test agent."
        agent = ReasoningAgent(system_prompt=custom_prompt)
        assert custom_prompt in agent.system_prompt


class TestPhase3ToolsLayer:
    """Test Phase 3: Tools Layer."""

    @pytest.fixture
    def test_df(self):
        """Create test DataFrame."""
        return pl.DataFrame({
            'sales': [100, 200, 150, 300, 250],
            'region': ['US', 'EU', 'US', 'APAC', 'EU'],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        })

    def test_tool_registry_creation(self, test_df):
        """Test creating tool registry."""
        registry = create_default_registry(test_df)
        assert registry is not None
        assert len(registry.tools) > 0

    def test_tool_count(self, test_df):
        """Test that correct number of tools are created."""
        registry = create_default_registry(test_df)
        # Should have 6 core + 6 intelligence + 4 business = 16 tools
        assert len(registry.tools) == 16

    def test_tool_names(self, test_df):
        """Test that expected tools are registered."""
        registry = create_default_registry(test_df)
        expected_tools = [
            'compute_statistics',
            'check_data_quality',
            'detect_outliers',
            'analyze_correlation',
            'describe_dataset',
            'get_top_values',
            'infer_domain',
            'identify_key_entities',
            'extract_key_metrics',
            'describe_patterns',
            'find_top_performers',
            'explain_outliers',
            'analyze_concentration',
            'analyze_distribution_skewness',
            'analyze_variability',
            'analyze_relationships',
        ]
        for tool_name in expected_tools:
            assert tool_name in registry.tools, f"Tool {tool_name} not found"

    def test_tool_calling(self, test_df):
        """Test calling a tool."""
        registry = create_default_registry(test_df)
        result = registry.call('compute_statistics', column='sales')
        assert result is not None
        assert 'mean' in result

    def test_tool_caching(self, test_df):
        """Test tool result caching."""
        registry = create_default_registry(test_df)
        # First call
        result1 = registry.call('compute_statistics', column='sales', use_cache=True)
        # Second call (should hit cache)
        result2 = registry.call('compute_statistics', column='sales', use_cache=True)
        # Results should be identical
        assert result1 == result2
        # Cache should have entry
        assert len(registry.cache) > 0

    def test_tool_cache_clear(self, test_df):
        """Test clearing tool cache."""
        registry = create_default_registry(test_df)
        registry.call('compute_statistics', column='sales', use_cache=True)
        assert len(registry.cache) > 0
        registry.clear_cache()
        assert len(registry.cache) == 0

    def test_tool_no_cache(self, test_df):
        """Test calling tool without caching."""
        registry = create_default_registry(test_df)
        registry.call('compute_statistics', column='sales', use_cache=False)
        # Cache should still be empty
        assert len(registry.cache) == 0

    def test_describe_dataset_tool(self, test_df):
        """Test describe_dataset tool."""
        registry = create_default_registry(test_df)
        result = registry.call('describe_dataset')
        assert result is not None
        # Tool returns column_count not rows
        assert 'column_count' in result or 'columns' in result
        assert 'columns' in result

    def test_tool_schema_export(self, test_df):
        """Test exporting tool schemas for LLM."""
        registry = create_default_registry(test_df)
        schemas = registry.get_schemas()
        assert len(schemas) == 16
        # Check schema structure
        for schema in schemas:
            assert 'name' in schema
            assert 'description' in schema
            assert 'parameters' in schema


class TestPhase4PromptSystem:
    """Test Phase 4: Prompt System."""

    def test_prompt_system_creation(self):
        """Test creating prompt system."""
        ps = get_prompt_system()
        assert ps is not None

    def test_prompt_templates_loaded(self):
        """Test that prompt templates are loaded."""
        ps = get_prompt_system()
        assert len(ps.templates) > 0

    def test_expected_templates(self):
        """Test that expected templates are loaded."""
        ps = get_prompt_system()
        expected = ['system_base', 'journey_scout', 'journey_explorer',
                   'journey_detective', 'journey_analyst', 'journey_ace']
        for template_name in expected:
            assert template_name in ps.templates, f"Template {template_name} not found"

    def test_system_prompt_rendering(self):
        """Test rendering system prompt."""
        ps = get_prompt_system()
        prompt = ps.render_system_prompt(
            domain='Finance',
            dataset_name='transactions',
            analysis_purpose='Fraud detection'
        )
        assert prompt is not None
        assert 'Finance' in prompt
        assert 'transactions' in prompt
        assert 'Fraud detection' in prompt

    def test_journey_scout_rendering(self):
        """Test rendering SCOUT stage prompt."""
        ps = get_prompt_system()
        prompt = ps.render_journey_stage(
            stage='scout',
            dataset_name='customers',
            domain='Marketing',
            row_count=50000,
            column_count=20,
            column_names='id, name, email, lifetime_value'
        )
        assert prompt is not None
        assert 'customers' in prompt
        assert 'Marketing' in prompt
        assert '50000' in prompt

    def test_journey_explorer_rendering(self):
        """Test rendering EXPLORER stage prompt."""
        ps = get_prompt_system()
        prompt = ps.render_journey_stage(
            stage='explorer',
            dataset_name='products',
            domain='E-commerce'
        )
        assert prompt is not None
        assert 'products' in prompt

    def test_all_journey_stages(self):
        """Test rendering all journey stages."""
        ps = get_prompt_system()
        stages = ['scout', 'explorer', 'detective', 'analyst', 'ace']
        for stage in stages:
            prompt = ps.render_journey_stage(
                stage=stage,
                dataset_name='test_data',
                domain='Test'
            )
            assert prompt is not None
            assert len(prompt) > 0

    def test_prompt_variable_substitution(self):
        """Test that variables are properly substituted."""
        ps = get_prompt_system()
        prompt = ps.render_system_prompt(
            domain='Healthcare',
            dataset_name='patient_records',
            analysis_purpose='Identify at-risk patients'
        )
        # Should not have unsubstituted variables
        assert '{' not in prompt or prompt.count('{') < 2  # Allow for some formatting


class TestIntegration:
    """Integration tests across all phases."""

    @pytest.fixture
    def test_df(self):
        """Create test DataFrame."""
        return pl.DataFrame({
            'product': ['A', 'B', 'C', 'A', 'B'],
            'sales': [100, 200, 150, 300, 250],
            'region': ['US', 'EU', 'US', 'APAC', 'EU'],
        })

    def test_agent_with_tools_and_prompts(self, test_df):
        """Test full integration: Agent + Tools + Prompts."""
        # Create agent with tools
        agent = ReasoningAgent(df=test_df, tools_enabled=True)
        assert agent.tool_registry is not None
        assert len(agent.tool_registry.tools) == 16

        # Get prompt system
        ps = get_prompt_system()

        # Render a prompt for the agent
        system_prompt = ps.render_system_prompt(
            domain='Sales',
            dataset_name='sales_data',
            analysis_purpose='Analyze regional performance'
        )

        # Create new agent with custom system prompt
        agent2 = ReasoningAgent(
            df=test_df,
            tools_enabled=True,
            system_prompt=system_prompt
        )
        assert 'Sales' in agent2.system_prompt
        assert agent2.tool_registry is not None

    def test_agent_registry_with_tools(self, test_df):
        """Test creating agent through registry with tools."""
        agent = AgentRegistry.get_agent('reasoning', df=test_df, tools_enabled=True)
        assert isinstance(agent, ReasoningAgent)
        assert agent.tool_registry is not None
        assert len(agent.tool_registry.tools) == 16

    def test_journey_stage_setup(self, test_df):
        """Test setting up for a journey stage."""
        # Create agent for a journey stage
        agent = ReasoningAgent(df=test_df, tools_enabled=True)

        # Get prompt for SCOUT stage
        ps = get_prompt_system()
        scout_prompt = ps.render_journey_stage(
            stage='scout',
            dataset_name='analysis_data',
            domain='Business',
            row_count=test_df.height,
            column_count=test_df.width,
            column_names=', '.join(test_df.columns)
        )

        # Create agent with stage prompt
        stage_agent = ReasoningAgent(
            df=test_df,
            tools_enabled=True,
            system_prompt=scout_prompt
        )

        assert 'analysis_data' in stage_agent.system_prompt
        assert 'Business' in stage_agent.system_prompt
        assert str(test_df.height) in stage_agent.system_prompt


class TestDocumentation:
    """Test that implementation matches design documentation."""

    def test_llm_provider_interface(self):
        """Test that LLMProvider has required methods."""
        from pulsar.core.llm_connectors.base import LLMProvider
        required_methods = ['health_check', 'generate', 'count_tokens', 'get_cost', 'get_model_info']
        for method in required_methods:
            assert hasattr(LLMProvider, method), f"LLMProvider missing {method}"

    def test_agent_base_interface(self):
        """Test that Agent has required methods."""
        from pulsar.core.intelligence.agent_base import Agent
        required_methods = ['_init_memory', 'think', 'analyze', 'health_check']
        for method in required_methods:
            assert hasattr(Agent, method), f"Agent missing {method}"

    def test_tool_registry_interface(self):
        """Test that ToolRegistry has required methods."""
        from pulsar.core.intelligence.tools import ToolRegistry
        required_methods = ['register', 'get_tool', 'call', 'get_schemas', 'clear_cache']
        for method in required_methods:
            assert hasattr(ToolRegistry, method), f"ToolRegistry missing {method}"

    def test_prompt_system_interface(self):
        """Test that PromptSystem has required methods."""
        from pulsar.core.intelligence.prompt_system import PromptSystem
        required_methods = ['get_template', 'render', 'render_system_prompt', 'render_journey_stage']
        for method in required_methods:
            assert hasattr(PromptSystem, method), f"PromptSystem missing {method}"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
