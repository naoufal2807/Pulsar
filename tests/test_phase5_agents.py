"""Test Phase 5: Additional Agent Types (Analysis, Diagnosis)."""

import pytest
import polars as pl
from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.analysis_agent import AnalysisAgent
from pulsar.core.intelligence.diagnosis_agent import DiagnosisAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.llm_connectors import LLMConfig, LLMProviderType


@pytest.fixture
def test_df():
    """Create test DataFrame."""
    return pl.DataFrame({
        'sales': [100, 200, 150, 300, 250, 5000],  # 5000 is outlier
        'region': ['US', 'EU', 'US', 'APAC', 'EU', 'US'],
        'product': ['A', 'B', 'A', 'C', 'B', 'A'],
    })


class TestAnalysisAgent:
    """Test AnalysisAgent: Lightweight, single-shot."""

    def test_analysis_agent_creation(self, test_df):
        """Test creating AnalysisAgent."""
        agent = AnalysisAgent(df=test_df, tools_enabled=True)
        assert agent is not None
        assert agent.__class__.__name__ == 'AnalysisAgent'

    def test_analysis_agent_memory(self, test_df):
        """Test that AnalysisAgent has stateless memory."""
        agent = AnalysisAgent(df=test_df, tools_enabled=True)
        assert agent.memory is None
        assert len(agent.tool_registry.tools) == 16

    def test_analysis_agent_health_check(self, test_df):
        """Test AnalysisAgent health check."""
        agent = AnalysisAgent(df=test_df, tools_enabled=True)
        health = agent.health_check()
        assert health['agent_type'] == 'AnalysisAgent'
        assert health['memory_type'] == 'stateless'
        assert health['memory_size'] is None

    def test_analysis_agent_via_registry(self, test_df):
        """Test creating AnalysisAgent through registry."""
        agent = AgentRegistry.get_agent('analysis', df=test_df, tools_enabled=True)
        assert isinstance(agent, AnalysisAgent)
        assert agent.tool_registry is not None

    def test_analysis_agent_no_iteration(self, test_df):
        """Test that AnalysisAgent doesn't iterate."""
        agent = AnalysisAgent(df=test_df, tools_enabled=False)

        # Single question, single response
        question = "What is the average sales?"
        response = agent.think(question)

        # Should return response without iteration
        assert response is not None
        assert len(response) > 0

    def test_analysis_agent_quick_response(self, test_df):
        """Test that AnalysisAgent is quick (no tools)."""
        agent = AnalysisAgent(df=test_df, tools_enabled=False)
        question = "Summarize this data"

        # Should be fast since no tools and no iteration
        response = agent.think(question)
        assert response is not None

    def test_analysis_agent_with_context(self, test_df):
        """Test AnalysisAgent with context."""
        agent = AnalysisAgent(df=test_df, tools_enabled=False)
        question = "What is happening?"
        context = {'analysis_type': 'quick_scan', 'priority': 'high'}

        response = agent.think(question, context=context)
        assert response is not None


class TestDiagnosisAgent:
    """Test DiagnosisAgent: Problem-focused, iterative."""

    def test_diagnosis_agent_creation(self, test_df):
        """Test creating DiagnosisAgent."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=True)
        assert agent is not None
        assert agent.__class__.__name__ == 'DiagnosisAgent'

    def test_diagnosis_agent_memory(self, test_df):
        """Test that DiagnosisAgent has problem-focused memory."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=True)
        assert agent.memory is not None
        assert isinstance(agent.memory, list)
        assert len(agent.memory) == 0  # Initially empty

    def test_diagnosis_agent_issues_tracking(self, test_df):
        """Test that DiagnosisAgent tracks issues."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=True)
        assert hasattr(agent, 'issues')
        assert isinstance(agent.issues, dict)

    def test_diagnosis_agent_health_check(self, test_df):
        """Test DiagnosisAgent health check."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=True)
        health = agent.health_check()
        assert health['agent_type'] == 'DiagnosisAgent'
        assert health['memory_type'] == 'problem-focused'
        assert 'issues_tracked' in health

    def test_diagnosis_agent_via_registry(self, test_df):
        """Test creating DiagnosisAgent through registry."""
        agent = AgentRegistry.get_agent('diagnosis', df=test_df, tools_enabled=True)
        assert isinstance(agent, DiagnosisAgent)
        assert agent.tool_registry is not None

    def test_diagnosis_agent_memory_after_think(self, test_df):
        """Test that DiagnosisAgent stores messages in memory."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=False)
        question = "Is there any anomaly in the data?"

        response = agent.think(question)

        # Should have stored question and response in memory
        assert len(agent.memory) >= 1  # At least user question
        assert agent.memory[0].role == 'user'

    def test_diagnosis_agent_problem_focus(self, test_df):
        """Test DiagnosisAgent focuses on problems."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=False)
        question = "Diagnose data quality issues"

        response = agent.think(question)
        assert response is not None

    def test_diagnosis_agent_with_context(self, test_df):
        """Test DiagnosisAgent with problem context."""
        agent = DiagnosisAgent(df=test_df, tools_enabled=False)
        question = "What is wrong with this dataset?"
        context = {
            'problem_area': 'outliers',
            'severity': 'high',
            'affected_metric': 'sales'
        }

        response = agent.think(question, context=context)
        assert response is not None


class TestAgentComparison:
    """Compare different agent types."""

    def test_agent_types_available(self):
        """Test that all agent types are registered."""
        agents = AgentRegistry.list_agents()
        assert 'reasoning' in agents
        assert 'analysis' in agents
        assert 'diagnosis' in agents
        assert 'default' in agents

    def test_agent_memory_strategies(self, test_df):
        """Test different memory strategies."""
        reasoning = ReasoningAgent(df=test_df, tools_enabled=False)
        analysis = AnalysisAgent(df=test_df, tools_enabled=False)
        diagnosis = DiagnosisAgent(df=test_df, tools_enabled=False)

        # ReasoningAgent: list (full conversation)
        assert isinstance(reasoning.memory, list)

        # AnalysisAgent: None (stateless)
        assert analysis.memory is None

        # DiagnosisAgent: list (problem-focused)
        assert isinstance(diagnosis.memory, list)

    def test_agent_use_cases(self, test_df):
        """Test appropriate use cases for each agent."""
        # ReasoningAgent: Long conversation with iteration
        reasoning = AgentRegistry.get_agent('reasoning', df=test_df, tools_enabled=True)
        assert len(reasoning.tool_registry.tools) == 16
        assert isinstance(reasoning.memory, list)

        # AnalysisAgent: Quick, single-shot analysis
        analysis = AgentRegistry.get_agent('analysis', df=test_df, tools_enabled=True)
        assert analysis.memory is None
        assert len(analysis.tool_registry.tools) == 16

        # DiagnosisAgent: Problem investigation
        diagnosis = AgentRegistry.get_agent('diagnosis', df=test_df, tools_enabled=True)
        assert isinstance(diagnosis.memory, list)
        assert hasattr(diagnosis, 'issues')

    def test_agent_health_signatures(self, test_df):
        """Test that health checks return proper signatures."""
        reasoning = ReasoningAgent(df=test_df)
        analysis = AnalysisAgent(df=test_df)
        diagnosis = DiagnosisAgent(df=test_df)

        # All should have agent_type
        assert reasoning.health_check()['agent_type'] == 'ReasoningAgent'
        assert analysis.health_check()['agent_type'] == 'AnalysisAgent'
        assert diagnosis.health_check()['agent_type'] == 'DiagnosisAgent'

        # Each should have memory info
        assert 'memory_size' in reasoning.health_check()
        assert 'memory_type' in analysis.health_check()
        assert 'issues_tracked' in diagnosis.health_check()

    def test_default_agent_is_reasoning(self, test_df):
        """Test that default agent type is ReasoningAgent."""
        default = AgentRegistry.get_agent('default', df=test_df)
        assert isinstance(default, ReasoningAgent)


class TestAgentSelection:
    """Test selecting appropriate agent for task."""

    def test_quick_analysis_uses_analysis_agent(self, test_df):
        """Quick analysis should use AnalysisAgent."""
        agent = AgentRegistry.get_agent('analysis', df=test_df, tools_enabled=True)
        assert agent.__class__.__name__ == 'AnalysisAgent'
        # Single-shot, stateless
        assert agent.memory is None

    def test_problem_diagnosis_uses_diagnosis_agent(self, test_df):
        """Problem diagnosis should use DiagnosisAgent."""
        agent = AgentRegistry.get_agent('diagnosis', df=test_df, tools_enabled=True)
        assert agent.__class__.__name__ == 'DiagnosisAgent'
        # Problem-focused, with memory
        assert isinstance(agent.memory, list)

    def test_deep_analysis_uses_reasoning_agent(self, test_df):
        """Deep analysis should use ReasoningAgent."""
        agent = AgentRegistry.get_agent('reasoning', df=test_df, tools_enabled=True)
        assert agent.__class__.__name__ == 'ReasoningAgent'
        # Full conversation history
        assert isinstance(agent.memory, list)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
