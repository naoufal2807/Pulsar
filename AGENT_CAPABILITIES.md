# Agent Capabilities Overview

## What is the Agent?

The Agent is an LLM-powered intelligence engine that analyzes data, maintains conversation memory, and can execute tools for intelligent reasoning about datasets.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          Agent                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │   LLM Provider   │         │  Tool Registry   │              │
│  │  (Ollama/Gemma)  │         │  (6 analysis    │              │
│  │  - think()       │         │   tools)         │              │
│  │  - generate()    │         │  - statistics    │              │
│  │  - health_check()│         │  - quality       │              │
│  └──────────────────┘         │  - outliers      │              │
│           ▲                    │  - correlation   │              │
│           │                    │  - describe      │              │
│           │                    │  - top_values    │              │
│  ┌────────┴─────────┐         └──────────────────┘              │
│  │                  │                    ▲                       │
│  │  Conversation    │                    │                       │
│  │  Memory          │                    │                       │
│  │  ┌────────────┐  │                    │                       │
│  │  │ Message 1  │  │          ┌─────────┴──────────┐            │
│  │  │ Message 2  │  │          │  Iterative Loop:   │            │
│  │  │ Message 3  │  │──────────│  1. LLM generates  │            │
│  │  │ ...        │  │          │  2. Parse [TOOL:..] calls       │
│  │  └────────────┘  │          │  3. Execute tools  │            │
│  │                  │          │  4. Feed results   │            │
│  └──────────────────┘          │  5. Loop back      │            │
│                                 └────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

## Core Features

### 1. LLM-Powered Reasoning
- **Default:** Ollama with Gemma3:270m (can be swapped via pluggable provider architecture)
- **System Prompt:** Customizable instructions with tool awareness
- **Conversation Memory:** Maintains full message history throughout session
- **Fallback:** Heuristic responses when LLM unavailable

### 2. Tool-Calling Capabilities
Tools are called during reasoning using format: `[TOOL: tool_name(param1=value1, param2=value2)]`

#### Available Tools:

| Tool | Purpose | Parameters |
|------|---------|------------|
| `compute_statistics` | Get numeric stats | `column` |
| `check_data_quality` | Data quality metrics | `column` |
| `detect_outliers` | Find anomalies (IQR) | `column` |
| `analyze_correlation` | Pearson correlation | `col1, col2` |
| `describe_dataset` | Dataset overview | (none) |
| `get_top_values` | Frequency analysis | `column, limit` |

#### Example Tool Call:
```
User: "Analyze the sales column"

Agent: "Let me compute statistics first. [TOOL: compute_statistics(column=sales)]

Tool Result: {'column': 'sales', 'mean': 1150.0, 'std': 190.5, ...}

Agent: "Based on the statistics, sales average $1150 with std dev $191, 
indicating moderate variability..."
```

### 3. Conversation Management
- **Memory Storage:** Each message stored with role, content, timestamp, metadata
- **Context Window:** Last 5 messages sent to LLM for context
- **Session Export:** Full session can be exported before clearing
- **Memory Clearing:** Session cleared at end to manage memory

### 4. Health Monitoring
```python
health = agent.health_check()
# Returns: {
#   'agent_status': 'healthy',
#   'llm_provider': 'gemma3:270m',
#   'provider_available': True,
#   'tools_enabled': True,
#   'tools_available': 6,
#   'memory_size': 5,
#   'session_duration': 23.5
# }
```

## Usage Patterns

### Basic Usage (No Tools)
```python
from pulsar.core.intelligence.agent import Agent

agent = Agent(tools_enabled=False)
response = agent.think("What is data quality?")
```

### With Tool-Calling
```python
import polars as pl
from pulsar.core.intelligence.agent import Agent

df = pl.read_csv("data.csv")
agent = Agent(df=df, tools_enabled=True)

response = agent.think("Analyze the revenue column for patterns")
# Agent will:
# 1. Call compute_statistics(column=revenue)
# 2. Call detect_outliers(column=revenue)
# 3. Reason about results
```

### Multi-Turn Conversation
```python
# Turn 1
response1 = agent.think("What are the key metrics?")

# Turn 2 (Agent remembers Turn 1)
response2 = agent.think("How do they relate to each other?")
# Agent has context from Turn 1

# Export before clearing
session = agent.export_session()
agent.clear_memory()
```

## Test Coverage

### Agent Tests (17 tests)
- ✓ Initialization (default, custom config, system prompt)
- ✓ Memory management (store, export, clear)
- ✓ LLM provider integration
- ✓ Fallback mechanism
- ✓ Health checks
- ✓ Session management

### Tool Tests (16 tests)
- ✓ ToolRegistry creation and management
- ✓ Tool registration and retrieval
- ✓ All 6 individual tools
- ✓ Tool calling from LLM responses
- ✓ Multiple tool call execution
- ✓ Argument parsing

**Total: 33 tests, all passing**

## Integration with Pulsar

The Agent integrates with Pulsar's data analysis pipeline:

1. **Intelligence Layer:** Agent understands what data represents
2. **Diagnosis Layer:** Agent can reason about data quality issues
3. **CLI (`pulsar infer`):** Uses Agent for intelligent insights
4. **Future Layers:** Remediation/Prevention can leverage Agent reasoning

## Extensibility

### Adding New Tools
```python
from pulsar.core.intelligence.tools import ToolDefinition, ToolParameter

# Define tool
def my_analysis(df, column):
    return {'result': 'analysis'}

tool = ToolDefinition(
    name='my_analysis',
    description='My custom analysis',
    parameters=[ToolParameter('column', 'string', 'Column to analyze')],
    function=lambda column: my_analysis(df, column)
)

agent.tool_registry.register(tool)
```

### Adding New LLM Providers
```python
from pulsar.core.diagnosis.llm_base import LLMProvider, LLMConfig

class MyLLMProvider(LLMProvider):
    def health_check(self) -> bool:
        # Check if service available
        pass
    
    def generate(self, prompt: str) -> str:
        # Generate response
        pass

# Register in get_llm_provider() factory
```

## Performance Considerations

- **Tool Iteration Limit:** Max 3 tool calls per think() to prevent infinite loops
- **Memory Window:** Last 5 messages sent to LLM (keeps prompt manageable)
- **Tool Execution:** All tools execute synchronously (can be parallelized)
- **Fallback:** Agent still works if Ollama unavailable

## Future Enhancements

1. **More Tools:**
   - Statistical hypothesis testing (t-test, ANOVA)
   - Dimensionality reduction (PCA, t-SNE)
   - Time series analysis (trend, seasonality)
   - Visualization generation

2. **Improved Tool Calling:**
   - Structured tool calling (JSON vs. text parsing)
   - Confidence scores for tool decisions
   - Tool dependency resolution

3. **Agent Learning:**
   - Learn which tools are most effective
   - Refine prompts based on conversation history
   - Pattern recognition across sessions

4. **Multi-Agent Reasoning:**
   - Agents collaborate on complex analysis
   - Divide-and-conquer for large datasets

## References

- Agent implementation: `pulsar/core/intelligence/agent.py`
- Tools module: `pulsar/core/intelligence/tools.py`
- Tests: `tests/intelligence/test_agent.py`, `tests/intelligence/test_agent_tools.py`
- Example: `examples/agent_tools_demo.py`
