"""
Example: Using Agent with Tool-Calling Capabilities

Demonstrates the Agent's ability to call tools for data analysis
and use results in intelligent reasoning.
"""

import polars as pl
from pulsar.core.intelligence.agent import Agent

# Create sample data
df = pl.DataFrame({
    'date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
    'sales': [1000, 1200, 950, 1500, 1100],
    'units': [50, 60, 45, 75, 55],
    'region': ['North', 'South', 'North', 'East', 'South'],
})

# Initialize Agent with tools enabled
print("Initializing Agent with tool-calling capabilities...")
agent = Agent(df=df, tools_enabled=True)

print(f"\n✓ Agent initialized")
print(f"  Tools available: {len(agent.tool_registry.tools)}")
print(f"  Available tools: {list(agent.tool_registry.tools.keys())}")

# Example 1: Ask agent to analyze data
print("\n" + "="*80)
print("Example 1: Agent analyzes sales data")
print("="*80)

question1 = "What are the key metrics in this sales dataset? Analyze sales patterns."
print(f"\nQuestion: {question1}")

response1 = agent.think(question1)
print(f"\nAgent Response:\n{response1}")

# Show memory
memory = agent.get_memory()
print(f"\nConversation Memory ({len(memory)} messages):")
for msg in memory:
    role = msg['role'].upper()
    content = msg['content'][:100] + "..." if len(msg['content']) > 100 else msg['content']
    print(f"  {role}: {content}")

# Example 2: Follow-up question
print("\n" + "="*80)
print("Example 2: Follow-up question with context")
print("="*80)

question2 = "Are there any outliers or anomalies in the sales?"
print(f"\nQuestion: {question2}")

response2 = agent.think(question2)
print(f"\nAgent Response:\n{response2}")

# Show updated memory
memory = agent.get_memory()
print(f"\nConversation Memory ({len(memory)} messages)")

# Example 3: Export and clear session
print("\n" + "="*80)
print("Example 3: Export session and clear memory")
print("="*80)

session = agent.export_session()
print(f"\nSession Export:")
print(f"  Model: {session['model']}")
print(f"  Provider: {session['provider_type']}")
print(f"  Total Messages: {session['total_messages']}")
print(f"  Duration: {session['total_messages']} messages")

agent.clear_memory()
print(f"\nMemory cleared. Current memory size: {len(agent.memory)}")

# Example 4: Tool schemas for integration
print("\n" + "="*80)
print("Example 4: Tool schemas available to LLM")
print("="*80)

schemas = agent.tool_registry.get_schemas()
print(f"\nAvailable {len(schemas)} tools:")
for schema in schemas:
    print(f"\n  {schema['name']}")
    print(f"    Description: {schema['description']}")
    print(f"    Parameters: {[p['name'] for p in schema['parameters']]}")

print("\n" + "="*80)
print("Tool-calling Agent Demo Complete!")
print("="*80)
