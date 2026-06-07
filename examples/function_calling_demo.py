"""
Demo: Structured JSON Function Calling

Shows how the Agent uses JSON-based function calling to analyze data.
"""

from pulsar.core.intelligence.function_calls import (
    FunctionCallParser, FunctionCall, create_function_definitions
)

print("=" * 80)
print("STRUCTURED JSON FUNCTION CALLING DEMO")
print("=" * 80)

# Example 1: JSON Code Block
print("\n1. JSON CODE BLOCK FORMAT")
print("-" * 80)

response1 = """
Let me analyze the sales data.

```json
{
  "function": "compute_statistics",
  "parameters": {"column": "sales"}
}
```

Based on the statistics, I can see the sales distribution.
"""

print("LLM Response:")
print(response1)

calls1 = FunctionCallParser.extract_function_calls(response1)
print(f"\nParsed Calls: {len(calls1)}")
for call in calls1:
    print(f"  - Function: {call.function}")
    print(f"    Parameters: {call.parameters}")

# Example 2: JSON Array (Multiple Functions)
print("\n\n2. MULTIPLE FUNCTIONS (JSON ARRAY)")
print("-" * 80)

response2 = """
Let me perform a comprehensive analysis.

```json
[
  {"function": "describe_dataset", "parameters": {}},
  {"function": "get_top_values", "parameters": {"column": "category", "limit": 5}},
  {"function": "check_data_quality", "parameters": {"column": "revenue"}}
]
```

These functions will give us a complete overview.
"""

print("LLM Response:")
print(response2)

calls2 = FunctionCallParser.extract_function_calls(response2)
print(f"\nParsed Calls: {len(calls2)}")
for i, call in enumerate(calls2, 1):
    print(f"  {i}. {call.function}")
    print(f"     Parameters: {call.parameters}")

# Example 3: Complex Parameters
print("\n\n3. COMPLEX PARAMETERS")
print("-" * 80)

response3 = """
Let me analyze the relationship between these variables.

```json
{
  "function": "analyze_correlation",
  "parameters": {
    "col1": "marketing_spend",
    "col2": "revenue",
    "method": "pearson"
  }
}
```

This correlation analysis will show...
"""

print("LLM Response:")
print(response3)

calls3 = FunctionCallParser.extract_function_calls(response3)
print(f"\nParsed Calls: {len(calls3)}")
if calls3:
    print(f"  Function: {calls3[0].function}")
    print(f"  Parameters:")
    for key, value in calls3[0].parameters.items():
        print(f"    - {key}: {value}")

# Example 4: Detection
print("\n\n4. FUNCTION CALL DETECTION")
print("-" * 80)

responses = [
    ("Has function calls", "```json\n{\"function\": \"test\", \"parameters\": {}}\n```"),
    ("Plain text only", "This dataset has 1000 rows."),
    ("Function in array", "[{\"function\": \"analyze\", \"parameters\": {}}]"),
]

for name, text in responses:
    has_calls = FunctionCallParser.response_has_function_calls(text)
    status = "[OK] DETECTED" if has_calls else "[NO] NOT DETECTED"
    print(f"{name:30} {status}")

# Example 5: Function Definitions
print("\n\n5. FUNCTION DEFINITIONS FOR LLM")
print("-" * 80)

definitions = create_function_definitions()
print(definitions[:500])  # Show first 500 chars
print("\n... (see create_function_definitions() for full list)")

# Example 6: Creating FunctionCall Objects
print("\n\n6. CREATING FUNCTION CALL OBJECTS")
print("-" * 80)

# Create programmatically
call = FunctionCall(
    function="detect_outliers",
    parameters={"column": "price"}
)

print(f"Created: {call.function}")
print(f"Parameters: {call.parameters}")
print(f"As dict: {call.to_dict()}")

# From dict
call_dict = {
    "function": "analyze_correlation",
    "parameters": {"col1": "x", "col2": "y"}
}
call_from_dict = FunctionCall.from_dict(call_dict)
print(f"\nFrom dict: {call_from_dict.function}")
print(f"Matches: {call_from_dict == FunctionCall.from_dict(call_from_dict.to_dict())}")

print("\n" + "=" * 80)
print("BENEFITS OF JSON FUNCTION CALLING:")
print("=" * 80)
print("""
[OK] Structured: Clear, machine-readable format
[OK] Reliable: JSON parsing more robust than regex
[OK] Standard: Aligns with OpenAI/Claude function calling
[OK] Flexible: Supports JSON blocks, arrays, and objects
[OK] Complex: Handles nested parameters easily
[OK] Parseable: Error handling for malformed JSON
[OK] Debuggable: Easy to see what functions are called
""")

print("=" * 80)
