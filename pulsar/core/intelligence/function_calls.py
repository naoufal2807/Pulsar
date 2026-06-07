# pulsar/core/intelligence/function_calls.py
"""
Function Calling: Structured JSON-based tool invocation.

Instead of parsing text like [TOOL: name(args)], use structured JSON:
{
  "function": "compute_statistics",
  "parameters": {"column": "sales"}
}

This is more reliable and aligns with OpenAI/Claude function calling patterns.
"""

import json
import re
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FunctionCall:
    """A structured function call."""
    function: str
    parameters: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "function": self.function,
            "parameters": self.parameters,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FunctionCall":
        return cls(
            function=data["function"],
            parameters=data["parameters"],
        )


class FunctionCallParser:
    """Parse function calls from LLM responses."""

    @staticmethod
    def extract_function_calls(response: str) -> List[FunctionCall]:
        """
        Extract function calls from LLM response.

        Supports multiple formats:
        1. JSON blocks: ```json {...} ```
        2. Inline JSON: {"function": "...", "parameters": {...}}
        3. JSON arrays: [{"function": "...", ...}, ...]
        """
        calls = []

        # Try to find JSON code blocks first
        json_blocks = re.findall(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", response)
        if json_blocks:
            for block in json_blocks:
                try:
                    data = json.loads(block)
                    call = FunctionCallParser._parse_json(data)
                    if call:
                        calls.extend(call if isinstance(call, list) else [call])
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON block: {e}")
                    continue

        # Try to find JSON arrays
        if not calls:
            array_matches = re.findall(r"\[\s*\{[\s\S]*?\}\s*\]", response)
            for match in array_matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list):
                        for item in data:
                            call = FunctionCallParser._parse_json(item)
                            if call:
                                calls.extend(call if isinstance(call, list) else [call])
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON array: {e}")
                    continue

        # Try to find inline JSON objects
        if not calls:
            inline_matches = re.findall(r"\{[\s\S]*?\}", response)
            for match in inline_matches:
                try:
                    data = json.loads(match)
                    if "function" in data and "parameters" in data:
                        call = FunctionCall.from_dict(data)
                        calls.append(call)
                except json.JSONDecodeError:
                    continue

        return calls

    @staticmethod
    def _parse_json(data: Any) -> Optional[FunctionCall | List[FunctionCall]]:
        """Parse JSON data into FunctionCall(s)."""
        if isinstance(data, dict):
            if "function" in data and "parameters" in data:
                try:
                    return FunctionCall.from_dict(data)
                except Exception as e:
                    logger.warning(f"Failed to create FunctionCall: {e}")
                    return None
        elif isinstance(data, list):
            calls = []
            for item in data:
                result = FunctionCallParser._parse_json(item)
                if result:
                    calls.extend(result if isinstance(result, list) else [result])
            return calls if calls else None
        return None

    @staticmethod
    def response_has_function_calls(response: str) -> bool:
        """Check if response likely contains function calls."""
        return bool(
            re.search(r"```(?:json)?\s*\{", response) or
            re.search(r"\[\s*\{[\s\S]*?\"function\"", response) or
            re.search(r"\{[\s\S]*?\"function\"\s*:", response)
        )


def create_function_definitions() -> str:
    """
    Create function definitions for the LLM in JSON schema format.

    Returns a string describing available functions.
    """
    return """
You have access to the following functions:

1. compute_statistics
   - Description: Compute statistical metrics for a column
   - Parameters: {"column": "string"}
   - Returns: {"mean": number, "median": number, "std": number, "min": number, "max": number}

2. check_data_quality
   - Description: Check data quality metrics
   - Parameters: {"column": "string"}
   - Returns: {"null_count": number, "null_percent": number, "distinct_count": number, "duplicate_rows": number}

3. detect_outliers
   - Description: Detect outliers using IQR method
   - Parameters: {"column": "string"}
   - Returns: {"outlier_count": number, "outlier_percent": number, "sample_outliers": array}

4. analyze_correlation
   - Description: Analyze correlation between two columns
   - Parameters: {"col1": "string", "col2": "string"}
   - Returns: {"correlation": number, "strength": "string", "direction": "string"}

5. describe_dataset
   - Description: Get dataset overview
   - Parameters: {}
   - Returns: {"row_count": number, "column_count": number, "memory_usage": number}

6. get_top_values
   - Description: Get top values in a column
   - Parameters: {"column": "string", "limit": number}
   - Returns: [{"value": "string", "count": number}]

IMPORTANT: When you need to analyze data, return function calls in this JSON format:

```json
{
  "function": "function_name",
  "parameters": {"param1": "value1", "param2": "value2"}
}
```

You can call multiple functions:
```json
[
  {"function": "describe_dataset", "parameters": {}},
  {"function": "compute_statistics", "parameters": {"column": "sales"}},
  {"function": "detect_outliers", "parameters": {"column": "sales"}}
]
```

After calling functions, explain the results and provide insights.
"""
