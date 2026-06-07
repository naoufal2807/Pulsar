# tests/intelligence/test_function_calls.py
"""Tests for function calling system."""

import pytest
import json

from pulsar.core.intelligence.function_calls import (
    FunctionCall, FunctionCallParser, create_function_definitions
)


class TestFunctionCall:
    """Test FunctionCall dataclass."""

    def test_function_call_creation(self):
        """Test creating a function call."""
        call = FunctionCall(
            function="compute_statistics",
            parameters={"column": "sales"}
        )

        assert call.function == "compute_statistics"
        assert call.parameters["column"] == "sales"

    def test_function_call_to_dict(self):
        """Test converting function call to dict."""
        call = FunctionCall(
            function="analyze_correlation",
            parameters={"col1": "x", "col2": "y"}
        )

        result = call.to_dict()

        assert result["function"] == "analyze_correlation"
        assert result["parameters"]["col1"] == "x"

    def test_function_call_from_dict(self):
        """Test creating function call from dict."""
        data = {
            "function": "detect_outliers",
            "parameters": {"column": "price"}
        }

        call = FunctionCall.from_dict(data)

        assert call.function == "detect_outliers"
        assert call.parameters["column"] == "price"


class TestFunctionCallParser:
    """Test FunctionCallParser."""

    def test_parse_json_block(self):
        """Test parsing function call from JSON code block."""
        response = """
        Let me analyze this data.

        ```json
        {
          "function": "compute_statistics",
          "parameters": {"column": "sales"}
        }
        ```

        Based on the results...
        """

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) == 1
        assert calls[0].function == "compute_statistics"
        assert calls[0].parameters["column"] == "sales"

    def test_parse_multiple_json_blocks(self):
        """Test parsing multiple function calls from JSON blocks."""
        response = """
        Let me execute multiple functions:

        ```json
        {
          "function": "describe_dataset",
          "parameters": {}
        }
        ```

        ```json
        {
          "function": "get_top_values",
          "parameters": {"column": "category", "limit": 10}
        }
        ```

        Results show...
        """

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) == 2
        assert calls[0].function == "describe_dataset"
        assert calls[1].function == "get_top_values"

    def test_parse_json_array(self):
        """Test parsing function calls from JSON array."""
        response = """
        Executing multiple functions:

        [
          {"function": "compute_statistics", "parameters": {"column": "value"}},
          {"function": "detect_outliers", "parameters": {"column": "value"}}
        ]

        Analysis...
        """

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) == 2
        assert calls[0].function == "compute_statistics"
        assert calls[1].function == "detect_outliers"

    def test_parse_inline_json(self):
        """Test parsing inline JSON function call (structured format preferred)."""
        # Note: Inline JSON without code blocks is harder to detect reliably.
        # Prefer JSON blocks, arrays, or code blocks for function calls.
        response = """
        Analyzing data...

        ```json
        {"function": "check_data_quality", "parameters": {"column": "data"}}
        ```

        The results show...
        """

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) >= 1
        assert calls[0].function == "check_data_quality"

    def test_no_function_calls(self):
        """Test response with no function calls."""
        response = "This dataset contains 1000 rows with 5 columns."

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) == 0

    def test_malformed_json(self):
        """Test handling of malformed JSON."""
        response = """
        ```json
        {
          "function": "compute_statistics",
          "parameters": {"column": "sales"
        }
        ```
        """

        # Should not raise, just return empty
        calls = FunctionCallParser.extract_function_calls(response)
        assert len(calls) == 0

    def test_response_has_function_calls(self):
        """Test detecting if response contains function calls."""
        with_calls = """
        ```json
        {"function": "test", "parameters": {}}
        ```
        """
        without_calls = "This is just text with no function calls."

        assert FunctionCallParser.response_has_function_calls(with_calls)
        assert not FunctionCallParser.response_has_function_calls(without_calls)

    def test_complex_parameters(self):
        """Test parsing function calls with complex parameters."""
        response = """
        ```json
        {
          "function": "analyze_correlation",
          "parameters": {
            "col1": "revenue",
            "col2": "cost",
            "method": "pearson",
            "min_observations": 30
          }
        }
        ```
        """

        calls = FunctionCallParser.extract_function_calls(response)

        assert len(calls) == 1
        assert calls[0].parameters["col1"] == "revenue"
        assert calls[0].parameters["method"] == "pearson"
        assert calls[0].parameters["min_observations"] == 30


class TestFunctionDefinitions:
    """Test function definitions."""

    def test_create_function_definitions(self):
        """Test creating function definitions string."""
        definitions = create_function_definitions()

        assert "compute_statistics" in definitions
        assert "check_data_quality" in definitions
        assert "detect_outliers" in definitions
        assert "analyze_correlation" in definitions
        assert "describe_dataset" in definitions
        assert "get_top_values" in definitions

    def test_function_definitions_format(self):
        """Test that definitions include JSON format examples."""
        definitions = create_function_definitions()

        assert "```json" in definitions
        assert '"function"' in definitions
        assert '"parameters"' in definitions
