"""DiagnosisAgent: Problem-focused issue detection and analysis."""

from typing import Any, Dict, List, Optional
import logging
import json
import traceback
from datetime import datetime

import polars as pl

from pulsar.core.intelligence.agent_base import Agent, Message
from pulsar.core.llm_connectors import LLMConfig
from pulsar.core.intelligence.function_calls import FunctionCallParser

logger = logging.getLogger(__name__)


class DiagnosisAgent(Agent):
    """
    Diagnostic analysis agent for issue detection and root cause analysis.

    Memory strategy: Problem-focused history (stores issue tracking)
    Tool behavior: Iterative (up to 2 iterations for root cause analysis)
    Typical use: Finding issues, diagnosing problems, assessing impact

    Ideal for:
    - Anomaly detection
    - Data quality issues
    - Root cause analysis
    - Impact assessment

    Error Handling:
    - Validates all inputs
    - Logs all operations with context
    - No silent tool failures
    - Memory consistency checks
    - Full traceback logging
    """

    def _init_memory(self) -> None:
        """Initialize problem-focused memory."""
        try:
            self.memory: List[Message] = []
            self.issues: Dict[str, Any] = {}  # Track identified issues
            self.execution_count = 0
            self.error_count = 0
            self.tool_execution_count = 0
            self.issues_found_count = 0

            logger.info(
                f"{self.__class__.__name__} initialized with problem-focused memory",
                extra={
                    'agent_type': self.__class__.__name__,
                    'memory_type': 'problem-focused',
                    'memory_initialized': True,
                }
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize DiagnosisAgent memory: {e}",
                extra={'error': str(e), 'traceback': traceback.format_exc()}
            )
            raise RuntimeError(f"DiagnosisAgent memory initialization failed: {e}")

    def think(self, question: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Diagnose problems with iterative analysis (max 2 iterations).

        Args:
            question: The problem to investigate (must be non-empty string)
            context: Optional context dictionary

        Returns:
            Diagnostic analysis with root causes and impact

        Error Handling:
        - No exceptions raised, returns fallback on error
        - All errors logged with traceback
        - Memory consistency maintained
        """
        # Input validation
        if not question or not isinstance(question, str):
            error_msg = f"Invalid question: must be non-empty string, got {type(question)}"
            logger.error(error_msg, extra={'input_type': type(question).__name__})
            return f"ERROR: {error_msg}"

        if context is not None and not isinstance(context, dict):
            error_msg = f"Invalid context: must be dict, got {type(context)}"
            logger.error(error_msg, extra={'context_type': type(context).__name__})
            return f"ERROR: {error_msg}"

        self.execution_count += 1
        start_time = datetime.now()

        logger.info(
            f"DiagnosisAgent.think() started (execution #{self.execution_count})",
            extra={
                'agent_type': self.__class__.__name__,
                'question_length': len(question),
                'has_context': context is not None,
                'memory_size': len(self.memory),
            }
        )

        if not self.provider_available:
            logger.warning(
                "LLM provider unavailable at think() time",
                extra={'provider_status': 'unavailable'}
            )
            return self._fallback_response(question)

        try:
            # Add user question to memory (with error handling)
            try:
                self._add_to_memory("user", question)
            except Exception as e:
                logger.error(
                    f"Failed to add question to memory: {e}",
                    extra={'error': str(e), 'traceback': traceback.format_exc()}
                )
                # Continue despite memory failure

            # Build prompt with problem context
            prompt_parts = [self.system_prompt]

            # Add previous issues if investigating further (with bounds checking)
            if self.memory and len(self.memory) > 1:
                prompt_parts.append("\nPrevious findings:")
                try:
                    for msg in self.memory[-2:]:
                        content_snippet = msg.content[:200] if msg.content else "[empty]"
                        prompt_parts.append(f"{msg.role.upper()}: {content_snippet}...")
                except Exception as e:
                    logger.warning(
                        f"Error adding previous findings to prompt: {e}",
                        extra={'error': str(e)}
                    )

            # Add context (with validation)
            if context:
                prompt_parts.append("\nContext:")
                try:
                    for key, value in context.items():
                        if not isinstance(key, str):
                            logger.warning(
                                f"Non-string context key: {type(key).__name__}",
                                extra={'key': str(key)[:100]}
                            )
                            continue
                        prompt_parts.append(f"- {key}: {str(value)[:500]}")
                except Exception as e:
                    logger.warning(
                        f"Error processing context: {e}",
                        extra={'error': str(e), 'traceback': traceback.format_exc()}
                    )

            # Add question
            prompt_parts.append(f"\nProblem to diagnose: {question}")

            prompt = "\n".join(prompt_parts)
            logger.debug(
                "Diagnostic prompt constructed",
                extra={'prompt_length': len(prompt)}
            )

            # First diagnostic pass
            logger.info("Calling LLM for initial diagnosis...")
            try:
                response = self.llm_provider.generate(prompt)
            except Exception as e:
                logger.error(
                    f"LLM generation failed during initial diagnosis: {e}",
                    extra={'error': str(e), 'traceback': traceback.format_exc()}
                )
                return self._fallback_response(question)

            if not response:
                logger.error("LLM returned empty response")
                return self._fallback_response(question)

            # Check for tool calls (anomalies, data quality checks)
            has_calls = FunctionCallParser.response_has_function_calls(response)

            if has_calls and self.tool_registry:
                logger.info("Tool calls detected - executing diagnostic tools")
                tool_results = self._execute_function_calls(response)

                if tool_results and isinstance(tool_results, dict) and len(tool_results) > 0:
                    self.issues_found_count += len([v for v in tool_results.values() if not isinstance(v, dict) or 'error' not in v])

                    # Second pass: analyze tool results for root causes
                    try:
                        tool_results_json = json.dumps(tool_results, default=str)
                        current_question = f"Tool results from diagnostic checks:\n{tool_results_json}\n\nBased on these findings, what is the root cause and impact?"
                        logger.info("Calling LLM for root cause analysis...")
                        response = self.llm_provider.generate(prompt + "\n\n" + current_question)

                        if not response:
                            logger.error("LLM returned empty response during root cause analysis")
                            return self._fallback_response(question)

                    except Exception as e:
                        logger.error(
                            f"Root cause analysis failed: {e}",
                            extra={'error': str(e), 'traceback': traceback.format_exc()}
                        )
                        # Return tool results even if root cause analysis fails

            # Store findings in memory
            try:
                self._add_to_memory("assistant", response)
            except Exception as e:
                logger.error(
                    f"Failed to store response in memory: {e}",
                    extra={'error': str(e), 'traceback': traceback.format_exc()}
                )
                # Continue despite memory failure

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"DiagnosisAgent.think() completed (execution #{self.execution_count})",
                extra={
                    'response_length': len(response),
                    'duration_seconds': duration,
                    'tool_execution_count': self.tool_execution_count,
                    'issues_found': self.issues_found_count,
                    'memory_size': len(self.memory),
                }
            )
            return response

        except Exception as e:
            self.error_count += 1
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"DiagnosisAgent.think() failed: {type(e).__name__}: {e}",
                extra={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'duration_seconds': duration,
                    'execution_count': self.execution_count,
                    'error_count': self.error_count,
                    'traceback': traceback.format_exc(),
                    'memory_size': len(self.memory) if self.memory else 0,
                }
            )
            return self._fallback_response(question)

    def _add_to_memory(self, role: str, content: str, metadata: Optional[Dict] = None):
        """
        Add message to problem-focused memory.

        Args:
            role: "user" or "assistant"
            content: Message content (will validate non-empty)
            metadata: Optional metadata dict

        Raises:
            ValueError: If role or content invalid
            RuntimeError: If memory append fails
        """
        try:
            # Validate inputs
            if role not in ['user', 'assistant']:
                raise ValueError(f"Invalid role: {role}, must be 'user' or 'assistant'")

            if not content or not isinstance(content, str):
                raise ValueError(f"Invalid content: must be non-empty string, got {type(content)}")

            message = Message(
                role=role,
                content=content,
                timestamp=datetime.now(),
                metadata=metadata or {'type': 'diagnosis'},
            )

            if self.memory is None:
                raise RuntimeError("Memory list not initialized (None)")

            self.memory.append(message)

            logger.debug(
                f"Message added to memory (role={role})",
                extra={
                    'role': role,
                    'content_length': len(content),
                    'memory_size': len(self.memory),
                }
            )

        except Exception as e:
            logger.error(
                f"Failed to add message to memory: {e}",
                extra={
                    'error': str(e),
                    'role': role,
                    'traceback': traceback.format_exc(),
                }
            )
            raise RuntimeError(f"Memory add failed: {e}")

    def _execute_function_calls(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Parse and execute diagnostic tool calls.

        Focus on: detect_anomalies, analyze_data_quality, find_root_causes, assess_impact

        Args:
            response: LLM response potentially containing function calls

        Returns:
            Dict of tool results or None if no tools to execute

        Error Handling:
        - All tool errors logged with traceback
        - Partial success: if some tools fail, others still return results
        - No silent failures
        """
        if not self.tool_registry:
            logger.warning("Tool registry not available for function call execution")
            return None

        try:
            # Parse function calls
            try:
                function_calls = FunctionCallParser.extract_function_calls(response)
            except Exception as e:
                logger.error(
                    f"Error parsing function calls from LLM response: {e}",
                    extra={'error': str(e), 'traceback': traceback.format_exc()}
                )
                return None

            if not function_calls:
                logger.debug("No function calls found in LLM response")
                return None

            logger.info(
                f"Found {len(function_calls)} diagnostic tool call(s)",
                extra={'call_count': len(function_calls)}
            )

            # Define allowed diagnostic tools
            diagnostic_tools = [
                'detect_anomalies',
                'analyze_data_quality',
                'find_root_causes',
                'assess_impact',
                'detect_outliers',
                'check_data_quality'
            ]

            results = {}
            successful_calls = 0
            failed_calls = 0

            # Execute each tool call
            for call in function_calls:
                tool_name = getattr(call, 'function', 'unknown')

                try:
                    # Validate tool
                    if tool_name not in diagnostic_tools:
                        logger.warning(
                            f"Skipping non-diagnostic tool: {tool_name}",
                            extra={'tool_name': tool_name}
                        )
                        continue

                    # Validate parameters
                    params = getattr(call, 'parameters', {})
                    if not isinstance(params, dict):
                        logger.error(
                            f"Invalid parameters for tool {tool_name}: expected dict, got {type(params)}",
                            extra={'tool_name': tool_name, 'param_type': type(params).__name__}
                        )
                        results[tool_name] = {'error': f'Invalid parameters type: {type(params).__name__}'}
                        failed_calls += 1
                        continue

                    logger.debug(
                        f"Executing diagnostic tool: {tool_name}",
                        extra={'tool_name': tool_name, 'param_count': len(params)}
                    )

                    # Execute tool
                    try:
                        result = self.tool_registry.call(tool_name, **params, use_cache=True)
                        results[tool_name] = result
                        successful_calls += 1

                        logger.info(
                            f"Diagnostic tool executed successfully: {tool_name}",
                            extra={'tool_name': tool_name, 'result_type': type(result).__name__}
                        )

                    except Exception as e:
                        failed_calls += 1
                        logger.error(
                            f"Diagnostic tool execution failed: {tool_name}",
                            extra={
                                'tool_name': tool_name,
                                'error': str(e),
                                'error_type': type(e).__name__,
                                'traceback': traceback.format_exc(),
                            }
                        )
                        results[tool_name] = {'error': str(e), 'error_type': type(e).__name__}

                except Exception as e:
                    failed_calls += 1
                    logger.error(
                        f"Unexpected error processing tool call: {e}",
                        extra={
                            'tool_name': str(tool_name),
                            'error': str(e),
                            'error_type': type(e).__name__,
                            'traceback': traceback.format_exc(),
                        }
                    )
                    if isinstance(tool_name, str):
                        results[tool_name] = {'error': f'Unexpected error: {str(e)}'}

            self.tool_execution_count += successful_calls

            # Log summary
            logger.info(
                f"Tool execution summary: {successful_calls} successful, {failed_calls} failed",
                extra={
                    'successful_calls': successful_calls,
                    'failed_calls': failed_calls,
                    'total_results': len(results),
                    'total_execution_count': self.tool_execution_count,
                }
            )

            return results if results else None

        except Exception as e:
            logger.error(
                f"Unexpected error in _execute_function_calls: {e}",
                extra={
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc(),
                }
            )
            return None

    def _fallback_response(self, question: str) -> str:
        """
        Fallback response when LLM unavailable or error occurs.

        Logs the fallback invocation for debugging and audit trail.
        """
        logger.warning(
            "Returning fallback response (LLM provider unavailable or error)",
            extra={
                'agent_type': self.__class__.__name__,
                'fallback_reason': 'provider_unavailable_or_error',
                'question_truncated': question[:100],
                'error_count': self.error_count,
            }
        )
        return (
            "Diagnostic analysis unavailable. "
            f"Problem: {question[:100]}... "
            "Please ensure LLM provider is available."
        )

    def health_check(self) -> Dict[str, Any]:
        """
        Check agent health with issue tracking info and metrics.

        No exceptions raised - returns error info if health check fails.
        """
        try:
            base_health = super().health_check()

            if self.memory is None:
                logger.warning("Memory is None in health check")

            base_health.update({
                'memory_type': 'problem-focused',
                'memory_size': len(self.memory) if self.memory else 0,
                'issues_tracked': len(self.issues) if self.issues else 0,
                'execution_count': self.execution_count,
                'error_count': self.error_count,
                'error_rate': (
                    self.error_count / self.execution_count
                    if self.execution_count > 0 else 0
                ),
                'tool_execution_count': self.tool_execution_count,
                'issues_found': self.issues_found_count,
            })

            logger.debug(
                "Health check completed",
                extra={'health': base_health}
            )
            return base_health

        except Exception as e:
            logger.error(
                f"Health check failed: {e}",
                extra={
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc(),
                }
            )
            return {
                'agent_type': self.__class__.__name__,
                'health_check_status': 'FAILED',
                'error': str(e),
                'error_type': type(e).__name__,
            }
