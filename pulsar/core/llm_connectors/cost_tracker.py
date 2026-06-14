"""Cost tracking for LLM API calls."""

from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


@dataclass
class CostRecord:
    """Record of a single LLM API call."""
    timestamp: datetime
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    cost: float


class CostTracker:
    """Track costs across LLM API calls."""

    def __init__(self):
        """Initialize cost tracker."""
        self.records: List[CostRecord] = []

    def record(
        self,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost: float
    ) -> None:
        """
        Record an API call.

        Args:
            provider: Provider name (openai, anthropic, etc)
            model: Model name
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            cost: Cost in USD
        """
        record = CostRecord(
            timestamp=datetime.now(),
            provider=provider,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost
        )
        self.records.append(record)
        logger.debug(f"Cost recorded: {provider}/{model} ${cost:.6f}")

    def get_daily_cost(self, days_back: int = 1) -> float:
        """
        Get total cost for last N days.

        Args:
            days_back: Number of days to look back (default: 1)

        Returns:
            Total cost in USD
        """
        cutoff = datetime.now() - timedelta(days=days_back)
        total = sum(
            r.cost for r in self.records
            if r.timestamp >= cutoff
        )
        return total

    def get_summary(self) -> Dict[str, Any]:
        """
        Get cost summary grouped by provider and model.

        Returns:
            Dictionary with cost breakdown
        """
        if not self.records:
            return {
                'total_calls': 0,
                'total_cost': 0.0,
                'by_provider': {},
                'by_model': {},
            }

        by_provider = {}
        by_model = {}
        total_cost = 0.0
        total_tokens = 0

        for record in self.records:
            # By provider
            if record.provider not in by_provider:
                by_provider[record.provider] = {
                    'calls': 0,
                    'input_tokens': 0,
                    'output_tokens': 0,
                    'cost': 0.0
                }
            by_provider[record.provider]['calls'] += 1
            by_provider[record.provider]['input_tokens'] += record.input_tokens
            by_provider[record.provider]['output_tokens'] += record.output_tokens
            by_provider[record.provider]['cost'] += record.cost

            # By model
            if record.model not in by_model:
                by_model[record.model] = {
                    'calls': 0,
                    'input_tokens': 0,
                    'output_tokens': 0,
                    'cost': 0.0
                }
            by_model[record.model]['calls'] += 1
            by_model[record.model]['input_tokens'] += record.input_tokens
            by_model[record.model]['output_tokens'] += record.output_tokens
            by_model[record.model]['cost'] += record.cost

            total_cost += record.cost
            total_tokens += record.input_tokens + record.output_tokens

        return {
            'total_calls': len(self.records),
            'total_cost': total_cost,
            'total_tokens': total_tokens,
            'by_provider': by_provider,
            'by_model': by_model,
        }
