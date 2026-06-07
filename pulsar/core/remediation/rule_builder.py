# pulsar/core/remediation/rule_builder.py
"""
Rule Builder: Build reusable data cleaning rules.

Generates rules that can be:
- Applied repeatedly to new data
- Shared across teams
- Versioned and audited
- Integrated with data pipelines (dbt, Airflow)
"""

from typing import Any, Dict, List, Optional, Callable
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class CleaningRule:
    """Reusable data cleaning rule."""
    name: str
    description: str
    rule_type: str                          # null, duplicate, outlier, format, type
    column_name: str

    # Rule definition
    condition: str                          # Condition that triggers the rule
    action: str                             # Action to take (DELETE_ROWS, FILL_MEAN, etc.)
    parameters: Dict[str, Any]              # Action parameters

    # Metadata
    created_at: datetime
    created_by: Optional[str] = None
    version: int = 1
    enabled: bool = True

    # Applicability
    data_types: List[str] = None            # Applicable data types
    min_rows: int = 100                     # Minimum rows required
    max_rows: Optional[int] = None          # Maximum rows

    # Validation
    expected_rows_affected: int = 0
    expected_data_loss_percent: float = 0.0
    reversible: bool = False                # Can rule be undone?

    # Execution
    sql_expression: Optional[str] = None    # SQL version of rule
    python_function: Optional[Callable] = None  # Python callable

    def __hash__(self):
        return hash(self.name)


class RuleBuilder:
    """
    Build reusable data cleaning rules.

    Rules capture the business logic for fixing data quality issues
    and can be applied consistently across datasets and over time.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize rule builder.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.rules: Dict[str, CleaningRule] = {}
        self.logger = logger

    def build_null_rule(
        self,
        column_name: str,
        action: str,
        parameters: Dict[str, Any],
        description: Optional[str] = None,
    ) -> CleaningRule:
        """
        Build rule for handling nulls.

        Args:
            column_name: Column to apply rule to
            action: Action (DELETE_ROWS, FILL_MEAN, FILL_MODE, etc.)
            parameters: Action parameters
            description: Optional rule description

        Returns:
            Cleaning rule
        """
        rule_name = f"null_{column_name}_{action.lower()}"

        rule = CleaningRule(
            name=rule_name,
            description=description or f"Handle nulls in '{column_name}' by {action}",
            rule_type="null",
            column_name=column_name,
            condition=f"{column_name} IS NULL",
            action=action,
            parameters=parameters,
            created_at=datetime.now(),
        )

        self.rules[rule_name] = rule
        return rule

    def build_duplicate_rule(
        self,
        columns: List[str],
        action: str = "REMOVE_DUPLICATES",
        keep: str = "first",
        description: Optional[str] = None,
    ) -> CleaningRule:
        """
        Build rule for handling duplicates.

        Args:
            columns: Columns that define a duplicate
            action: Action (REMOVE_DUPLICATES, KEEP_FIRST, KEEP_LAST, etc.)
            keep: Which occurrence to keep ('first' or 'last')
            description: Optional rule description

        Returns:
            Cleaning rule
        """
        rule_name = f"duplicate_{'_'.join(columns)}"

        rule = CleaningRule(
            name=rule_name,
            description=description or f"Remove duplicates based on {columns}",
            rule_type="duplicate",
            column_name=",".join(columns),
            condition=f"DUPLICATE({','.join(columns)})",
            action=action,
            parameters={'subset': columns, 'keep': keep},
            created_at=datetime.now(),
            expected_data_loss_percent=0.0,
            reversible=False,
        )

        self.rules[rule_name] = rule
        return rule

    def build_outlier_rule(
        self,
        column_name: str,
        method: str = "iqr",
        action: str = "CAP_OUTLIERS",
        description: Optional[str] = None,
    ) -> CleaningRule:
        """
        Build rule for handling outliers.

        Args:
            column_name: Column to apply rule to
            method: Detection method (iqr, zscore)
            action: Action (CAP_OUTLIERS, REMOVE_OUTLIERS)
            description: Optional rule description

        Returns:
            Cleaning rule
        """
        rule_name = f"outlier_{column_name}_{method}"

        rule = CleaningRule(
            name=rule_name,
            description=description or f"Handle outliers in '{column_name}' using {method}",
            rule_type="outlier",
            column_name=column_name,
            condition=f"IS_OUTLIER({column_name}, '{method}')",
            action=action,
            parameters={'method': method, 'column': column_name},
            created_at=datetime.now(),
            reversible=(action == "CAP_OUTLIERS"),
        )

        self.rules[rule_name] = rule
        return rule

    def build_format_rule(
        self,
        column_name: str,
        target_format: str,
        description: Optional[str] = None,
    ) -> CleaningRule:
        """
        Build rule for standardizing formats.

        Args:
            column_name: Column to apply rule to
            target_format: Target format (email, phone, date_iso, etc.)
            description: Optional rule description

        Returns:
            Cleaning rule
        """
        rule_name = f"format_{column_name}_{target_format}"

        rule = CleaningRule(
            name=rule_name,
            description=description or f"Standardize '{column_name}' to {target_format} format",
            rule_type="format",
            column_name=column_name,
            condition=f"NOT MATCHES({column_name}, '{target_format}')",
            action="STANDARDIZE_FORMAT",
            parameters={'format': target_format, 'column': column_name},
            created_at=datetime.now(),
            reversible=True,
        )

        self.rules[rule_name] = rule
        return rule

    def build_type_rule(
        self,
        column_name: str,
        target_type: str,
        description: Optional[str] = None,
    ) -> CleaningRule:
        """
        Build rule for type conversion.

        Args:
            column_name: Column to apply rule to
            target_type: Target data type (int, float, string, date, etc.)
            description: Optional rule description

        Returns:
            Cleaning rule
        """
        rule_name = f"type_{column_name}_{target_type}"

        rule = CleaningRule(
            name=rule_name,
            description=description or f"Convert '{column_name}' to {target_type}",
            rule_type="type",
            column_name=column_name,
            condition=f"TYPE({column_name}) != '{target_type}'",
            action="CONVERT_TYPE",
            parameters={'to_type': target_type, 'column': column_name},
            created_at=datetime.now(),
            reversible=True,
        )

        self.rules[rule_name] = rule
        return rule

    def add_rule(self, rule: CleaningRule):
        """
        Add rule to collection.

        Args:
            rule: Cleaning rule to add
        """
        self.rules[rule.name] = rule
        self.logger.info(f"Added rule: {rule.name}")

    def remove_rule(self, rule_name: str):
        """
        Remove rule from collection.

        Args:
            rule_name: Name of rule to remove
        """
        if rule_name in self.rules:
            del self.rules[rule_name]
            self.logger.info(f"Removed rule: {rule_name}")

    def get_rules_for_column(self, column_name: str) -> List[CleaningRule]:
        """
        Get all rules applicable to a column.

        Args:
            column_name: Column name

        Returns:
            List of applicable rules
        """
        return [r for r in self.rules.values() if r.column_name == column_name and r.enabled]

    def get_rules_by_type(self, rule_type: str) -> List[CleaningRule]:
        """
        Get rules by type.

        Args:
            rule_type: Rule type (null, duplicate, outlier, format, type)

        Returns:
            List of rules matching type
        """
        return [r for r in self.rules.values() if r.rule_type == rule_type and r.enabled]

    def export_rules(self) -> Dict[str, Dict[str, Any]]:
        """
        Export rules as dictionary (for serialization/sharing).

        Returns:
            Dictionary representation of rules
        """
        return {
            name: {
                'name': rule.name,
                'description': rule.description,
                'rule_type': rule.rule_type,
                'column_name': rule.column_name,
                'condition': rule.condition,
                'action': rule.action,
                'parameters': rule.parameters,
                'version': rule.version,
                'enabled': rule.enabled,
                'reversible': rule.reversible,
            }
            for name, rule in self.rules.items()
        }

    def generate_sql(self, rule: CleaningRule) -> str:
        """
        Generate SQL for a rule.

        Args:
            rule: Cleaning rule

        Returns:
            SQL expression
        """
        if rule.sql_expression:
            return rule.sql_expression

        # Generate based on rule type
        if rule.rule_type == "null":
            if rule.action == "DELETE_ROWS":
                return f"WHERE {rule.column_name} IS NOT NULL"
            elif rule.action == "FILL_MEAN":
                return f"CASE WHEN {rule.column_name} IS NULL THEN AVG({rule.column_name}) OVER () ELSE {rule.column_name} END"

        elif rule.rule_type == "duplicate":
            subset = rule.parameters.get('subset', [rule.column_name])
            subset_str = ", ".join(subset)
            return f"WHERE ROW_NUMBER() OVER (PARTITION BY {subset_str}) = 1"

        elif rule.rule_type == "outlier":
            method = rule.parameters.get('method', 'iqr')
            if rule.action == "REMOVE_OUTLIERS":
                return f"WHERE {rule.column_name} NOT IN (SELECT value FROM outliers WHERE method='{method}')"

        return rule.condition
