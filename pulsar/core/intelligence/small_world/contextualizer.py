# pulsar/core/intelligence/small_world/contextualizer.py
"""
Contextualizer: Build contextual understanding of data.

The Contextualizer is the fourth step in the Small World Framework:
1. Isolator extracts a bounded sample
2. Learner discovers deep patterns
3. Expander validates patterns layer-by-layer
4. Contextualizer infers business meaning and context
5. Analyzer generates understanding

The Contextualizer answers:
- WHAT is this data? (type: customer, transaction, event, log, etc.)
- WHY does it exist? (purpose: acquisition, ml, analytics, operations, etc.)
- WHAT makes it good? (quality requirements and validation rules)
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Inferred data type classifications."""
    CUSTOMER = "customer"           # Customer/user data
    TRANSACTION = "transaction"     # Payment/purchase records
    EVENT = "event"                 # Event stream data
    LOG = "log"                     # System/application logs
    TIMESERIES = "timeseries"       # Time series data
    MEASUREMENT = "measurement"     # Sensor/metric data
    DIMENSION = "dimension"         # Reference/lookup table
    UNKNOWN = "unknown"             # Cannot determine


class DataPurpose(Enum):
    """Inferred data purpose."""
    ACQUISITION = "acquisition"     # Lead generation, customer acquisition
    RETENTION = "retention"         # Customer retention, churn prediction
    MONETIZATION = "monetization"   # Revenue, upsell, pricing
    ANALYTICS = "analytics"         # Business intelligence, reporting
    ML = "ml"                       # Machine learning model training
    OPERATIONS = "operations"       # Operational, workflow, automation
    MONITORING = "monitoring"       # Health, monitoring, alerting
    UNKNOWN = "unknown"             # Cannot determine


class QualityTier(Enum):
    """Quality requirement tiers."""
    REAL_TIME = "real_time"         # <1 second - requires 99.99% accuracy
    CRITICAL = "critical"           # <5 minutes - requires 99.9% accuracy
    HIGH = "high"                   # <1 hour - requires 99% accuracy
    MEDIUM = "medium"               # <1 day - requires 95% accuracy
    LOW = "low"                     # <1 week - requires 90% accuracy
    UNKNOWN = "unknown"             # Cannot determine


class Contextualizer:
    """
    Build contextual understanding of data.

    Infers data type, purpose, and quality requirements based on
    patterns discovered by the Learner and validated by the Expander.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Contextualizer.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = logger

    def contextualize(
        self,
        patterns: Dict[str, Any],
        coverage_ratio: float,
    ) -> Dict[str, Any]:
        """
        Infer context from patterns.

        Args:
            patterns: Patterns from Learner
            coverage_ratio: Data coverage ratio from Expander (0-1)

        Returns:
            Dictionary with inferred context
        """
        self.logger.debug("Building contextual understanding")

        context = {
            'data_type': self.infer_data_type(patterns),
            'data_purpose': self.infer_data_purpose(patterns),
            'quality_tier': self.infer_quality_tier(patterns, coverage_ratio),
            'validation_rules': self.build_validation_rules(patterns),
            'quality_requirements': self.extract_quality_requirements(patterns, coverage_ratio),
            'characteristics': self._extract_characteristics(patterns),
        }

        return context

    def infer_data_type(self, patterns: Dict[str, Any]) -> DataType:
        """
        Infer the type of data based on patterns.

        Args:
            patterns: Patterns from Learner

        Returns:
            DataType enum value
        """
        try:
            ranges = patterns.get('ranges', {})
            formats = patterns.get('formats', {})
            relationships = patterns.get('relationships', {})
            categorical_patterns = patterns.get('patterns', {}).get('categorical_patterns', {})

            # Collect evidence for each data type
            evidence = {
                'customer': 0,
                'transaction': 0,
                'event': 0,
                'log': 0,
                'timeseries': 0,
                'measurement': 0,
                'dimension': 0,
            }

            # Customer data: email, phone, region, name
            if 'email' in formats:
                evidence['customer'] += 2
            if any(col in ranges for col in ['phone', 'address', 'country', 'region']):
                evidence['customer'] += 1

            # Transaction data: amount, total, price, order, payment
            if any(col in ranges for col in ['amount', 'total', 'price', 'order_value', 'transaction_amount']):
                evidence['transaction'] += 2
            if any(col in formats for col in ['payment_method', 'card_type']):
                evidence['transaction'] += 1

            # Event data: event_type, event_name, action
            if any(col in categorical_patterns for col in ['event_type', 'event_name', 'action']):
                evidence['event'] += 2
            if 'relationships' in patterns and len(patterns['relationships']) > 0:
                evidence['event'] += 1

            # Log data: severity, level, message, status_code
            if any(col in categorical_patterns for col in ['level', 'severity', 'status', 'error']):
                evidence['log'] += 2

            # Timeseries data: date/time columns with many unique values
            temporal = patterns.get('patterns', {}).get('temporal_patterns', {})
            if temporal and len(temporal) > 0:
                evidence['timeseries'] += 2

            # Measurement data: numeric columns with small ranges
            numeric_cols = [col for col in ranges if ranges[col].get('type') == 'numeric']
            if len(numeric_cols) > 5:
                evidence['measurement'] += 1

            # Dimension data: low cardinality, categorical dominant
            low_cardinality_cats = [
                col for col in ranges
                if ranges[col].get('type') == 'categorical'
                and ranges[col].get('cardinality_ratio', 0) < 0.2
            ]
            if len(low_cardinality_cats) > 3:
                evidence['dimension'] += 2

            # Pick the type with highest evidence
            best_type = max(evidence.items(), key=lambda x: x[1])[0]

            if evidence[best_type] == 0:
                return DataType.UNKNOWN

            return DataType[best_type.upper()]

        except Exception as e:
            self.logger.debug(f"Error inferring data type: {e}")
            return DataType.UNKNOWN

    def infer_data_purpose(self, patterns: Dict[str, Any]) -> DataPurpose:
        """
        Infer the purpose of the data based on patterns.

        Args:
            patterns: Patterns from Learner

        Returns:
            DataPurpose enum value
        """
        try:
            ranges = patterns.get('ranges', {})
            formats = patterns.get('formats', {})
            categorical_patterns = patterns.get('patterns', {}).get('categorical_patterns', {})

            evidence = {
                'acquisition': 0,
                'retention': 0,
                'monetization': 0,
                'analytics': 0,
                'ml': 0,
                'operations': 0,
                'monitoring': 0,
            }

            # Acquisition: email, phone, source, campaign
            if 'email' in formats:
                evidence['acquisition'] += 1
            if any(col in ranges for col in ['source', 'campaign', 'channel']):
                evidence['acquisition'] += 1

            # Retention: tenure, churn, lifetime_value, last_purchase
            if any(col in ranges for col in ['tenure', 'lifetime_value', 'days_since_purchase']):
                evidence['retention'] += 2

            # Monetization: revenue, price, amount, order_value
            if any(col in ranges for col in ['revenue', 'price', 'amount', 'order_value', 'gmv']):
                evidence['monetization'] += 2

            # Analytics: dimension tables, aggregates
            if 'date' in ranges or any(col in ranges for col in ['month', 'week', 'quarter']):
                evidence['analytics'] += 1

            # ML: many numeric features, high cardinality
            numeric_cols = [col for col in ranges if ranges[col].get('type') == 'numeric']
            if len(numeric_cols) > 10:
                evidence['ml'] += 1

            # Operations: workflow, status, queue
            if any(col in categorical_patterns for col in ['status', 'state', 'workflow', 'queue']):
                evidence['operations'] += 1

            # Monitoring: metric, value, timestamp
            if any(col in ranges for col in ['metric', 'value', 'count']):
                evidence['monitoring'] += 1

            best_purpose = max(evidence.items(), key=lambda x: x[1])[0]

            if evidence[best_purpose] == 0:
                return DataPurpose.UNKNOWN

            return DataPurpose[best_purpose.upper()]

        except Exception as e:
            self.logger.debug(f"Error inferring data purpose: {e}")
            return DataPurpose.UNKNOWN

    def infer_quality_tier(
        self,
        patterns: Dict[str, Any],
        coverage_ratio: float,
    ) -> QualityTier:
        """
        Infer quality tier requirement based on patterns and coverage.

        Args:
            patterns: Patterns from Learner
            coverage_ratio: Data coverage ratio (0-1)

        Returns:
            QualityTier enum value
        """
        try:
            # Base tier on coverage
            if coverage_ratio >= 0.999:
                base_tier = QualityTier.REAL_TIME
            elif coverage_ratio >= 0.99:
                base_tier = QualityTier.CRITICAL
            elif coverage_ratio >= 0.95:
                base_tier = QualityTier.HIGH
            elif coverage_ratio >= 0.9:
                base_tier = QualityTier.MEDIUM
            else:
                base_tier = QualityTier.LOW

            return base_tier

        except Exception as e:
            self.logger.debug(f"Error inferring quality tier: {e}")
            return QualityTier.UNKNOWN

    def build_validation_rules(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build explicit validation rules from patterns.

        Args:
            patterns: Patterns from Learner

        Returns:
            Dictionary of validation rules
        """
        rules = {
            'format_rules': {},
            'range_rules': {},
            'relationship_rules': {},
            'cardinality_rules': {},
        }

        try:
            # Format rules
            if 'formats' in patterns:
                for col_name, col_formats in patterns['formats'].items():
                    high_confidence_formats = [
                        fmt for fmt, info in col_formats.items()
                        if info.get('match_ratio', 0) > 0.8
                    ]
                    if high_confidence_formats:
                        rules['format_rules'][col_name] = {
                            'required_formats': high_confidence_formats,
                            'confidence': 0.8,
                        }

            # Range rules
            if 'ranges' in patterns:
                for col_name, col_range in patterns['ranges'].items():
                    if col_range.get('type') == 'numeric':
                        rules['range_rules'][col_name] = {
                            'min': col_range.get('min'),
                            'max': col_range.get('max'),
                            'type': 'numeric',
                        }
                    elif col_range.get('type') == 'categorical':
                        top_values = [v[0] for v in col_range.get('top_values', [])[:5]]
                        rules['cardinality_rules'][col_name] = {
                            'expected_values': top_values,
                            'cardinality_ratio': col_range.get('cardinality_ratio'),
                        }

            # Relationship rules
            if 'relationships' in patterns:
                for col_a, correlations in patterns['relationships'].items():
                    strong_corrs = [c for c in correlations if abs(c[1]) > 0.7]
                    if strong_corrs:
                        rules['relationship_rules'][col_a] = {
                            'correlated_columns': [c[0] for c in strong_corrs],
                            'min_correlation': 0.7,
                        }

        except Exception as e:
            self.logger.debug(f"Error building validation rules: {e}")

        return rules

    def extract_quality_requirements(
        self,
        patterns: Dict[str, Any],
        coverage_ratio: float,
    ) -> Dict[str, Any]:
        """
        Extract quality requirements from patterns and coverage.

        Args:
            patterns: Patterns from Learner
            coverage_ratio: Data coverage ratio (0-1)

        Returns:
            Dictionary of quality requirements
        """
        requirements = {
            'minimum_coverage': 0.95,
            'maximum_null_ratio': 0.05,
            'duplicate_tolerance': 0.01,
            'format_compliance': 0.95,
            'range_compliance': 0.98,
        }

        try:
            # Adjust based on current coverage
            if coverage_ratio < 0.9:
                requirements['minimum_coverage'] = coverage_ratio * 1.1  # Set slightly higher
                requirements['format_compliance'] = max(0.8, coverage_ratio)

            # Check null ratios
            data_quality = patterns.get('patterns', {}).get('data_quality_indicators', {})
            if 'null_ratio_by_column' in data_quality:
                max_null = max(data_quality['null_ratio_by_column'].values()) if data_quality['null_ratio_by_column'] else 0
                requirements['maximum_null_ratio'] = max(0.01, max_null * 1.5)

            # Check duplicates
            if data_quality.get('duplicate_ratio', 0) > 0:
                requirements['duplicate_tolerance'] = max(0.01, data_quality['duplicate_ratio'] * 2)

        except Exception as e:
            self.logger.debug(f"Error extracting quality requirements: {e}")

        return requirements

    def _extract_characteristics(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key characteristics from patterns.

        Args:
            patterns: Patterns from Learner

        Returns:
            Dictionary of characteristics
        """
        characteristics = {
            'has_temporal_dimension': False,
            'is_high_volume': False,
            'has_geographic_dimension': False,
            'has_user_dimension': False,
            'is_aggregated': False,
            'has_outliers': False,
            'is_skewed': False,
        }

        try:
            ranges = patterns.get('ranges', {})
            distributions = patterns.get('distributions', {})
            temporal_patterns = patterns.get('patterns', {}).get('temporal_patterns', {})
            categorical_patterns = patterns.get('patterns', {}).get('categorical_patterns', {})

            # Temporal dimension
            if temporal_patterns:
                characteristics['has_temporal_dimension'] = True

            # High volume (many numeric columns)
            numeric_cols = [col for col in ranges if ranges[col].get('type') == 'numeric']
            if len(numeric_cols) > 5:
                characteristics['is_high_volume'] = True

            # Geographic dimension
            geo_keywords = ['country', 'region', 'state', 'city', 'location', 'zip']
            if any(col in ranges for col in geo_keywords):
                characteristics['has_geographic_dimension'] = True

            # User dimension
            user_keywords = ['user', 'customer', 'email', 'phone', 'id']
            if any(col in ranges or col in patterns.get('formats', {}) for col in user_keywords):
                characteristics['has_user_dimension'] = True

            # Aggregated (low row count with many metrics)
            if len(numeric_cols) > 3:
                characteristics['is_aggregated'] = True

            # Has outliers
            for col_name, dist in distributions.items():
                outliers = dist.get('outliers_iqr', {})
                if outliers.get('outlier_count', 0) > 0:
                    characteristics['has_outliers'] = True
                    break

            # Is skewed
            for col_name, dist in distributions.items():
                skewness = dist.get('skewness', 0)
                if abs(skewness) > 1:
                    characteristics['is_skewed'] = True
                    break

        except Exception as e:
            self.logger.debug(f"Error extracting characteristics: {e}")

        return characteristics
