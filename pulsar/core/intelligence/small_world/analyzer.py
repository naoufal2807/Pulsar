# pulsar/core/intelligence/small_world/analyzer.py
"""
Analyzer: Generate comprehensive understanding of data.

The Analyzer is the fifth step in the Small World Framework:
1. Isolator extracts a bounded sample
2. Learner discovers deep patterns
3. Expander validates patterns layer-by-layer
4. Contextualizer infers business meaning
5. Analyzer generates comprehensive understanding

The Analyzer answers:
- What IS this data? (summary statement)
- How GOOD is it? (quality score 0-100)
- What should happen NEXT? (recommendations)
- How does it COMPARE? (baseline vs current)
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Understanding:
    """Complete understanding of a dataset."""
    summary: str                          # "This is B2B customer acquisition data. Quality: 92%"
    data_type: str                        # "customer"
    purpose: str                          # "acquisition"
    quality_score: float                  # 0-100
    quality_grade: str                    # A, B, C, D, F
    coverage_ratio: float                 # 0-1 (% of data that's valid)
    characteristics: List[str]            # ["has_temporal_dimension", "is_high_volume"]
    strengths: List[str]                  # ["high completeness", "consistent formats"]
    weaknesses: List[str]                 # ["high duplicates", "outliers in revenue"]
    recommendations: List[str]            # ["remove 100 duplicate rows", "validate emails"]


class Analyzer:
    """
    Analyze and understand data comprehensively.

    Takes patterns, context, and validation results to generate
    a complete understanding of what the data is, how good it is,
    and what to do next.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Analyzer.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = logger

    def analyze(
        self,
        patterns: Dict[str, Any],
        context: Dict[str, Any],
        expansion_result: Dict[str, Any],
    ) -> Understanding:
        """
        Generate comprehensive understanding of data.

        Args:
            patterns: Patterns from Learner
            context: Context from Contextualizer
            expansion_result: Results from Expander

        Returns:
            Understanding object with complete analysis
        """
        self.logger.debug("Generating comprehensive understanding")

        # Calculate quality score
        quality_score = self.calculate_quality_score(patterns, expansion_result)
        quality_grade = self._score_to_grade(quality_score)

        # Extract features
        coverage_ratio = expansion_result.get('summary', {}).get('coverage_ratio', 0)
        data_type = context.get('data_type', {}).name.lower() if context.get('data_type') else 'unknown'
        purpose = context.get('data_purpose', {}).name.lower() if context.get('data_purpose') else 'unknown'
        characteristics = self._describe_characteristics(context.get('characteristics', {}))

        # Identify strengths and weaknesses
        strengths = self._identify_strengths(patterns, expansion_result)
        weaknesses = self._identify_weaknesses(patterns, expansion_result)

        # Generate recommendations
        recommendations = self._generate_recommendations(patterns, expansion_result, weaknesses)

        # Generate summary statement with context
        summary = self._generate_summary(data_type, purpose, quality_grade, coverage_ratio, patterns)

        return Understanding(
            summary=summary,
            data_type=data_type,
            purpose=purpose,
            quality_score=quality_score,
            quality_grade=quality_grade,
            coverage_ratio=round(coverage_ratio, 3),
            characteristics=characteristics,
            strengths=strengths,
            weaknesses=weaknesses,
            recommendations=recommendations,
        )

    def calculate_quality_score(
        self,
        patterns: Dict[str, Any],
        expansion_result: Dict[str, Any],
    ) -> float:
        """
        Calculate overall quality score (0-100).

        Args:
            patterns: Patterns from Learner
            expansion_result: Results from Expander

        Returns:
            Quality score (0-100)
        """
        try:
            scores = []

            # Coverage score (0-100)
            coverage = expansion_result.get('summary', {}).get('coverage_ratio', 0)
            coverage_score = coverage * 100
            scores.append(('coverage', coverage_score, 0.40))  # 40% weight

            # Completeness score (null ratio)
            data_quality = patterns.get('patterns', {}).get('data_quality_indicators', {})
            null_ratios = data_quality.get('null_ratio_by_column', {})
            if null_ratios:
                avg_null = sum(null_ratios.values()) / len(null_ratios)
                completeness_score = max(0, 100 - (avg_null * 100))
            else:
                completeness_score = 100
            scores.append(('completeness', completeness_score, 0.20))  # 20% weight

            # Consistency score (duplicate ratio)
            duplicate_ratio = data_quality.get('duplicate_ratio', 0)
            consistency_score = max(0, 100 - (duplicate_ratio * 100))
            scores.append(('consistency', consistency_score, 0.15))  # 15% weight

            # Validity score (format compliance)
            formats = patterns.get('formats', {})
            if formats:
                format_matches = [
                    info.get('match_ratio', 0)
                    for col_formats in formats.values()
                    for info in col_formats.values()
                ]
                if format_matches:
                    avg_match = sum(format_matches) / len(format_matches)
                    validity_score = avg_match * 100
                else:
                    validity_score = 100
            else:
                validity_score = 100
            scores.append(('validity', validity_score, 0.15))  # 15% weight

            # Distribution score (outliers)
            distributions = patterns.get('distributions', {})
            if distributions:
                outlier_counts = [
                    d.get('outliers_iqr', {}).get('outlier_percentage', 0)
                    for d in distributions.values()
                ]
                if outlier_counts:
                    avg_outliers = sum(outlier_counts) / len(outlier_counts)
                    distribution_score = max(0, 100 - avg_outliers)
                else:
                    distribution_score = 100
            else:
                distribution_score = 100
            scores.append(('distribution', distribution_score, 0.10))  # 10% weight

            # Calculate weighted average
            total_score = sum(score * weight for _, score, weight in scores)
            total_weight = sum(weight for _, _, weight in scores)

            final_score = (total_score / total_weight) if total_weight > 0 else 0

            self.logger.debug(
                f"Quality scores: coverage={coverage_score:.1f}, "
                f"completeness={completeness_score:.1f}, "
                f"consistency={consistency_score:.1f}, "
                f"validity={validity_score:.1f}, "
                f"distribution={distribution_score:.1f} "
                f"-> Final={final_score:.1f}"
            )

            return round(final_score, 1)

        except Exception as e:
            self.logger.debug(f"Error calculating quality score: {e}")
            return 50.0

    def _score_to_grade(self, score: float) -> str:
        """Convert quality score to letter grade."""
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"

    def _describe_characteristics(self, characteristics: Dict[str, bool]) -> List[str]:
        """Convert characteristics dict to human-readable list."""
        descriptions = {
            'has_temporal_dimension': 'has temporal dimension',
            'is_high_volume': 'high volume',
            'has_geographic_dimension': 'geographic dimension',
            'has_user_dimension': 'user dimension',
            'is_aggregated': 'aggregated data',
            'has_outliers': 'has outliers',
            'is_skewed': 'skewed distribution',
        }

        return [
            descriptions[key]
            for key, value in characteristics.items()
            if value and key in descriptions
        ]

    def _identify_strengths(
        self,
        patterns: Dict[str, Any],
        expansion_result: Dict[str, Any],
    ) -> List[str]:
        """Identify data strengths."""
        strengths = []

        try:
            # High coverage
            coverage = expansion_result.get('summary', {}).get('coverage_ratio', 0)
            if coverage > 0.95:
                strengths.append(f"high coverage ({100*coverage:.1f}%)")

            # Low null ratio
            data_quality = patterns.get('patterns', {}).get('data_quality_indicators', {})
            null_ratios = data_quality.get('null_ratio_by_column', {})
            if not null_ratios or max(null_ratios.values(), default=0) < 0.01:
                strengths.append("high completeness (few nulls)")

            # Low duplicates
            if data_quality.get('duplicate_ratio', 1) < 0.01:
                strengths.append("low duplicate ratio")

            # Consistent formats
            formats = patterns.get('formats', {})
            if formats:
                high_confidence = sum(
                    1 for col_formats in formats.values()
                    for info in col_formats.values()
                    if info.get('match_ratio', 0) > 0.9
                )
                if high_confidence > 0:
                    strengths.append(f"consistent formats ({high_confidence} columns)")

            # High cardinality variety
            ranges = patterns.get('ranges', {})
            categorical_cols = [
                col for col in ranges
                if ranges[col].get('type') == 'categorical'
                and ranges[col].get('cardinality_ratio', 0) > 0.8
            ]
            if categorical_cols:
                strengths.append(f"high variety ({len(categorical_cols)} high-cardinality columns)")

        except Exception as e:
            self.logger.debug(f"Error identifying strengths: {e}")

        return strengths

    def _identify_weaknesses(
        self,
        patterns: Dict[str, Any],
        expansion_result: Dict[str, Any],
    ) -> List[str]:
        """Identify data weaknesses."""
        weaknesses = []

        try:
            # Low coverage
            coverage = expansion_result.get('summary', {}).get('coverage_ratio', 0)
            invalid_rows = expansion_result.get('summary', {}).get('invalid_rows', 0)
            if coverage < 0.95 and invalid_rows > 0:
                weaknesses.append(f"low coverage ({invalid_rows} invalid rows)")

            # High null ratio
            data_quality = patterns.get('patterns', {}).get('data_quality_indicators', {})
            null_ratios = data_quality.get('null_ratio_by_column', {})
            if null_ratios:
                high_null_cols = [
                    col for col, ratio in null_ratios.items()
                    if ratio > 0.1
                ]
                if high_null_cols:
                    weaknesses.append(f"high nulls in {len(high_null_cols)} columns")

            # High duplicates
            if data_quality.get('duplicate_ratio', 0) > 0.05:
                dup_ratio = data_quality['duplicate_ratio']
                dup_count = int(expansion_result.get('summary', {}).get('total_rows', 0) * dup_ratio)
                weaknesses.append(f"duplicates ({dup_count} rows)")

            # Format mismatches
            formats = patterns.get('formats', {})
            low_confidence = [
                col for col_formats in formats.values()
                for fmt, info in col_formats.items()
                if info.get('match_ratio', 0) < 0.8
            ]
            if low_confidence:
                weaknesses.append(f"inconsistent formats ({len(low_confidence)} issues)")

            # Outliers
            distributions = patterns.get('distributions', {})
            outlier_cols = [
                col for col, dist in distributions.items()
                if dist.get('outliers_iqr', {}).get('outlier_count', 0) > 10
            ]
            if outlier_cols:
                weaknesses.append(f"outliers in {len(outlier_cols)} columns")

            # Skewness
            skewed_cols = [
                col for col, dist in distributions.items()
                if abs(dist.get('skewness', 0)) > 2
            ]
            if skewed_cols:
                weaknesses.append(f"highly skewed ({len(skewed_cols)} columns)")

        except Exception as e:
            self.logger.debug(f"Error identifying weaknesses: {e}")

        return weaknesses

    def _generate_recommendations(
        self,
        patterns: Dict[str, Any],
        expansion_result: Dict[str, Any],
        weaknesses: List[str],
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []

        try:
            data_quality = patterns.get('patterns', {}).get('data_quality_indicators', {})
            expansion_summary = expansion_result.get('summary', {})

            # Duplicate handling
            dup_count = expansion_summary.get('invalid_rows', 0)
            if dup_count > 0:
                recommendations.append(f"Remove or deduplicate {dup_count} invalid rows")

            # Null handling
            null_ratios = data_quality.get('null_ratio_by_column', {})
            high_null_cols = [col for col, ratio in null_ratios.items() if ratio > 0.1]
            if high_null_cols:
                for col in high_null_cols:
                    recommendations.append(f"Handle nulls in '{col}' ({null_ratios[col]:.1%})")

            # Format validation
            formats = patterns.get('formats', {})
            for col, col_formats in formats.items():
                for fmt, info in col_formats.items():
                    if info.get('match_ratio', 0) < 0.9:
                        recommendations.append(f"Validate {fmt} format in '{col}'")

            # Outlier handling
            distributions = patterns.get('distributions', {})
            for col, dist in distributions.items():
                outlier_count = dist.get('outliers_iqr', {}).get('outlier_count', 0)
                if outlier_count > 10:
                    recommendations.append(f"Review {outlier_count} outliers in '{col}'")

            # Add priority if no recommendations yet
            if not recommendations:
                if expansion_summary.get('coverage_ratio', 1) > 0.99:
                    recommendations.append("Data quality is excellent. Maintain current standards.")
                else:
                    recommendations.append(f"Focus on improving coverage from {expansion_summary.get('coverage_ratio', 0):.1%}")

        except Exception as e:
            self.logger.debug(f"Error generating recommendations: {e}")

        return recommendations[:5]  # Limit to top 5

    def _generate_summary(
        self,
        data_type: str,
        purpose: str,
        quality_grade: str,
        coverage_ratio: float,
        patterns: Dict[str, Any] = None,
    ) -> str:
        """Generate human-readable contextualized summary statement."""
        coverage_pct = round(coverage_ratio * 100, 1)

        # Build summary with context
        if patterns:
            ranges = patterns.get('ranges', {})
            numeric_cols = [col for col, info in ranges.items() if info.get('type') == 'numeric']
            categorical_cols = [col for col, info in ranges.items() if info.get('type') == 'categorical']
            col_count = len(ranges)

            # Infer data nature from columns
            if any(term in str(ranges.keys()).lower() for term in ['medal', 'gold', 'silver', 'bronze']):
                context = "sports competition data"
            elif any(term in str(ranges.keys()).lower() for term in ['customer', 'user', 'account']):
                context = "customer/user data"
            elif any(term in str(ranges.keys()).lower() for term in ['sales', 'revenue', 'amount']):
                context = "financial/sales data"
            elif any(term in str(ranges.keys()).lower() for term in ['date', 'time', 'timestamp']):
                context = "temporal/time-series data"
            elif any(term in str(ranges.keys()).lower() for term in ['country', 'region', 'location']):
                context = "geographic data"
            else:
                context = f"{data_type if data_type != 'unknown' else 'transactional'} data"

            summary = f"This dataset contains {context}. "
            summary += f"Structure: {col_count} columns ({len(numeric_cols)} numeric, {len(categorical_cols)} categorical). "
            summary += f"Quality Grade: {quality_grade} ({coverage_pct}% coverage). "
            summary += "Assessment: " + self._quality_assessment(quality_grade)
        else:
            summary = f"This is {data_type} data used for {purpose}. "
            summary += f"Quality: {quality_grade} ({coverage_pct}% valid rows). "
            summary += self._quality_assessment(quality_grade)

        return summary

    def _quality_assessment(self, quality_grade: str) -> str:
        """Generate quality assessment text."""
        assessments = {
            'A': "Data is in excellent condition with high completeness and minimal issues.",
            'B': "Data is in good condition with minor issues that should be addressed.",
            'C': "Data quality is acceptable but has notable issues requiring remediation.",
            'D': "Data quality is poor with significant issues requiring immediate attention.",
            'F': "Data quality is critical with severe issues that must be resolved."
        }
        return assessments.get(quality_grade, "Data quality assessment pending.")

    def compare(
        self,
        baseline_understanding: Understanding,
        current_understanding: Understanding,
    ) -> Dict[str, Any]:
        """
        Compare baseline vs current understanding.

        Args:
            baseline_understanding: Previous understanding
            current_understanding: Current understanding

        Returns:
            Comparison results with deltas
        """
        try:
            delta_quality = current_understanding.quality_score - baseline_understanding.quality_score
            delta_coverage = current_understanding.coverage_ratio - baseline_understanding.coverage_ratio

            # Determine status
            if delta_quality > 5:
                status = "improved"
            elif delta_quality < -5:
                status = "degraded"
            else:
                status = "stable"

            return {
                'status': status,
                'baseline_quality': baseline_understanding.quality_score,
                'current_quality': current_understanding.quality_score,
                'delta_quality': round(delta_quality, 1),
                'baseline_coverage': baseline_understanding.coverage_ratio,
                'current_coverage': current_understanding.coverage_ratio,
                'delta_coverage': round(delta_coverage, 3),
                'new_strengths': [
                    s for s in current_understanding.strengths
                    if s not in baseline_understanding.strengths
                ],
                'new_weaknesses': [
                    w for w in current_understanding.weaknesses
                    if w not in baseline_understanding.weaknesses
                ],
                'resolved_weaknesses': [
                    w for w in baseline_understanding.weaknesses
                    if w not in current_understanding.weaknesses
                ],
            }
        except Exception as e:
            self.logger.debug(f"Error comparing understandings: {e}")
            return {}
