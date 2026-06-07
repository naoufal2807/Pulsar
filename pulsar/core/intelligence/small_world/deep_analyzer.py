# pulsar/core/intelligence/small_world/deep_analyzer.py
"""
Deep Analyzer: Provide reasoning and depth to agent's intelligence.

Transforms raw patterns into insightful analysis with:
- Why patterns matter
- Business implications
- Data quality risks
- Opportunities and threats
- Competitive insights
- Strategic recommendations
"""

from typing import Any, Dict, List, Optional
import logging

import polars as pl

logger = logging.getLogger(__name__)


class DeepAnalyzer:
    """
    Generate deep, reasoned analysis of data patterns.

    Answers:
    - Why do patterns matter?
    - What are the implications?
    - What risks exist?
    - What opportunities emerge?
    - What should be done?
    """

    def __init__(self, df: pl.DataFrame, patterns: Dict[str, Any], intelligence: Dict[str, Any]):
        """
        Initialize deep analyzer.

        Args:
            df: The DataFrame
            patterns: Discovered patterns
            intelligence: Initial intelligence findings
        """
        self.df = df
        self.patterns = patterns
        self.intelligence = intelligence

    def analyze_concentration(self) -> Dict[str, Any]:
        """
        Deep analysis of market/resource concentration.

        Reasoning:
        - High concentration = power in few hands
        - Low concentration = distributed/competitive
        - Implications for strategy and risk
        """
        analysis = {}

        if 'Total_Medals' in self.df.columns:
            total = self.df['Total_Medals'].sum()
            top_10 = self.df.sort('Total_Medals', descending=True).head(10)['Total_Medals'].sum()
            top_5 = self.df.sort('Total_Medals', descending=True).head(5)['Total_Medals'].sum()

            concentration_10 = (top_10 / total * 100) if total > 0 else 0
            concentration_5 = (top_5 / total * 100) if total > 0 else 0

            analysis['concentration_level'] = self._classify_concentration(concentration_10)
            analysis['herfindahl_index'] = self._calculate_herfindahl()
            analysis['reasoning'] = self._concentration_reasoning(concentration_10, concentration_5)
            analysis['implications'] = self._concentration_implications(concentration_10)
            analysis['opportunities'] = self._identify_opportunities(concentration_10)

        return analysis

    def analyze_distribution_skewness(self) -> Dict[str, Any]:
        """
        Deep analysis of distribution characteristics.

        Reasoning:
        - Right-skewed = few winners, many losers (competitive)
        - Left-skewed = few losers, many winners (saturated)
        - Normal = balanced/fair
        """
        analysis = {}
        distributions = self.patterns.get('distributions', {})

        skewed_columns = []
        for col, dist in distributions.items():
            skewness = dist.get('skewness', 0)
            if abs(skewness) > 1.0:
                skewed_columns.append({
                    'column': col,
                    'skewness': skewness,
                    'type': 'right' if skewness > 0 else 'left',
                    'interpretation': self._interpret_skewness(skewness)
                })

        analysis['skewed_columns'] = skewed_columns
        analysis['overall_pattern'] = self._overall_distribution_pattern(skewed_columns)
        analysis['business_meaning'] = self._distribution_business_meaning(skewed_columns)

        return analysis

    def analyze_variability(self) -> Dict[str, Any]:
        """
        Deep analysis of variability and consistency.

        Reasoning:
        - High variability = inconsistent/unstable
        - Low variability = consistent/stable
        - Implications for forecasting and planning
        """
        analysis = {}
        ranges = self.patterns.get('ranges', {})

        numeric_cols = {}
        for col, info in ranges.items():
            if info.get('type') == 'numeric':
                mean = info.get('mean', 1)
                std = info.get('std', 0)
                max_val = info.get('max', 0)
                min_val = info.get('min', 0)

                # Coefficient of variation
                cv = (std / mean * 100) if mean != 0 else 0
                # Range as % of mean
                range_pct = ((max_val - min_val) / mean * 100) if mean != 0 else 0

                numeric_cols[col] = {
                    'coefficient_of_variation': cv,
                    'range_percent': range_pct,
                    'consistency': self._classify_consistency(cv),
                    'interpretation': self._interpret_variability(cv, range_pct)
                }

        analysis['variability_metrics'] = numeric_cols
        analysis['stable_columns'] = [c for c, m in numeric_cols.items() if m['consistency'] == 'high']
        analysis['volatile_columns'] = [c for c, m in numeric_cols.items() if m['consistency'] == 'low']
        analysis['implications'] = self._variability_implications(numeric_cols)

        return analysis

    def analyze_relationships(self) -> Dict[str, Any]:
        """
        Deep analysis of relationships between variables.

        Reasoning:
        - Strong correlations = dependency/causation potential
        - Weak correlations = independence
        - Implications for modeling and prediction
        """
        analysis = {}
        relationships = self.patterns.get('relationships', {})

        strong_relationships = []

        # Handle both dict and list formats
        if isinstance(relationships, dict):
            for col1, correlations in relationships.items():
                if isinstance(correlations, dict):
                    for col2, corr_value in correlations.items():
                        if isinstance(corr_value, (int, float)) and abs(corr_value) > 0.7:
                            strong_relationships.append({
                                'variable_1': col1,
                                'variable_2': col2,
                                'correlation': corr_value,
                                'interpretation': self._interpret_correlation(corr_value),
                                'causation_likelihood': self._assess_causation(col1, col2)
                            })
        elif isinstance(relationships, list):
            for rel in relationships:
                if isinstance(rel, dict) and 'correlation' in rel:
                    if abs(rel.get('correlation', 0)) > 0.7:
                        strong_relationships.append(rel)

        analysis['strong_relationships'] = strong_relationships
        analysis['total_relationships'] = len(relationships) if isinstance(relationships, (dict, list)) else 0
        analysis['network_density'] = len(strong_relationships) / max(analysis['total_relationships'], 1)
        analysis['strategic_insights'] = self._relationship_strategic_insights(strong_relationships)

        return analysis

    def analyze_data_quality_risks(self) -> Dict[str, Any]:
        """
        Deep analysis of data quality risks and concerns.
        """
        risks = []

        # Check for concentration risk
        conc_str = self.intelligence.get('concentration', {}).get('concentration_in_top_10', '0%')
        try:
            conc_val = float(conc_str.rstrip('%'))
        except (ValueError, AttributeError):
            conc_val = 0

        if conc_val > 70:
            risks.append({
                'risk': 'High Concentration Risk',
                'severity': 'HIGH',
                'description': 'Power concentrated in few entities - vulnerable to disruption',
                'mitigation': 'Diversify or create fallback plans for top performers'
            })

        # Check for skewness risk
        distributions = self.patterns.get('distributions', {})
        highly_skewed = [c for c, d in distributions.items() if abs(d.get('skewness', 0)) > 2.0]
        if highly_skewed:
            risks.append({
                'risk': f'Extreme Skewness in {len(highly_skewed)} column(s)',
                'severity': 'MEDIUM',
                'description': f'Columns {highly_skewed} have extreme outliers',
                'mitigation': 'Investigate outliers, consider transformation or removal'
            })

        # Check for high variability
        ranges = self.patterns.get('ranges', {})
        high_variation = []
        for col, info in ranges.items():
            if info.get('type') == 'numeric':
                mean = info.get('mean', 1)
                std = info.get('std', 0)
                if mean != 0 and std / mean > 1.0:
                    high_variation.append(col)

        if high_variation:
            risks.append({
                'risk': f'High Variability in {len(high_variation)} column(s)',
                'severity': 'MEDIUM',
                'description': f'Columns {high_variation} show extreme volatility',
                'mitigation': 'Investigate root causes, implement stabilization measures'
            })

        return {'risks': risks, 'overall_risk_level': self._calculate_risk_level(risks)}

    # Helper methods for reasoning

    def _classify_concentration(self, pct: float) -> str:
        """Classify concentration level."""
        if pct > 80:
            return "Extremely High (Oligopoly)"
        elif pct > 60:
            return "High (Concentrated)"
        elif pct > 40:
            return "Moderate (Competitive)"
        else:
            return "Low (Distributed)"

    def _calculate_herfindahl(self) -> float:
        """Calculate Herfindahl-Hirschman Index (concentration measure)."""
        if 'Total_Medals' not in self.df.columns:
            return 0.0

        total = self.df['Total_Medals'].sum()
        if total == 0:
            return 0.0

        market_shares = (self.df['Total_Medals'] / total * 100) ** 2
        return market_shares.sum() / 10000  # Normalize to 0-1

    def _concentration_reasoning(self, conc_10: float, conc_5: float) -> str:
        """Provide reasoning about concentration."""
        if conc_10 > 75:
            return f"Top 10 control {conc_10:.1f}% of market - extremely concentrated. Top 5 control {conc_5:.1f}%. This indicates oligopolistic structure with clear dominance."
        elif conc_10 > 50:
            return f"Top 10 control {conc_10:.1f}% - moderately concentrated. Competitive but with clear leaders."
        else:
            return f"Top 10 control {conc_10:.1f}% - distributed market. Highly competitive landscape."

    def _concentration_implications(self, concentration: float) -> List[str]:
        """List business implications of concentration."""
        implications = []

        if concentration > 75:
            implications.extend([
                "Risk: Few dominant players can dictate market conditions",
                "Opportunity: Clear leaders have proven success model",
                "Challenge: Hard for new entrants to compete",
                "Strategy: Either join leaders or find niche"
            ])
        elif concentration > 50:
            implications.extend([
                "Moderately competitive with established leaders",
                "New entrants have reasonable chance if differentiated",
                "Disruption possible if leaders become complacent",
                "Monitor: Track if concentration increasing or decreasing"
            ])
        else:
            implications.extend([
                "Highly competitive, fragmented market",
                "Easy entry, but low profitability for individuals",
                "Consolidation pressure - expect M&A activity",
                "Opportunity: Build scale and efficiency"
            ])

        return implications

    def _identify_opportunities(self, concentration: float) -> List[str]:
        """Identify strategic opportunities based on concentration."""
        opportunities = []

        if concentration > 75:
            opportunities.append("Partner with leaders to gain market access")
            opportunities.append("Identify underserved segments leaders ignore")
            opportunities.append("Build specialized expertise leaders lack")
        else:
            opportunities.append("Build scale through consolidation")
            opportunities.append("Differentiate through specialized offerings")
            opportunities.append("Create network effects to increase switching costs")

        return opportunities

    def _interpret_skewness(self, skewness: float) -> str:
        """Interpret what skewness means."""
        if skewness > 2.0:
            return "Extremely right-skewed: Few big winners, many small losers (highly competitive)"
        elif skewness > 1.0:
            return "Right-skewed: Some clear winners with many smaller participants"
        elif skewness < -1.0:
            return "Left-skewed: Few losers, most participants perform well"
        else:
            return "Approximately normal: Balanced distribution"

    def _overall_distribution_pattern(self, skewed_cols: List[Dict]) -> str:
        """Describe overall distribution pattern."""
        if not skewed_cols:
            return "Balanced distribution across all metrics"

        right_skewed = sum(1 for c in skewed_cols if c['type'] == 'right')
        left_skewed = sum(1 for c in skewed_cols if c['type'] == 'left')

        if right_skewed > left_skewed:
            return "Competitive landscape: Few winners, many losers (right-skewed)"
        elif left_skewed > right_skewed:
            return "Saturated market: Most succeed, few fail (left-skewed)"
        else:
            return "Mixed distribution patterns across metrics"

    def _distribution_business_meaning(self, skewed_cols: List[Dict]) -> str:
        """Explain business meaning of distribution."""
        if not skewed_cols:
            return "Market shows balanced competition with no extreme patterns"

        return (
            f"The {len(skewed_cols)} right-skewed metrics indicate a competitive market "
            f"where success is concentrated. This suggests {len(skewed_cols)} factors drive "
            f"performance inequality - understanding these factors is critical for strategy."
        )

    def _classify_consistency(self, cv: float) -> str:
        """Classify variability/consistency."""
        if cv < 20:
            return "high"
        elif cv < 50:
            return "moderate"
        else:
            return "low"

    def _interpret_variability(self, cv: float, range_pct: float) -> str:
        """Interpret variability meaning."""
        if cv < 20:
            return "Highly consistent and predictable"
        elif cv < 50:
            return "Moderate variation - somewhat predictable with planning"
        else:
            return "Highly variable - difficult to predict or plan"

    def _variability_implications(self, numeric_cols: Dict) -> List[str]:
        """List implications of variability patterns."""
        implications = []

        stable = [c for c, m in numeric_cols.items() if m['consistency'] == 'high']
        volatile = [c for c, m in numeric_cols.items() if m['consistency'] == 'low']

        if stable:
            implications.append(f"Stable metrics ({', '.join(stable)}): Use for forecasting and planning")
        if volatile:
            implications.append(f"Volatile metrics ({', '.join(volatile)}): Require hedging or contingency planning")

        return implications

    def _interpret_correlation(self, corr: float) -> str:
        """Interpret correlation strength and direction."""
        if corr > 0.9:
            return "Very strong positive: Almost perfect linear relationship"
        elif corr > 0.7:
            return "Strong positive: Clear positive relationship"
        elif corr < -0.7:
            return "Strong negative: Clear inverse relationship"
        else:
            return "Moderate relationship"

    def _assess_causation(self, col1: str, col2: str) -> str:
        """Assess likelihood of causation (not just correlation)."""
        # Simple heuristic: if col1 looks like it could cause col2
        if 'time' in col1.lower() or 'date' in col1.lower():
            return "High (temporal precedence)"
        elif 'input' in col1.lower() or 'resource' in col1.lower():
            return "Moderate (plausible causal pathway)"
        else:
            return "Uncertain (requires further investigation)"

    def _relationship_strategic_insights(self, relationships: List[Dict]) -> List[str]:
        """Generate strategic insights from relationships."""
        insights = []

        if relationships:
            insights.append(f"Discovered {len(relationships)} strong relationships - these are key drivers")
            insights.append("Optimize the strongest relationships for maximum impact")
            insights.append("Monitor relationship strength over time - changes signal market shifts")

        return insights

    def _calculate_risk_level(self, risks: List[Dict]) -> str:
        """Calculate overall risk level."""
        high_risks = sum(1 for r in risks if r.get('severity') == 'HIGH')
        medium_risks = sum(1 for r in risks if r.get('severity') == 'MEDIUM')

        if high_risks > 0:
            return "CRITICAL"
        elif medium_risks > 2:
            return "HIGH"
        elif medium_risks > 0:
            return "MEDIUM"
        else:
            return "LOW"
