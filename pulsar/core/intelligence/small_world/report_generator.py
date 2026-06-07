# pulsar/core/intelligence/small_world/report_generator.py
"""
Report Generator: Create detailed markdown intelligence reports.

Transforms agent's learned knowledge into comprehensive markdown documentation
with deep reasoning and strategic analysis.
"""

from typing import Any, Dict, Optional
from datetime import datetime
import logging

import polars as pl

logger = logging.getLogger(__name__)


class IntelligenceReportGenerator:
    """Generate detailed markdown intelligence reports with deep analysis."""

    def __init__(
        self,
        dataset_name: str,
        intelligence: Dict[str, Any],
        df: Optional[pl.DataFrame] = None,
        patterns: Optional[Dict[str, Any]] = None,
        deep_analysis: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize report generator.

        Args:
            dataset_name: Name of the dataset
            intelligence: Intelligence dictionary from IntelligenceGenerator
            df: Optional DataFrame for deep analysis
            patterns: Optional patterns dictionary
            deep_analysis: Optional pre-computed deep analysis
        """
        self.dataset_name = dataset_name
        self.intelligence = intelligence
        self.df = df
        self.patterns = patterns or {}
        self.deep_analysis = deep_analysis or {}
        self.generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def generate_report(self) -> str:
        """
        Generate comprehensive markdown intelligence report.

        Returns:
            Markdown formatted report
        """
        report = []

        # Header
        report.append("# Data Intelligence Report\n")
        report.append(f"**Dataset**: `{self.dataset_name}`\n")
        report.append(f"**Generated**: {self.generated_at}\n")
        report.append(f"**Status**: [OK] Intelligence Acquisition Complete\n\n")

        # Executive Summary
        report.append(self._executive_summary())

        # Domain Understanding
        report.append(self._domain_section())

        # Key Entities
        report.append(self._entities_section())

        # Key Metrics
        report.append(self._metrics_section())

        # Top Performers
        report.append(self._leaders_section())

        # Patterns Discovered
        report.append(self._patterns_section())

        # Outliers Analysis
        report.append(self._outliers_section())

        # Concentration Analysis
        report.append(self._concentration_section())

        # Deep Analysis Sections
        if self.deep_analysis:
            report.append(self._concentration_deep_analysis())
            report.append(self._distribution_analysis())
            report.append(self._variability_analysis())
            report.append(self._relationships_analysis())
            report.append(self._risk_analysis())

        # Recommendations
        report.append(self._recommendations_section())

        return "\n".join(report)

    def _executive_summary(self) -> str:
        """Generate executive summary section."""
        section = []
        section.append("## Executive Summary\n")
        section.append(self.intelligence.get('summary_statement', 'No summary available'))
        section.append("\n")
        return "\n".join(section)

    def _domain_section(self) -> str:
        """Generate domain understanding section."""
        section = []
        section.append("## Domain Understanding\n")
        section.append(f"**What is this data about?**\n")
        section.append(f"> {self.intelligence.get('domain', 'Unknown Domain')}\n\n")
        section.append("This dataset represents ")
        section.append(self.intelligence.get('domain', 'business data').lower())
        section.append(". The agent has acquired deep knowledge of the domain through isolated analysis.\n")
        return "\n".join(section)

    def _entities_section(self) -> str:
        """Generate key entities section."""
        section = []
        section.append("## Key Entities & Terms\n")

        entities = self.intelligence.get('entities', {})
        if entities:
            for entity_type, values in entities.items():
                section.append(f"### {entity_type}\n")
                for value in values[:5]:
                    section.append(f"- `{value}`\n")
                section.append("")
        else:
            section.append("No key entities identified.\n")

        return "\n".join(section)

    def _metrics_section(self) -> str:
        """Generate key metrics section."""
        section = []
        section.append("## Key Metrics\n")

        metrics = self.intelligence.get('key_metrics', {})
        if metrics:
            section.append("| Metric | Average | Maximum | Total |\n")
            section.append("|--------|---------|---------|-------|\n")

            for metric, stats in metrics.items():
                avg = f"{stats.get('average', 0):.2f}" if stats else "N/A"
                max_val = stats.get('max', 'N/A') if stats else "N/A"
                total = stats.get('total', 'N/A') if stats else "N/A"
                section.append(f"| {metric} | {avg} | {max_val} | {total} |\n")
        else:
            section.append("No numeric metrics found.\n")

        section.append("")
        return "\n".join(section)

    def _leaders_section(self) -> str:
        """Generate top performers/leaders section."""
        section = []
        section.append("## Top Performers & Leaders\n")

        performers = self.intelligence.get('top_performers', {})
        if performers:
            for category, items in performers.items():
                section.append(f"### {category}\n")
                for i, (name, count) in enumerate(items[:5], 1):
                    section.append(f"{i}. **{name}**: {count}\n")
                section.append("")
        else:
            section.append("No top performers identified.\n")

        return "\n".join(section)

    def _patterns_section(self) -> str:
        """Generate discovered patterns section."""
        section = []
        section.append("## Patterns Discovered\n")

        patterns = self.intelligence.get('patterns_discovered', [])
        if patterns:
            section.append("The agent discovered the following patterns:\n\n")
            for pattern in patterns:
                section.append(f"- **{pattern}**\n")
        else:
            section.append("No significant patterns discovered.\n")

        section.append("")
        return "\n".join(section)

    def _outliers_section(self) -> str:
        """Generate outliers analysis section."""
        section = []
        section.append("## Outliers & Their Meaning\n")

        outliers = self.intelligence.get('outliers_meaning', [])
        if outliers:
            section.append("Anomalies detected and their significance:\n\n")
            for outlier in outliers:
                section.append(f"- **{outlier}**\n")
        else:
            section.append("No significant outliers detected.\n")

        section.append("")
        return "\n".join(section)

    def _concentration_section(self) -> str:
        """Generate concentration analysis section."""
        section = []
        section.append("## Concentration & Distribution Analysis\n")

        concentration = self.intelligence.get('concentration', {})
        if concentration:
            section.append(concentration.get('meaning', 'Concentration analysis unavailable'))
            section.append("\n\n")

            if 'total_medals' in concentration:
                section.append(f"**Total Units**: {concentration.get('total_medals', 0)}\n\n")
            if 'countries_with_medals' in concentration:
                section.append(f"**Participants with Activity**: {concentration.get('countries_with_medals', 0)}\n\n")
            if 'concentration_in_top_10' in concentration:
                section.append(f"**Top 10% Control**: {concentration.get('concentration_in_top_10', 'N/A')}\n\n")
        else:
            section.append("Concentration analysis not available.\n")

        section.append("")
        return "\n".join(section)

    def _concentration_deep_analysis(self) -> str:
        """Generate deep analysis of concentration."""
        section = []
        section.append("## Deep Analysis: Market Concentration\n")

        conc = self.deep_analysis.get('concentration', {})
        if conc:
            section.append(f"**Level**: {conc.get('concentration_level', 'Unknown')}\n\n")
            section.append(f"**HHI Index**: {conc.get('herfindahl_index', 0):.3f}\n\n")
            section.append(f"**Reasoning**:\n{conc.get('reasoning', 'N/A')}\n\n")

            implications = conc.get('implications', [])
            if implications:
                section.append("**Business Implications**:\n")
                for impl in implications:
                    section.append(f"- {impl}\n")
                section.append("")

            opportunities = conc.get('opportunities', [])
            if opportunities:
                section.append("**Strategic Opportunities**:\n")
                for opp in opportunities:
                    section.append(f"- {opp}\n")
                section.append("")

        return "\n".join(section)

    def _distribution_analysis(self) -> str:
        """Generate analysis of distribution patterns."""
        section = []
        section.append("## Deep Analysis: Distribution Patterns\n")

        dist = self.deep_analysis.get('distribution', {})
        if dist:
            section.append(f"**Pattern**: {dist.get('overall_pattern', 'N/A')}\n\n")
            section.append(f"**Business Meaning**: {dist.get('business_meaning', 'N/A')}\n\n")

            skewed = dist.get('skewed_columns', [])
            if skewed:
                section.append("**Skewed Metrics**:\n")
                for col_data in skewed[:5]:
                    section.append(f"- {col_data['column']}: {col_data['interpretation']}\n")
                section.append("")

        return "\n".join(section)

    def _variability_analysis(self) -> str:
        """Generate analysis of variability."""
        section = []
        section.append("## Deep Analysis: Consistency & Stability\n")

        var = self.deep_analysis.get('variability', {})
        if var:
            stable = var.get('stable_columns', [])
            volatile = var.get('volatile_columns', [])

            if stable:
                section.append(f"**Stable Metrics**: {', '.join(stable)}\n")
                section.append("Use these for forecasting and planning - they are predictable.\n\n")

            if volatile:
                section.append(f"**Volatile Metrics**: {', '.join(volatile)}\n")
                section.append("These require risk management and contingency planning.\n\n")

            implications = var.get('implications', [])
            for impl in implications:
                section.append(f"- {impl}\n")

        return "\n".join(section)

    def _relationships_analysis(self) -> str:
        """Generate analysis of relationships."""
        section = []
        section.append("## Deep Analysis: Key Relationships\n")

        rel = self.deep_analysis.get('relationships', {})
        if rel:
            strong_rels = rel.get('strong_relationships', [])

            if strong_rels:
                section.append(f"**Found {len(strong_rels)} strong relationships**:\n\n")
                for r in strong_rels[:5]:
                    section.append(
                        f"- **{r.get('variable_1')} <-> {r.get('variable_2')}**: "
                        f"Correlation {r.get('correlation'):.2f}\n"
                    )
                    section.append(f"  - {r.get('interpretation')}\n")
                    section.append(f"  - Causation likelihood: {r.get('causation_likelihood')}\n\n")

            insights = rel.get('strategic_insights', [])
            if insights:
                section.append("**Strategic Insights**:\n")
                for insight in insights:
                    section.append(f"- {insight}\n")

        return "\n".join(section)

    def _risk_analysis(self) -> str:
        """Generate data quality risk analysis."""
        section = []
        section.append("## Data Quality Risk Assessment\n")

        risks = self.deep_analysis.get('risks', {})
        if risks:
            overall = risks.get('overall_risk_level', 'UNKNOWN')
            section.append(f"**Overall Risk Level**: {overall}\n\n")

            risk_list = risks.get('risks', [])
            if risk_list:
                section.append("**Identified Risks**:\n\n")
                for risk in risk_list:
                    section.append(f"### {risk.get('risk')} ({risk.get('severity')})\n")
                    section.append(f"{risk.get('description')}\n\n")
                    section.append(f"**Mitigation**: {risk.get('mitigation')}\n\n")

        return "\n".join(section)

    def _recommendations_section(self) -> str:
        """Generate recommendations section."""
        section = []
        section.append("## Agent Recommendations\n")

        section.append("Based on the acquired intelligence:\n\n")

        # Generate recommendations from patterns
        patterns = self.intelligence.get('patterns_discovered', [])
        if patterns:
            section.append("### Address Discovered Patterns\n")
            for pattern in patterns[:2]:
                section.append(f"- Investigate: {pattern.lower()}\n")
            section.append("")

        # Generate recommendations from outliers
        outliers = self.intelligence.get('outliers_meaning', [])
        if outliers:
            section.append("### Monitor Outliers\n")
            for outlier in outliers[:2]:
                section.append(f"- Track: {outlier.lower()}\n")
            section.append("")

        # Concentration recommendations
        concentration = self.intelligence.get('concentration', {})
        if concentration and 'concentration_in_top_10' in concentration:
            section.append("### Manage Concentration\n")
            section.append("- Monitor market/resource concentration\n")
            section.append("- Track if concentration increasing or decreasing\n")
            section.append("- Identify opportunities in underrepresented segments\n")

        return "\n".join(section)
