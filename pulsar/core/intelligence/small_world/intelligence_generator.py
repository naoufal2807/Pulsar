# pulsar/core/intelligence/small_world/intelligence_generator.py
"""
Intelligence Generator: Generate HIGH-LEVEL insights about data.

Converts low-level patterns into business intelligence:
- What IS the data about?
- Key terms and entities
- Important patterns and outliers
- What matters in this domain
"""

from typing import Any, Dict, List, Optional
import logging

import polars as pl

logger = logging.getLogger(__name__)


class IntelligenceGenerator:
    """
    Generate high-level intelligence from data.

    Answers:
    - What is this data about? (domain understanding)
    - Who are the key entities? (countries, users, products)
    - What patterns matter? (leaders, outliers, trends)
    - What's the business story? (insights, recommendations)
    """

    def __init__(self, df: pl.DataFrame, patterns: Dict[str, Any]):
        """
        Initialize intelligence generator.

        Args:
            df: The DataFrame to analyze
            patterns: Patterns discovered by Learner
        """
        self.df = df
        self.patterns = patterns
        self.logger = logger

    def generate_intelligence(self) -> Dict[str, Any]:
        """
        Generate comprehensive intelligence about the data.

        Returns:
            Dictionary with data understanding, key terms, insights
        """
        intelligence = {
            'domain': self._infer_domain(),
            'entities': self._identify_key_entities(),
            'key_metrics': self._extract_key_metrics(),
            'top_performers': self._find_top_performers(),
            'patterns_discovered': self._describe_patterns(),
            'outliers_meaning': self._explain_outliers(),
            'concentration': self._analyze_concentration(),
            'summary_statement': self._generate_summary(),
        }

        return intelligence

    def _infer_domain(self) -> str:
        """Infer what domain/industry this data represents."""
        columns = self.df.columns
        columns_lower = [c.lower() for c in columns]

        # Domain detection
        if any(term in columns_lower for term in ['medal', 'gold', 'silver', 'bronze', 'games', 'esports']):
            return "Esports Competition Data"
        elif any(term in columns_lower for term in ['revenue', 'sales', 'amount', 'price']):
            return "Sales/Financial Data"
        elif any(term in columns_lower for term in ['customer', 'user', 'account']):
            return "Customer/User Data"
        elif any(term in columns_lower for term in ['product', 'inventory', 'stock']):
            return "Product/Inventory Data"
        elif any(term in columns_lower for term in ['country', 'region', 'city']):
            return "Geographic/Location Data"
        else:
            return "Business Data"

    def _identify_key_entities(self) -> Dict[str, List[str]]:
        """Identify main entities in the data."""
        entities = {}
        ranges = self.patterns.get('ranges', {})

        for col_name, col_info in ranges.items():
            if col_info.get('type') == 'categorical':
                top_values = col_info.get('top_values', [])
                if top_values:
                    entities[col_name] = [v[0] for v in top_values[:5]]

        return entities

    def _extract_key_metrics(self) -> Dict[str, Any]:
        """Extract important numeric metrics."""
        metrics = {}
        ranges = self.patterns.get('ranges', {})

        for col_name, col_info in ranges.items():
            if col_info.get('type') == 'numeric':
                metrics[col_name] = {
                    'total': col_info.get('sum'),
                    'average': col_info.get('mean'),
                    'max': col_info.get('max'),
                    'min': col_info.get('min'),
                }

        return metrics

    def _find_top_performers(self) -> Dict[str, List[tuple]]:
        """Find top values (what's leading/winning)."""
        top_performers = {}
        ranges = self.patterns.get('ranges', {})

        for col_name, col_info in ranges.items():
            if col_info.get('type') == 'categorical':
                top_values = col_info.get('top_values', [])
                if top_values and col_name not in ['Region', 'Top_Game']:
                    top_performers[col_name] = top_values[:3]

        # Get numeric leaders
        if 'Total_Medals' in self.df.columns:
            top_medals = self.df.select(['Country', 'Total_Medals']).sort('Total_Medals', descending=True).head(3)
            top_performers['Medal_Leaders'] = [
                (row['Country'], row['Total_Medals'])
                for row in top_medals.to_dicts()
            ]

        return top_performers

    def _describe_patterns(self) -> List[str]:
        """Describe important patterns found."""
        patterns_found = []
        ranges = self.patterns.get('ranges', {})
        distributions = self.patterns.get('distributions', {})

        # Identify numeric patterns
        for col_name, col_info in ranges.items():
            if col_info.get('type') == 'numeric':
                mean = col_info.get('mean', 0)
                max_val = col_info.get('max', 0)
                min_val = col_info.get('min', 0)

                # Pattern: high concentration at zero
                if mean < max_val * 0.3:
                    patterns_found.append(f"{col_name}: Highly concentrated (mean {mean:.2f} vs max {max_val})")

                # Pattern: wide range
                if (max_val - min_val) > mean * 5:
                    patterns_found.append(f"{col_name}: Wide variation (range: {min_val}-{max_val})")

        # Check distributions
        for col_name, dist_info in distributions.items():
            skewness = dist_info.get('skewness', 0)
            if abs(skewness) > 1.5:
                direction = "right" if skewness > 0 else "left"
                patterns_found.append(f"{col_name}: Strongly {direction}-skewed (skewness: {skewness:.2f})")

        return patterns_found if patterns_found else ["Standard distribution patterns"]

    def _explain_outliers(self) -> List[str]:
        """Explain what outliers mean in context."""
        outliers_meaning = []

        # Analyze medal distribution
        if 'Total_Medals' in self.df.columns:
            q3 = self.df['Total_Medals'].quantile(0.75)
            outlier_threshold = q3 * 1.5
            outlier_countries = self.df.filter(self.df['Total_Medals'] > outlier_threshold)

            if len(outlier_countries) > 0:
                leaders = outlier_countries.select(['Country', 'Total_Medals']).sort('Total_Medals', descending=True)
                top_count = len(leaders)
                outliers_meaning.append(
                    f"Top {top_count} countries ({100*top_count/len(self.df):.1f}%) dominate with "
                    f"{int(leaders['Total_Medals'].max())} medals"
                )

        # Analyze player distribution
        if 'Total_Players' in self.df.columns:
            avg_players = self.df['Total_Players'].mean()
            high_participation = self.df.filter(self.df['Total_Players'] > avg_players * 2)

            if len(high_participation) > 0:
                outliers_meaning.append(
                    f"{len(high_participation)} countries have above-average participation "
                    f"(>{avg_players*2:.0f} players)"
                )

        return outliers_meaning if outliers_meaning else ["No significant outliers detected"]

    def _analyze_concentration(self) -> Dict[str, Any]:
        """Analyze concentration and distribution."""
        if 'Total_Medals' in self.df.columns:
            total_medals = self.df['Total_Medals'].sum()
            top_10_medals = self.df.sort('Total_Medals', descending=True).head(10)['Total_Medals'].sum()

            concentration = (top_10_medals / total_medals * 100) if total_medals > 0 else 0

            return {
                'total_medals': int(total_medals),
                'countries_with_medals': len(self.df.filter(self.df['Total_Medals'] > 0)),
                'concentration_in_top_10': f"{concentration:.1f}%",
                'meaning': f"Top 10 countries ({100*10/len(self.df):.1f}%) control {concentration:.1f}% of medals"
            }

        return {'meaning': 'Concentration analysis not applicable'}

    def _generate_summary(self) -> str:
        """Generate executive summary."""
        domain = self._infer_domain()
        entities = self._identify_key_entities()

        summary = f"This is {domain}. "

        # Add entity summary
        if 'Country' in entities:
            countries = entities['Country']
            summary += f"Data covers {len(self.df)} countries including {', '.join(countries[:3])}. "

        if 'Top_Game' in entities:
            games = entities['Top_Game']
            summary += f"Popular games: {', '.join(games[:3])}. "

        # Add key insight
        concentration = self._analyze_concentration()
        if 'concentration_in_top_10' in concentration:
            summary += f"{concentration['meaning']}. "

        # Add pattern insight
        patterns = self._describe_patterns()
        if patterns:
            summary += f"Key pattern: {patterns[0].lower()}"

        return summary
