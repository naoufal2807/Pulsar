"""
PresentationLayer: Separates internal logs from user-facing output.

Responsibility: Format agent insights and journey progress for end-users,
keeping all tool execution details, reasoning, and debug info out of the output.

Usage:
    formatter = JourneyPresenter(dataset_name="JPM")
    formatter.present_stage(stage, checkpoint)
    formatter.present_summary(journey_summary)
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class StagePresentation:
    """Formatted output for a single journey stage."""
    stage_name: str
    progress_percent: int
    title: str
    analysis: str  # Agent's final analysis (without tool call details)
    key_insights: List[str]
    next_stage_hint: Optional[str] = None


class JourneyPresenter:
    """Format journey output for end users (clean, no internal mechanics visible)."""

    def __init__(self, dataset_name: str):
        """Initialize presenter for a dataset."""
        self.dataset_name = dataset_name
        self.stages_presented = []

    def present_stage(self, stage_name: str, progress: int, analysis: str, insights: List[str], hint: Optional[str] = None) -> str:
        """
        Format a single stage for user presentation.

        Args:
            stage_name: Stage identifier (SCOUT, EXPLORER, etc.)
            progress: Progress percentage (0, 25, 50, 75, 100)
            analysis: Clean analysis text (agent's final response)
            insights: Bullet-point insights from this stage
            hint: Optional hint for next stage

        Returns:
            Formatted stage output for printing
        """
        output = []

        # Stage header
        output.append(f"\n{'='*80}")
        output.append(f"[{stage_name}] {self._stage_title(stage_name)}")
        output.append(f"Progress: {progress}% Complete")
        output.append(f"{'='*80}\n")

        # Clean analysis (agent's response without tool mechanics)
        output.append(analysis)

        # Key insights
        if insights:
            output.append(f"\n[INSIGHTS] Key Findings This Stage:")
            for insight in insights:
                output.append(f"  • {insight}")

        # Next stage hint
        if hint:
            output.append(f"\n[NEXT] {hint}")

        output.append(f"\n{'='*80}\n")

        self.stages_presented.append({
            'stage': stage_name,
            'progress': progress,
            'timestamp': datetime.now().isoformat(),
        })

        return "\n".join(output)

    def present_summary(self, dataset_name: str, rows: int, cols: int, current_stage: str, progress: int, insights_count: int) -> str:
        """
        Format journey summary for end user.

        Returns:
            Formatted summary output
        """
        output = []
        output.append(f"\n{'='*80}")
        output.append("Journey Summary")
        output.append(f"{'='*80}")
        output.append(f"Dataset: {dataset_name}")
        output.append(f"Size: {rows:,} rows × {cols} columns")
        output.append(f"Current Stage: {current_stage.upper()}")
        output.append(f"Progress: {progress}%")
        output.append(f"Total Insights Gained: {insights_count}")
        output.append(f"{'='*80}\n")

        return "\n".join(output)

    def present_journey_intro(self) -> str:
        """Format journey introduction header."""
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"[JOURNEY] DATA EXPLORATION JOURNEY: {self.dataset_name}")
        output.append(f"{'='*80}")
        output.append(f"Your guide: Intelligent Agent")
        output.append(f"Goal: Become an 'ACE' at understanding your data")
        output.append(f"Progress: SCOUT (0%) > EXPLORER (25%) > DETECTIVE (50%) > ANALYST (75%) > ACE (100%)")
        output.append(f"{'='*80}\n")

        return "\n".join(output)

    @staticmethod
    def _stage_title(stage_name: str) -> str:
        """Get friendly title for stage."""
        titles = {
            'scout': 'Dataset Overview',
            'explorer': 'Pattern Discovery',
            'detective': 'Anomaly Investigation',
            'analyst': 'Advanced Analysis',
            'ace': 'Expert Mastery',
        }
        return titles.get(stage_name.lower(), stage_name)

    def get_stages_presented(self) -> List[Dict[str, Any]]:
        """Get list of stages that were presented."""
        return self.stages_presented


class AnalysisPresenter:
    """Format analysis output (infer, profile, validate) for end users."""

    @staticmethod
    def present_analysis(analysis_type: str, title: str, content: str, recommendations: Optional[List[str]] = None) -> str:
        """
        Format analysis output for user.

        Args:
            analysis_type: Type of analysis (infer, profile, etc.)
            title: Analysis title
            content: Main analysis content
            recommendations: Optional list of recommendations

        Returns:
            Formatted output
        """
        output = []
        output.append(f"\n{'='*80}")
        output.append(f"[{analysis_type.upper()}] {title}")
        output.append(f"{'='*80}\n")
        output.append(content)

        if recommendations:
            output.append(f"\n[RECOMMENDATIONS]")
            for rec in recommendations:
                output.append(f"  • {rec}")

        output.append(f"\n{'='*80}\n")

        return "\n".join(output)


class LogSuppressor:
    """
    Suppress INFO/DEBUG logs from console, keep only output.

    Usage:
        with LogSuppressor():
            # Code here - logs won't print to console
            result = agent.think(question)
        # Back to normal logging
    """

    def __init__(self):
        """Initialize log suppressor."""
        import logging
        self.logger = logging.getLogger()
        self.original_level = None
        self.console_handler = None

    def __enter__(self):
        """Suppress console logging."""
        import logging

        # Save original level
        self.original_level = self.logger.level

        # Find and suppress console handler
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                self.console_handler = handler
                handler.setLevel(logging.WARNING)  # Only show WARNINGS and above

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore console logging."""
        if self.console_handler:
            self.console_handler.setLevel(self.original_level or logging.INFO)
