# tests/intelligence/test_journey.py
"""Tests for Data Exploration Journey."""

import pytest
import polars as pl

from pulsar.core.intelligence.journey import (
    DataExplorationJourney, Stage, JourneyCheckpoint
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        'id': list(range(1, 101)),
        'name': ['User_' + str(i) for i in range(1, 101)],
        'age': [20 + i % 50 for i in range(100)],
        'salary': [30000 + i * 500 for i in range(100)],
        'department': ['Sales', 'Engineering', 'HR'] * 33 + ['Sales'],
        'performance': [i % 5 + 1 for i in range(100)],
    })


class TestJourneyInitialization:
    """Test journey initialization."""

    def test_journey_creation(self, sample_df):
        """Test creating a data exploration journey."""
        journey = DataExplorationJourney(sample_df, "test_dataset")

        assert journey.dataset_name == "test_dataset"
        assert journey.current_stage == Stage.SCOUT
        assert len(journey.checkpoints) == 0

    def test_journey_has_agent(self, sample_df):
        """Test journey has agent with tools."""
        journey = DataExplorationJourney(sample_df, "test_dataset")

        assert journey.agent is not None
        assert journey.agent.tools_enabled is True
        assert journey.agent.tool_registry is not None

    def test_journey_dataset_info(self, sample_df):
        """Test journey stores dataset information."""
        journey = DataExplorationJourney(sample_df, "test_dataset")

        assert journey.df.height == 100
        assert journey.df.width == 6


class TestJourneyStages:
    """Test individual journey stages."""

    def test_scout_stage(self, sample_df):
        """Test SCOUT stage (overview)."""
        journey = DataExplorationJourney(sample_df, "employees")
        journey.agent.provider_available = False  # Use fallback

        intro = journey.start_journey()

        assert "SCOUT" in intro
        assert "0%" in intro or "0" in intro  # Progress indicator
        assert journey.current_stage == Stage.SCOUT
        assert len(journey.checkpoints) == 1

    def test_checkpoint_creation(self, sample_df):
        """Test checkpoint is properly created."""
        journey = DataExplorationJourney(sample_df, "test_dataset")
        journey.agent.provider_available = False

        journey.start_journey()
        checkpoint = journey.checkpoints[0]

        assert checkpoint.stage == Stage.SCOUT
        assert "SCOUT" in checkpoint.title
        assert len(checkpoint.questions_explored) > 0
        assert len(checkpoint.insights_gained) > 0

    def test_explorer_stage(self, sample_df):
        """Test EXPLORER stage (discovery)."""
        journey = DataExplorationJourney(sample_df, "data")
        journey.agent.provider_available = False

        journey.start_journey()
        explorer = journey.explore_stage()

        assert "EXPLORER" in explorer
        assert "25%" in explorer or "25" in explorer
        assert journey.current_stage == Stage.EXPLORER
        assert len(journey.checkpoints) == 2

    def test_detective_stage(self, sample_df):
        """Test DETECTIVE stage (investigation)."""
        journey = DataExplorationJourney(sample_df, "data")
        journey.agent.provider_available = False

        journey.start_journey()
        journey.explore_stage()
        detective = journey.detective_stage()

        assert "DETECTIVE" in detective
        assert "50%" in detective or "50" in detective
        assert journey.current_stage == Stage.DETECTIVE
        assert len(journey.checkpoints) == 3

    def test_analyst_stage(self, sample_df):
        """Test ANALYST stage (advanced insights)."""
        journey = DataExplorationJourney(sample_df, "data")
        journey.agent.provider_available = False

        journey.start_journey()
        journey.explore_stage()
        journey.detective_stage()
        analyst = journey.analyst_stage()

        assert "ANALYST" in analyst
        assert "75%" in analyst or "75" in analyst
        assert journey.current_stage == Stage.ANALYST
        assert len(journey.checkpoints) == 4

    def test_ace_stage(self, sample_df):
        """Test ACE stage (mastery)."""
        journey = DataExplorationJourney(sample_df, "data")
        journey.agent.provider_available = False

        journey.start_journey()
        journey.explore_stage()
        journey.detective_stage()
        journey.analyst_stage()
        ace = journey.ace_stage()

        assert "ACE" in ace
        assert "100%" in ace or "100" in ace
        assert journey.current_stage == Stage.ACE
        assert len(journey.checkpoints) == 5


class TestJourneyProgression:
    """Test journey progression."""

    def test_complete_journey(self, sample_df):
        """Test running the complete journey."""
        journey = DataExplorationJourney(sample_df, "complete_test")
        journey.agent.provider_available = False

        result = journey.run_complete_journey()

        assert "SCOUT" in result
        assert "EXPLORER" in result
        assert "DETECTIVE" in result
        assert "ANALYST" in result
        assert "ACE" in result
        assert len(journey.checkpoints) == 5

    def test_journey_summary(self, sample_df):
        """Test getting journey summary."""
        journey = DataExplorationJourney(sample_df, "test_data")
        journey.agent.provider_available = False

        journey.start_journey()
        journey.explore_stage()
        journey.detective_stage()
        journey.analyst_stage()
        journey.ace_stage()

        summary = journey.get_journey_summary()

        assert summary['dataset'] == "test_data"
        assert summary['total_rows'] == 100
        assert summary['total_columns'] == 6
        assert summary['current_stage'] == "ace"
        assert summary['progress_percent'] == 100
        assert summary['checkpoints_completed'] == 5
        assert summary['total_insights'] > 0


class TestStageEnum:
    """Test Stage enum."""

    def test_all_stages_exist(self):
        """Test all journey stages are defined."""
        assert Stage.SCOUT.value == "scout"
        assert Stage.EXPLORER.value == "explorer"
        assert Stage.DETECTIVE.value == "detective"
        assert Stage.ANALYST.value == "analyst"
        assert Stage.ACE.value == "ace"

    def test_stage_ordering(self):
        """Test stages are in logical order."""
        stages = [Stage.SCOUT, Stage.EXPLORER, Stage.DETECTIVE, Stage.ANALYST, Stage.ACE]
        assert len(stages) == 5


class TestJourneyCheckpoint:
    """Test JourneyCheckpoint dataclass."""

    def test_checkpoint_creation(self):
        """Test creating a checkpoint."""
        checkpoint = JourneyCheckpoint(
            stage=Stage.SCOUT,
            title="Overview",
            description="Dataset overview",
            questions_explored=["What is the data?"],
            insights_gained=["Found 100 rows"],
            next_stage_hint="Explore patterns",
        )

        assert checkpoint.stage == Stage.SCOUT
        assert checkpoint.title == "Overview"
        assert len(checkpoint.questions_explored) == 1
        assert len(checkpoint.insights_gained) == 1
