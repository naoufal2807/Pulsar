"""
Interactive Chat CLI for Agent Interaction.

Allows users to chat with agents, ask questions, and continue investigating
the dataset after or during journey execution.
"""

import logging
import sys
from typing import Optional
from pathlib import Path

import click
import polars as pl

from pulsar.core.journey_orchestrator import Journey
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.stage_agents import AceAgent
from pulsar.core.llm_connectors import LLMConfig
from pulsar.logging_config import setup_logging, get_logger

# Setup logging
setup_logging(level=logging.INFO)
logger = get_logger(__name__)


class InteractiveAgent:
    """Wrapper for interactive chat with agents."""

    def __init__(self, shared_state: SharedStateStore, llm_config: Optional[LLMConfig] = None):
        """
        Initialize interactive agent.

        Args:
            shared_state: Shared state from journey
            llm_config: LLM configuration
        """
        self.shared_state = shared_state
        self.llm_config = llm_config
        self.agent = AceAgent(
            llm_config=llm_config,
            shared_state=shared_state,
            conversation_id="chat_session",
        )

    def chat(self, message: str) -> str:
        """
        Send a message to the agent and get a response.

        Args:
            message: User message

        Returns:
            Agent response
        """
        response = self.agent.think(message)
        return response

    def show_context(self) -> None:
        """Show current shared state context."""
        stats = self.shared_state.get_stats()
        print("\n" + "=" * 60)
        print("SHARED STATE CONTEXT")
        print("=" * 60)
        print(f"Total keys: {stats['total_keys']}")
        print(f"Memory used: {stats['total_mb']:.2f} MB")
        print(f"Total accesses: {stats['total_accesses']}")
        print(f"\nKeys by stage:")
        for stage, count in stats["writes_by_stage"].items():
            print(f"  {stage}: {count} keys")
        print("=" * 60 + "\n")

    def show_keys(self) -> None:
        """Show all available keys."""
        keys = self.shared_state.keys()
        print("\nAvailable keys:")
        for key in sorted(keys):
            print(f"  - {key}")
        print()


@click.group()
def cli():
    """Interactive Chat CLI for Pulsar Agents."""
    pass


@cli.command()
@click.option(
    "--dataset",
    type=click.Path(exists=True),
    help="Path to dataset CSV/Parquet file",
)
@click.option(
    "--skip-journey",
    is_flag=True,
    help="Skip journey and go directly to chat",
)
def chat(dataset: Optional[str], skip_journey: bool):
    """
    Start interactive chat with agents.

    Runs the complete journey, then opens interactive chat to continue analysis.
    """
    df = None
    shared_state = None

    if not skip_journey and dataset:
        # Load dataset
        logger.info(f"Loading dataset: {dataset}")
        try:
            if dataset.endswith(".parquet"):
                df = pl.read_parquet(dataset)
            else:
                df = pl.read_csv(dataset)
            logger.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            sys.exit(1)

        # Run journey
        dataset_name = Path(dataset).stem
        journey = Journey(
            dataset_name=dataset_name,
            df=df,
        )

        logger.info("Starting journey...")
        try:
            results = journey.execute()
            shared_state = journey.shared_state
            logger.info("Journey completed successfully!")
            print("\n✓ Journey completed successfully!")
            print(f"  Stages: {len(results['results'])} completed")
            print(f"  Memory: {results['shared_state_stats']['total_mb']:.2f} MB")
        except Exception as e:
            logger.error(f"Journey failed: {e}")
            sys.exit(1)
    else:
        # Create empty shared state for chat-only mode
        shared_state = SharedStateStore()
        logger.info("Chat mode: no journey. Using empty shared state.")

    # Start interactive chat
    logger.info("Starting interactive chat...")
    interactive = InteractiveAgent(shared_state)

    print("\n" + "=" * 60)
    print("INTERACTIVE CHAT MODE")
    print("=" * 60)
    print("Type your questions below. Commands:")
    print("  /context - Show current context")
    print("  /keys - List all available keys")
    print("  /help - Show this help")
    print("  /quit - Exit chat")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("You: ").strip()

            if not user_input:
                continue

            if user_input.lower() == "/quit":
                print("\nGoodbye!")
                break
            elif user_input.lower() == "/help":
                print("\nCommands:")
                print("  /context - Show current context")
                print("  /keys - List all available keys")
                print("  /help - Show this help")
                print("  /quit - Exit chat\n")
            elif user_input.lower() == "/context":
                interactive.show_context()
            elif user_input.lower() == "/keys":
                interactive.show_keys()
            else:
                # Send message to agent
                print("\nAgent: Thinking...", end="", flush=True)
                print("\r" + " " * 30 + "\r", end="", flush=True)  # Clear "Thinking..."

                response = interactive.chat(user_input)
                print(f"Agent: {response}\n")

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            print(f"Error: {e}\n")


@cli.command()
@click.option(
    "--dataset",
    type=click.Path(exists=True),
    required=True,
    help="Path to dataset CSV/Parquet file",
)
def journey(dataset: str):
    """
    Run the complete journey without interactive chat.
    """
    # Load dataset
    logger.info(f"Loading dataset: {dataset}")
    try:
        if dataset.endswith(".parquet"):
            df = pl.read_parquet(dataset)
        else:
            df = pl.read_csv(dataset)
        logger.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        sys.exit(1)

    # Run journey
    dataset_name = Path(dataset).stem
    journey_obj = Journey(
        dataset_name=dataset_name,
        df=df,
    )

    logger.info("Starting journey...")
    try:
        results = journey_obj.execute()
        print("\n✓ Journey completed successfully!")
        print(f"  Dataset: {dataset_name}")
        print(f"  Run ID: {results['run_id']}")
        print(f"  Stages completed: {len(results['results'])}")
        print(f"  Memory used: {results['shared_state_stats']['total_mb']:.2f} MB")
        print(f"  Total keys: {results['shared_state_stats']['total_keys']}")
    except Exception as e:
        logger.error(f"Journey failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
