"""
Layer 2: ConversationStore - Persistent conversation storage.

Stores full conversation history in SQLite for:
- Resume conversations after restart
- Replay conversation for debugging
- Audit trail (who did what, when)
- Multi-session analytics

Problem solved:
- Agent conversations lost in memory on restart
- No conversation history for compliance/debugging
- Can't replay to find where issue occurred

Solution:
- SQLite JSONB storage (fast, queryable)
- Index by agent_id and timestamp
- Support resume from any checkpoint
- Enable conversation replay
"""

import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class ConversationStore:
    """Persistent conversation storage with SQLite."""

    def __init__(self, db_path: str = "conversations.db"):
        """
        Initialize conversation store.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        logger.info(f"ConversationStore initialized at {db_path}")

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with self.connection() as conn:
            # Conversations table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    dataset_name TEXT,
                    messages TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    metadata TEXT,
                    total_turns INTEGER DEFAULT 0,
                    total_tokens INTEGER DEFAULT 0
                )
            """)

            # Indexes for fast queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_agent_id
                ON conversations(agent_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_created_at
                ON conversations(created_at DESC)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status
                ON conversations(status)
            """)

            conn.commit()
            logger.debug("Database schema initialized")

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def save(
        self,
        conversation_id: str,
        agent_id: str,
        messages: List[Dict[str, str]],
        dataset_name: Optional[str] = None,
        status: str = "active",
        metadata: Optional[Dict] = None,
    ) -> None:
        """
        Save or update conversation.

        Args:
            conversation_id: Unique conversation ID
            agent_id: Agent identifier
            messages: List of message dicts
            dataset_name: Optional dataset name
            status: Conversation status (active, completed, error)
            metadata: Optional metadata dict
        """
        now = datetime.utcnow().isoformat()

        with self.connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO conversations
                (id, agent_id, dataset_name, messages, created_at, updated_at, status, metadata, total_turns, total_tokens)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                conversation_id,
                agent_id,
                dataset_name,
                json.dumps(messages),
                now,
                now,
                status,
                json.dumps(metadata) if metadata else None,
                len(messages),
                sum(len(m.get("content", "")) // 4 for m in messages),  # Estimate tokens
            ))
            conn.commit()

        logger.debug(f"Saved conversation {conversation_id}: {len(messages)} messages")

    def load(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """
        Load conversation by ID.

        Args:
            conversation_id: Conversation ID

        Returns:
            Dict with messages, status, metadata or None if not found
        """
        with self.connection() as conn:
            cursor = conn.execute("""
                SELECT messages, status, metadata, created_at, updated_at
                FROM conversations
                WHERE id = ?
            """, (conversation_id,))
            row = cursor.fetchone()

            if not row:
                return None

            return {
                "messages": json.loads(row[0]),
                "status": row[1],
                "metadata": json.loads(row[2]) if row[2] else {},
                "created_at": row[3],
                "updated_at": row[4],
            }

    def list_by_agent(
        self, agent_id: str, limit: int = 20, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List conversations for an agent.

        Args:
            agent_id: Agent identifier
            limit: Max results
            status: Filter by status (optional)

        Returns:
            List of conversation summaries
        """
        with self.connection() as conn:
            if status:
                cursor = conn.execute("""
                    SELECT id, created_at, updated_at, status, total_turns, total_tokens
                    FROM conversations
                    WHERE agent_id = ? AND status = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (agent_id, status, limit))
            else:
                cursor = conn.execute("""
                    SELECT id, created_at, updated_at, status, total_turns, total_tokens
                    FROM conversations
                    WHERE agent_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (agent_id, limit))

            return [
                {
                    "id": row[0],
                    "created_at": row[1],
                    "updated_at": row[2],
                    "status": row[3],
                    "total_turns": row[4],
                    "total_tokens": row[5],
                }
                for row in cursor.fetchall()
            ]

    def list_by_dataset(
        self, dataset_name: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        List conversations for a dataset.

        Args:
            dataset_name: Dataset name
            limit: Max results

        Returns:
            List of conversation summaries
        """
        with self.connection() as conn:
            cursor = conn.execute("""
                SELECT id, agent_id, created_at, status, total_turns
                FROM conversations
                WHERE dataset_name = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (dataset_name, limit))

            return [
                {
                    "id": row[0],
                    "agent_id": row[1],
                    "created_at": row[2],
                    "status": row[3],
                    "total_turns": row[4],
                }
                for row in cursor.fetchall()
            ]

    def delete(self, conversation_id: str) -> None:
        """Archive a conversation (soft delete)."""
        with self.connection() as conn:
            conn.execute("""
                UPDATE conversations SET status = 'archived'
                WHERE id = ?
            """, (conversation_id,))
            conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        with self.connection() as conn:
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total_conversations,
                    COUNT(DISTINCT agent_id) as unique_agents,
                    SUM(total_turns) as total_turns,
                    SUM(total_tokens) as total_tokens
                FROM conversations
                WHERE status != 'archived'
            """)
            row = cursor.fetchone()

            return {
                "total_conversations": row[0] or 0,
                "unique_agents": row[1] or 0,
                "total_turns": row[2] or 0,
                "total_tokens": row[3] or 0,
            }
