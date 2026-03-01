"""Neo4j graph database service for persisting Reddit data.

Graph schema
============

Nodes:
    (:Keyword  {term})
    (:User     {username})
    (:Subreddit{name})
    (:Post     {post_id, title, body, url, up_votes, num_comments, created_at, scraped_at})
    (:Comment  {comment_id, body, up_votes, num_replies, created_at, scraped_at})

Relationships:
    (:Keyword)  -[:RETURNED  {searched_at}]-> (:Post)
    (:User)     -[:AUTHORED  {at}]->          (:Post)
    (:User)     -[:AUTHORED  {at}]->          (:Comment)
    (:Post)     -[:BELONGS_TO]->              (:Subreddit)
    (:Post)     -[:HAS_COMMENT]->             (:Comment)
    (:Comment)  -[:REPLY_TO]->                (:Comment)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from neo4j import GraphDatabase, Driver

from src.models.schemas import RedditComment, RedditPost

logger = logging.getLogger(__name__)


class Neo4jService:
    """Manages all Neo4j interactions for the Reddit scraper.

    Usage::

        svc = Neo4jService("bolt://localhost:7687", "neo4j", "password")
        svc.save_results("Python", posts, comments)
        records = svc.run_cypher("MATCH (n) RETURN n LIMIT 10")
        svc.close()
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("Neo4j driver connected to %s", uri)
        self._ensure_constraints()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Shut down the driver and release resources."""
        self._driver.close()
        logger.info("Neo4j driver closed")

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def _ensure_constraints(self) -> None:
        """Create uniqueness constraints / indexes on first run."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Keyword)   REQUIRE k.term IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)      REQUIRE u.username IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Post)      REQUIRE p.post_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Comment)   REQUIRE c.comment_id IS UNIQUE",
        ]
        with self._driver.session() as session:
            for cypher in constraints:
                session.run(cypher)
        logger.info("Neo4j constraints ensured")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_results(
        self,
        keyword: str,
        posts: list[RedditPost],
        comments: list[RedditComment],
    ) -> dict[str, int]:
        """Persist scraped posts and comments into the graph.

        All writes use MERGE for idempotency — re-scraping the same keyword
        is safe and will update existing nodes.

        Returns:
            Dict with counts of merged nodes by type.
        """
        now = datetime.now(timezone.utc).isoformat()
        stats = {"posts": 0, "comments": 0}

        with self._driver.session() as session:
            for post in posts:
                session.execute_write(
                    self._merge_post, keyword, post, now,
                )
                stats["posts"] += 1

            for comment in comments:
                session.execute_write(
                    self._merge_comment, keyword, comment, now,
                )
                stats["comments"] += 1

        logger.info("Saved %d posts and %d comments for keyword=%r",
                     stats["posts"], stats["comments"], keyword)
        return stats

    # ------------------------------------------------------------------
    # Cypher query runner
    # ------------------------------------------------------------------

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute an arbitrary Cypher query and return records as dicts.

        This is the main entry point for ad-hoc graph exploration.
        """
        with self._driver.session() as session:
            result = session.run(query, params or {})
            records = [record.data() for record in result]
        logger.debug("Cypher query returned %d records", len(records))
        return records

    # ------------------------------------------------------------------
    # Transaction functions (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_post(
        tx: Any,
        keyword: str,
        post: RedditPost,
        searched_at: str,
    ) -> None:
        """Merge a single post with all related nodes and relationships."""
        tx.run(
            """
            // Keyword node
            MERGE (k:Keyword {term: $keyword})

            // User node
            MERGE (u:User {username: $username})

            // Subreddit node
            MERGE (s:Subreddit {name: $subreddit})

            // Post node
            MERGE (p:Post {post_id: $post_id})
            SET p.title         = $title,
                p.body          = $body,
                p.url           = $url,
                p.up_votes      = $up_votes,
                p.num_comments  = $num_comments,
                p.created_at    = $created_at,
                p.scraped_at    = $scraped_at

            // Relationships
            MERGE (k)-[r1:RETURNED]->(p)
            SET r1.searched_at  = $searched_at

            MERGE (u)-[r2:AUTHORED]->(p)
            SET r2.at           = $created_at

            MERGE (p)-[:BELONGS_TO]->(s)
            """,
            keyword=keyword,
            username=post.username,
            subreddit=post.community_name,
            post_id=post.id,
            title=post.title,
            body=post.body,
            url=post.url,
            up_votes=post.up_votes,
            num_comments=post.number_of_comments,
            created_at=post.created_at.isoformat(),
            scraped_at=post.scraped_at.isoformat(),
            searched_at=searched_at,
        )

    @staticmethod
    def _merge_comment(
        tx: Any,
        keyword: str,
        comment: RedditComment,
        searched_at: str,
    ) -> None:
        """Merge a single comment with user and parent relationships.

        parentId prefix determines the relationship type:
          - t3_ → top-level comment on a post  → (:Post)-[:HAS_COMMENT]->(:Comment)
          - t1_ → reply to another comment      → (:Comment)-[:REPLY_TO]->(:Comment)
        """
        # Always merge the user and comment node
        tx.run(
            """
            MERGE (u:User {username: $username})

            MERGE (c:Comment {comment_id: $comment_id})
            SET c.body         = $body,
                c.up_votes     = $up_votes,
                c.num_replies  = $num_replies,
                c.created_at   = $created_at,
                c.scraped_at   = $scraped_at

            MERGE (u)-[r:AUTHORED]->(c)
            SET r.at           = $created_at
            """,
            username=comment.username,
            comment_id=comment.id,
            body=comment.body,
            up_votes=comment.up_votes,
            num_replies=comment.number_of_replies,
            created_at=comment.created_at.isoformat(),
            scraped_at=comment.scraped_at.isoformat(),
        )

        # Link to parent — post or comment
        parent_id = comment.parent_id
        if parent_id.startswith("t3_"):
            # Top-level comment on a post
            tx.run(
                """
                MATCH (c:Comment {comment_id: $comment_id})
                MERGE (p:Post {post_id: $parent_id})
                MERGE (p)-[:HAS_COMMENT]->(c)
                """,
                comment_id=comment.id,
                parent_id=parent_id,
            )
        elif parent_id.startswith("t1_"):
            # Reply to another comment
            tx.run(
                """
                MATCH (c:Comment {comment_id: $comment_id})
                MERGE (parent:Comment {comment_id: $parent_id})
                MERGE (c)-[:REPLY_TO]->(parent)
                """,
                comment_id=comment.id,
                parent_id=parent_id,
            )
