"""Production-ready Apify Reddit scraper client.

Wraps the Apify Reddit Posts Scraper actor (2aTxJQei6EYjQsD9A) in a clean,
configurable class with error handling and structured logging.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

from apify_client import ApifyClient

from src.models.schemas import RedditComment, RedditPost

logger = logging.getLogger(__name__)

# Default actor ID for the Reddit Posts Scraper on Apify
_DEFAULT_ACTOR_ID = "2aTxJQei6EYjQsD9A"


class ApifyRedditScraper:
    """Client for scraping Reddit posts and comments via the Apify platform."""

    def __init__(
        self,
        api_token: str,
        actor_id: str = _DEFAULT_ACTOR_ID,
    ) -> None:
        if not api_token:
            raise ValueError("Apify API token is required")
        self._client = ApifyClient(api_token)
        self._actor_id = actor_id
        logger.info("ApifyRedditScraper initialized (actor=%s)", actor_id)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(
        self,
        keyword: str,
        limit: int = 10,
        sort: str = "relevance",
        time_filter: str = "day",
    ) -> tuple[list[RedditPost], list[RedditComment]]:
        """Run the Apify actor and return parsed posts and comments."""
        run_input: dict[str, Any] = {
            "keyword": keyword,
            "limit": limit,
            "sort": sort,
            "time_filter": time_filter,
        }
        logger.info("Starting scrape: keyword=%r, limit=%d, sort=%s, time=%s",
                     keyword, limit, sort, time_filter)

        try:
            run = self._client.actor(self._actor_id).call(run_input=run_input)
        except Exception as exc:
            logger.error("Apify actor run failed: %s", exc)
            raise RuntimeError(f"Apify actor run failed: {exc}") from exc

        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.warning("Actor run returned no dataset ID")
            return [], []

        return self._parse_results(dataset_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_results(
        self,
        dataset_id: str,
    ) -> tuple[list[RedditPost], list[RedditComment]]:
        """Iterate over the actor's dataset and parse the hierarchical data."""
        posts: list[RedditPost] = []
        comments_list: list[RedditComment] = []
        now = datetime.now(timezone.utc)

        for item in self._client.dataset(dataset_id).iterate_items():
            try:
                # 1. Parse the Post
                permalink = item.get("permalink", "")
                # Extract post ID from permalink (e.g. /r/sub/comments/POST_ID/...)
                parts = permalink.split("/")
                post_id_raw = parts[4] if len(parts) > 4 else self._generate_id(str(item))
                post_id = f"t3_{post_id_raw}"

                created_utc = item.get("created_utc", 0)
                try:
                    created_at = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                except (ValueError, TypeError):
                    created_at = now

                post = RedditPost(
                    id=post_id,
                    url=item.get("url", ""),
                    username=item.get("author", "[deleted]"),
                    title=item.get("title", ""),
                    community_name=item.get("subreddit", ""),
                    body=item.get("body", ""),
                    up_votes=item.get("score", 0),
                    number_of_comments=item.get("num_comments", 0),
                    created_at=created_at,
                    scraped_at=now,
                )
                posts.append(post)

                # 2. Parse the recursive Comments tree
                raw_comments = item.get("comments", [])
                if raw_comments:
                    self._extract_comments(raw_comments, post_id, comments_list, created_at, now)

            except Exception as exc:
                logger.warning("Failed to parse post item: %s", exc)

        logger.info("Parsed %d posts and %d comments", len(posts), len(comments_list))
        return posts, comments_list

    def _extract_comments(
        self,
        raw_list: list[dict[str, Any]],
        parent_id: str,
        results: list[RedditComment],
        post_created_at: datetime,
        scraped_at: datetime,
    ) -> None:
        """Recursively flatten the comments tree into a list."""
        for c in raw_list:
            if not isinstance(c, dict):
                continue
            
            author = c.get("author", "[deleted]")
            body = c.get("body", "")
            # Generate deterministic ID since actor doesn't provide one
            comment_id = "t1_" + self._generate_id(f"{author}:{body}:{parent_id}")
            
            replies = c.get("replies", [])
            
            comment = RedditComment(
                id=comment_id,
                parent_id=parent_id,
                username=author,
                body=body,
                up_votes=c.get("score", 0),
                number_of_replies=len(replies) if isinstance(replies, list) else 0,
                created_at=post_created_at,
                scraped_at=scraped_at,
            )
            results.append(comment)
            
            # Recurse for replies
            if isinstance(replies, list) and replies:
                self._extract_comments(replies, comment_id, results, post_created_at, scraped_at)

    @staticmethod
    def _generate_id(text: str) -> str:
        """Generate a short deterministic hash for missing IDs."""
        return hashlib.md5(text.encode("utf-8")).hexdigest()[:10]
