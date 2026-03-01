"""Pydantic models for Reddit scraper API request/response and data entities."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# API Request / Response
# ---------------------------------------------------------------------------


class SortOption(str, Enum):
    """Supported sort options for the Apify Reddit actor."""

    RELEVANCE = "relevance"
    HOT = "hot"
    TOP = "top"
    NEW = "new"
    COMMENTS = "comments"


class TimeFilter(str, Enum):
    """Supported time filters for the Apify Reddit actor."""

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


class ScrapeRequest(BaseModel):
    """Input parameters for the /scrape endpoint."""

    keyword: str = Field(..., min_length=1, description="Search keyword")
    limit: int = Field(default=10, ge=1, le=100, description="Max results")
    sort: SortOption = Field(default=SortOption.RELEVANCE, description="Sort order")
    time_filter: TimeFilter = Field(default=TimeFilter.DAY, description="Time range")


# ---------------------------------------------------------------------------
# Domain Models — Mapped and generated from Apify raw output
# ---------------------------------------------------------------------------


class RedditPost(BaseModel):
    """A Reddit post as translated from raw Apify output."""

    id: str             # Extracted from permalink, e.g., t3_...
    url: str
    username: str       # mapped from 'author'
    title: str
    community_name: str # mapped from 'subreddit'
    body: str = ""
    up_votes: int       # mapped from 'score'
    number_of_comments: int
    created_at: datetime
    scraped_at: datetime


class RedditComment(BaseModel):
    """A Reddit comment as translated from raw Apify output."""

    id: str             # Generated hash (t1_...) since not provided natively
    parent_id: str      # ID of post (t3_...) or parent comment (t1_...)
    username: str       # mapped from 'author'
    body: str = ""
    up_votes: int       # mapped from 'score'
    number_of_replies: int
    created_at: datetime
    scraped_at: datetime


class ScrapeResponse(BaseModel):
    """Response envelope for the /scrape endpoint."""

    keyword: str
    total_items: int
    posts: list[RedditPost]
    comments: list[RedditComment]

    model_config = {"populate_by_name": True}
