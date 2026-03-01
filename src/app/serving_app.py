"""Minimal FastAPI application — health check and scraper endpoint."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from src.api.apify_client import ApifyRedditScraper
from src.db.neo4j_service import Neo4jService
from src.models.schemas import ScrapeRequest, ScrapeResponse

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared service instances (initialized at startup, cleaned up at shutdown)
# ---------------------------------------------------------------------------
scraper: ApifyRedditScraper | None = None
neo4j_svc: Neo4jService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize and tear down shared services."""
    global scraper, neo4j_svc  # noqa: PLW0603

    apify_token = os.getenv("APIFY_TOKEN", "")
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

    scraper = ApifyRedditScraper(api_token=apify_token)
    neo4j_svc = Neo4jService(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)

    logger.info("Services initialized — ready to serve")
    yield

    # Shutdown
    if neo4j_svc:
        neo4j_svc.close()
    logger.info("Services shut down")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Reddit Scraper API",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness / readiness probe."""
    return {"status": "ok"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(request: ScrapeRequest) -> ScrapeResponse:
    """Run the Reddit scraper for the given keyword and persist results to Neo4j.

    Accepts keyword, limit, sort, and time_filter as input parameters.
    Returns the scraped posts and comments.
    """
    if scraper is None or neo4j_svc is None:
        raise HTTPException(status_code=503, detail="Services not initialized")

    try:
        posts, comments = scraper.scrape(
            keyword=request.keyword,
            limit=request.limit,
            sort=request.sort.value,
            time_filter=request.time_filter.value,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    # Persist to Neo4j
    try:
        neo4j_svc.save_results(
            keyword=request.keyword,
            posts=posts,
            comments=comments,
        )
    except Exception as exc:
        logger.error("Neo4j persistence failed: %s", exc)
        # Still return scraped data even if persistence fails
        logger.warning("Returning scraped data without graph persistence")

    return ScrapeResponse(
        keyword=request.keyword,
        total_items=len(posts) + len(comments),
        posts=posts,
        comments=comments,
    )
