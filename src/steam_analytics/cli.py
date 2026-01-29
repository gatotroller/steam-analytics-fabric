"""
Command-line interface for Steam Analytics Platform.

Provides commands to test extractors and run ingestion manually.
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel

from src.steam_analytics.config import get_settings
from src.steam_analytics.ingestion.orchestrator import IngestionOrchestrator, IngestionProgress
from src.steam_analytics.logger import get_logger, setup_logging

# Initialize logging
setup_logging()
logger = get_logger(__name__, component="cli")


class CLIOutput(BaseModel):
    """Structured output for CLI commands."""

    success: bool
    command: str
    timestamp: datetime = datetime.now(timezone.utc)
    data: dict[str, Any] | list[Any] | None = None
    error: str | None = None


def print_json(output: CLIOutput) -> None:
    """Print output as formatted JSON."""
    print(json.dumps(output.model_dump(), indent=2, default=str))


def print_result(result: Any, format: str = "json") -> None:
    """Print extraction result in specified format."""
    if format == "json":
        if hasattr(result, "model_dump"):
            print(json.dumps(result.model_dump(), indent=2, default=str))
        else:
            print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


async def cmd_extract_store(app_id: int, country: str = "US") -> None:
    """Extract game details from Steam Store API."""
    from src.steam_analytics.ingestion.extractors import SteamStoreExtractor

    logger.info("Extracting store data", app_id=app_id, country=country)

    async with SteamStoreExtractor() as extractor:
        result = await extractor.extract(app_id=app_id, country_code=country)

    output = CLIOutput(
        success=result.success,
        command="extract-store",
        data=result.model_dump() if result.success else None,
        error=result.error_message,
    )
    print_json(output)


async def cmd_extract_reviews(app_id: int, num_reviews: int = 10) -> None:
    """Extract reviews from Steam Reviews API."""
    from src.steam_analytics.ingestion.extractors import SteamReviewsExtractor

    logger.info("Extracting reviews", app_id=app_id, num_reviews=num_reviews)

    async with SteamReviewsExtractor() as extractor:
        result = await extractor.extract(app_id=app_id, num_per_page=num_reviews)

    output = CLIOutput(
        success=result.success,
        command="extract-reviews",
        data=result.model_dump() if result.success else None,
        error=result.error_message,
    )
    print_json(output)


async def cmd_extract_players(app_id: int) -> None:
    """Extract current player count from Steam Player Stats API."""
    from src.steam_analytics.ingestion.extractors import SteamPlayerStatsExtractor

    logger.info("Extracting player count", app_id=app_id)

    async with SteamPlayerStatsExtractor() as extractor:
        result = await extractor.extract(app_id=app_id)

    output = CLIOutput(
        success=result.success,
        command="extract-players",
        data=result.model_dump() if result.success else None,
        error=result.error_message,
    )
    print_json(output)


async def cmd_extract_all(app_id: int) -> None:
    """Extract all data for a game (store, reviews, players)."""
    from src.steam_analytics.ingestion.extractors import (
        SteamPlayerStatsExtractor,
        SteamReviewsExtractor,
        SteamStoreExtractor,
    )

    logger.info("Extracting all data", app_id=app_id)

    results = {}

    # Extract store data
    async with SteamStoreExtractor() as extractor:
        store_result = await extractor.extract(app_id=app_id)
        results["store"] = store_result.model_dump()

    # Extract reviews
    async with SteamReviewsExtractor() as extractor:
        reviews_result = await extractor.extract_summary_only(app_id=app_id)
        results["reviews"] = reviews_result.model_dump()

    # Extract player count
    async with SteamPlayerStatsExtractor() as extractor:
        players_result = await extractor.extract(app_id=app_id)
        results["players"] = players_result.model_dump()

    all_success = all(
        [
            store_result.success,
            reviews_result.success,
            players_result.success,
        ]
    )

    output = CLIOutput(
        success=all_success,
        command="extract-all",
        data=results,
    )
    print_json(output)


async def cmd_test_config() -> None:
    """Test configuration loading."""
    settings = get_settings()

    output = CLIOutput(
        success=True,
        command="test-config",
        data={
            "environment": settings.environment,
            "steam_base_url": settings.steam.base_url,
            "steam_store_url": settings.steam.store_url,
            "steam_requests_per_minute": settings.steam.requests_per_minute,
            "retry_max_attempts": settings.retry.max_attempts,
            "log_level": settings.logging.level,
            "api_key_configured": bool(settings.steam.api_key.get_secret_value()),
        },
    )
    print_json(output)


def print_usage() -> None:
    """Print CLI usage information."""
    usage = """
Steam Analytics Platform CLI
============================

Usage: python -m src.cli <command> [arguments]

Commands:
  test-config                   Test configuration loading
  extract-store <app_id>        Extract game details from Store API
  extract-reviews <app_id>      Extract reviews from Reviews API
  extract-players <app_id>      Extract player count from Player Stats API
  extract-all <app_id>          Extract all data for a game

Examples:
  python -m src.cli test-config
  python -m src.cli extract-store 1091500
  python -m src.cli extract-reviews 1091500
  python -m src.cli extract-players 1091500
  python -m src.cli extract-all 1091500

Popular App IDs:
  1091500  - Cyberpunk 2077
  570      - Dota 2
  730      - Counter-Strike 2
  440      - Team Fortress 2
  1245620  - Elden Ring
  292030   - The Witcher 3

  ingest <app_ids>              Run full ingestion (comma-separated IDs)

Examples:
  ...
  python -m src.cli ingest 1091500,570,730
"""
    print(usage)


def main() -> None:
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "test-config":
            asyncio.run(cmd_test_config())

        elif command == "extract-store":
            if len(sys.argv) < 3:
                print("Error: app_id required")
                sys.exit(1)
            app_id = int(sys.argv[2])
            asyncio.run(cmd_extract_store(app_id))

        elif command == "extract-reviews":
            if len(sys.argv) < 3:
                print("Error: app_id required")
                sys.exit(1)
            app_id = int(sys.argv[2])
            asyncio.run(cmd_extract_reviews(app_id))

        elif command == "extract-players":
            if len(sys.argv) < 3:
                print("Error: app_id required")
                sys.exit(1)
            app_id = int(sys.argv[2])
            asyncio.run(cmd_extract_players(app_id))

        elif command == "extract-all":
            if len(sys.argv) < 3:
                print("Error: app_id required")
                sys.exit(1)
            app_id = int(sys.argv[2])
            asyncio.run(cmd_extract_all(app_id))

        elif command in ("help", "--help", "-h"):
            print_usage()

        elif command == "ingest":
            if len(sys.argv) < 3:
                print("Error: app_ids required (comma-separated)")
                sys.exit(1)
            app_ids_str = sys.argv[2]
            asyncio.run(cmd_ingest(app_ids_str))

        else:
            print(f"Unknown command: {command}")
            print_usage()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception("CLI error", error=str(e))
        output = CLIOutput(
            success=False,
            command=command,
            error=str(e),
        )
        print_json(output)
        sys.exit(1)


async def cmd_ingest(app_ids_str: str) -> None:
    """Run full ingestion for a list of app IDs."""

    # Parse app IDs (comma-separated)
    app_ids = [int(x.strip()) for x in app_ids_str.split(",")]

    logger.info("Starting ingestion", app_ids=app_ids)

    def on_progress(progress: IngestionProgress) -> None:
        """Print progress updates."""
        print(
            f"\rProgress: {progress.completed}/{progress.total} "
            f"({progress.percentage:.1f}%) - "
            f"Success: {progress.successful}, Failed: {progress.failed}",
            end="",
            flush=True,
        )

    orchestrator = IngestionOrchestrator()
    result = await orchestrator.run_full_ingestion(
        app_ids=app_ids,
        on_progress=on_progress,
    )

    print()  # New line after progress

    output = CLIOutput(
        success=result.success_rate == 100.0,
        command="ingest",
        data={
            "run_id": str(result.run_id),
            "duration_seconds": result.duration_seconds,
            "total_apps": result.total_apps,
            "successful": result.successful,
            "failed": result.failed,
            "success_rate": result.success_rate,
            "batches_written": result.batches_written,
            "errors": result.errors[:10],  # Limit errors shown
        },
    )
    print_json(output)


if __name__ == "__main__":
    main()
