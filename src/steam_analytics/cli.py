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
            "fabric_workspace_id": settings.fabric.workspace_id,
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
  test-config                 Test configuration loading
  extract-store <app_id>      Extract game details from Store API
  extract-reviews <app_id>    Extract reviews from Reviews API
  extract-players <app_id>    Extract player count from Player Stats API
  extract-all <app_id>        Extract all data for a game
  ingest <app_ids>            Run full ingestion pipeline

Options:
  --target <target>           Output target: 'local' (default) or 'onelake'

Examples:
  python -m src.steam_analytics.cli ingest 1091500,570 --target onelake
"""
    print(usage)


async def cmd_ingest(app_ids_str: str, target_str: str = "local") -> None:
    """
    Run full ingestion pipeline.

    Args:
        app_ids_str: Comma-separated app IDs
        target_str: Output target ('local' or 'onelake')
    """
    # 1. IMPORTAMOS OUTPUTTARGET PARA PODER USARLO
    from src.steam_analytics.ingestion.orchestrator import IngestionOrchestrator, OutputTarget

    # Parse app IDs
    app_ids = [int(x.strip()) for x in app_ids_str.split(",")]

    # 2. VALIDAMOS EL TARGET
    try:
        target = OutputTarget(target_str.lower())
    except ValueError:
        print(f"Error: Invalid target '{target_str}'. Use 'local' or 'onelake'.")
        sys.exit(1)

    print("Steam Analytics Platform - Ingestion")
    print(f"{'='*50}")
    print(f"  Target: {target.value}")
    print(f"  Apps: {len(app_ids)}")
    print(f"  App IDs: {app_ids[:5]}{'...' if len(app_ids) > 5 else ''}")
    print(f"{'='*50}\n")

    def on_progress(progress) -> None:
        bar_length = 30
        filled = int(bar_length * progress.percentage / 100)
        bar = "█" * filled + "░" * (bar_length - filled)
        print(
            f"\r  [{bar}] {progress.percentage:.1f}% "
            f"| {progress.completed}/{progress.total} "
            f"| {progress.current_source or ''} "
            f"| app_id={progress.current_app_id or ''}     ",
            end="",
            flush=True,
        )

    # 3. PASAMOS EL TARGET AL ORQUESTADOR
    orchestrator = IngestionOrchestrator(target=target)
    result = await orchestrator.run(app_ids=app_ids, on_progress=on_progress)

    print("\n")  # New line after progress bar

    # Print summary
    print("Ingestion Complete!")
    print(f"{'='*50}")
    print(f"  Run ID: {result.run_id}")
    print(f"  Duration: {result.duration_seconds:.2f}s")
    print(f"  Success Rate: {result.success_rate:.1f}%")
    print(f"  Batches Written: {len(result.batches_written)}")

    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for err in result.errors[:5]:
            print(f"    - {err['source']}/{err['app_id']}: {err['error'][:50]}")

    print("\nBatches:")
    for f in result.batches_written:
        print(f"    - {f}")


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

        elif command == "ingest":
            if len(sys.argv) < 3:
                print("Error: app_ids required (comma-separated)")
                sys.exit(1)
            app_ids_str = sys.argv[2]

            # 4. RESTAURAMOS EL PARSEO DE ARGUMENTOS
            target = "local"
            if "--target" in sys.argv:
                idx = sys.argv.index("--target")
                if idx + 1 < len(sys.argv):
                    target = sys.argv[idx + 1]

            asyncio.run(cmd_ingest(app_ids_str, target))

        elif command in ("help", "--help", "-h"):
            print_usage()

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


if __name__ == "__main__":
    main()
