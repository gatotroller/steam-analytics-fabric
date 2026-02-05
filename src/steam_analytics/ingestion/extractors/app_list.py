"""
Steam App List Extractor.

Fetches the complete list of Steam apps using the official IStoreService API.
"""

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger(__name__)


@dataclass
class SteamApp:
    """Basic Steam app info."""

    app_id: int
    name: str


@dataclass
class AppPlayerCount:
    """Player count result for an app."""

    app_id: int
    player_count: int | None
    success: bool
    error: str | None = None


class AppListExtractor:
    """
    Extracts Steam app list using the newer IStoreService (v1).
    """

    # Nuevo endpoint oficial (Requiere API Key)
    STEAM_STORE_SERVICE_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"

    # Endpoint para jugadores
    STEAM_PLAYER_COUNT_URL = (
        "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    )

    # --- LISTA QUE FALTABA (Palabras clave para filtrar basura) ---
    SKIP_KEYWORDS: ClassVar[list[str]] = [
        "soundtrack",
        "ost",
        " ost",
        " dlc",
        "dlc ",
        "pack",
        "bundle",
        "demo",
        "beta test",
        "dedicated server",
        "sdk",
        "tool",
        "editor",
        "skin pack",
        "costume",
        "art book",
        "artbook",
        "wallpaper",
        "bonus content",
        "pre-order",
        "preorder",
        "season pass",
        "expansion pass",
        "supporter",
        "upgrade",
    ]
    # ------------------------------------------------------------

    def __init__(self, requests_per_second: float = 10.0):
        """
        Initialize the extractor.
        """
        self.requests_per_second = requests_per_second
        self._request_interval = 1.0 / requests_per_second

        # Necesitamos la API Key para el nuevo endpoint
        self.api_key = os.environ.get("STEAM__API_KEY")
        if not self.api_key:
            logger.warning("STEAM__API_KEY not found! IStoreService calls will fail.")

    async def get_all_apps(self) -> list[SteamApp]:
        """
        Get complete list using pagination (max 50k per page).
        """
        logger.info("Fetching complete Steam app list via IStoreService...")

        apps = []
        last_appid = 0
        more_results = True

        async with httpx.AsyncClient(timeout=60.0) as client:
            while more_results:
                # Parámetros oficiales de IStoreService
                params = {
                    "key": self.api_key,
                    "max_results": 50000,
                    "last_appid": last_appid,
                    "include_games": "true",  # Filtro de la API: Solo juegos
                    "include_dlc": "false",  # Filtro de la API: No DLCs
                    "include_software": "false",  # Filtro de la API: No Software
                }

                try:
                    response = await client.get(self.STEAM_STORE_SERVICE_URL, params=params)
                    response.raise_for_status()
                    data = response.json()

                    # La estructura es response -> apps
                    batch = data.get("response", {}).get("apps", [])

                    if not batch:
                        more_results = False
                        break

                    for item in batch:
                        # Convertimos a nuestro dataclass
                        apps.append(SteamApp(app_id=item["appid"], name=item["name"]))

                    # Preparamos el ID para la siguiente página
                    last_appid = batch[-1]["appid"]
                    logger.info(f"Fetched batch. Total so far: {len(apps)}")

                    # Si la página trajo menos de 50k, es la última
                    if len(batch) < 50000:
                        more_results = False

                except Exception as e:
                    logger.error(f"Error fetching catalog page: {e}")
                    break

        logger.info("Catalog fetch complete", total_apps=len(apps))
        return apps

    def filter_likely_games(self, apps: list[SteamApp]) -> list[SteamApp]:
        """
        Double-check filtering (even though API filters, some junk slips through).
        """
        logger.info("Filtering apps to likely games", input_count=len(apps))

        filtered = []
        for app in apps:
            name_lower = app.name.lower()

            # Skip if name contains any skip keyword
            should_skip = any(keyword in name_lower for keyword in self.SKIP_KEYWORDS)

            if not should_skip:
                filtered.append(app)

        logger.info(
            "Filtered to likely games",
            input_count=len(apps),
            output_count=len(filtered),
            removed=len(apps) - len(filtered),
        )
        return filtered

    # --- MÉTODOS DE PLAYER COUNT (Se mantienen igual) ---

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=0.5, max=5))
    async def _get_player_count_single(
        self,
        client: httpx.AsyncClient,
        app_id: int,
    ) -> AppPlayerCount:
        """Get player count for a single app."""
        try:
            response = await client.get(
                self.STEAM_PLAYER_COUNT_URL,
                params={"appid": app_id},
            )

            if response.status_code == 200:
                data = response.json()
                player_count = data.get("response", {}).get("player_count")

                return AppPlayerCount(
                    app_id=app_id,
                    player_count=player_count,
                    success=True,
                )
            else:
                return AppPlayerCount(
                    app_id=app_id,
                    player_count=None,
                    success=False,
                    error=f"HTTP {response.status_code}",
                )

        except Exception as e:
            return AppPlayerCount(
                app_id=app_id,
                player_count=None,
                success=False,
                error=str(e),
            )

    async def get_player_counts_batch(
        self,
        app_ids: list[int],
        concurrency: int = 5,
        progress_callback: Callable | None = None,
    ) -> list[AppPlayerCount]:
        """
        Get player counts for multiple apps with rate limiting.
        """
        logger.info(
            "Starting batch player count fetch",
            total_apps=len(app_ids),
            concurrency=concurrency,
        )

        results: list[AppPlayerCount] = []
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(client: httpx.AsyncClient, app_id: int) -> AppPlayerCount:
            async with semaphore:
                result = await self._get_player_count_single(client, app_id)
                await asyncio.sleep(self._request_interval)
                return result

        async with httpx.AsyncClient(timeout=30.0) as client:
            chunk_size = 100
            completed = 0

            for i in range(0, len(app_ids), chunk_size):
                chunk = app_ids[i : i + chunk_size]

                tasks = [fetch_with_semaphore(client, app_id) for app_id in chunk]
                chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in chunk_results:
                    if isinstance(result, Exception):
                        results.append(
                            AppPlayerCount(
                                app_id=-1,
                                player_count=None,
                                success=False,
                                error=str(result),
                            )
                        )
                    else:
                        results.append(result)

                completed += len(chunk)

                if progress_callback:
                    progress_callback(completed, len(app_ids))

                if completed % 1000 == 0 or completed == len(app_ids):
                    logger.info(
                        "Player count fetch progress",
                        completed=completed,
                        total=len(app_ids),
                        percent=f"{completed / len(app_ids) * 100:.1f}%",
                    )

        successful = sum(1 for r in results if r.success)
        logger.info(
            "Batch player count fetch complete",
            total=len(results),
            successful=successful,
            failed=len(results) - successful,
        )

        return results
