# test_app_list.py
import asyncio

from dotenv import load_dotenv

from src.steam_analytics.ingestion.extractors.app_list import AppListExtractor

load_dotenv()


async def test():
    extractor = AppListExtractor()

    # 1. Get all apps
    all_apps = await extractor.get_all_apps()
    print(f"Total apps: {len(all_apps):,}")

    # 2. Filter to likely games
    games = extractor.filter_likely_games(all_apps)
    print(f"Likely games: {len(games):,}")

    # 3. Test player count for a few
    test_ids = [730, 570, 1091500]  # CS2, Dota2, Cyberpunk
    results = await extractor.get_player_counts_batch(test_ids)

    for r in results:
        print(f"  {r.app_id}: {r.player_count:,} players")


asyncio.run(test())
