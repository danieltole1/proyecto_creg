import asyncio
import logging
from datetime import datetime

from src.discovery_creg import CREGDiscovery, DiscoveryConfig

logging.basicConfig(level=logging.INFO)

async def main():
    current_year = datetime.now().year

    cfg = DiscoveryConfig(
        start_year=2024,
        end_year=current_year,
        headless=False  # para VER qué pasa con el filtro
    )

    discovery = CREGDiscovery(cfg)
    urls = await discovery.discover_all_years(verbose=True)
    discovery.save_urls_to_file(urls, "urls_discovered_all_years.txt")

asyncio.run(main())
