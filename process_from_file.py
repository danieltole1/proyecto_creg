import asyncio
from src.scraper_creg import CREGScraper

async def main():
    with open("urls_discovered_all_years.txt", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    scraper = CREGScraper()
    await scraper.process_batch(urls, batch_size=200, skip_duplicates=True)

asyncio.run(main())
