"""
Eenmalig setup: open een zichtbare browser, log in op BGA, sla sessie op.
Daarna kan de automatische import de opgeslagen sessie hergebruiken.

Gebruik:
    python scripts/bga_save_session.py
"""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

SESSION_PATH = Path("data/bga_session")


async def save_session():
    async with async_playwright() as pw:
        # Persistent context slaat cookies + localStorage op
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_PATH),
            headless=False,  # ZICHTBAAR — je logt zelf in
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = await context.new_page()
        await page.goto("https://boardgamearena.com/account", wait_until="networkidle")

        print("\n" + "="*55)
        print("Log in op BGA in het browservenster dat is geopend.")
        print("Zodra je op de BGA lobby bent, kom je hier terug.")
        print("="*55 + "\n")

        # Wacht tot de gebruiker ingelogd is (URL bevat 'lobby' of 'welcome')
        await page.wait_for_url(
            lambda url: any(s in url for s in ["/lobby", "/welcome", "/gamelobby"]),
            timeout=120000,  # 2 minuten om in te loggen
        )

        print(f"Ingelogd! URL: {page.url}")
        print("Sessie wordt opgeslagen ...")
        await context.close()
        print(f"Sessie opgeslagen in: {SESSION_PATH}")
        print("\nJe kan nu de automatische import draaien met:")
        print("  python -m src.importers.bga_fetcher --players 93464744 84635111 65246746 --since 2020-01-01")


asyncio.run(save_session())
