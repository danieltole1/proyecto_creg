# src/discovery_creg.py
import asyncio
import logging
import re
from dataclasses import dataclass
from typing import List, Set, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryConfig:
    base_url: str = "https://gestornormativo.creg.gov.co"
    index_paths: Optional[List[str]] = None
    start_year: int = 1994
    end_year: int = 2025
    headless: bool = True


class CREGDiscovery:
    def __init__(self, config: Optional[DiscoveryConfig] = None):
        self.config = config or DiscoveryConfig()
        if self.config.index_paths is None:
            # Listados oficiales del Gestor Normativo CREG. [web:173][web:174]
            self.config.index_paths = [
                "/gestor/entorno/resoluciones_por_orden_cronologico.html",
                "/gestor/entorno/resoluciones_por_orden_cronologico_derogadas.html",
                "/gestor/entorno/resoluciones_caracter_particular_por_orden_cronologico.html",
            ]

    def _index_urls(self) -> List[str]:
        return [urljoin(self.config.base_url, p) for p in (self.config.index_paths or [])]

    def _normalize_url(self, href: str) -> str:
        """
        Normalizar URLs mal formateadas del CREG.
        - Quitar dobles guiones bajos (__) -> un solo guión (_)
        - Corregir posibles espacios en el número. [web:173]
        """
        href = href.replace("__", "_")
        href = href.replace("- ", "_")
        return href.strip()

    def _extract_urls_from_html(self, html: str, base_for_join: str, year: Optional[int] = None) -> Set[str]:
        soup = BeautifulSoup(html, "html.parser")
        found: Set[str] = set()

        year_pat = re.compile(r"_(\d{4})\.htm$", re.IGNORECASE)

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            href_l = href.lower()

            # Aceptar resoluciones, conceptos y acuerdos relacionados CREG. [web:173]
            if not any(x in href_l for x in ["resolucion_creg", "concepto_creg", "acuerdo_creg"]):
                continue
            if not href_l.endswith(".htm"):
                continue

            href = self._normalize_url(href)
            abs_url = urljoin(base_for_join, href)

            if year is not None:
                m = year_pat.search(abs_url)
                if not m or int(m.group(1)) != int(year):
                    continue

            found.add(abs_url)

        return found

    async def _open_year_filter(self, page: Page) -> bool:
        """
        Abrir el dropdown "FILTRAR POR AÑO" del selector custom.
        Basado en el DOM visto: div.panel-selector-year > div.select-selected. [image:1][web:173]
        """
        try:
            await page.locator("div.panel-selector-year div.select-selected").click(timeout=5000)
            return True
        except Exception:
            return False

    async def _click_year_anyhow(self, page: Page, year: int) -> bool:
        """
        Click robusto sobre el año dentro del dropdown custom.
        Usa el contenedor: div.panel-selector-year div.select-items > div (texto = año). [image:1][web:173]
        """
        year_str = str(year)

        try:
            items_container = page.locator("div.panel-selector-year div.select-items")
            if not await items_container.is_visible():
                await page.locator("div.panel-selector-year div.select-selected").click(timeout=3000)
        except Exception:
            return False

        locator = page.locator(
            "div.panel-selector-year div.select-items div",
            has_text=year_str
        )

        try:
            await locator.first.click(timeout=3000)
            return True
        except Exception:
            return False

    async def _select_year(self, page: Page, year: int) -> bool:
        opened = await self._open_year_filter(page)
        if not opened:
            return False
        return await self._click_year_anyhow(page, year)

    async def discover_all_years(self, verbose: bool = True) -> List[str]:
        index_urls = self._index_urls()
        years = list(range(self.config.end_year, self.config.start_year - 1, -1))

        all_urls: Set[str] = set()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            page = await browser.new_page()

            try:
                for index_url in index_urls:
                    logger.info(f"📍 Índice: {index_url}")
                    logger.info(f"   ⏳ Abriendo: {index_url}")

                    try:
                        await page.goto(index_url, wait_until="domcontentloaded", timeout=60000)
                    except Exception as e:
                        logger.warning(f"   ⚠️ No se pudo abrir índice ({e}). Saltando…")
                        continue

                    # Default (normalmente año actual). [web:173]
                    html0 = await page.content()
                    urls_default = self._extract_urls_from_html(html0, page.url)
                    if urls_default:
                        before = len(all_urls)
                        all_urls.update(urls_default)
                        if verbose:
                            logger.info(
                                f"   ✅ Default: {len(urls_default)} URLs "
                                f"(acum {len(all_urls)}; +{len(all_urls) - before})"
                            )

                    logger.info(f"   🔁 Probando años: {years[0]} ... {years[-1]} (total {len(years)})")

                    for year in years:
                        if year % 5 == 0:
                            logger.info(f"   🗓️ Intentando año: {year}")

                        ok = await self._select_year(page, year)
                        if not ok:
                            if verbose and year in (years[0], years[-1], 2024, 2023):
                                logger.info(f"   ⚠️ No pude seleccionar {year} (selector UI no coincide)")
                            continue

                        await asyncio.sleep(1.3)

                        html = await page.content()
                        urls_year = self._extract_urls_from_html(html, page.url, year=year)

                        if urls_year:
                            before = len(all_urls)
                            all_urls.update(urls_year)
                            if verbose:
                                logger.info(
                                    f"   ✅ {year}: {len(urls_year)} URLs "
                                    f"(acum {len(all_urls)}; +{len(all_urls) - before})"
                                )

                urls_list = sorted(all_urls)
                logger.info(f"✅ TOTAL URLs descubiertas (global): {len(urls_list)}")
                return urls_list

            finally:
                await browser.close()

    def save_urls_to_file(self, urls: List[str], filename: str = "urls_discovered_all_years.txt") -> None:
        with open(filename, "w", encoding="utf-8") as f:
            for u in urls:
                f.write(u.strip() + "\n")
        logger.info(f"💾 URLs guardadas en: {filename}")
