#src/scraper_creg.py

import asyncio
import logging
from typing import Optional, List, Dict
import re

from playwright.async_api import async_playwright, Browser, Page
from bs4 import BeautifulSoup
import psycopg2

from src.discovery_creg import CREGDiscovery
from src.vectordb_qdrant import VectorDB
from src.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
    POSTGRES_PASSWORD, POSTGRES_DB
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CREGScraper:
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.db_conn = None
        self.base_url = "https://gestornormativo.creg.gov.co"
        self.vectordb: Optional[VectorDB] = None

    async def init_browser(self):
        """Inicializar navegador Playwright"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        logger.info("✅ Navegador iniciado")

    async def close_browser(self):
        """Cerrar navegador"""
        if self.browser:
            await self.browser.close()
            logger.info("✅ Navegador cerrado")

    def init_db(self):
        """Inicializar conexión a PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB
            )
            logger.info("✅ Conectado a PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            raise

    def init_vectordb(self):
        """Inicializar cliente de Qdrant"""
        try:
            self.vectordb = VectorDB(host="localhost", port=6333)
            logger.info("✅ VectorDB (Qdrant) inicializado")
        except Exception as e:
            logger.error(f"❌ Error inicializando VectorDB: {e}")
            self.vectordb = None

    def close_db(self):
        """Cerrar conexión a BD"""
        if self.db_conn:
            self.db_conn.close()
            logger.info("✅ Conexión a BD cerrada")

    # -------- Manejo de errores en BD --------

    def _create_error_table(self):
        """Crear tabla de errores si no existe"""
        if not self.db_conn:
            return

        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls_errores (
                    id SERIAL PRIMARY KEY,
                    url TEXT NOT NULL,
                    tipo_error VARCHAR(100),
                    mensaje_error TEXT,
                    intentos INT DEFAULT 1,
                    fecha TIMESTAMP DEFAULT NOW()
                )
            """)
            self.db_conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ Error creando tabla urls_errores: {e}")
        finally:
            cursor.close()

    def _log_error(self, url: str, tipo_error: str, mensaje: str):
        """Registrar error en BD"""
        if not self.db_conn:
            return

        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO urls_errores (url, tipo_error, mensaje_error)
                VALUES (%s, %s, %s)
            """, (url, tipo_error, mensaje[:2000]))
            self.db_conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ Error registrando fallo: {e}")
        finally:
            cursor.close()

    # -------- Variantes de URL / reintentos --------

    def _generate_url_variants(self, url: str) -> List[str]:
        """
        Generar variantes de URLs para reintentos inteligentes.
        Maneja:
        - Dobles guiones bajos (__)
        - Ceros extra (00001 vs 0001)
        """
        variants = [url]

        if "__" in url:
            variant = url.replace("__", "_")
            if variant not in variants:
                variants.append(variant)
                logger.info(f" 📌 Variante (sin __): {variant}")

        match = re.search(r'(resolucion|concepto|acuerdo)_creg_0+(\d+)_(\d{4})', url, re.IGNORECASE)
        if match:
            doc_type = match.group(1)
            num = match.group(2)
            year = match.group(3)

            for padding in [3, 4]:
                variant_name = f"{doc_type}_creg_{num.zfill(padding)}_{year}.htm"
                if variant_name not in url and variant_name not in variants:
                    full_url = url.replace(match.group(0), variant_name.replace(".htm", ""))
                    if full_url not in variants:
                        variants.append(full_url)
                        logger.info(f" 📌 Variante (padding {padding}): {full_url}")

        return variants

    async def fetch_document(self, url: str) -> Optional[str]:
        """Descargar documento HTML con reintentos inteligentes."""
        variants = self._generate_url_variants(url)
        last_error = None

        for idx, try_url in enumerate(variants, 1):
            page: Page = await self.browser.new_page()
            try:
                if idx == 1:
                    logger.info(f"📥 Descargando: {try_url}")
                else:
                    logger.info(f"📥 Reintento {idx - 1}: {try_url}")

                await page.goto(try_url, wait_until="networkidle", timeout=30000)

                try:
                    await page.wait_for_selector('main, [role="main"], body', timeout=10000)
                except Exception:
                    logger.warning("⚠️ Timeout esperando selector, continuando...")

                html = await page.content()
                if len(html) > 500:
                    logger.info(f"✅ Descargado: {try_url} ({len(html)} bytes)")
                    return html

                last_error = f"HTML muy corto ({len(html)} bytes) - posible error 404"
                logger.warning(f"⚠️ {last_error}")

            except Exception as e:
                last_error = str(e)
                logger.warning(f"⚠️ Error en intento {idx}: {e}")
            finally:
                await page.close()
                await asyncio.sleep(0.5)

        logger.error(f"❌ Error descargando {url}: {last_error}")
        self._log_error(url, "DESCARGA_FALLIDA", last_error or "Desconocido")
        return None

    # -------- Extracción de metadatos y texto --------

    def extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extraer metadatos del documento incluyendo doc_key"""
        match = re.search(
            r'(resolucion|concepto|acuerdo)_creg_0*(\d+)[-_]?(\d+[a-z]?)?_(\d{4})',
            url,
            re.IGNORECASE
        )

        if match:
            doc_type = match.group(1).upper()
            numero_base = match.group(2)
            numero_resolucion = match.group(3) or ""
            año = int(match.group(4))
        else:
            doc_type = "DOCUMENTO"
            numero_base = "DESCONOCIDO"
            numero_resolucion = "DESCONOCIDO"
            año = None

        titulo = None
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            text = heading.get_text(strip=True).upper()
            if any(x in text for x in ["RESOLUCIÓN", "RESOLUCION", "CONCEPTO", "ACUERDO"]):
                titulo = text
                break

        if not titulo:
            doc_div = soup.find(class_='documento')
            if doc_div:
                first_heading = doc_div.find(['h1', 'h2', 'h3'])
                if first_heading:
                    titulo = first_heading.get_text(strip=True)

        if not titulo:
            first_para = soup.find('p')
            if first_para:
                titulo = first_para.get_text(strip=True)[:200]

        texto_completo = soup.get_text()
        fecha_match = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', texto_completo, re.IGNORECASE)
        fecha_publicacion = None

        if fecha_match:
            meses = {
                'enero': '01', 'febrero': '02', 'marzo': '03',
                'abril': '04', 'mayo': '05', 'junio': '06',
                'julio': '07', 'agosto': '08', 'septiembre': '09',
                'octubre': '10', 'noviembre': '11', 'diciembre': '12'
            }
            dia = fecha_match.group(1).zfill(2)
            mes = meses.get(fecha_match.group(2).lower(), '01')
            año_fecha = fecha_match.group(3)
            fecha_publicacion = f"{año_fecha}-{mes}-{dia}"

        numero_db = f"{numero_base}_{numero_resolucion}".rstrip("_")
        doc_key = f"{doc_type}_{numero_db}_{año}" if año else f"{doc_type}_{numero_db}"

        return {
            'numero': numero_db,
            'año': año,
            'titulo': titulo or "SIN TÍTULO",
            'url': url,
            'fecha_publicacion': fecha_publicacion,
            'estado': 'procesada',
            'tipo': doc_type,
            'doc_key': doc_key
        }

    def extract_text_by_articles(self, soup: BeautifulSoup) -> List[Dict]:
        """Extraer texto dividido por artículos"""
        chunks = []
        main_content = soup.find('main') or soup.find(class_='documento') or soup.find('body')
        if not main_content:
            logger.warning("No se encontró contenido principal")
            return []

        texto = main_content.get_text(separator='\n', strip=True)
        articulos = re.split(r'ART[ÍI]CULO\s+(\d+[o°]?\.?)', texto, flags=re.IGNORECASE)

        if len(articulos) > 2:
            considerandos = articulos[0].strip()
            if considerandos and len(considerandos) > 20:
                chunks.append({
                    'indice': 0,
                    'tipo': 'considerandos',
                    'texto': considerandos[:2000]
                })

            for i in range(1, len(articulos), 2):
                if i + 1 < len(articulos):
                    num_articulo = articulos[i].strip()
                    contenido = articulos[i + 1].strip()
                    if contenido and len(contenido) > 20:
                        chunks.append({
                            'indice': (i // 2) + 1,
                            'tipo': 'articulo',
                            'numero': num_articulo,
                            'texto': contenido[:2000]
                        })
        else:
            paragrafos = [p.strip() for p in texto.split('\n\n') if p.strip() and len(p.strip()) > 20]
            for idx, para in enumerate(paragrafos[:20]):
                chunks.append({'indice': idx, 'tipo': 'parrafo', 'texto': para[:2000]})

        if not chunks:
            chunks = [{'indice': 0, 'tipo': 'documento', 'texto': texto[:2000]}]

        return chunks

    # -------- Guardado en BD y Qdrant --------

    def check_if_exists(self, doc_key: str) -> bool:
        """Verificar si una norma ya existe por doc_key (UNIQUE en BD)."""
        if not self.db_conn:
            return False

        cursor = self.db_conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM normas WHERE doc_key = %s",
                (doc_key,)
            )
            exists = cursor.fetchone() is not None
            if exists:
                logger.info(f"⏭️  Duplicado detectado: {doc_key} - SALTANDO")
            return exists
        except Exception as e:
            logger.error(f"❌ Error verificando duplicado: {e}")
            return False
        finally:
            cursor.close()

    def save_to_db(self, metadata: Dict, chunks: List[Dict]):
        """Guardar norma y chunks en PostgreSQL (idempotente por doc_key)."""
        if not self.db_conn:
            logger.error("Conexión a BD no inicializada")
            return

        cursor = self.db_conn.cursor()
        try:
            # Intentar INSERT con ON CONFLICT (doc_key)
            cursor.execute("""
                INSERT INTO normas (numero, año, titulo, url, fecha_publicacion, estado, tipo, doc_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_key) DO UPDATE SET
                    titulo = EXCLUDED.titulo,
                    url = EXCLUDED.url,
                    fecha_publicacion = EXCLUDED.fecha_publicacion,
                    estado = EXCLUDED.estado,
                    fecha_ultima_revision = NOW()
                RETURNING id
            """, (
                metadata['numero'],
                metadata['año'],
                metadata['titulo'][:255],
                metadata['url'],
                metadata['fecha_publicacion'],
                metadata['estado'],
                metadata['tipo'],
                metadata['doc_key']
            ))

            norma_id = cursor.fetchone()[0]
            logger.info(f"✅ Norma guardada/actualizada: {metadata['doc_key']} (ID: {norma_id})")

            # Reemplazar chunks para evitar duplicados en reintentos
            cursor.execute("DELETE FROM chunks WHERE norma_id = %s", (norma_id,))
            for chunk in chunks:
                cursor.execute("""
                    INSERT INTO chunks (norma_id, indice, texto)
                    VALUES (%s, %s, %s)
                """, (norma_id, chunk['indice'], chunk['texto']))

            self.db_conn.commit()
            logger.info(f"✅ {len(chunks)} chunks insertados en PostgreSQL")

            self.save_to_qdrant(metadata, chunks, norma_id)

        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error guardando en BD: {e}")
            self._log_error(metadata.get('url', ''), "ERROR_BD", str(e))
        finally:
            cursor.close()

    def save_to_qdrant(self, metadata: Dict, chunks: List[Dict], norma_id: int):
        """Guardar chunks en Qdrant"""
        if not self.vectordb:
            logger.warning("⚠️ VectorDB no está inicializado, saltando Qdrant")
            return

        try:
            texts = [chunk['texto'] for chunk in chunks]
            ids = [int(f"{norma_id}{str(i).zfill(3)}") for i in range(len(chunks))]

            metadatas = []
            for i, chunk in enumerate(chunks):
                meta = {
                    'norma_numero': metadata['numero'],
                    'norma_id': norma_id,
                    'chunk_index': i,
                    'año': metadata.get('año'),
                    'url': metadata['url'],
                    'titulo': metadata.get('titulo', '')[:100] if metadata.get('titulo') else '',
                    'tipo_chunk': chunk.get('tipo', 'texto'),
                    'doc_key': metadata.get('doc_key', '')
                }
                metadatas.append(meta)

            inserted = self.vectordb.add_documents(texts, ids, metadatas)
            logger.info(f"✅ {inserted} chunks insertados en Qdrant")

        except Exception as e:
            logger.error(f"❌ Error insertando en Qdrant: {e}")

    # -------- Flujo de scraping --------

    async def scrape_url(self, url: str) -> bool:
        """Descargar y procesar una URL"""
        logger.info(f"\n{'=' * 70}")
        logger.info(f"PROCESANDO: {url}")
        logger.info(f"{'=' * 70}")

        html = await self.fetch_document(url)
        if not html:
            return False

        try:
            soup = BeautifulSoup(html, 'html.parser')
            metadata = self.extract_metadata(soup, url)
            logger.info(f"📄 Metadatos extraídos: {metadata['numero']} ({metadata['año']})")

            chunks = self.extract_text_by_articles(soup)
            if not chunks:
                logger.error("❌ No se extrajeron chunks")
                self._log_error(url, "CHUNKS_VACIO", "No se pudieron extraer chunks")
                return False

            logger.info(f"📚 {len(chunks)} chunks extraídos")
            self.save_to_db(metadata, chunks)
            return True

        except Exception as e:
            logger.error(f"❌ Error procesando: {e}")
            self._log_error(url, "ERROR_PROCESAMIENTO", str(e))
            return False

    async def process_batch(self, urls: List[str], batch_size: int = 10, skip_duplicates: bool = True):
        """Procesar un lote de URLs con deduplicación (doc_key) en memoria y BD."""
        await self.init_browser()
        self.init_db()
        self._create_error_table()
        self.init_vectordb()

        total = len(urls)
        successful = 0
        failed = 0
        duplicates = 0

        # ✅ Deduplicación en memoria por doc_key
        seen_keys = set()

        logger.info(f"\n{'=' * 80}")
        logger.info("🚀 INICIANDO PROCESAMIENTO DE BATCH")
        logger.info(f"📊 Total URLs: {total}")
        logger.info(f"✅ Modo: {'Saltando duplicados' if skip_duplicates else 'Actualizando duplicados'}")
        logger.info(f"{'=' * 80}\n")

        for idx, url in enumerate(urls, 1):
            match = re.search(
                r'(resolucion|concepto|acuerdo)_creg_0*(\d+)[-_]?(\d+[a-z]?)?_(\d{4})',
                url,
                re.IGNORECASE
            )

            if not match:
                logger.warning(f"⚠️ [{idx}/{total}] No se pudo extraer número de: {url}")
                self._log_error(url, "REGEX_NO_MATCH", "No coincide con patrón esperado")
                failed += 1
                continue

            doc_type = match.group(1).upper()
            numero_base = match.group(2)
            numero_res = match.group(3) or ""
            numero_db = f"{numero_base}_{numero_res}".rstrip("_")
            año = int(match.group(4))

            # ✅ Clave única: TIPO_NUMERO_AÑO
            doc_key = f"{doc_type}_{numero_db}_{año}"

            # Dedup en memoria
            if doc_key in seen_keys:
                logger.info(f"⏭️  Duplicado en batch: {doc_key} - SALTANDO")
                duplicates += 1
                continue
            seen_keys.add(doc_key)

            # Dedup en BD
            if skip_duplicates and self.check_if_exists(doc_key):
                duplicates += 1
                continue

            logger.info(f"\n[{idx}/{total}] Procesando: {doc_key}")

            if await self.scrape_url(url):
                successful += 1
            else:
                failed += 1

        logger.info(f"\n{'=' * 80}")
        logger.info("📊 RESUMEN FINAL")
        logger.info(f"{'=' * 80}")
        logger.info(f"✅ Exitosas:    {successful}")
        logger.info(f"❌ Fallidas:    {failed}")
        logger.info(f"⏭️  Duplicados:  {duplicates}")
        logger.info(f"📈 Total procesado: {successful + failed + duplicates}/{total}")
        logger.info(f"{'=' * 80}\n")

        await self.close_browser()
        self.close_db()

    async def run_from_file(self, file_path: str, batch_size: int = 10, year_filter: int = None):
        """Leer URLs desde archivo y procesar sin discovery."""
        logger.info("\n" + "=" * 80)
        logger.info("MODO: Leyendo URLs desde archivo (sin discovery)")
        logger.info("=" * 80 + "\n")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                all_urls = [line.strip() for line in f if line.strip()]

            logger.info(f"📂 Leídas {len(all_urls)} URLs de {file_path}")

            # Filtrar por año si se especifica
            if year_filter:
                year_str = str(year_filter)
                filtered_urls = [u for u in all_urls if f"_{year_str}.htm" in u]
                logger.info(f"🔍 Filtradas {len(filtered_urls)} URLs para año {year_filter}")
                urls_to_process = filtered_urls
            else:
                urls_to_process = all_urls

            # Procesar batch
            batch = urls_to_process[:batch_size]
            logger.info(f"⚙️  Procesando batch de {len(batch)} URLs\n")

            await self.process_batch(batch, batch_size=batch_size, skip_duplicates=True)

        except FileNotFoundError:
            logger.error(f"❌ Archivo no encontrado: {file_path}")
        except Exception as e:
            logger.error(f"❌ Error leyendo archivo: {e}")

    async def run_discovery_and_batch(self, batch_size: int = 10, year_filter: int = None):
        """Ejecutar descubrimiento de URLs y procesar primer batch."""
        logger.info("\n" + "=" * 80)
        logger.info("PASO 1: DESCUBRIMIENTO DE URLs")
        logger.info("=" * 80 + "\n")

        discovery = CREGDiscovery()
        all_urls = await discovery.discover_all_years(verbose=True)

        if not all_urls:
            logger.error("❌ No se encontraron URLs")
            return

        if year_filter:
            filtered_urls = [u for u in all_urls if f"_{year_filter}.htm" in u]
            logger.info(f"\n🔍 Filtrado por año {year_filter}: {len(filtered_urls)} URLs")
            urls_to_process = filtered_urls
        else:
            urls_to_process = all_urls

        batch = urls_to_process[:batch_size]

        logger.info("\n" + "=" * 80)
        logger.info(f"PASO 2: PROCESAMIENTO DE BATCH (primeras {len(batch)} URLs)")
        logger.info("=" * 80 + "\n")

        await self.process_batch(batch, batch_size=batch_size, skip_duplicates=True)

        logger.info("\n" + "=" * 80)
        logger.info("PASO 3: GUARDANDO REFERENCIAS")
        logger.info("=" * 80 + "\n")

        discovery.save_urls_to_file(all_urls)
        logger.info("\n✅ Próximas URLs disponibles en: urls_discovered_all_years.txt")
        logger.info("📝 Para procesar más, ejecuta con --batch-size N o --from-file urls_discovered_all_years.txt")

    async def run_test(self, urls: List[str]):
        """Ejecutar scraper en modo test"""
        await self.init_browser()
        self.init_db()
        self._create_error_table()
        self.init_vectordb()

        try:
            successful = 0
            failed = 0

            for url in urls:
                try:
                    if await self.scrape_url(url):
                        successful += 1
                    else:
                        failed += 1
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"❌ Error procesando {url}: {e}")
                    failed += 1

            logger.info(f"\n{'=' * 70}")
            logger.info(f"RESUMEN: {successful} exitosas, {failed} fallidas")
            logger.info(f"{'=' * 70}")

        finally:
            await self.close_browser()
            self.close_db()


async def main():
    """
    Punto de entrada principal con soporte para modos.

    Uso:
      python -m src.scraper_creg
      python -m src.scraper_creg --batch-size 50
      python -m src.scraper_creg --year 2024
      python -m src.scraper_creg --from-file urls_discovered_all_years.txt
      python -m src.scraper_creg --from-file urls_discovered_all_years.txt --year 2009
      python -m src.scraper_creg --test
    """
    import argparse

    parser = argparse.ArgumentParser(description="CREG Scraper - Descarga y vectoriza documentos normativos")
    parser.add_argument('--batch-size', type=int, default=10, help='URLs a procesar (default: 10)')
    parser.add_argument('--year', type=int, help='Filtrar por año (ej: 2024)')
    parser.add_argument('--from-file', type=str, help='Leer URLs desde archivo (omite discovery)')
    parser.add_argument('--test', action='store_true', help='Ejecutar con test_urls hardcodeadas')

    args = parser.parse_args()
    scraper = CREGScraper()

    try:
        if args.test:
            logger.info("🧪 MODO TEST - URLs hardcodeadas")
            test_urls = [
                "https://gestornormativo.creg.gov.co/gestor/entorno/docs/resolucion_creg_101-34a_2022.htm",
                "https://gestornormativo.creg.gov.co/gestor/entorno/docs/resolucion_creg_101-55_2024.htm"
            ]
            await scraper.run_test(test_urls)

        elif args.from_file:
            logger.info(f"📂 MODO: Archivo ({args.from_file})")
            await scraper.run_from_file(
                file_path=args.from_file,
                batch_size=args.batch_size,
                year_filter=args.year
            )

        else:
            logger.info("🔍 MODO: Discovery + Procesamiento")
            await scraper.run_discovery_and_batch(
                batch_size=args.batch_size,
                year_filter=args.year
            )

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupción por usuario (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
    finally:
        logger.info("🔌 Cerrando recursos...")


if __name__ == "__main__":
    asyncio.run(main())