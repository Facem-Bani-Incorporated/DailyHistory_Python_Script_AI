import asyncio
import os
import httpx
from datetime import datetime
from sqlalchemy import text
from core.logger import setup_logger
from core.config import config
from core.database import engine, AsyncSessionLocal, IngestionLog
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- FUNCȚIA DE SALVARE ÎN DB (PYTHON SIDE) ---
async def log_to_db(status: str, year: int = None, score: float = None, error: str = None):
    """Salvează rezultatul execuției în PostgreSQL."""
    if engine is None:
        logger.warning("⚠️ DB Engine neconfigurat, se sare peste logging.")
        return

    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                new_log = IngestionLog(
                    main_event_year=year,
                    status=status,
                    impact_score=score,
                    error_message=error[:500] if error else None  # Limităm eroarea pentru DB
                )
                session.add(new_log)
            await session.commit()
        logger.info(f"📊 Status [{status}] salvat în baza de date.")
    except Exception as e:
        logger.error(f"❌ Nu s-a putut salva log-ul în DB: {e}")


# --- FUNCȚIA DE TRANSMISIE CĂTRE JAVA ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            config.JAVA_BACKEND_URL,
            json=payload.model_dump(mode='json'),
            headers=headers
        )
        response.raise_for_status()
        return response.status_code


async def main():
    logger.info("🚀 Pornire Pipeline cu Database Logging...")

    # 0. Inițializare Tabele (Doar dacă nu există)
    try:
        from core.database import init_db
        await init_db()
    except Exception as e:
        logger.error(f"❌ Eroare fatală la inițializarea DB: {e}")
        # Nu ne oprim aici, dar logăm problema

    # Init variabile pentru logging
    current_main_year = None
    current_score = None

    try:
        # 1. SETUP MODULE
        try:
            scraper = WikiScraper()
            processor = AIProcessor()
            ranker = ScoringEngine()
            logger.info("⚙️ Module inițializate.")
        except Exception as e:
            await log_to_db("INIT_ERROR", error=str(e))
            raise

        # 2. FETCH & HEURISTIC RANK
        try:
            raw_events = await scraper.fetch_today()
            if not raw_events:
                raise ValueError("Wikipedia nu a returnat evenimente.")

            for item in raw_events:
                item['h_score'] = ranker.heuristic_score(item)

            candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:50]
            logger.info(f"📚 Preluat {len(candidates)} candidați.")
        except Exception as e:
            await log_to_db("SCRAPER_ERROR", error=str(e))
            raise

        # 3. AI SCORING
        try:
            ai_data = await processor.batch_score_and_translate_titles(candidates)
            for idx, item in enumerate(candidates):
                res = ai_data['results'].get(f"ID_{idx}", {"score": 50, "titles": {}})
                item['final_score'] = ranker.hybrid_calculate(item['h_score'], res['score'])
                item['titles'] = res['titles']

            candidates.sort(key=lambda x: x['final_score'], reverse=True)
            top_data = candidates[0]
            current_main_year = top_data['year']
            current_score = top_data['final_score']
            logger.info(f"🤖 AI Scoring gata pentru anul {current_main_year}.")
        except Exception as e:
            await log_to_db("AI_SCORING_ERROR", error=str(e))
            raise

        # 4. CONTENT & MEDIA
        try:
            main_content = await processor.generate_multilingual_main_event(top_data['text'], top_data['year'])

            p_main = top_data.get("pages", [])
            slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"

            wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
            main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                            enumerate(wiki_imgs)]
            logger.info("🖼️ Media urcată pe Cloudinary.")
        except Exception as e:
            await log_to_db("MEDIA_CONTENT_ERROR", year=current_main_year, error=str(e))
            raise

        # 5. SECONDARY & PAYLOAD
        try:
            secondary_objs = []
            for idx, item in enumerate(candidates[1:6]):
                p_sec = item.get("pages", [])
                slug_sec = p_sec[0].get("titles", {}).get("canonical") if p_sec else ""
                thumb = None
                if slug_sec:
                    imgs_sec = await scraper.fetch_gallery_urls(slug_sec, limit=1)
                    if imgs_sec:
                        thumb = scraper.upload_to_cloudinary(imgs_sec[0], f"sec_{item['year']}_{idx}")

                secondary_objs.append(SecondaryEvent(
                    title_translations=item['titles'],
                    year=item['year'],
                    source_url=f"https://en.wikipedia.org/wiki/{slug_sec}",
                    ai_relevance_score=item['final_score'],
                    thumbnail_url=thumb
                ))

            payload = DailyPayload(
                date_processed=datetime.now().date(),
                api_secret=config.INTERNAL_API_SECRET,
                main_event=MainEvent(
                    title_translations=main_content['titles'],
                    year=top_data['year'],
                    source_url=f"https://en.wikipedia.org/wiki/{slug_main}",
                    event_date=datetime.now().date(),
                    narrative_translations=main_content['narratives'],
                    impact_score=top_data['final_score'],
                    gallery=[img for img in main_gallery if img]
                ),
                secondary_events=secondary_objs
            )

            await send_to_java(payload)
            logger.info("✅ Pipeline terminat și trimis la Java!")
            await log_to_db(status="SUCCESS", year=current_main_year, score=current_score)
        except Exception as e:
            await log_to_db("JAVA_DELIVERY_ERROR", year=current_main_year, error=str(e))
            raise

    except Exception as e:
        logger.error(f"🚨 Pipeline crashed: {e}")

    finally:
        if engine:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())