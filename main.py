import asyncio
import os
import httpx
from datetime import datetime
from sqlalchemy import text
from core.logger import setup_logger
from core.config import config
from core.database import engine, AsyncSessionLocal, IngestionLog, ProcessedEvent
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- 1. FUNCȚIA DE LOGGING (AUDIT) ---
# Mutată sus pentru a fi vizibilă în tot scriptul
async def log_to_db(status: str, year: int = None, score: float = None, error: str = None):
    """Salvează statusul execuției în tabelul ingestion_logs."""
    if engine is None: return
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                new_log = IngestionLog(
                    main_event_year=year,
                    status=status,
                    impact_score=score,
                    error_message=error[:500] if error else None
                )
                session.add(new_log)
            await session.commit()
        logger.info(f"📊 Status [{status}] salvat în baza de date.")
    except Exception as e:
        logger.error(f"❌ Nu s-a putut salva log-ul: {e}")


# --- 2. FUNCȚIA DE ARHIVARE (DATE REALE) ---
# Reparată pentru a converti obiectele Translations în dict (JSON serializable)
async def save_event_content(payload: DailyPayload):
    """Salvează conținutul tradus în tabelul processed_events."""
    if engine is None: return
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event

                # Conversie explicită în dict pentru a evita eroarea de JSON
                new_entry = ProcessedEvent(
                    event_date=payload.date_processed,
                    year=main.year,
                    titles=dict(main.title_translations),
                    narrative=dict(main.narrative_translations),
                    image_url=main.gallery[0] if main.gallery else None,
                    impact_score=main.impact_score,
                    source_url=main.source_url
                )
                session.add(new_entry)
            await session.commit()
        logger.info(f"🏛️ Conținutul din {main.year} a fost ARHIVAT în DB.")
    except Exception as e:
        logger.error(f"❌ Eroare la arhivarea conținutului: {e}")
        raise  # Ridicăm eroarea ca să știm în main() că a eșuat


# --- 3. FUNCȚIA DE TRANSMISIE JAVA (COMENTATĂ) ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    headers = {"X-Internal-Api-Key": config.INTERNAL_API_SECRET, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(config.JAVA_BACKEND_URL, json=payload.model_dump(mode='json'), headers=headers)
        response.raise_for_status()
        return response.status_code


# --- 4. PIPELINE-UL PRINCIPAL ---
async def main():
    logger.info("🚀 Pornire Pipeline...")

    try:
        from core.database import init_db
        await init_db()
    except Exception as e:
        logger.error(f"❌ Init DB failed: {e}")

    current_main_year = None
    current_score = None

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # FETCH
        raw_events = await scraper.fetch_today()
        if not raw_events: raise ValueError("Wikipedia empty response")

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:50]

        # AI SCORING (Protejat la KeyError)
        ai_data = await processor.batch_score_and_translate_titles(candidates)
        results = ai_data.get('results', {})

        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {})
            item['final_score'] = ranker.hybrid_calculate(item['h_score'], res.get('score', 50))
            item['titles'] = res.get('titles', {})  # Protecție KeyError 'titles'

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]
        current_main_year = top_data['year']
        current_score = top_data['final_score']

        logger.info(f"🤖 AI Scoring gata pentru anul {current_main_year}.")

        # MEDIA & CONTENT
        main_content = await processor.generate_multilingual_main_event(top_data['text'], top_data['year'])
        p_main = top_data.get("pages", [])
        slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"

        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                        enumerate(wiki_imgs)]

        # SECONDARY
        secondary_objs = []
        for idx, item in enumerate(candidates[1:6]):
            p_sec = item.get("pages", [])
            slug_sec = p_sec[0].get("titles", {}).get("canonical") if p_sec else ""
            thumb = None
            if slug_sec:
                imgs_sec = await scraper.fetch_gallery_urls(slug_sec, limit=1)
                if imgs_sec: thumb = scraper.upload_to_cloudinary(imgs_sec[0], f"sec_{item['year']}_{idx}")

            secondary_objs.append(SecondaryEvent(
                title_translations=item.get('titles', {}),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}",
                ai_relevance_score=item.get('final_score', 0),
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

        # SALVARE & FINALIZE
        await save_event_content(payload)

        # await send_to_java(payload)
        logger.info("⚠️ Java Bridge ignorat. SUCCESS local.")
        await log_to_db(status="SUCCESS", year=current_main_year, score=current_score)

    except Exception as e:
        error_msg = str(e)
        logger.error(f"🚨 Pipeline Crash: {error_msg}")
        # Aici nu mai dă NameError pentru că log_to_db e definită sus
        await log_to_db(status="ERROR", year=current_main_year, error=error_msg)

    finally:
        if engine:
            await engine.dispose()
            logger.info("🔌 Conexiune DB închisă.")


if __name__ == "__main__":
    asyncio.run(main())