import asyncio
import os
import httpx
from datetime import datetime
from sqlalchemy import text
from core.logger import setup_logger
from core.config import config
# Importăm și noul model ProcessedEvent pentru arhivare
from core.database import engine, AsyncSessionLocal, IngestionLog, ProcessedEvent
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.ranker import ScoringEngine
from schema.models import DailyPayload, MainEvent, SecondaryEvent
from tenacity import retry, stop_after_attempt, wait_fixed

logger = setup_logger("MainPipeline")


# --- FUNCȚIA DE SALVARE LOGURI (AUDIT) ---
async def save_event_content(payload: DailyPayload):
    """Salvează conținutul evenimentului principal în tabelul processed_events."""
    if engine is None: return

    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event

                # Transformăm manual obiectele în dicționare simple
                # Folosim dict() pentru a fi siguri că eliminăm clasa Translations
                titles_dict = dict(main.title_translations)
                narrative_dict = dict(main.narrative_translations)

                new_entry = ProcessedEvent(
                    event_date=payload.date_processed,
                    year=main.year,
                    titles=titles_dict,
                    narrative=narrative_dict,
                    image_url=main.gallery[0] if main.gallery else None,
                    impact_score=main.impact_score,
                    source_url=main.source_url
                )
                session.add(new_entry)
            await session.commit()
        logger.info(f"🏛️ Conținutul evenimentului din {main.year} a fost ARHIVAT în DB.")
    except Exception as e:
        logger.error(f"❌ Eroare la arhivarea conținutului: {e}")
        # Foarte important: ridicăm eroarea mai departe pentru a fi prinsă de blocul general
        raise


# --- NOUA FUNCȚIE DE ARHIVARE CONȚINUT (DATE REALE) ---
async def save_event_content(payload: DailyPayload):
    """Salvează efectiv textele traduse și link-urile pozelor în DB."""
    if engine is None: return

    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event
                new_entry = ProcessedEvent(
                    event_date=payload.date_processed,
                    year=main.year,
                    titles=main.title_translations,
                    narrative=main.narrative_translations,
                    image_url=main.gallery[0] if main.gallery else None,
                    impact_score=main.impact_score,
                    source_url=main.source_url
                )
                session.add(new_entry)
            await session.commit()
        logger.info(f"🏛️ Conținutul evenimentului din {main.year} a fost ARHIVAT în DB.")
    except Exception as e:
        logger.error(f"❌ Eroare la arhivarea conținutului: {e}")


# --- FUNCȚIA DE TRANSMISIE CĂTRE JAVA (COMENTATĂ ÎN MAIN) ---
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


# --- PIPELINE-UL PRINCIPAL ---
async def main():
    logger.info("🚀 Pornire Pipeline cu Arhivare Locală...")

    # 0. Sincronizare Tabele
    try:
        from core.database import init_db
        await init_db()
    except Exception as e:
        logger.error(f"❌ Eroare init DB: {e}")

    current_main_year = None
    current_score = None

    try:
        # 1. SETUP MODULE
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()
        logger.info("⚙️ Module inițializate.")

        # 2. FETCH & RANK
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Nu s-au găsit evenimente pe Wikipedia.")

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:50]
        logger.info(f"📚 Preluat {len(candidates)} candidați.")

        # 3. AI SCORING & TRANSLATION
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

        # 4. MEDIA CONTENT (CLOUDINARY)
        main_content = await processor.generate_multilingual_main_event(top_data['text'], top_data['year'])
        p_main = top_data.get("pages", [])
        slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"

        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                        enumerate(wiki_imgs)]
        logger.info("🖼️ Media urcată pe Cloudinary.")

        # 5. SECONDARY EVENTS & PAYLOAD
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

        # --- SALVARE DATE ---
        # Salvăm conținutul complet în baza de date locală
        await save_event_content(payload)

        # Java Bridge - COMENTAT
        # await send_to_java(payload)
        # logger.info("✅ Trimis către Java!")

        logger.info("⚠️ Java Bridge ignorat (Simulare). Datele sunt în DB.")
        await log_to_db(status="SUCCESS", year=current_main_year, score=current_score)
        logger.info("✨ Pipeline finalizat cu succes!")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"🚨 Pipeline Crash: {error_msg}")
        await log_to_db(status="ERROR", year=current_main_year, error=error_msg)

    finally:
        if engine:
            await engine.dispose()
            logger.info("🔌 Conexiune DB închisă.")


if __name__ == "__main__":
    asyncio.run(main())