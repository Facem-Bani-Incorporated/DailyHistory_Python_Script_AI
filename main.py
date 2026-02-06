import asyncio
import os
import httpx
import json  # Adăugat pentru printarea frumoasă a JSON-ului
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
async def log_to_db(status: str, year: int = None, score: float = None, error: str = None):
    """Salvează statusul execuției în baza de date locală pentru monitorizare."""
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
        logger.info(f"📊 Status [{status}] salvat în DB locală.")
    except Exception as e:
        logger.error(f"❌ Nu s-a putut salva log-ul: {e}")


# --- 2. FUNCȚIA DE ARHIVARE LOCALĂ ---
async def save_event_content(payload: DailyPayload):
    """Salvează o copie a evenimentului în DB locală (Backup)."""
    if engine is None: return
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                main = payload.main_event
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
        logger.info(f"🏛️ Conținut arhivat local pentru anul {main.year}.")
    except Exception as e:
        logger.error(f"❌ Eroare la arhivarea locală: {e}")


# --- 3. FUNCȚIA DE TRANSMISIE CĂTRE JAVA SPRING BOOT ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
async def send_to_java(payload: DailyPayload):
    """Trimite payload-ul către backend-ul de Java prin POST."""
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }

    payload_json = payload.model_dump(mode='json')

    async with httpx.AsyncClient(timeout=120.0) as client:
        logger.info(f"📤 Trimitere date către Java la: {config.JAVA_BACKEND_URL}")
        response = await client.post(
            config.JAVA_BACKEND_URL,
            json=payload_json,
            headers=headers
        )
        response.raise_for_status()
        return response.status_code


# --- 4. PIPELINE-UL PRINCIPAL ---
async def main():
    logger.info("🚀 Pornire Pipeline (Python -> Java Bridge)...")

    try:
        from core.database import init_db
        await init_db()
    except Exception as e:
        logger.error(f"❌ Init DB local failed: {e}")

    current_main_year = None
    current_score = None

    try:
        scraper = WikiScraper()
        processor = AIProcessor()
        ranker = ScoringEngine()

        # FETCH & PRE-RANKING
        raw_events = await scraper.fetch_today()
        if not raw_events:
            raise ValueError("Wikipedia empty response")

        for item in raw_events:
            item['h_score'] = ranker.heuristic_score(item)

        candidates = sorted(raw_events, key=lambda x: x['h_score'], reverse=True)[:50]

        # AI SCORING & TRANSLATION
        ai_data = await processor.batch_score_and_translate_titles(candidates)
        results = ai_data.get('results', {})

        for idx, item in enumerate(candidates):
            res = results.get(f"ID_{idx}", {})
            item['final_score'] = ranker.hybrid_calculate(item['h_score'], res.get('score', 50))
            item['titles'] = res.get('titles', {})

        candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_data = candidates[0]
        current_main_year = top_data['year']
        current_score = top_data['final_score']

        logger.info(f"🤖 AI Scoring finalizat pentru anul {current_main_year}.")

        # GENERARE CONȚINUT MULTILINGV & MEDIA
        main_content = await processor.generate_multilingual_main_event(top_data['text'], top_data['year'])
        p_main = top_data.get("pages", [])
        slug_main = p_main[0].get("titles", {}).get("canonical") if p_main else "history"

        wiki_imgs = await scraper.fetch_gallery_urls(slug_main, limit=3)
        main_gallery = [scraper.upload_to_cloudinary(url, f"main_{top_data['year']}_{i}") for i, url in
                        enumerate(wiki_imgs)]

        # SECONDARY EVENTS
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
                title_translations=item.get('titles', {}),
                year=item['year'],
                source_url=f"https://en.wikipedia.org/wiki/{slug_sec}",
                ai_relevance_score=item.get('final_score', 0),
                thumbnail_url=thumb
            ))

        # CONSTRUCȚIE PAYLOAD
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

        # --- LOGGING PAYLOAD ÎNAINTE DE TRANSMISIE ---
        payload_preview = payload.model_dump(mode='json')
        print("\n" + "="*50)
        print("🔍 INSPECTOR PAYLOAD (Ce pleacă spre Sergiu):")
        print("-" * 50)
        print(json.dumps(payload_preview, indent=4, ensure_ascii=False))
        print("="*50 + "\n")

        # PASUL FINAL: TRANSMISIE & BACKUP
        await save_event_content(payload)  # Backup local

        status_code = await send_to_java(payload)

        if status_code in [200, 201]:
            logger.info(f"✅ SUCCES: Datele au fost integrate în Spring Boot.")
            await log_to_db(status="SUCCESS_UPLOADED", year=current_main_year, score=current_score)
        else:
            logger.warning(f"⚠️ Atenție: Spring Boot a răspuns cu codul {status_code}")
            await log_to_db(status="PARTIAL_SUCCESS", year=current_main_year, error=f"Status Java: {status_code}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"🚨 Pipeline Crash: {error_msg}")
        await log_to_db(status="ERROR", year=current_main_year, error=error_msg)

    finally:
        if engine:
            await engine.dispose()
            logger.info("🔌 Pipeline terminat. Conexiuni închise.")


if __name__ == "__main__":
    asyncio.run(main())