import asyncio
import httpx
import json  # Importat pentru a printa JSON-ul frumos
from datetime import datetime
from core.config import config
from schema.models import DailyPayload, MainEvent, SecondaryEvent


async def test_java_connection():
    print("\n" + "=" * 50)
    print("🧪 DEBUG MODE: Vizualizare date și Test Conexiune")
    print("=" * 50)

    # 1. Datele de test pentru traduceri
    translations_mock = {
        "en": "Test Content", "ro": "Continut Test",
        "es": "Contenido de prueba", "de": "Testinhalt", "fr": "Contenu de test"
    }

    try:
        dummy_payload = DailyPayload(
            date_processed=datetime.now().date(),
            api_secret=config.INTERNAL_API_SECRET,
            main_event=MainEvent(
                title_translations=translations_mock,
                year=2024,
                source_url="https://wikipedia.org",
                event_date=datetime.now().date(),
                narrative_translations=translations_mock,
                impact_score=95.5,
                gallery=["https://via.placeholder.com/600"]
            ),
            secondary_events=[]
        )
    except Exception as ve:
        print(f"❌ Eroare Validare Modele: {ve}")
        return

    # 2. GENERARE JSON PENTRU INSPECTIE
    # Aici transformăm obiectul Python în JSON-ul final (string)
    payload_to_send = dummy_payload.model_dump(mode='json')
    json_preview = json.dumps(payload_to_send, indent=4, ensure_ascii=False)

    print("\n🔍 IATĂ CE TRIMITEM CĂTRE JAVA (JSON PAYLOAD):")
    print("-" * 30)
    print(json_preview)
    print("-" * 30)

    # 3. Trimiterea propriu-zisă
    headers = {
        "X-Internal-Api-Key": config.INTERNAL_API_SECRET,
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            print(f"\n📤 Se trimite request-ul către {config.JAVA_BACKEND_URL}...")
            response = await client.post(
                config.JAVA_BACKEND_URL,
                json=payload_to_send,
                headers=headers
            )

            print(f"📡 Status Code: {response.status_code}")

            if response.status_code in [200, 201]:
                print("✅ SUCCES! Datele au fost acceptate.")
            else:
                print(f"⚠️ Răspuns Server: {response.text or 'Fără mesaj de eroare'}")

        except Exception as e:
            print(f"🚨 Eroare conexiune: {e}")


if __name__ == "__main__":
    asyncio.run(test_java_connection())