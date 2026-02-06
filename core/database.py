import logging
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, text
from datetime import datetime
from core.config import config

logger = logging.getLogger("Database")

# 1. Base class pentru modele
Base = declarative_base()

# 2. Definirea Modelului (Inclus în Base)
class IngestionLog(Base):
    __tablename__ = "ingestion_logs"
    id = Column(Integer, primary_key=True)
    event_date = Column(DateTime, default=datetime.utcnow)
    main_event_year = Column(Integer, nullable=True)
    status = Column(String)  # SUCCESS / ERROR
    impact_score = Column(Float, nullable=True)
    error_message = Column(String, nullable=True)

# 3. Configurare Engine
if not config.DATABASE_URL:
    logger.critical("🚨 DATABASE_URL lipsește din configurație!")
    engine = None
else:
    engine = create_async_engine(
        config.DATABASE_URL,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args={"command_timeout": 60}
    )

# 4. Factory pentru sesiuni
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# 5. Helper pentru a obține o sesiune
async def get_db():
    if engine is None:
        raise ConnectionError("Engine-ul bazei de date nu a fost inițializat.")
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# --- SCRIPT DE TEST ȘI INITIALIZARE ---
async def init_db():
    """Verifică conexiunea și creează tabelele dacă nu există."""
    if engine is None:
        print("❌ Engine-ul nu este configurat.")
        return

    print("⏳ [DB] Se verifică conexiunea și structura tabelelor...")
    try:
        async with engine.begin() as conn:
            # Creează tabelele definite în Base (ex: ingestion_logs)
            # Această linie te scapă de scris SQL manual pe Railway
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("SELECT 1"))
            print("✅ [DB] Conexiune reușită și tabele sincronizate!")
    except Exception as e:
        print(f"❌ [DB] Eroare la inițializare: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(init_db())