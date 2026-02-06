import logging
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, text, JSON, Date
from datetime import datetime
from core.config import config

logger = logging.getLogger("Database")

Base = declarative_base()

class IngestionLog(Base):
    __tablename__ = "ingestion_logs"
    id = Column(Integer, primary_key=True)
    event_date = Column(DateTime, default=datetime.utcnow)
    main_event_year = Column(Integer, nullable=True)
    status = Column(String)
    impact_score = Column(Float, nullable=True)
    error_message = Column(String, nullable=True)

class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    id = Column(Integer, primary_key=True, index=True)
    event_date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    titles = Column(JSON, nullable=False)
    narrative = Column(JSON, nullable=False)
    image_url = Column(String, nullable=True)
    impact_score = Column(Float, nullable=True)
    source_url = Column(String, nullable=True)

if not config.DATABASE_URL:
    logger.critical("🚨 DATABASE_URL lipsește!")
    engine = None
else:
    engine = create_async_engine(config.DATABASE_URL, pool_pre_ping=True)

AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    if engine is None: return
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            print("✅ [DB] Tabele sincronizate (Log + Events)!")
    except Exception as e:
        print(f"❌ [DB] Eroare: {e}")