from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import date

class Translations(BaseModel):
    en: str
    ro: str
    es: str
    de: str
    fr: str

class SecondaryEvent(BaseModel):
    title_translations: Translations
    year: int
    source_url: str
    thumbnail_url: Optional[str] = None
    ai_relevance_score: float

class MainEvent(BaseModel):
    title_translations: Translations
    year: int
    source_url: str
    event_date: date
    narrative_translations: Translations
    impact_score: float
    gallery: List[str] = []

class DailyPayload(BaseModel):
    date_processed: date
    api_secret: str
    main_event: MainEvent
    secondary_events: List[SecondaryEvent]
    metadata: dict = Field(default_factory=dict)