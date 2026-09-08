from datetime import date
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class EventCategory(str, Enum):
    # ── Existing free categories ──
    WAR_CONFLICT = "war_conflict"
    TECH_INNOVATION = "tech_innovation"
    SCIENCE_DISCOVERY = "science_discovery"
    POLITICS_STATE = "politics_state"
    CULTURE_ARTS = "culture_arts"
    NATURAL_DISASTER = "natural_disaster"
    EXPLORATION = "exploration"
    RELIGION_PHIL = "religion_phil"

    # ── NEW: PRO-only categories ──
    PERSONALITIES = "personalities"
    MEDIA = "media"
    SPORT = "sport"


# Set of categories that are exclusively PRO content
PRO_CATEGORIES = {
    EventCategory.PERSONALITIES.value,
    EventCategory.MEDIA.value,
    EventCategory.SPORT.value,
}


class Translations(BaseModel):
    """Per-language strings. The default is empty, deliberately.

    It used to be "Data pending", which meant any dict missing a language silently
    filled it with that text, and the app printed it as the story. An empty string
    hits the client's own "no story available" fallback instead, and callers that
    care about a missing language now check for it rather than shipping a placeholder
    dressed as content.
    """
    en: str = ""
    ro: str = ""
    es: str = ""
    de: str = ""
    fr: str = ""


def _empty_translations() -> "Translations":
    """Blank per-language strings — used as the notification default so the app's
    client-side fallback kicks in when a language has no generated hook."""
    return Translations(en="", ro="", es="", de="", fr="")


class QuizOption(BaseModel):
    id: str
    text: str


class QuizQuestion(BaseModel):
    id: str
    question: str
    options: List[QuizOption]
    correct_id: str = Field(alias="correctId")
    explanation: str

    class Config:
        populate_by_name = True


class QuizTranslations(BaseModel):
    en: List[QuizQuestion] = []
    ro: List[QuizQuestion] = []
    es: List[QuizQuestion] = []
    de: List[QuizQuestion] = []
    fr: List[QuizQuestion] = []


class DeepDiveChapter(BaseModel):
    title: str
    body: str


class DeepDiveHighlight(BaseModel):
    """One point of interest, shown above the article as the reason to read it.

    The label is chosen per event rather than drawn from a fixed set: what a reader
    wants to know about a person ("what he was actually known for") is not what they
    want to know about a treaty ("whether it held"). Keeping it free text is what lets
    one generator serve a coronation, a shipwreck and a schism.
    """
    label: str = ""
    text: str = ""


class FigureStat(BaseModel):
    """One number in the band above the article.

    `value` is a string, not a number: "20,000", "6 h" and "1 in 300" all have to
    survive to the screen exactly as written, and the app never does arithmetic on it.
    """
    value: str = ""    # "11", "20,000", "6 h"
    unit: str = ""     # "of 300", "soldiers" -- the small line under the number
    label: str = ""    # "walked out" -- what the number is


class FigureBarPoint(BaseModel):
    """One bar. `value` IS numeric here, because the bar's length is drawn from it."""
    label: str = ""
    value: float = 0.0


class FigureRow(BaseModel):
    """One line of a fact grid or a comparison table.

    Two shapes in one model because they are the same row seen from two angles: the
    grid uses `value` (one column), the table uses `cells` (one per column). Keeping
    them apart would mean two near-identical models and two near-identical parsers.
    """
    label: str = ""              # "Forces", "Duration", "Outcome"
    value: str = ""              # fact_grid: kept short, the app gives it one line
    cells: List[str] = []        # table: one entry per column, same order


class Figure(BaseModel):
    """A chart, a band of numbers or a fact grid, drawn by the app.

    Figures are the one place in the pipeline where a hallucinated number would be
    indistinguishable from a real one: prose hedges, a bar chart does not. So the
    generator is told to omit a figure rather than estimate it, `note` carries the
    provenance to the screen, and everything here is optional at every level. An event
    with no dependable numbers ships no figure and the app renders nothing.
    """
    kind: str = "stat_row"   # stat_row | bar | fact_grid | compare | table | share | guess
    title: str = ""
    unit: str = ""                            # axis unit for a bar: "soldiers", "GBP"
    note: str = ""                            # "Ammianus' estimate; figures vary"
    # `stats` serves two kinds: a band of 2-4 numbers, or exactly 2 for a
    # before/after comparison. Same shape, different arrangement on screen.
    stats: List[FigureStat] = []              # kind == "stat_row" | "compare"
    # `points` likewise: bars measured against the largest, or slices of one whole.
    points: List[FigureBarPoint] = []         # kind == "bar" | "share"
    rows: List[FigureRow] = []                # kind == "fact_grid" | "table"
    columns: List[str] = []                   # kind == "table", headers left to right

    # ── The guess ──────────────────────────────────────────────────────────
    # One number from the event, hidden behind three choices. It is the only figure
    # the reader touches, and the only one that can be wrong in an interesting way:
    # being surprised by the answer is the point, so the options are chosen to make
    # the true one hard to pick rather than obvious.
    question: str = ""                        # kind == "guess"
    options: List[str] = []                   # exactly 3, one of them true
    answer_index: int = -1                    # 0-based index into `options`
    reveal: str = ""                          # what the answer means, 25-45 words


class DeepDive(BaseModel):
    """The long-form PRO narrative for one event, in one language.

    Structured rather than a single text blob for two reasons: the app renders the
    chapters, the timeline and the sources as distinct UI, and the *teaser* plus the
    chapter titles can be shipped to free users as the paywall pitch without the
    body text ever leaving the server.
    """
    chapters: List[DeepDiveChapter] = []
    highlights: List[DeepDiveHighlight] = []   # points of interest, above the article
    # Numbers worth drawing. The first stat_row also travels in the teaser, so free
    # users get one figure without the long read itself leaking.
    figures: List[Figure] = []
    timeline: List[str] = []      # "14:32 — the first signal reaches Lisbon"
    misconception: str = ""       # "what everyone gets wrong about this day"
    aftermath: List[str] = []     # consequences at +10y / +50y / +100y
    sources: List[str] = []       # "Author, Title (Year)" — never invented URLs
    teaser: str = ""              # opening ~70 words; safe to send to free users
    word_count: int = 0


class WorldEffects(BaseModel):
    """How one choice moves the four world meters. Deltas, not absolutes, so a run is
    the sum of its decisions. Range roughly -30..+30 each; the generator is told to
    make trade-offs rather than let every meter rise together."""
    stability: int = 0   # order vs chaos
    lives: int = 0       # human cost
    progress: int = 0    # science, industry, knowledge
    freedom: int = 0     # liberty vs control


class Actor(BaseModel):
    """A real faction or person with a stake in the outcome. Their standing moves with
    every choice, which is what turns four abstract meters into a political situation."""
    id: str
    name: str             # "The Bohemian Estates", "Maximilian of Bavaria"
    start: int = 50       # 0-100


class NodeFact(BaseModel):
    """A hard number about the situation, shown as a data strip above the choices.
    This is the antidote to a wall of prose: the player should see the constraints
    they are deciding under, not read about them."""
    label: str            # "Bohemian army"
    value: str            # "15,000, unpaid since spring"


class EraVoice(BaseModel):
    """One person alive at that moment, reacting to what the player just did.

    `mood` is a closed vocabulary (see VALID_MOODS in engine/parallel.py) because the app
    switches on it for colour, icon and the running public-mood reading — a mood outside
    the set would be a voice the UI cannot draw. It is therefore never translated; the
    quote around it is."""
    who: str              # "A Prague pewterer", "Tilly's unpaid sergeants"
    mood: str             # elated | hopeful | relieved | defiant | uneasy |
                          # resigned | afraid | angry | betrayed | grieving
    quote: str            # one sentence, in their own voice


class ChoiceOutcome(BaseModel):
    """The one concrete thing this choice commits, previewed on the card."""
    label: str            # "Troops committed"
    value: str            # "24,000"


class EndingStat(BaseModel):
    """A number from your world set against the real one. The ending's payload."""
    label: str            # "Deaths in the German lands"
    real: str             # "8 million"
    alt: str              # "under 1 million"


class UniverseChoice(BaseModel):
    id: str
    label: str            # short — it sits on a button
    detail: str           # one line of what this actually means
    effects: WorldEffects
    # actor id -> standing delta. Every choice pleases someone and costs someone else.
    actor_effects: dict = {}
    risk: int = 50        # 0-100, how likely this backfires
    outcome: Optional[ChoiceOutcome] = None
    # Two or three voices from the week this was decided. The meters say what changed;
    # these say who is furious about it, which is the half a bar cannot carry.
    reactions: List[EraVoice] = []
    next: str             # id of the node this leads to


class UniverseNode(BaseModel):
    """One beat of the branching story. An empty `choices` list marks an ending."""
    id: str
    year: str             # shown on the branching timeline
    title: str
    text: str             # 45-85 words. Short on purpose — the data carries the rest.
    facts: List[NodeFact] = []
    choices: List[UniverseChoice] = []

    # ── Endings only ──
    verdict: str = ""     # one line: better, worse, or unrecognisable
    epitaph: str = ""     # the last line the player is left with
    rarity: str = ""      # common | uncommon | rare — drives the badge and the collection
    stats: List[EndingStat] = []
    # The same idea as a choice's reactions, generations later: how the people who ended
    # up living in this world talk about what was decided.
    legacy: List[EraVoice] = []


class ParallelUniverse(BaseModel):
    """The whole decision tree for one event, in one language.

    Three choices at the first two levels, two at the last: 1 + 3 + 9 = 13 decision
    nodes and 18 endings, 31 in all. Width sits where agency matters most; three-wide
    throughout would mean 40 endings, more than anyone will ever collect.
    """
    pivot_year: str       # the moment history forks
    pivot_title: str
    premise: str          # what actually happened, in two sentences, before we change it
    root: str             # id of the opening node
    actors: List[Actor] = []
    nodes: List[UniverseNode] = []


class ParallelUniverseTranslations(BaseModel):
    en: Optional[ParallelUniverse] = None
    ro: Optional[ParallelUniverse] = None
    es: Optional[ParallelUniverse] = None
    de: Optional[ParallelUniverse] = None
    fr: Optional[ParallelUniverse] = None


class DeepDiveTranslations(BaseModel):
    en: Optional[DeepDive] = None
    ro: Optional[DeepDive] = None
    es: Optional[DeepDive] = None
    de: Optional[DeepDive] = None
    fr: Optional[DeepDive] = None


class EventDetail(BaseModel):
    category: EventCategory
    year: int
    # A string, not a `date`. Python's `datetime.date` cannot hold a year below 1, so
    # every BC event failed to build one and silently fell back to today, shipping the
    # Ides of March stamped 2026. Java's LocalDate and Postgres both accept the signed
    # ISO form ("-0044-03-15"), and the app already reads it (utils/year.ts).
    event_date: str = Field(pattern=r"^-?\d{4}-\d{2}-\d{2}$")
    source_url: str
    title_translations: Translations
    narrative_translations: Translations
    # Long-form PRO narrative. None when the event predates the feature or the
    # generator failed — the app then shows no teaser at all rather than promising
    # content that does not exist.
    deep_dive: Optional[DeepDiveTranslations] = None
    # Branching what-if game. Generated only for the top events of each tier (see
    # PARALLEL_PER_TIER in main.py), so most events legitimately have none.
    parallel: Optional[ParallelUniverseTranslations] = None
    # Per-language push-notification hook (TikTok-style). Two parallel Translations so the
    # Java backend can reuse its existing translations table for each.
    notification_title_translations: Translations = Field(default_factory=_empty_translations)
    notification_body_translations: Translations = Field(default_factory=_empty_translations)
    impact_score: float
    page_views_30d: int = 0
    gallery: List[str] = []
    quiz: Optional[QuizTranslations] = None

    # ── NEW PRO fields ──
    is_pro: bool = False
    location: Optional[str] = None


class DailyPayload(BaseModel):
    date_processed: date
    events: List[EventDetail]
    metadata: dict = {}