import asyncio
import json
import re
from datetime import datetime
from core.config import config
from core.llm import (
    get_async_client, build_params, parse_json_response, achat, BudgetExhausted,
)
from core.onthisday import fetch_otd_reference, fetch_otd
from core.text import strip_prose_dashes
from schema.models import EventCategory
from core.logger import setup_logger

logger = setup_logger("AIProcessor")

# Longer than this and the rate limit is not a hiccup to sleep through — it is the
# account's window being spent. Groq states the wait in the error body; anything past a
# couple of minutes outlives the run, so the stage gives up and lets the pipeline fall
# back rather than burning attempts that are refused on arrival.
_RETRY_GIVE_UP_SECONDS = 120

_RETRY_AFTER_RE = re.compile(
    r"try again in (?:(\d+)h)?(?:(\d+)m)?([\d.]+)s", re.I
)

# The free article is a 2 minute read, and the label the app prints is wordCount/200,
# so the ask is 350 to 450 words. It was 600 to 800 and read as padded at that length.
# The band enforced below is wider than the ask on both sides: a retry costs a whole
# generation, so only a piece that misses by a visible margin is worth paying to
# rewrite, and a translation runs roughly a tenth longer than the English it came from.
MIN_NARRATIVE_WORDS = 280
MAX_NARRATIVE_WORDS = 560

# Push-notification limits. The title is cut on whole words and never gets an
# ellipsis: the 9 AM notification is the one the user judges the app by, and a title
# that trails off reads as a bug rather than as a tease. The phone appends its own
# "..." the moment the text does not fit the line, which is the whole reason the
# prompt asks for 5 to 8 words. The body keeps its ellipsis, because there the
# unfinished sentence IS the hook.
NOTIF_TITLE_MAX_WORDS = 8
NOTIF_TITLE_MAX_CHARS = 58
NOTIF_BODY_MAX_CHARS = 130


def _retry_after_seconds(err: Exception) -> float | None:
    """Seconds the provider asked us to wait, or None if it did not say."""
    m = _RETRY_AFTER_RE.search(str(err))
    if not m:
        return None
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = float(m.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _is_request_too_large(err: Exception) -> bool:
    """A 413: input + max_tokens exceeds the whole per-minute allowance. Deterministic
    at a given size, so resending it unchanged is guaranteed to fail again."""
    if getattr(err, "status_code", None) == 413:
        return True
    return "request too large" in str(err).lower()


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = get_async_client()
        self.model = model
        self.thinking_budget = config.AI_THINKING_BUDGET
        self.categories_list = [c.value for c in EventCategory]
        self.languages = ["en", "ro", "es", "de", "fr"]

        # ══════════════════════════════════════════════════════════
        # 8 angles — each decides which facts the piece opens on and
        # in what order it works through the event. They vary the
        # SHAPE of the article, never its register: the free piece is
        # an objective explainer whichever angle it draws. All angles
        # anchor date and place within the first two sentences.
        # ══════════════════════════════════════════════════════════
        self.storytelling_angles = [
            {
                "name": "THE_SITUATION_BEFORE",
                "instruction": (
                    "Lead with the state of affairs immediately before this happened. "
                    "Who held what, what the arrangement was, what had been building and for how long. "
                    "Establish the conditions factually, in dates, holdings and numbers, so the event reads "
                    "as the outcome of a situation rather than as a surprise. No atmosphere, no weather, "
                    "no imagined interiors: only what the record establishes."
                ),
            },
            {
                "name": "HUMAN_FOCUS",
                "instruction": (
                    "Organise the article around the people: their ages, positions, and what each of them "
                    "was actually responsible for. Include the documented minor figures alongside the famous "
                    "ones, and say what each one did. Attribute motives only where a source records them, "
                    "and say which source. Specific people made specific decisions; name them and the decisions."
                ),
            },
            {
                "name": "THE_BIG_MOMENT",
                "instruction": (
                    "Give most of the article to the event itself, in strict sequence. What happened first, "
                    "what followed, who acted and in what order. Use exact times wherever they are recorded. "
                    "The value here is precision about the order of events, not immediacy: reconstruct the "
                    "sequence, do not stage it."
                ),
            },
            {
                "name": "WHY_IT_MATTERED",
                "instruction": (
                    "Focus on consequences and scale: how many people were affected, what changed, for whom and how much. "
                    "Spend more time on the aftermath than the event itself. "
                    "Let the ripple effects — weeks, years, decades later — carry the weight of the story."
                ),
            },
            {
                "name": "THE_CONTRAST",
                "instruction": (
                    "Build the article as a documented before and after. What the arrangement was on the "
                    "previous day, what was assumed to be permanent, and what specifically replaced it. "
                    "State both sides in comparable terms, the same institutions, the same borders, the same "
                    "numbers, so the size of the change is measurable rather than asserted."
                ),
            },
            {
                "name": "THE_NUMBERS_TELL",
                "instruction": (
                    "Let measurements and figures carry the article. Every paragraph should hold at least one "
                    "number that proves something: distances, costs, casualties, durations, temperatures, ages. "
                    "Evidence, not decoration. Give the unit and the basis for every figure, and say when a "
                    "number is an estimate and whose estimate it is."
                ),
            },
            {
                "name": "THE_WORLD_CHANGED",
                "instruction": (
                    "Frame it as a before/after in human understanding or capability. "
                    "What was impossible or unimaginable before this day? What became ordinary after? "
                    "Show the long arc: what took centuries to build, and what this event broke or created."
                ),
            },
            {
                "name": "THE_STORY_BEHIND",
                "instruction": (
                    "Lead with the part of the record most people never hear: the underlying cause, the "
                    "decision taken weeks earlier, the figure left out of the retellings. Then correct the "
                    "familiar version against it, saying plainly what the popular account gets wrong and what "
                    "the evidence actually shows."
                ),
            },
        ]

        # ══════════════════════════════════════════════════════════
        # 6 EMPHASES — a second axis of variation, orthogonal to the
        # angle above. The angle decides what the piece leads with;
        # the emphasis decides which facts get the most room. Neither
        # touches the register: the free article is one calm factual
        # voice, and two pieces on the same day differ in what they
        # cover, not in how they sound. Each event gets a unique
        # (angle, emphasis) pair.
        # ══════════════════════════════════════════════════════════
        self.narrative_voices = [
            {
                "name": "THE_EXPLAINER",
                "instruction": (
                    "Explain how the thing actually worked. The mechanism, the law, the "
                    "engineering, the chain of command. Assume an intelligent reader who "
                    "does not know the subject. The explanation is the point of the piece."
                ),
            },
            {
                "name": "THE_REPORTER",
                "instruction": (
                    "Report it. Who did what, when, where, and what followed. Short "
                    "sentences, verified facts, no atmosphere. If you cannot source a "
                    "detail, leave it out rather than dressing the gap."
                ),
            },
            {
                "name": "THE_ANALYST",
                "instruction": (
                    "Trace cause and consequence. What made this possible, what it forced "
                    "next, what it cost and who paid. Use numbers wherever they exist and "
                    "say what they mean."
                ),
            },
            {
                "name": "THE_CONTEXT_SETTER",
                "instruction": (
                    "Establish what was normal before, then what changed. The reader "
                    "should finish knowing why this mattered at the time rather than only "
                    "that it happened."
                ),
            },
            {
                "name": "THE_CORRECTOR",
                "instruction": (
                    "Lead with what is commonly believed about this and what the record "
                    "actually shows. Be specific about the evidence. Correct without "
                    "smugness."
                ),
            },
            {
                "name": "THE_DETAIL_HUNTER",
                "instruction": (
                    "Build the piece from concrete particulars: names, quantities, dates, "
                    "distances, prices, the text of the order. Every detail checkable. No "
                    "scene-setting that is not documented."
                ),
            },
        ]

    def _get_target_date_str(self, target_date: datetime) -> str:
        return target_date.strftime("%B %d")

    def _get_month_day(self, target_date: datetime) -> tuple:
        return target_date.month, target_date.day

    def _ensure_langs(self, data: dict, fallback_text: str = "Data pending") -> dict:
        if not isinstance(data, dict):
            data = {}
        return {lang: data.get(lang) or fallback_text for lang in self.languages}

    @staticmethod
    def _title_from_slug(item: dict) -> str:
        """The Wikipedia title, humanised, as the last-resort event title.

        "Data pending" used to be that last resort, and events shipped with it as
        their headline in all five languages. The slug is always present, it is the
        real name of the thing, and an untranslated real name beats a placeholder in
        every language including English.
        """
        slug = str(item.get("slug") or "").strip()
        if slug:
            return slug.replace("_", " ")
        text = str(item.get("text") or "").strip()
        return text[:80] if text else "Historical Event"

    def _normalize_location(self, e: dict) -> dict:
        loc = e.get("location")
        if isinstance(loc, str) and loc.strip().lower() in ("null", "none", "", "n/a"):
            e["location"] = None
        return e

    # ══════════════════════════════════════════════════════════════
    # OTD FALLBACK — a day must never come back empty
    # ══════════════════════════════════════════════════════════════
    # Wikipedia's "On This Day" feed is already fetched to ground the discovery prompt,
    # and it is curated, date-exact and structured. So when the model returns nothing —
    # rate limited, refused, or simply having a bad day — there is no reason to ship a
    # blank date: the real events are sitting in memory, they just have not been dressed
    # up by an LLM. On 2026-09-02 two dates published nothing at all while this feed was
    # holding 411 verified entries for them.
    #
    # These candidates cost zero tokens and skip the date validator by construction (the
    # feed is indexed BY date), so they are the one path that still works when the
    # account's whole allowance is gone.
    _OTD_CATEGORY_HINTS = [
        ("war_conflict", ("war", "battle", "invasion", "siege", "troops", "army",
                          "rebellion", "revolt", "massacre", "bomb", "attack")),
        ("politics_state", ("treaty", "president", "parliament", "elected", "signed",
                            "constitution", "independence", "government", "minister")),
        ("science_discovery", ("discover", "scientist", "experiment", "species",
                               "astronom", "physic", "chemic", "vaccine", "medical")),
        ("tech_innovation", ("invent", "patent", "engine", "computer", "telegraph",
                             "railway", "aircraft", "launch", "satellite", "spacecraft")),
        ("natural_disaster", ("earthquake", "eruption", "hurricane", "flood", "tsunami",
                              "cyclone", "wildfire", "famine", "epidemic", "plague")),
        ("exploration", ("expedition", "voyage", "explorer", "summit", "pole",
                         "circumnavigat", "landed on")),
        ("religion_phil", ("pope", "church", "bishop", "monastery", "cathedral",
                           "philosoph", "council of")),
        ("sport", ("olympic", "world cup", "championship", "match", "tournament",
                   "record", "athlet", "football", "boxing")),
        ("media", ("film", "movie", "album", "single", "broadcast", "premiere",
                   "television", "radio", "novel", "published")),
    ]

    @classmethod
    def _guess_category(cls, text: str, default: str) -> str:
        """Keyword match, not a model call. A rough category on a real event beats a
        perfect one that never arrives."""
        low = (text or "").lower()
        for cat, needles in cls._OTD_CATEGORY_HINTS:
            if any(n in low for n in needles):
                return cat
        return default

    async def _events_from_otd(
        self, target_date: datetime, exclude_slugs: set = None, pro: bool = False,
        want: int = 25,
    ) -> list:
        """Build discovery candidates straight from the OTD feed. No LLM, no cost."""
        try:
            otd = await fetch_otd(target_date)
        except Exception as e:
            logger.error(f"🚨 OTD fallback unavailable: {e!r}")
            return []

        # "selected" is Wikipedia's own front-page pick for the day, so it leads.
        sources = (("deaths", 58), ("births", 58)) if pro else                   (("selected", 72), ("events", 62))
        default_cat = "personalities" if pro else "culture_arts"

        excluded = {s for s in (exclude_slugs or set())}
        out, seen = [], set()
        for endpoint, base_score in sources:
            for e in otd.get(endpoint, []):
                slug, year = e.get("slug"), e.get("year")
                text = (e.get("text") or "").strip()
                if not slug or not isinstance(year, int) or not text:
                    continue
                if slug in seen or slug in excluded:
                    continue
                seen.add(slug)
                out.append({
                    "year": year,
                    "text": text,
                    "slug": slug,
                    "category": self._guess_category(text, default_cat),
                    "ai_score": base_score,
                    "date_confidence": "HIGH",
                    "date_source": f"Wikipedia On This Day feed ({endpoint})",
                    "location": None,
                })

        out.sort(key=lambda x: (-x["ai_score"], -x["year"]))
        selected = out[:want]
        tier = "PRO" if pro else "FREE"
        logger.warning(
            f"🛟 {tier} OTD fallback: built {len(selected)} candidates from the "
            f"Wikipedia feed without the model"
        )
        return selected

    @staticmethod
    def _build_avoid_block(exclude_slugs: set = None) -> str:
        """Prompt block listing already-published slugs the AI must not repeat."""
        if not exclude_slugs:
            return ""
        listed = ", ".join(sorted(exclude_slugs))
        return (
            "\nALREADY PUBLISHED — do NOT return any of these Wikipedia articles. "
            "Find DIFFERENT events instead (go more niche if you must):\n"
            f"{listed}\n"
        )

    @staticmethod
    def _build_otd_block(otd_reference: str) -> str:
        """Ground the model in Wikipedia's curated, verified On-This-Day list so it picks
        real, correctly-dated events instead of hallucinating. Empty when OTD is
        unavailable — discovery then falls back to pure recall."""
        if not otd_reference:
            return ""
        return (
            "\nWIKIPEDIA ON-THIS-DAY — curated and VERIFIED for this exact day. These are "
            "real events with correct dates and exact article titles. Treat this as your "
            "PRIMARY source: prefer these, and copy the slug (Title) exactly. You MAY add "
            "other events only if you are certain of the exact date; NEVER invent.\n"
            f"{otd_reference}\n"
        )

    # ══════════════════════════════════════════════════════════════
    # PASS 1 — Discovery
    # ══════════════════════════════════════════════════════════════
    async def discover_events(self, target_date: datetime, exclude_slugs: set = None) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)
        avoid_block = self._build_avoid_block(exclude_slugs)
        otd_block = self._build_otd_block(await fetch_otd_reference(target_date))

        prompt = f"""
You are a meticulous Senior Historian and Fact-Checker.
List historical events that occurred EXACTLY on {date_str} (month={month}, day={day}).

CRITICAL RULES:
1. DATE INTEGRITY: Every event MUST have occurred on EXACTLY {date_str}.
   - If an event started on a different day, it does NOT count.
2. WIKIPEDIA: "slug" MUST be the exact Wikipedia article title.
3. YEAR ACCURACY: Exact year of the event.
4. NO HALLUCINATIONS: If unsure, EXCLUDE. Accuracy always beats volume.
5. QUANTITY: Aim for 25 accurate events. That is already several times what a day
   publishes, so it leaves room for the date validator to reject some — but asking
   for 60+ only bought a longer response to throw away.
6. DIVERSITY: Different centuries, regions, categories.
7. DEPTH OVER FAME: if there aren't many globally famous events on this date,
   dig deeper instead of giving up — include well-documented but lesser-known
   ones (regional milestones, scientific/technical firsts, notable births &
   deaths, cultural or sporting curiosities). A thin or empty list is a failure:
   always come back with a rich set of real events. Lesser-known is welcome;
   invented or misdated is not.
8. INTERESTING, NOT MERELY DATED. Every candidate should be something you could tell
   someone about in one sentence and have them want the second. Prefer the dramatic,
   the surprising, the first-of-its-kind and the human over routine administrative
   milestones — a department founded, a charter renewed, an office filled on schedule.
   Obscure-but-gripping beats famous-but-procedural. Set ai_score accordingly: reserve
   80+ for events with a real story, and score dutiful institutional entries below 50.
{otd_block}{avoid_block}
STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1945,
      "text": "Precise 1-2 sentence description.",
      "slug": "Exact_Wikipedia_Article_Title",
      "category": "one_from_allowed_list",
      "ai_score": 75,
      "date_confidence": "HIGH/MEDIUM",
      "date_source": "Brief note",
      "location": "City, Country (or null)"
    }}
  ]
}}

ALLOWED CATEGORIES: {self.categories_list}
ONLY HIGH confidence.
"""

        # 3072, not the 4096 default: Groq bills input + max_tokens against the
        # per-minute budget, and this prompt's ~4.5k of input leaves only so much room
        # under an 8000 TPM tier. Twenty-five events fit in well under 3k of JSON.
        res = await self._safe_ai_call(
            prompt, f"Discovery ({date_str})", {"events": []}, max_tokens=3072
        )
        events = res.get("events", [])

        validated = []
        seen_slugs = set()
        for e in events:
            slug = e.get("slug")
            if not isinstance(e.get("year"), int) or not slug or slug in seen_slugs:
                continue
            if e.get("category") not in self.categories_list:
                e["category"] = EventCategory.CULTURE_ARTS.value
            if e.get("date_confidence", "").upper() != "HIGH":
                logger.warning(f"⚠️ Skipping low-confidence: {slug}")
                continue

            e = self._normalize_location(e)
            seen_slugs.add(slug)
            validated.append(e)

        if not validated:
            logger.warning(
                f"⚠️ Discovery returned nothing usable for {date_str} — falling back to "
                f"the Wikipedia On This Day feed"
            )
            validated = await self._events_from_otd(
                target_date, exclude_slugs=exclude_slugs, pro=False
            )

        logger.info(f"✅ Found {len(validated)} HIGH-confidence events for {date_str}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO DISCOVERY
    # ══════════════════════════════════════════════════════════════
    async def discover_pro_events(self, target_date: datetime, exclude_slugs: set = None) -> list:
        date_str = self._get_target_date_str(target_date)
        month, day = self._get_month_day(target_date)
        pro_cats = ["personalities", "media", "sport"]
        avoid_block = self._build_avoid_block(exclude_slugs)
        otd_block = self._build_otd_block(await fetch_otd_reference(target_date))

        prompt = f"""
You are a Senior Pop-Culture & Entertainment Historian.
List PREMIUM historical events for {date_str} (month={month}, day={day}) — STRICTLY from:

1. **personalities** — births/deaths of globally iconic people
2. **media** — milestone events in film, TV, music, radio, publishing
3. **sport** — historic sporting moments

HARD RULES:
1. DATE: Must be EXACTLY {date_str}.
2. WIKIPEDIA: "slug" must match exact article title.
3. FAME: Prefer globally famous people/events — but if a category is thin for this
   date, go deeper and add strong regional or era-defining picks so every category
   is represented. Never leave a category empty. Never invent; accuracy over fame.
4. Aim for 6+ per category, 25-40 total. More is better.
5. Only HIGH confidence.
{otd_block}{avoid_block}
STRICT JSON SCHEMA:
{{
  "events": [
    {{
      "year": 1977,
      "text": "Precise description.",
      "slug": "Exact_Wikipedia_Title",
      "category": "personalities | media | sport",
      "ai_score": 85,
      "date_confidence": "HIGH",
      "date_source": "Brief note",
      "location": "City, Country (or null)"
    }}
  ]
}}

ALLOWED: {pro_cats}
"""

        res = await self._safe_ai_call(
            prompt, f"PRO Discovery ({date_str})", {"events": []}, max_tokens=3072
        )
        events = res.get("events", [])

        validated = []
        seen_slugs = set()
        for e in events:
            slug = e.get("slug")
            cat = e.get("category", "").lower()

            if not isinstance(e.get("year"), int) or not slug or slug in seen_slugs:
                continue
            if cat not in pro_cats:
                continue
            if e.get("date_confidence", "").upper() != "HIGH":
                continue

            e = self._normalize_location(e)
            seen_slugs.add(slug)
            validated.append(e)

        by_cat = {}
        for e in validated:
            by_cat[e["category"]] = by_cat.get(e["category"], 0) + 1
        if not validated:
            logger.warning(
                f"⚠️ PRO discovery returned nothing usable for {date_str} — falling back "
                f"to the Wikipedia On This Day feed (births/deaths)"
            )
            validated = await self._events_from_otd(
                target_date, exclude_slugs=exclude_slugs, pro=True
            )
            by_cat = {}
            for e in validated:
                by_cat[e["category"]] = by_cat.get(e["category"], 0) + 1

        logger.info(f"✅ PRO discovery: {len(validated)} events → {by_cat}")
        return validated

    # ══════════════════════════════════════════════════════════════
    # PRO RANK
    # ══════════════════════════════════════════════════════════════
    async def deep_rank_pro_per_category(self, candidates: list, target_date: datetime) -> list:
        """
        Selects 4 PRO events:
          - 1 event from each of the 3 categories (personalities, media, sport)
          - 1 EXTRA event (the next-best one across all categories)
        Total: 4 events, with at least 1 per category guaranteed.
        """
        if not candidates:
            return []

        date_str = self._get_target_date_str(target_date)
        buckets = {"personalities": [], "media": [], "sport": []}
        for c in candidates:
            cat = c.get("category", "").lower()
            if cat in buckets:
                buckets[cat].append(c)

        prompt_blocks = []
        id_map = {}
        counter = 0
        for cat_name, items in buckets.items():
            if not items:
                continue
            prompt_blocks.append(f"\n━━━ CATEGORY: {cat_name.upper()} ━━━")
            for item in items:
                key = f"ID_{counter}"
                id_map[key] = item
                prompt_blocks.append(f"{key} ({item['year']}): {item['text'][:180]}")
                counter += 1

        candidates_text = "\n".join(prompt_blocks)

        prompt = f"""
You are a premium content curator for a history app's PAID TIER.
For {date_str}, select 4 events total:
  - 1 BEST event from EACH of the 3 categories (personalities, media, sport)
  - 1 EXTRA event (the next-best one, from any category)

That's 4 events total. The extra must be different from the 3 main picks.

CRITERIA: global fame, storytelling potential, emotional impact, shareability.

STRICT JSON:
{{
  "selections": [
    {{
      "original_id": "ID_0",
      "category": "personalities",
      "deep_score": 92,
      "is_extra": false,
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }},
    {{
      "original_id": "ID_3",
      "category": "media",
      "deep_score": 88,
      "is_extra": false,
      "titles": {{ ... }}
    }},
    {{
      "original_id": "ID_8",
      "category": "sport",
      "deep_score": 85,
      "is_extra": false,
      "titles": {{ ... }}
    }},
    {{
      "original_id": "ID_2",
      "category": "personalities",
      "deep_score": 90,
      "is_extra": true,
      "titles": {{ ... }}
    }}
  ]
}}

CANDIDATES:
{candidates_text}
"""

        res = await self._safe_ai_call(prompt, "PRO Deep Rank", {"selections": []})

        selected = []
        seen_ids = set()
        cats_filled = set()  # tracks which of the 3 main category slots are filled

        # First pass: fill the 3 main category slots (1 per category)
        for entry in res.get("selections", []):
            original_id = entry.get("original_id")
            cat = entry.get("category", "").lower()
            is_extra = entry.get("is_extra", False)

            if is_extra:
                continue  # handle extras after main slots
            if original_id not in id_map or original_id in seen_ids:
                continue
            if cat not in {"personalities", "media", "sport"}:
                continue
            if cat in cats_filled:
                continue

            item = id_map[original_id]
            item.update({
                "deep_score": entry.get("deep_score", 50),
                "titles": self._ensure_langs(
                    entry.get("titles", {}), self._title_from_slug(item)
                ),
                "is_pro": True,
            })
            selected.append(item)
            seen_ids.add(original_id)
            cats_filled.add(cat)

        # Fallback: fill any missing main category from highest ai_score in that bucket
        for cat_name in ["personalities", "media", "sport"]:
            if cat_name in cats_filled:
                continue
            pool = sorted(
                [c for c in buckets[cat_name] if f"ID_{list(id_map.values()).index(c)}" not in seen_ids]
                if buckets[cat_name] else [],
                key=lambda x: x.get("ai_score", 0),
                reverse=True,
            )
            # Simpler fallback — just take the top of the bucket if not already picked
            for cand in sorted(buckets[cat_name], key=lambda x: x.get("ai_score", 0), reverse=True):
                cand_id = next((k for k, v in id_map.items() if v is cand), None)
                if cand_id and cand_id not in seen_ids:
                    cand.update({
                        "deep_score": cand.get("ai_score", 50),
                        "titles": self._ensure_langs({}, self._title_from_slug(cand)),
                        "is_pro": True,
                    })
                    selected.append(cand)
                    seen_ids.add(cand_id)
                    cats_filled.add(cat_name)
                    logger.warning(f"⚠️ PRO fallback for '{cat_name}': {cand['slug']}")
                    break

        # Second pass: add the EXTRA (4th event)
        extra_added = False
        for entry in res.get("selections", []):
            if not entry.get("is_extra", False):
                continue
            original_id = entry.get("original_id")
            if original_id not in id_map or original_id in seen_ids:
                continue

            item = id_map[original_id]
            item.update({
                "deep_score": entry.get("deep_score", 50),
                "titles": self._ensure_langs(entry.get("titles", {})),
                "is_pro": True,
            })
            selected.append(item)
            seen_ids.add(original_id)
            extra_added = True
            logger.info(f"⭐ PRO extra added: [{item.get('category')}] {item.get('slug')}")
            break  # only one extra

        # Fallback for extra: pick highest-score remaining candidate
        if not extra_added:
            remaining = []
            for cat_name in ["personalities", "media", "sport"]:
                for cand in buckets[cat_name]:
                    cand_id = next((k for k, v in id_map.items() if v is cand), None)
                    if cand_id and cand_id not in seen_ids:
                        remaining.append((cand, cand_id))

            if remaining:
                remaining.sort(key=lambda x: x[0].get("ai_score", 0), reverse=True)
                cand, cand_id = remaining[0]
                cand.update({
                    "deep_score": cand.get("ai_score", 50),
                    "titles": self._ensure_langs({}),
                    "is_pro": True,
                })
                selected.append(cand)
                seen_ids.add(cand_id)
                logger.warning(
                    f"⚠️ PRO extra fallback: [{cand.get('category')}] {cand['slug']}"
                )

        # Log summary
        by_cat = {}
        for ev in selected:
            c = ev.get("category", "?")
            by_cat[c] = by_cat.get(c, 0) + 1
        logger.info(f"🏆 PRO selected {len(selected)} events → {by_cat}")
        return selected

    # ══════════════════════════════════════════════════════════════
    # PASS 2 — Deep ranking for FREE
    # ══════════════════════════════════════════════════════════════
    async def deep_rank_and_select(self, candidates: list, target_date: datetime) -> list:
        if not candidates:
            return []

        date_str = self._get_target_date_str(target_date)
        candidates_text = "\n".join(
            [f"ID_{i}: ({e['year']}) {e['text'][:200]}" for i, e in enumerate(candidates)]
        )

        prompt = f"""
You are a rigorous historian curating a "Today in History" feed for {date_str}.
From the CANDIDATES below, select and rank the 15 most significant events.

Reason through each candidate before scoring it. Score on five components:
  • STORY POWER (0–30) — is there drama here? Stakes, a reversal, a decision taken
    under pressure, a human being one can picture. Would a reader stop scrolling?
  • PERMANENCE (0–20) — are the consequences still felt today?
  • GLOBAL REACH (0–20) — did it affect the whole world, or just one region?
  • UNIQUENESS (0–15) — a real first, a turning point, or something genuinely strange.
    Routine institutional business scores zero here no matter how large the institution.
  • UNIVERSAL RECOGNITION (0–15) — would an educated person anywhere recognize it?

deep_score = the sum of those five components (0–100). Rank by deep_score, highest first.

HARD RULES:
  1. INTERESTING BEATS MERELY IMPORTANT. This is a feed people read for pleasure, not
     a syllabus. A department being founded, a treaty being signed without incident, an
     office changing hands on schedule — these are consequential and they are dull, and
     they must lose to an event with a story in it. If the only thing you can say about
     a candidate is that it was significant, it does not belong in the top ranks.
  2. World-changing beats locally-important, ALL ELSE EQUAL. Significance is a tie-break
     between two events that are both worth reading, never a reason to promote a dull
     one over a vivid one.
  3. DIVERSITY: the top 15 must span at least 3 different centuries AND 3 different
     categories. Do not stack the list with one era or one theme.
  4. Only rank candidates that are given, by their exact ID. Never invent events.
  5. Titles are short, vivid, specific headlines — never "An event on {date_str}".

Return ONLY this JSON (score_breakdown MUST sum to deep_score):
{{
  "top15": [
    {{
      "original_id": "ID_0",
      "deep_score": 95,
      "score_breakdown": {{
        "global_reach": 30, "permanence": 25, "universal_recognition": 20,
        "emotional_power": 15, "uniqueness": 5
      }},
      "titles": {{ "en": "...", "ro": "...", "es": "...", "de": "...", "fr": "..." }}
    }}
  ]
}}

CANDIDATES:
{candidates_text}
"""

        res = await self._safe_ai_call(prompt, "Deep Rank", {"top15": []})
        id_map = {f"ID_{i}": e for i, e in enumerate(candidates)}

        enriched = []
        for entry in res.get("top15", []):
            original_id = entry.get("original_id")
            if original_id in id_map:
                item = id_map[original_id]
                item.update({
                    "deep_score": entry.get("deep_score", 50),
                    "score_breakdown": entry.get("score_breakdown", {}),
                    "titles": self._ensure_langs(entry.get("titles", {})),
                })
                enriched.append(item)
        return enriched

    # ══════════════════════════════════════════════════════════════
    # NARRATIVES — Accessible storytelling
    # Structure: Hook → What happened → WHY it happened → AFTERMATH → Legacy
    # ══════════════════════════════════════════════════════════════
    async def generate_secondary_narratives(self, top_events: list, target_date: datetime) -> dict:
        date_str = self._get_target_date_str(target_date)

        style_assignments = self._assign_narrative_styles(top_events)
        for idx, item in enumerate(top_events):
            style = style_assignments[idx]
            logger.info(
                f"📖 Event {idx} ({item.get('slug', '')[:30]}) → "
                f"{style['angle']['name']} / {style['voice']['name']}"
            )

        async def process_single(idx, item):
            style = style_assignments[idx]
            # Write once, translate four times.
            #
            # This used to generate all five languages from scratch, which meant the
            # full instruction block — lens, voice, tone, format — was re-sent five
            # times per event to produce the same article in five languages. It also
            # let the facts drift: five independent generations from the same seed are
            # five slightly different articles, not one article in five languages.
            # deep_dive.py and parallel.py already work this way; this brings the
            # narratives in line.
            en_result = await self._fetch_narrative_lang(idx, item, "en", date_str, style)
            english = {
                "content": en_result[1],
                "notification_title": (en_result[2] or {}).get("title", ""),
                "notification_body": (en_result[2] or {}).get("body", ""),
            }
            translated = await asyncio.gather(*[
                self._translate_narrative(idx, english, lang)
                for lang in self.languages if lang != "en"
            ])
            lang_results = [en_result] + [t for t in translated if t]
            # Each result is (lang, content, notification{title,body}).
            # Narrative text goes into the results map; the per-language notification
            # hook is stashed on the item so _build_event_details can attach it.
            content_map = {}
            notif_map = item.setdefault("notifications", {})
            for lang, content, notif in lang_results:
                content_map[lang] = content
                notif_map[lang] = notif
            return f"EVENT_{idx}", content_map

        results = dict(
            await asyncio.gather(*[process_single(i, item) for i, item in enumerate(top_events)])
        )

        results = await self._verify_and_patch_narratives(
            results, top_events, date_str, style_assignments
        )
        self._audit_opening_diversity(results)
        return results

    async def _translate_narrative(self, idx: int, english: dict, lang: str) -> tuple | None:
        """Carry one finished article into another language.

        Sent as a single JSON payload rather than field by field: three round trips
        per language buys nothing, and the notification hook translates better when
        the model can see the article it belongs to."""
        lang_names = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        lang_full = lang_names.get(lang, lang.upper())

        prompt = f"""
Translate this article into {lang_full}.

Keep the register: plain, factual, explanatory, with the same paragraph breaks. Do not
summarise, do not add, do not drop anything, and do not smooth the explanations into
vaguer language. Numbers stay as digits. Proper nouns take their standard {lang_full}
form. Stay within a tenth of the English word count in either direction: the article is
published as a 2 minute read in every language.

The notification is a push hook, not a headline, so translate its pull rather than its
words. The title stays 5 to 8 words and at most 55 characters, and it must be a complete
phrase that ends on its own: never let it trail off and never end it on an ellipsis or a
preposition. The body stays one sentence of at most 120 characters.

ARTICLE:
{english["content"]}

NOTIFICATION TITLE: {english["notification_title"]}
NOTIFICATION BODY: {english["notification_body"]}

Return JSON only:
{{
  "content": "the full article in {lang_full}, paragraphs separated by blank lines",
  "notification_title": "5-8 word complete hook in {lang_full}, at most 55 chars",
  "notification_body": "<=120 chars in {lang_full}"
}}
"""
        res = await self._safe_ai_call(
            prompt, f"Narrative translate {idx} -> {lang}", {}, thinking_budget=0
        )
        content = (res or {}).get("content", "")
        if not content:
            logger.warning(f"⚠️ Narrative translation to {lang} came back empty (event {idx})")
            return None
        notif = self._clean_notification(
            (res or {}).get("notification_title", ""),
            (res or {}).get("notification_body", ""),
        )
        return lang, content, notif

    def _assign_narrative_styles(self, items: list) -> list:
        """
        Assign each event a unique (angle, voice) pair.

        - The angle controls the STRUCTURE of the piece, the emphasis which facts get
          the most room. Neither varies the register: the free article is one objective
          explanatory voice, and two pieces differ in coverage, not in tone.
        - Seed = day + the batch's slugs, so the FREE batch and the PRO batch get
          different shuffles (no more "event 0 of both tiers gets the same angle").
        - Angles (8) and voices (6) are walked in lockstep with a uniqueness guard,
          so for any realistic batch size every event gets a distinct combination.
        """
        import random
        import hashlib

        count = len(items)
        day_seed = datetime.now().strftime("%Y-%m-%d")
        salt = "|".join(sorted((it.get("slug") or "") for it in items))
        seed = int(hashlib.md5(f"{day_seed}::{salt}".encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)

        angles = list(self.storytelling_angles)
        voices = list(self.narrative_voices)
        rng.shuffle(angles)
        rng.shuffle(voices)

        styles = []
        used_pairs = set()
        for i in range(count):
            angle = angles[i % len(angles)]
            voice = voices[i % len(voices)]
            # Guarantee the (angle, voice) pair is unique within this batch.
            guard = 0
            max_guard = len(angles) * len(voices)
            while (angle["name"], voice["name"]) in used_pairs and guard < max_guard:
                guard += 1
                voice = voices[(i + guard) % len(voices)]
                if (angle["name"], voice["name"]) in used_pairs:
                    angle = angles[(i + guard) % len(angles)]
            used_pairs.add((angle["name"], voice["name"]))
            styles.append({"angle": angle, "voice": voice})

        return styles

    async def _fetch_narrative_lang(
        self, idx: int, item: dict, lang: str, date_str: str, style: dict
    ) -> tuple:
        max_retries = 3
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")
        location = item.get("location") or "the location"
        angle = style["angle"]
        voice = style["voice"]

        lang_names = {
            "en": "English",
            "ro": "Romanian",
            "es": "Spanish",
            "de": "German",
            "fr": "French",
        }
        lang_full = lang_names.get(lang, lang.upper())

        last_content = ""

        for attempt in range(1, max_retries + 1):
            prompt = f"""
You are writing the daily article for a history app. One event, explained properly, for
a reader who knows nothing about it and has two minutes. Write in {lang_full}.

Two minutes is the whole constraint. It is not a summary and not an abstract, it is a
short article that still explains, which means most of what you know about this event
does not go in. Choose ruthlessly and then write those things properly.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}

WHAT THIS PIECE IS: an objective, accurate account of what happened, why it happened and
what followed. Plain language, never simplified facts. Explain the way a good science
journalist explains: precise about the substance, ordinary in the words. The reader should
finish knowing the event, not having sat through a performance about it.

WHAT THIS PIECE IS NOT: a story, an essay or a column. No scene-setting, no invented
thoughts or dialogue, no atmosphere, no suspense, no jokes, no moral at the end. Nothing
is dramatised and nothing is guessed at. If the record does not support it, it does not
go in the article.

STRUCTURAL LENS — {angle['name']}:
{angle['instruction']}

EMPHASIS — {voice['name']}:
{voice['instruction']}

The lens decides what the piece leads with, the emphasis decides which facts get the most
room. Neither changes the register. Every article published today is the same calm,
factual voice; they differ in what they cover, never in how they sound.

THE KEY POINTS — this is the core of the job.
Work out first what KIND of event this is, then pick the THREE OR FOUR points that kind
of event genuinely turns on, and answer those properly. Not all of them. At 400 words,
listing six points shallowly is worse than answering three of them with real detail, and
the reader can tell the difference immediately. Examples of what different kinds of event
turn on, from which you choose:

  • A BATTLE OR MILITARY ACTION — who fought and with what numbers, the ground and the
    plan, the decision or failure that settled it, the casualties on both sides, what the
    result changed on the map.
  • A TREATY, LAW, CHARTER OR FOUNDING — the problem it was written to solve, the parties
    and what each of them conceded, the specific terms that mattered, who was left out,
    whether it held and for how long.
  • A DISCOVERY, INVENTION OR SCIENTIFIC FIRST — what was believed or possible before it,
    how the thing actually works in plain words, the evidence that convinced people, who
    else was close, what it made possible afterwards.
  • A DISASTER, EPIDEMIC OR ACCIDENT — the physical cause, the chain of failures that let
    it happen, the scale in real numbers, the response at the time, and what was changed
    afterwards to stop it happening again.
  • A DEATH, BIRTH, ACCESSION OR SUCCESSION — who the person actually was and what they
    controlled, the state of affairs they inherited or left behind, who took over and by
    what right, what the handover did to the balance of power.
  • A POLITICAL EVENT, COUP OR REVOLUTION — the grievance and who held it, the sequence of
    the day itself, who controlled the army and the money, what the new arrangement was
    and who ended up worse off.
  • AN EXPLORATION, VOYAGE OR EXPEDITION — the objective and who paid for it, the route and
    the distances, the conditions and the losses, what was actually found, and what was
    claimed afterwards that was not true.
  • A CULTURAL OR RELIGIOUS EVENT — what was actually produced, decided or performed, how
    it was received at the time, the doctrine or practice that changed, and what is still
    argued about because of it.

Those are illustrations, not a form to fill in. A coronation, a strike, a trial and a
premiere each turn on their own points. Decide which three or four THIS event turns on,
and answer those with numbers and names. Everything you leave out was a deliberate
choice, not an oversight. Never print the key points as headings or bullets: they are
answered inside the prose, in the order that makes the event make sense.

HOW TO WRITE IT:
- Open on the fact that makes this event worth reading about, stated plainly. No scene,
  no rhetorical question, no date, never "on this day". The first sentence carries a fact.
- Explain the one mechanism that matters. If there is engineering, law, medicine, money
  or military logic underneath, unpack that one in ordinary words. This is what the reader
  came for and it earns the most room in the piece.
- Numbers as evidence, inside the sentences. "Many died" says nothing; "of the 300 who
  went in, 11 walked out" says everything. Real figures only, never rounded into vagueness.
- Name the people who decided things and say what each one did. Quote only actual words.
- If the record is thin or contested on a point you are using, say so in a clause, not a
  paragraph. Never present a contested detail as settled, never invent detail to fill a gap.
- Close on what the event led to: one concrete consequence, not a summary and not a lesson.

RHYTHM: short paragraphs, blank line between them, no paragraph longer than four
sentences. At this length there is no room for a sentence that only sets up the next one.
Every sentence carries a fact the reader did not have. If one does not, it is cut, and
what it was making room for gets the space instead.

PUNCTUATION, and this one is not negotiable:
NEVER use a dash as punctuation. No em dash, no en dash, no " - " standing in for a
comma, a colon or a full stop. If a clause needs joining, use a comma. If it is a new
thought, start a new sentence. A dash is only allowed inside a hyphenated compound
(record-breaking) or a numeric range (1914-1918).

BANNED PHRASES (the cliches that make every article sound the same):
"it is worth noting" / "history tells us" / "changed the course of history" /
"left an indelible mark" / "without a doubt" / "subsequently" / "in conclusion" /
"serves as a reminder" / "stands as a testament" / "it is no coincidence" /
"little did they know" / "on this day" / "fast forward" / "needless to say".

LENGTH: 350-450 words. Aim for 400, which is the 2 minute read the app promises. Under
350 the event is not explained, over 450 it stops being the short piece. This is a hard
constraint, not a target to drift past: write the article, then cut it to fit rather than
stopping early. No headers. Paragraphs separated by blank lines.
LANGUAGE: Entire text in {lang_full}. Zero English except proper nouns.

PUSH NOTIFICATION — also write the phone notification for THIS event, in {lang_full}.
It arrives at nine in the morning and it is the only thing standing between this article
and being ignored. It is a HOOK, not a label, and never an announcement that the app has
new content.
  BAD:  "Your daily event is ready!"
  BAD:  "Today in history: the French Revolution"
  GOOD: "She hid a kitchen knife under her cloak"
- notification_title: 5 to 8 words, and at most 55 characters, so the phone shows it whole.
  It must be a COMPLETE phrase that ends on its own: never trail off, never end on an
  ellipsis, a dash or a preposition, never read as though it was cut short. Build it on
  something specific and strange from THIS event, a number, a name, an object, a stake.
  Not the event's label and not its Wikipedia title.
- notification_body: MAX 120 characters, one sentence, and it must fit without truncation.
  It half-answers the title so that opening the app is the only way to get the rest.
  Every word of it is true.
- Both entirely in {lang_full}. No emoji, no quotation marks, no hashtags.

Return JSON:
{{
  "content": "full article here, paragraphs separated by blank lines",
  "notification_title": "5-8 word complete hook in {lang_full}, at most 55 chars",
  "notification_body": "≤120 char hook body in {lang_full}"
}}
"""

            res = await self._safe_ai_call(
                prompt,
                f"Narrative {idx}:{lang} (attempt {attempt})",
                {"content": ""},
                temperature=0.8,
                max_tokens=4096,
                thinking_budget=0,  # a 700-word explainer does not repay reasoning tokens
            )
            # The prompt forbids dashes-as-punctuation and the model still produces
            # them, so the rule is enforced here rather than hoped for.
            content = strip_prose_dashes(res.get("content", ""))
            notif = self._clean_notification(
                res.get("notification_title", ""), res.get("notification_body", "")
            )
            last_content = content

            is_valid, reason = self._validate_narrative(content, lang, style)
            if is_valid:
                logger.info(f"✅ Narrative {idx}:{lang} — passed (attempt {attempt})")
                return lang, content, notif
            else:
                logger.warning(
                    f"⚠️ Narrative {idx}:{lang} attempt {attempt}: {reason}"
                )

        logger.error(f"🚨 Narrative {idx}:{lang} — all {max_retries} attempts failed")
        return lang, (last_content if last_content else ""), {"title": "", "body": ""}

    @staticmethod
    def _clean_notification(title: str, body: str) -> dict:
        """Trim a generated hook to push-notification limits.

        The title comes back whole or not at all: it is cut on word boundaries and
        never carries an ellipsis, because a notification title that trails off is
        indistinguishable from one the phone truncated. The body is allowed to end
        on one, since a body that stops mid-thought is the hook working.
        """
        title = strip_prose_dashes((title or "").strip().strip('"“”'))
        body = strip_prose_dashes((body or "").strip().strip('"“”'))

        words = title.split()
        if len(words) > NOTIF_TITLE_MAX_WORDS:
            title = " ".join(words[:NOTIF_TITLE_MAX_WORDS])
        while len(title) > NOTIF_TITLE_MAX_CHARS and " " in title:
            title = title.rsplit(" ", 1)[0]
        title = title.rstrip(" ,;:.…-–—")

        if len(body) > NOTIF_BODY_MAX_CHARS:
            body = body[:NOTIF_BODY_MAX_CHARS - 1].rstrip()
            if " " in body:
                body = body.rsplit(" ", 1)[0]
            body = body.rstrip(" ,;:") + "…"
        return {"title": title, "body": body}

    def _validate_narrative(self, content: str, lang: str, style: dict) -> tuple:
        if not content or len(content.strip()) < 50:
            return False, "Empty or too short"

        word_count = len(content.split())

        # Anything under 200 words is a broken or truncated response rather than a
        # brief article, and it is worth saying so separately in the log.
        if word_count < 200:
            return False, f"Too short: {word_count} words (hard min 200)"

        # The rest of the band is the 3 to 4 minute read the app promises. See
        # MIN_NARRATIVE_WORDS for why it is wider than what the prompt asks for.
        if word_count < MIN_NARRATIVE_WORDS:
            return False, f"Too short: {word_count} words (min {MIN_NARRATIVE_WORDS})"
        if word_count > MAX_NARRATIVE_WORDS:
            return False, f"Too long: {word_count} words (max {MAX_NARRATIVE_WORDS})"

        # Broken/placeholder content → always retry
        bad_markers = [
            "narrative pending", "content pending", "error generating",
            "i apologize", "i'm sorry", "as an ai", "i cannot",
            "let me tell you", "in this article", "in this story",
        ]
        content_lower = content.lower()
        for marker in bad_markers:
            if marker in content_lower:
                return False, f"Contains placeholder/AI text: '{marker}'"

        # Check it's actually in target language (rough heuristic)
        if lang != "en":
            english_giveaways = ["the ", "and ", "was ", "were ", "this ", "that ", "with ", "from "]
            count = sum(1 for w in english_giveaways if w in content_lower)
            ratio = count / len(english_giveaways)
            if ratio > 0.8:
                return False, f"Appears to be English instead of {lang}"

        # Numbers check: warn but don't retry — a short factual piece may naturally have fewer
        numbers_found = re.findall(r'\b\d[\d.,]*\b', content)
        if len(numbers_found) < 3:
            logger.warning(f"⚠️ Narrative [{lang}]: only {len(numbers_found)} numbers found (prefer ≥3)")

        return True, "OK"

    async def _verify_and_patch_narratives(
        self, results: dict, top_events: list, date_str: str, style_assignments: list
    ) -> dict:
        patch_tasks = []

        for idx, item in enumerate(top_events):
            event_key = f"EVENT_{idx}"
            narratives = results.get(event_key, {})

            en_content = narratives.get("en", "")
            if not en_content or len(en_content.split()) < 200:
                logger.error(f"🚨 Event {idx}: English missing or too short — regenerating")
                patch_tasks.append(
                    self._emergency_regenerate(
                        idx, item, "en", date_str, style_assignments[idx], results
                    )
                )

            for lang in ["ro", "es", "de", "fr"]:
                content = narratives.get(lang, "")
                is_valid, reason = self._validate_narrative(
                    content, lang, style_assignments[idx]
                )
                if not is_valid:
                    logger.warning(f"⚠️ Event {idx}:{lang} failed: {reason} — patching")
                    patch_tasks.append(self._patch_from_english(idx, item, lang, results))

        if patch_tasks:
            logger.info(f"🔧 Patching {len(patch_tasks)} narrative(s)...")
            await asyncio.gather(*patch_tasks)

        for idx in range(len(top_events)):
            event_key = f"EVENT_{idx}"
            if event_key not in results:
                results[event_key] = {}
            # Use `or` so that an empty string ("") also triggers the fallback,
            # not just a missing key. Empty string means generation failed entirely.
            en_text = results[event_key].get("en") or ""
            fallback_text = en_text if len(en_text.strip()) >= 200 else "Narrative unavailable."
            for lang in self.languages:
                current = results[event_key].get(lang) or ""
                if not current or len(current.strip()) < 200:
                    logger.error(f"🚨 CRITICAL: {idx}:{lang} still missing or too short")
                    results[event_key][lang] = fallback_text

        return results

    async def _emergency_regenerate(
        self, idx: int, item: dict, lang: str, date_str: str, style: dict, results: dict
    ):
        _, content, notif = await self._fetch_narrative_lang(idx, item, lang, date_str, style)
        event_key = f"EVENT_{idx}"
        if event_key not in results:
            results[event_key] = {}
        # Only update if we actually got something back
        if content and len(content.strip()) >= 200:
            results[event_key][lang] = content
            if notif.get("title") or notif.get("body"):
                item.setdefault("notifications", {})[lang] = notif
        else:
            logger.error(f"🚨 Emergency regenerate for {idx}:{lang} also failed — keeping previous value")
            if not results[event_key].get(lang):
                results[event_key][lang] = content  # keep whatever we have, even if short

    async def _patch_from_english(self, idx: int, item: dict, target_lang: str, results: dict):
        event_key = f"EVENT_{idx}"
        en_content = results.get(event_key, {}).get("en", "")
        if not en_content or len(en_content.split()) < 200:
            logger.error(f"🚨 Cannot patch {idx}:{target_lang} — English missing or too short")
            return

        lang_names = {"ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        lang_full = lang_names.get(target_lang, target_lang.upper())

        # Carry the English notification hook along so we translate it in the same call
        # (no extra API request) instead of leaving this language without a hook.
        en_notif = item.get("notifications", {}).get("en", {"title": "", "body": ""})

        prompt = f"""
Translate this historical narrative AND its push notification into {lang_full}.

Keep the register: plain, factual, explanatory. Do not smooth it into academic prose
and do not summarise. All numbers stay as digits. Proper nouns use their standard
{lang_full} form. Blank lines between paragraphs. Stay within a tenth of the English
word count: the article is published as a 2 minute read in every language.
The notification stays a curiosity HOOK, not a label. The title is 5 to 8 words and at
most 55 characters, a complete phrase that never trails off or ends on an ellipsis.
The body is one sentence of at most 120 characters.
Output only in {lang_full} — no English except proper nouns.

ENGLISH ARTICLE:
{en_content}

ENGLISH NOTIFICATION TITLE: {en_notif.get('title', '')}
ENGLISH NOTIFICATION BODY: {en_notif.get('body', '')}

Return JSON:
{{
  "content": "translated narrative in {lang_full}",
  "notification_title": "translated hook title, 5-8 words, at most 55 chars",
  "notification_body": "translated ≤120 char hook body"
}}
"""

        res = await self._safe_ai_call(
            prompt, f"Translation {idx}:{target_lang}", {"content": ""},
            temperature=0.3, max_tokens=4096, thinking_budget=0,
        )
        translated = res.get("content", "")
        notif = self._clean_notification(
            res.get("notification_title", ""), res.get("notification_body", "")
        )
        if notif.get("title") or notif.get("body"):
            item.setdefault("notifications", {})[target_lang] = notif

        if translated and len(translated.split()) >= 200:
            results[event_key][target_lang] = translated
            logger.info(f"✅ Patched {idx}:{target_lang} via translation")
        else:
            results[event_key][target_lang] = en_content
            logger.warning(f"⚠️ Translation failed {idx}:{target_lang} — using English fallback")

    def _audit_opening_diversity(self, results: dict):
        logger.info("🔍 Opening diversity audit (EN):")
        openings = []
        for key in sorted(results.keys()):
            en = results[key].get("en", "")
            first_words = " ".join(en.split()[:15])
            openings.append(first_words)
            logger.info(f"  {key}: \"{first_words}...\"")

        first_three = [" ".join(o.split()[:3]).lower() for o in openings if o]
        duplicates = len(first_three) - len(set(first_three))
        if duplicates > 0:
            logger.warning(f"⚠️ {duplicates} events share opening 3 words!")
        else:
            logger.info("✅ All events have unique openings")

    # ══════════════════════════════════════════════════════════════
    # TITLE TRANSLATION VERIFICATION
    # ══════════════════════════════════════════════════════════════
    async def verify_and_fix_titles(self, events: list) -> list:
        """Re-translate any title that came back missing or placeholder.

        Costs nothing on a healthy batch: a repair task is only created for an event
        that actually has a gap. It was written, never wired into main.py, and
        "Data pending" reached production as a headline for months as a result. It is
        now called before every `_build_event_details`.
        """
        repair_tasks = []
        for idx, item in enumerate(events):
            titles = item.get("titles", {})
            missing = [
                lang for lang in self.languages
                if not titles.get(lang) or titles.get(lang) in ("Event", "Data pending", "")
            ]
            if missing:
                repair_tasks.append(self._repair_titles(idx, item, missing))

        if repair_tasks:
            logger.info(f"🔧 Repairing titles for {len(repair_tasks)} event(s)...")
            await asyncio.gather(*repair_tasks)
        return events

    async def _repair_titles(self, idx: int, item: dict, missing_langs: list):
        en_title = item.get("titles", {}).get("en", item.get("text", "Historical Event")[:80])
        year = item.get("year", "")
        slug = item.get("slug", "")

        lang_names = {"en": "English", "ro": "Romanian", "es": "Spanish", "de": "German", "fr": "French"}
        langs_str = ", ".join([f"{lang_names[l]} ({l})" for l in missing_langs])

        prompt = f"""
Translate this historical event title into: {langs_str}

ORIGINAL (English): {en_title}
CONTEXT: Year {year}, Wikipedia: {slug}

RULES: Concise (5-15 words), natural phrasing, standard proper nouns.

Return JSON with language codes as keys:
{{ {', '.join([f'"{l}": "title in {lang_names[l]}"' for l in missing_langs])} }}
"""

        res = await self._safe_ai_call(prompt, f"Title repair {idx}", {}, thinking_budget=0)
        titles = item.get("titles", {})
        for lang in missing_langs:
            fixed = res.get(lang, "")
            if fixed and len(fixed) > 2:
                titles[lang] = fixed
            else:
                titles[lang] = en_title
        item["titles"] = titles

    # ══════════════════════════════════════════════════════════════
    # SAFE AI CALL  (Groq gpt-oss + reasoning_effort)
    # ══════════════════════════════════════════════════════════════
    async def _safe_ai_call(
        self,
        prompt: str,
        context: str,
        fallback: dict,
        temperature: float = 0.4,
        max_tokens: int = 4096,
        thinking_budget: int | None = None,
    ) -> dict:
        budget = self.thinking_budget if thinking_budget is None else thinking_budget
        params = build_params(
            model=self.model,
            system=(
                "You are a strict History API. Output ONLY valid JSON. "
                "No markdown, no code fences, no commentary."
            ),
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            thinking_budget=budget,
        )
        # The Groq SDK already retries 429s (honouring retry-after); this outer loop
        # additionally recovers from an occasional empty/invalid generation.
        #
        # What it must NOT do is retry an error that cannot change. On 2026-09-02 this
        # loop spent twelve minutes re-sending discovery calls against a 429 whose own
        # message read "try again in 22m", three times each, for two dates — every one
        # of them refused, and the run ended with two blank days. A rate limit measured
        # in tens of minutes and a request too large for the window are both verdicts,
        # not hiccups: take them the first time.
        last_err = None
        for attempt in range(3):
            try:
                message = await achat(params)
                return parse_json_response(message)
            except BudgetExhausted as e:
                logger.error(f"🛑 AI skipped ({context}) — spend cap reached: {e}")
                return fallback
            except json.JSONDecodeError as e:
                last_err = e
                logger.warning(f"⚠️ JSON parse retry ({context}, attempt {attempt + 1}): {e}")
            except Exception as e:
                last_err = e
                wait = _retry_after_seconds(e)
                if _is_request_too_large(e):
                    logger.error(
                        f"🚨 AI Error ({context}) — request too large for the tokens-per-minute "
                        f"tier; retrying the same size would fail identically. Lower this call's "
                        f"max_tokens or raise AI_TPM_LIMIT to match your Groq tier."
                    )
                    return fallback
                if wait is not None and wait > _RETRY_GIVE_UP_SECONDS:
                    logger.error(
                        f"🚨 AI Error ({context}) — rate limited for {wait / 60:.0f} more minutes; "
                        f"not retrying. Remaining stages this run will be skipped."
                    )
                    return fallback
                logger.warning(f"⚠️ AI error retry ({context}, attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(2 * (attempt + 1))
        logger.error(f"🚨 AI Error ({context}) — all attempts failed: {last_err}")
        return fallback

