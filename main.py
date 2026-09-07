import asyncio
import httpx
import hmac
import hashlib
import time
import base64
import json
from datetime import datetime, timedelta

from core.logger import setup_logger
from core.config import config
from engine.scraper import WikiScraper
from engine.processor import AIProcessor
from engine.deep_dive import DeepDiveGenerator
from engine.parallel import ParallelGenerator
from engine.quiz_generator import QuizGenerator
from engine.ranker import ScoringEngine
from engine.social_agent import SocialMediaAgent
from engine.deduplicator import EventDeduplicator
from engine.wiki_date_validator import WikiDateValidator
from schema.models import (
    DailyPayload,
    EventDetail,
    EventCategory,
    Translations,
    DeepDive,
    DeepDiveChapter,
    DeepDiveHighlight,
    DeepDiveTranslations,
    Actor,
    ChoiceOutcome,
    EndingStat,
    EraVoice,
    NodeFact,
    ParallelUniverse,
    ParallelUniverseTranslations,
    UniverseChoice,
    UniverseNode,
    WorldEffects,
)

logger = setup_logger("MainPipeline")

# How many events we want at the end of each pipeline.
# App layout: 2 "main" heroes (1 FREE + 1 PRO, the highest-impact of each tier) and
# 7 secondary (4 FREE + 3 PRO). Totals: FREE = 5, PRO = 4. The main/secondary split is
# derived on the client from impact_score ordering, not stored — the pipeline just
# produces the right counts.
# 2 for the main screen + 2 in discovery. Down from 5 in Sept 2026: every per-event
# cost — narrative, translations, quiz, long read — multiplies by this number.
TARGET_FREE_COUNT = 5
TARGET_PRO_COUNT = 3  # 3 in discovery. Was 4 (personalities, media, sport, best-of-rest).

# How many events per tier get a Parallel Universes game. Two: the game now has its own
# tab rather than living at the bottom of one story, and a hub with a single entry is not
# a tab. Four games a day (2 FREE + 2 PRO) is the ceiling the budget takes — the tree
# costs roughly a Long Read each, and every event having one would multiply the daily
# bill for content most players will never open.
#
# Held at 1 while the API budget is tight. Each game is now roughly six calls (one trunk,
# one for the endings, four translations), so 1 costs about 12 a day across both tiers and
# 2 costs 24. The hub shows the whole 60-day archive, so one new fork a day still fills
# the tab — it just fills it over a fortnight rather than a weekend. Raise when the
# budget allows.
#
# This is a target, not a slice: the generator is handed the whole tier and falls down
# the list when a story has no usable hinge, escalating from the obvious fork to the
# argument contemporaries were having to the near miss that almost happened. It gives up
# on the extras before it gives up on the first one, so a day ends with no game only if
# every event in the tier failed at every stance.
PARALLEL_PER_TIER = 1

# Minimum acceptable count before we fall back to non-validated events
MIN_FREE_COUNT = 4
MIN_PRO_COUNT = 2

# Minimum final_score (0-100) for a NEW event to be accepted in refresh mode.
# New events below this threshold are considered low-relevance and are replaced
# by the best existing events from the DB.
REFRESH_SCORE_THRESHOLD = 60


def _db_row_to_event_detail(row: dict) -> EventDetail:
    """
    Convert a raw DB row dict (from load_top_events_for_date) into an EventDetail.
    Used in refresh mode to fill slots with existing high-quality events.
    No narrative generation or image uploading — uses stored data as-is.
    """
    def _safe_dict(val, default=None):
        if isinstance(val, dict):
            return val
        return default or {}

    def _safe_list(val):
        if isinstance(val, list):
            return val
        return []

    lang_defaults = {"en": "", "ro": "", "es": "", "de": "", "fr": ""}

    title_data = {**lang_defaults, **_safe_dict(row.get("title_translations"))}
    narrative_data = {**lang_defaults, **_safe_dict(row.get("narrative_translations"))}

    try:
        category_enum = EventCategory(str(row.get("category") or "").lower())
    except ValueError:
        category_enum = EventCategory.CULTURE_ARTS

    event_date = row.get("event_date")
    try:
        year = int(event_date.year) if hasattr(event_date, "year") else 0
    except Exception:
        year = 0

    return EventDetail(
        category=category_enum,
        year=year,
        event_date=event_date,
        source_url=str(row.get("source_url") or ""),
        title_translations=Translations(
            en=str(title_data.get("en") or ""),
            ro=str(title_data.get("ro") or ""),
            es=str(title_data.get("es") or ""),
            de=str(title_data.get("de") or ""),
            fr=str(title_data.get("fr") or ""),
        ),
        narrative_translations=Translations(
            en=str(narrative_data.get("en") or ""),
            ro=str(narrative_data.get("ro") or ""),
            es=str(narrative_data.get("es") or ""),
            de=str(narrative_data.get("de") or ""),
            fr=str(narrative_data.get("fr") or ""),
        ),
        impact_score=float(row.get("impact_score") or 0),
        page_views_30d=int(row.get("page_views_30d") or 0),
        gallery=_safe_list(row.get("gallery")),
        quiz=None,  # quiz preserved in DB; not re-sent to avoid overwrite
        is_pro=bool(row.get("is_pro", False)),
        location=row.get("location"),
        # Carry the stored long read forward. The Java upsert clears and repopulates the
        # whole day, so a filler event sent without its deep dive would delete one that
        # was already generated and paid for.
        deep_dive=_deep_dive_from_db(row.get("deep_dive")),
        # Same reasoning: the Java upsert clears the day, so a filler event sent without
        # its game would delete one that was already generated and paid for.
        parallel=_parallel_from_db(row.get("parallel_universe")),
    )


def _parallel_from_db(raw) -> ParallelUniverseTranslations | None:
    """Rebuild the game from the stored column. Anything malformed yields None — a
    filler event without a game is a small loss, a crashed run is a large one."""
    if not raw:
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            return None
    except Exception:
        logger.warning("⚠️ Filler event has unparseable parallel_universe — dropping it")
        return None

    langs = {}
    for lang in ("en", "ro", "es", "de", "fr"):
        p = data.get(lang)
        if not isinstance(p, dict) or not p.get("nodes"):
            continue
        try:
            langs[lang] = ParallelUniverse(
                pivot_year=str(p.get("pivotYear") or ""),
                pivot_title=str(p.get("pivotTitle") or ""),
                premise=str(p.get("premise") or ""),
                root=str(p.get("root") or "n0"),
                actors=[
                    Actor(id=str(a["id"]), name=str(a.get("name") or ""),
                          start=int(a.get("start") or 50))
                    for a in p.get("actors", []) if isinstance(a, dict) and a.get("id")
                ],
                nodes=[
                    UniverseNode(
                        id=str(n["id"]), year=str(n.get("year") or ""),
                        title=str(n.get("title") or ""), text=str(n.get("text") or ""),
                        verdict=str(n.get("verdict") or ""),
                        epitaph=str(n.get("epitaph") or ""),
                        rarity=str(n.get("rarity") or ""),
                        facts=[
                            NodeFact(label=str(f.get("label") or ""),
                                     value=str(f.get("value") or ""))
                            for f in n.get("facts", []) if isinstance(f, dict)
                        ],
                        stats=[
                            EndingStat(label=str(st.get("label") or ""),
                                       real=str(st.get("real") or ""),
                                       alt=str(st.get("alt") or ""))
                            for st in n.get("stats", []) if isinstance(st, dict)
                        ],
                        legacy=[
                            EraVoice(who=str(v.get("who") or ""),
                                     mood=str(v.get("mood") or ""),
                                     quote=str(v.get("quote") or ""))
                            for v in n.get("legacy", []) if isinstance(v, dict)
                        ],
                        choices=[
                            UniverseChoice(
                                id=str(c["id"]), label=str(c.get("label") or ""),
                                detail=str(c.get("detail") or ""), next=str(c["next"]),
                                risk=int(c.get("risk") or 50),
                                actor_effects=c.get("actorEffects") or {},
                                reactions=[
                                    EraVoice(who=str(v.get("who") or ""),
                                             mood=str(v.get("mood") or ""),
                                             quote=str(v.get("quote") or ""))
                                    for v in c.get("reactions", []) if isinstance(v, dict)
                                ],
                                outcome=(
                                    ChoiceOutcome(
                                        label=str((c.get("outcome") or {}).get("label") or ""),
                                        value=str((c.get("outcome") or {}).get("value") or ""),
                                    ) if c.get("outcome") else None
                                ),
                                effects=WorldEffects(**(c.get("effects") or {})),
                            )
                            for c in n.get("choices", []) if isinstance(c, dict)
                        ],
                    )
                    for n in p["nodes"] if isinstance(n, dict) and n.get("id")
                ],
            )
        except Exception:
            continue

    return ParallelUniverseTranslations(**langs) if langs else None


def _deep_dive_from_db(raw) -> DeepDiveTranslations | None:
    """Rebuild DeepDiveTranslations from the `deep_dive` text column.

    Anything malformed yields None rather than raising: a filler event without a long
    read is a small loss, a crashed daily run is a large one.
    """
    if not raw:
        return None
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(data, dict):
            return None
    except Exception:
        logger.warning("⚠️ Filler event has unparseable deep_dive — dropping it")
        return None

    langs = {}
    for lang in ("en", "ro", "es", "de", "fr"):
        entry = data.get(lang)
        if not isinstance(entry, dict) or not entry.get("chapters"):
            continue
        try:
            langs[lang] = DeepDive(
                chapters=[
                    DeepDiveChapter(
                        title=str(c.get("title") or ""),
                        body=str(c.get("body") or ""),
                    )
                    for c in entry.get("chapters", [])
                    if isinstance(c, dict)
                ],
                # Rows written before highlights existed simply have none, and the app
                # renders the section only when it is non-empty.
                highlights=[
                    DeepDiveHighlight(
                        label=str(h.get("label") or ""),
                        text=str(h.get("text") or ""),
                    )
                    for h in entry.get("highlights", [])
                    if isinstance(h, dict) and h.get("text")
                ],
                timeline=[str(x) for x in entry.get("timeline", [])],
                misconception=str(entry.get("misconception") or ""),
                aftermath=[str(x) for x in entry.get("aftermath", [])],
                sources=[str(x) for x in entry.get("sources", [])],
                teaser=str(entry.get("teaser") or ""),
                word_count=int(entry.get("word_count") or 0),
            )
        except Exception:
            continue

    return DeepDiveTranslations(**langs) if langs else None


def _figure_dict(f) -> dict:
    """One figure, flattened for the app. Empty lists are kept out of the payload so a
    stat row does not ship an empty `points` array to every device."""
    out = {"kind": f.kind, "title": f.title, "unit": f.unit, "note": f.note}
    if f.stats:
        out["stats"] = [{"value": s.value, "unit": s.unit, "label": s.label} for s in f.stats]
    if f.points:
        out["points"] = [{"label": p.label, "value": p.value} for p in f.points]
    if f.rows:
        out["rows"] = [
            {"label": r.label, "value": r.value, **({"cells": r.cells} if r.cells else {})}
            for r in f.rows
        ]
    if f.columns:
        out["columns"] = f.columns
    if f.kind == "guess":
        out.update({
            "question": f.question,
            "options": f.options,
            "answerIndex": f.answer_index,
            "reveal": f.reveal,
        })
    return out


def _teaser_figure(dd) -> dict | None:
    """The one figure a free user gets: the band of numbers, never a chart.

    The band is a fact about the event; the bars are the analysis the subscription
    pays for. Picking it here rather than in the app means the long read's other
    figures never travel to a device that may not open them.
    """
    for f in dd.figures:
        if f.kind == "stat_row" and f.stats:
            return _figure_dict(f)
    # Since the long read became mostly structure, a piece often has no stat row at
    # all: the numbers went into the fact grid instead. Rather than leave the free
    # article with nothing drawn on it, the grid stands in, trimmed to four rows so
    # the subscriber still has the rest.
    for f in dd.figures:
        if f.kind == "fact_grid" and len(f.rows) >= 2:
            out = _figure_dict(f)
            out["rows"] = out["rows"][:4]
            return out
    return None


def _serialize_deep_dive(deep_dive) -> str | None:
    """Full long read as a JSON string for the `deep_dive` column — PRO users only.

    A string rather than a jsonb column: the database never queries inside this blob,
    and text avoids every Hibernate/driver JSON-typing question.
    """
    if deep_dive is None:
        return None
    payload = {}
    for lang in ("en", "ro", "es", "de", "fr"):
        dd = getattr(deep_dive, lang, None)
        if dd is None or not dd.chapters:
            continue
        payload[lang] = {
            "chapters": [{"title": c.title, "body": c.body} for c in dd.chapters],
            "highlights": [{"label": h.label, "text": h.text} for h in dd.highlights],
            "figures": [_figure_dict(f) for f in dd.figures],
            "timeline": dd.timeline,
            "misconception": dd.misconception,
            "aftermath": dd.aftermath,
            "sources": dd.sources,
            "teaser": dd.teaser,
            "word_count": dd.word_count,
        }
    return json.dumps(payload, ensure_ascii=False) if payload else None


def _serialize_deep_dive_teaser(deep_dive) -> str | None:
    """The part every user receives: opening words, chapter titles, and the numbers.

    Kept in its own column so the free endpoint can ship the pitch without the body
    text ever being loaded — the chapter titles ARE the pitch, so they are not secret.
    """
    if deep_dive is None:
        return None
    payload = {}
    for lang in ("en", "ro", "es", "de", "fr"):
        dd = getattr(deep_dive, lang, None)
        if dd is None or not dd.chapters:
            continue
        payload[lang] = {
            "teaser": dd.teaser,
            "chapters": [c.title for c in dd.chapters],
            "wordCount": dd.word_count,
            "sourceCount": len(dd.sources),
        }
        figure = _teaser_figure(dd)
        if figure:
            payload[lang]["figure"] = figure
    return json.dumps(payload, ensure_ascii=False) if payload else None


def _serialize_parallel(parallel) -> str | None:
    """The whole branching game as a JSON string, keyed by language.

    Sent to every user, not just PRO. The client decides who may play it — unlike the
    long read there is no line to draw inside the payload, and the tree is worthless
    without the UI that runs it.
    """
    if parallel is None:
        return None
    payload = {}
    for lang in ("en", "ro", "es", "de", "fr"):
        pu = getattr(parallel, lang, None)
        if pu is None or not pu.nodes:
            continue
        payload[lang] = {
            "pivotYear": pu.pivot_year,
            "pivotTitle": pu.pivot_title,
            "premise": pu.premise,
            "root": pu.root,
            "actors": [
                {"id": a.id, "name": a.name, "start": a.start} for a in pu.actors
            ],
            "nodes": [
                {
                    "id": n.id, "year": n.year, "title": n.title, "text": n.text,
                    "verdict": n.verdict, "epitaph": n.epitaph, "rarity": n.rarity,
                    "facts": [{"label": f.label, "value": f.value} for f in n.facts],
                    "stats": [
                        {"label": st.label, "real": st.real, "alt": st.alt}
                        for st in n.stats
                    ],
                    "legacy": [
                        {"who": v.who, "mood": v.mood, "quote": v.quote}
                        for v in n.legacy
                    ],
                    "choices": [
                        {
                            "id": c.id, "label": c.label, "detail": c.detail,
                            "next": c.next,
                            "risk": c.risk,
                            "actorEffects": c.actor_effects,
                            "reactions": [
                                {"who": v.who, "mood": v.mood, "quote": v.quote}
                                for v in c.reactions
                            ],
                            "outcome": (
                                {"label": c.outcome.label, "value": c.outcome.value}
                                if c.outcome else None
                            ),
                            "effects": {
                                "stability": c.effects.stability,
                                "lives": c.effects.lives,
                                "progress": c.effects.progress,
                                "freedom": c.effects.freedom,
                            },
                        }
                        for c in n.choices
                    ],
                }
                for n in pu.nodes
            ],
        }
    return json.dumps(payload, ensure_ascii=False) if payload else None


def _parallel_for_index(pmap: dict | None, idx: int) -> ParallelUniverseTranslations | None:
    """Pick this event's game out of the generator output. Absent is normal — only the
    hero of each tier gets one."""
    if not pmap:
        return None
    entry = pmap.get(f"EVENT_{idx}")
    if not entry:
        return None
    langs = {}
    for lang, p in entry.items():
        langs[lang] = ParallelUniverse(
            pivot_year=p["pivot_year"], pivot_title=p["pivot_title"],
            premise=p["premise"], root=p["root"],
            actors=[Actor(**a) for a in p.get("actors", [])],
            nodes=[
                UniverseNode(
                    id=n["id"], year=n["year"], title=n["title"], text=n["text"],
                    verdict=n.get("verdict", ""), epitaph=n.get("epitaph", ""),
                    rarity=n.get("rarity", ""),
                    facts=[NodeFact(**f) for f in n.get("facts", [])],
                    stats=[EndingStat(**st) for st in n.get("stats", [])],
                    legacy=[EraVoice(**v) for v in n.get("legacy", [])],
                    choices=[
                        UniverseChoice(
                            id=c["id"], label=c["label"], detail=c["detail"],
                            next=c["next"], effects=WorldEffects(**c["effects"]),
                            actor_effects=c.get("actor_effects", {}),
                            risk=c.get("risk", 50),
                            reactions=[EraVoice(**v) for v in c.get("reactions", [])],
                            outcome=ChoiceOutcome(**c["outcome"]) if c.get("outcome") else None,
                        )
                        for c in n["choices"]
                    ],
                )
                for n in p["nodes"]
            ],
        )
    return ParallelUniverseTranslations(**langs)


def _serialize_quiz(quiz) -> dict | None:
    """
    Serialize QuizTranslations into the JSON structure expected by the Java backend.
    Uses camelCase keys (correctId) to match the Java DTO.
    Returns None if quiz is missing — backend treats null as "no quiz yet".
    """
    if quiz is None:
        return None
    result = {}
    for lang in ["en", "ro", "es", "de", "fr"]:
        questions = getattr(quiz, lang, [])
        result[lang] = [
            {
                "id": q.id,
                "question": q.question,
                "options": [{"id": opt.id, "text": opt.text} for opt in q.options],
                "correctId": q.correct_id,
                "explanation": q.explanation,
            }
            for q in questions
        ]
    return result


def _dedupe_by_slug(events: list, label: str = "") -> list:
    """Remove same-slug duplicates within a single list. Keeps first occurrence."""
    seen_slugs = set()
    result = []
    dropped = 0
    for ev in events:
        slug = (ev.get("slug") or "").strip()
        if not slug:
            continue
        if slug in seen_slugs:
            dropped += 1
            logger.warning(f"🔁 [{label}] In-payload duplicate dropped: {slug}")
            continue
        seen_slugs.add(slug)
        result.append(ev)

    if dropped:
        logger.info(f"🧹 [{label}] In-payload dedup: {len(result)} unique, {dropped} dropped")
    return result


async def send_to_java(payload: DailyPayload):
    target_url = config.JAVA_BACKEND_URL
    secret = config.INTERNAL_API_SECRET

    events_final = []
    for ev in payload.events:
        ev_dict = {
            "category": ev.category.value,
            "titleTranslations": ev.title_translations.model_dump(),
            "narrativeTranslations": ev.narrative_translations.model_dump(),
            "notificationTitleTranslations": ev.notification_title_translations.model_dump(),
            "notificationBodyTranslations": ev.notification_body_translations.model_dump(),
            "eventDate": ev.event_date.isoformat(),
            "impactScore": float(ev.impact_score),
            "sourceUrl": str(ev.source_url),
            "pageViews30d": int(ev.page_views_30d),
            "gallery": ev.gallery if ev.gallery else [],
            "isPro": bool(ev.is_pro),
            "location": ev.location,
            "quiz": _serialize_quiz(ev.quiz),
            # Long read. `deepDive` is served only to PRO users; `deepDiveTeaser`
            # carries the chapter titles and word count and goes to everyone.
            "deepDive": _serialize_deep_dive(ev.deep_dive),
            "deepDiveTeaser": _serialize_deep_dive_teaser(ev.deep_dive),
            "parallelUniverse": _serialize_parallel(ev.parallel),
        }
        events_final.append(ev_dict)

    payload_to_serialize = {
        "dateProcessed": payload.date_processed.isoformat(),
        "events": events_final,
    }

    body_json = json.dumps(payload_to_serialize, separators=(",", ":"))
    body_bytes = body_json.encode("utf-8")
    timestamp = str(int(time.time()))
    auth_payload = f"{timestamp}.{body_json}"

    signature = hmac.new(
        secret.encode("utf-8"),
        auth_payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    signature_base64 = base64.b64encode(signature).decode("utf-8")

    headers = {
        "X-Timestamp": timestamp,
        "X-Signature": signature_base64,
        "Content-Type": "application/json",
    }

    # The long read multiplies the payload by roughly five (9 events x 5 languages x
    # ~2000 words), so the old 30s ceiling is no longer comfortable on a cold backend.
    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            logger.info(f"📤 Sending to Java: {target_url}")
            logger.info(
                f"📦 Payload: {len(events_final)} events "
                f"({sum(1 for e in events_final if e['isPro'])} PRO, "
                f"{sum(1 for e in events_final if not e['isPro'])} FREE, "
                f"{sum(1 for e in events_final if e['deepDive'])} with long read)"
            )
            response = await client.post(target_url, content=body_bytes, headers=headers)
            if response.status_code in [200, 201]:
                logger.info(f"✅ SUCCESS! Payload accepted for date: {payload.date_processed}")
            else:
                logger.error(f"❌ Status {response.status_code}: {response.text}")
                logger.error(f"🧪 JSON sent (first 2000 chars):\n{body_json[:2000]}")
                if len(body_json) > 2000:
                    logger.error(f"🧪 JSON sent (last 1000 chars):\n{body_json[-1000:]}")
        except Exception as e:
            logger.error(f"🚨 Connection error: {e}")


async def safe_upload(scraper: WikiScraper, url: str, public_id: str):
    if not url:
        return None
    try:
        return await asyncio.to_thread(scraper.upload_to_cloudinary, url, public_id)
    except Exception:
        return None


def _is_wikipedia_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    return "wikipedia.org" in url_lower or "wikimedia.org" in url_lower


def _backfill_to_minimum(
    validated: list,
    all_candidates: list,
    minimum: int,
    label: str = "FREE",
) -> list:
    """
    If validator was too aggressive and rejected events we actually need,
    backfill from the original candidate pool (sorted by ai_score) to
    reach at least `minimum` events.

    Backfilled events are ranked by AI confidence score so we still get
    the AI's best picks, just without the wiki-date check.
    """
    if len(validated) >= minimum:
        return validated

    needed = minimum - len(validated)
    used_slugs = {(v.get("slug") or "").lower() for v in validated}

    backfill_pool = [
        c for c in all_candidates
        if (c.get("slug") or "").lower() not in used_slugs
    ]
    backfill_pool.sort(key=lambda x: x.get("ai_score", 0), reverse=True)

    backfilled = backfill_pool[:needed]
    if backfilled:
        logger.warning(
            f"⚠️ [{label}] Validator too strict — backfilling {len(backfilled)} "
            f"events from AI candidate pool to reach min {minimum}"
        )
        for ev in backfilled:
            logger.warning(
                f"  ↩ Backfilled: {ev.get('year')} {ev.get('slug')} "
                f"(ai_score: {ev.get('ai_score', 0)})"
            )

    return validated + backfilled


# ══════════════════════════════════════════════════════════════════
# FREE PIPELINE
# Target: 5 events labeled is_pro=False
# ══════════════════════════════════════════════════════════════════
async def _enrich(quiz_gen, processor, items: list, narratives_map: dict, today):
    """Quizzes, long reads and the branching game, at the same time.

    All three need only the narratives, and none needs the others, so running them in
    sequence just added their latencies together — in the 28 Aug run the game did not
    start until four minutes into the tier and then held the whole pipeline open on its
    own. Concurrency here is free: the LLM client is async and every generator already
    fans out internally.

    Returns (quizzes, deep_dives, parallel). A generator that raises takes only its own
    slice down — a failed quiz must not cost the day its long reads.
    """
    results = await asyncio.gather(
        quiz_gen.generate_quizzes(items, narratives_map),
        DeepDiveGenerator(processor).generate_deep_dives(items, narratives_map, today),
        ParallelGenerator(processor).generate(items, narratives_map, today, want=PARALLEL_PER_TIER),
        return_exceptions=True,
    )
    out = []
    for label, res in zip(("quizzes", "long reads", "parallel universes"), results):
        if isinstance(res, BaseException):
            logger.error(f"🚨 {label} failed for this tier: {res}", exc_info=res)
            out.append({})
        else:
            out.append(res)
    return tuple(out)


async def run_free_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
    deduper: EventDeduplicator,
    date_validator: WikiDateValidator,
    is_refresh: bool = False,
) -> tuple:
    mode = "REFRESH" if is_refresh else "INITIAL"
    logger.info(f"🆓 FREE [{mode}] — Discovering events for {today.strftime('%B %d')}...")
    known_slugs = deduper.existing_slugs_for_date(today)
    all_events = await processor.discover_events(today, exclude_slugs=known_slugs)
    all_events = _dedupe_by_slug(all_events, "FREE-discover")
    logger.info(f"📋 FREE got {len(all_events)} unique validated events")

    if not all_events:
        logger.error("❌ FREE: AI returned no events")
        return [], {"free_discovered": 0}

    original_pool = list(all_events)

    logger.info("🗓️ FREE — Validating dates against Wikipedia...")
    validated = await date_validator.validate_events(all_events, today, tier="FREE")

    if len(validated) < MIN_FREE_COUNT:
        validated = _backfill_to_minimum(
            validated, original_pool, MIN_FREE_COUNT, label="FREE"
        )

    if not validated:
        logger.error("❌ FREE: no events available even after backfill")
        return [], {"free_discovered": len(all_events), "free_after_validation": 0}

    logger.info(f"🔬 FREE — Deep ranking {len(validated)} events...")
    top_ranked = await processor.deep_rank_and_select(validated, today)
    top_ranked = _dedupe_by_slug(top_ranked, "FREE-rank")

    if not top_ranked:
        top_ranked = sorted(validated, key=lambda x: x.get("ai_score", 0), reverse=True)
        top_ranked = _dedupe_by_slug(top_ranked, "FREE-rank-fallback")

    logger.info("📊 FREE — Fetching pageviews...")
    view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in top_ranked]
    views = await asyncio.gather(*view_tasks)

    for idx, item in enumerate(top_ranked):
        item["views"] = views[idx] if isinstance(views[idx], int) else 0
        item["final_score"] = ranker.calculate_final_score(
            ai_score=item.get("deep_score", item.get("ai_score", 50)),
            views=item["views"],
            category=item.get("category", "culture_arts"),
            year=item.get("year", 0),
        )

    top_ranked.sort(key=lambda x: x.get("final_score", 0), reverse=True)

    logger.info("🔍 FREE — Filtering duplicates against existing DB events...")
    deduped = deduper.filter_duplicates(top_ranked, tier="FREE")
    deduped = _dedupe_by_slug(deduped, "FREE-final")

    if not deduped and not is_refresh:
        logger.error("❌ FREE: no non-duplicate events left")
        return [], {
            "free_discovered": len(all_events),
            "free_after_validation": len(validated),
            "free_after_dedup": 0,
            "free_final": 0,
        }

    # ── REFRESH MODE ──────────────────────────────────────────────
    # Keep only new events that score >= threshold.
    # Fill remaining slots with best existing DB events so total = TARGET_FREE_COUNT.
    if is_refresh:
        high_quality = [e for e in deduped if e.get("final_score", 0) >= REFRESH_SCORE_THRESHOLD]
        logger.info(
            f"🔄 FREE REFRESH: {len(high_quality)} new events score≥{REFRESH_SCORE_THRESHOLD} "
            f"(out of {len(deduped)} new candidates)"
        )

        slots_needed = TARGET_FREE_COUNT - len(high_quality)
        filler_details: list = []
        if slots_needed > 0:
            exclude_slugs = {e.get("slug", "").lower() for e in high_quality}
            filler_rows = deduper.load_top_events_for_date(
                today.date(), is_pro=False, limit=slots_needed, exclude_slugs=exclude_slugs
            )
            filler_details = [_db_row_to_event_detail(row) for row in filler_rows]
            logger.info(f"📂 FREE REFRESH: filling {len(filler_details)} slots from DB")

        new_details: list = []
        if high_quality:
            logger.info("✍️ FREE REFRESH — Generating narratives for new events...")
            narratives_map = await processor.generate_secondary_narratives(high_quality, today)
            logger.info("🧠📚🌌 FREE REFRESH — Quizzes, long reads and the game, together...")
            quizzes, deep_dives, parallel = await _enrich(
                quiz_gen, processor, high_quality, narratives_map, today
            )
            new_details = await _build_event_details(
                high_quality, narratives_map, quizzes, today, scraper, is_pro=False,
                deep_dives_map=deep_dives, parallel_map=parallel,
            )

        final_events_list = new_details + filler_details
        logger.info(
            f"🏆 FREE REFRESH TOTAL: {len(new_details)} new + "
            f"{len(filler_details)} from DB = {len(final_events_list)}"
        )
        return final_events_list, {
            "free_discovered": len(all_events),
            "free_after_validation": len(validated),
            "free_new_qualified": len(high_quality),
            "free_filler_from_db": len(filler_details),
            "free_final": len(final_events_list),
        }

    # ── INITIAL MODE ──────────────────────────────────────────────
    selected = deduped[:TARGET_FREE_COUNT]

    if len(deduped) < TARGET_FREE_COUNT:
        logger.warning(
            f"⚠️ FREE: only {len(deduped)} non-dup events available "
            f"(wanted {TARGET_FREE_COUNT})"
        )

    logger.info(f"🏆 FREE TOP {len(selected)} (post-dedup, {len(deduped)} non-dup candidates):")
    for i, ev in enumerate(selected):
        logger.info(f"  {i+1}. [{ev['year']}] {ev['text'][:80]} → {ev['final_score']}")

    logger.info("✍️ FREE — Generating narratives...")
    narratives_map = await processor.generate_secondary_narratives(selected, today)

    logger.info("🧠📚🌌 FREE — Quizzes, long reads and the game, together...")
    quizzes, deep_dives, parallel = await _enrich(
        quiz_gen, processor, selected, narratives_map, today
    )

    final_events_list = await _build_event_details(
        selected, narratives_map, quizzes, today, scraper, is_pro=False,
        deep_dives_map=deep_dives, parallel_map=parallel,
    )

    return final_events_list, {
        "free_discovered": len(all_events),
        "free_after_validation": len(validated),
        "free_after_dedup": len(deduped),
        "free_final": len(final_events_list),
        "free_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# PRO PIPELINE
# Target: 3 events (1 personalities + 1 media + 1 sport), all is_pro=True
# ══════════════════════════════════════════════════════════════════
async def run_pro_pipeline(
    today: datetime,
    processor: AIProcessor,
    scraper: WikiScraper,
    ranker: ScoringEngine,
    quiz_gen: QuizGenerator,
    deduper: EventDeduplicator,
    date_validator: WikiDateValidator,
    is_refresh: bool = False,
) -> tuple:
    mode = "REFRESH" if is_refresh else "INITIAL"
    logger.info(f"💎 PRO [{mode}] — Discovering personalities/media/sport for {today.strftime('%B %d')}...")
    pro_known_slugs = deduper.existing_slugs_for_date(today)
    pro_candidates = await processor.discover_pro_events(today, exclude_slugs=pro_known_slugs)
    pro_candidates = _dedupe_by_slug(pro_candidates, "PRO-discover")
    logger.info(f"📋 PRO got {len(pro_candidates)} unique candidates")

    if not pro_candidates:
        logger.warning("⚠️ PRO: no candidates")
        return [], {"pro_discovered": 0}

    original_pool = list(pro_candidates)

    logger.info("🗓️ PRO — Validating dates against Wikipedia...")
    validated = await date_validator.validate_events(pro_candidates, today, tier="PRO")

    # PRO needs at least 1 per category — backfill per category if validator was too strict
    pro_cats = ["personalities", "media", "sport"]
    by_cat_validated = {c: [] for c in pro_cats}
    for ev in validated:
        cat = ev.get("category", "").lower()
        if cat in by_cat_validated:
            by_cat_validated[cat].append(ev)

    by_cat_original = {c: [] for c in pro_cats}
    for ev in original_pool:
        cat = ev.get("category", "").lower()
        if cat in by_cat_original:
            by_cat_original[cat].append(ev)

    # Backfill missing categories
    final_pool = []
    for cat in pro_cats:
        cat_validated = by_cat_validated[cat]
        if not cat_validated and by_cat_original[cat]:
            backfill = sorted(
                by_cat_original[cat],
                key=lambda x: x.get("ai_score", 0),
                reverse=True,
            )[:3]
            logger.warning(
                f"⚠️ PRO [{cat}]: validator rejected all — backfilling {len(backfill)} candidates"
            )
            final_pool.extend(backfill)
        else:
            final_pool.extend(cat_validated)

    if not final_pool:
        logger.warning("⚠️ PRO: no candidates available even after backfill")
        return [], {"pro_discovered": 0, "pro_after_validation": 0}

    logger.info("🔍 PRO — Filtering candidates against existing DB events...")
    pro_clean = deduper.filter_duplicates(final_pool, tier="PRO")
    pro_clean = _dedupe_by_slug(pro_clean, "PRO-clean")

    if not pro_clean and not is_refresh:
        logger.warning("⚠️ PRO: all candidates were duplicates")
        return [], {
            "pro_discovered": len(pro_candidates),
            "pro_after_dedup": 0,
            "pro_final": 0,
        }

    logger.info("🔬 PRO — Selecting best event per category...")
    pro_selected = await processor.deep_rank_pro_per_category(pro_clean, today) if pro_clean else []
    pro_selected = _dedupe_by_slug(pro_selected, "PRO-rank")

    if not pro_selected and not is_refresh:
        logger.warning("⚠️ PRO: no events selected by ranker")
        return [], {
            "pro_discovered": len(pro_candidates),
            "pro_after_dedup": len(pro_clean),
            "pro_final": 0,
        }

    # Compute pageviews + final_score for all selected events
    if pro_selected:
        logger.info("📊 PRO — Fetching pageviews...")
        view_tasks = [scraper.fetch_page_views(item.get("slug", "")) for item in pro_selected]
        views = await asyncio.gather(*view_tasks)

        for idx, item in enumerate(pro_selected):
            item["views"] = views[idx] if isinstance(views[idx], int) else 0
            item["final_score"] = ranker.calculate_final_score(
                ai_score=item.get("deep_score", item.get("ai_score", 50)),
                views=item["views"],
                category=item.get("category", "personalities"),
                year=item.get("year", 0),
            )
            item["is_pro"] = True

    # ── REFRESH MODE ──────────────────────────────────────────────
    # Keep only new events that score >= threshold.
    # Fill remaining slots with best existing DB events so total = TARGET_PRO_COUNT.
    if is_refresh:
        high_quality = [e for e in pro_selected if e.get("final_score", 0) >= REFRESH_SCORE_THRESHOLD]
        logger.info(
            f"🔄 PRO REFRESH: {len(high_quality)} new events score≥{REFRESH_SCORE_THRESHOLD} "
            f"(out of {len(pro_selected)} new candidates)"
        )

        slots_needed = TARGET_PRO_COUNT - len(high_quality)
        filler_details: list = []
        if slots_needed > 0:
            exclude_slugs = {e.get("slug", "").lower() for e in high_quality}
            filler_rows = deduper.load_top_events_for_date(
                today.date(), is_pro=True, limit=slots_needed, exclude_slugs=exclude_slugs
            )
            filler_details = [_db_row_to_event_detail(row) for row in filler_rows]
            logger.info(f"📂 PRO REFRESH: filling {len(filler_details)} slots from DB")

        new_details: list = []
        if high_quality:
            logger.info("✍️ PRO REFRESH — Generating narratives for new events...")
            narratives_map = await processor.generate_secondary_narratives(high_quality, today)
            logger.info("🧠📚🌌 PRO REFRESH — Quizzes, long reads and the game, together...")
            quizzes, deep_dives, parallel = await _enrich(
                quiz_gen, processor, high_quality, narratives_map, today
            )
            new_details = await _build_event_details(
                high_quality, narratives_map, quizzes, today, scraper, is_pro=True,
                deep_dives_map=deep_dives, parallel_map=parallel,
            )

        final_pro_list = new_details + filler_details
        logger.info(
            f"🏆 PRO REFRESH TOTAL: {len(new_details)} new + "
            f"{len(filler_details)} from DB = {len(final_pro_list)}"
        )
        return final_pro_list, {
            "pro_discovered": len(pro_candidates),
            "pro_after_validation": len(validated),
            "pro_new_qualified": len(high_quality),
            "pro_filler_from_db": len(filler_details),
            "pro_final": len(final_pro_list),
        }

    # ── INITIAL MODE ──────────────────────────────────────────────
    logger.info(f"🏆 PRO SELECTED {len(pro_selected)} events:")
    for i, ev in enumerate(pro_selected):
        logger.info(
            f"  {i+1}. [{ev['category']}] [{ev['year']}] "
            f"{ev['text'][:70]} → {ev['final_score']}"
        )

    logger.info("✍️ PRO — Generating narratives...")
    narratives_map = await processor.generate_secondary_narratives(pro_selected, today)

    logger.info("🧠📚🌌 PRO — Quizzes, long reads and the game, together...")
    quizzes, deep_dives, parallel = await _enrich(
        quiz_gen, processor, pro_selected, narratives_map, today
    )

    final_pro_list = await _build_event_details(
        pro_selected, narratives_map, quizzes, today, scraper, is_pro=True,
        deep_dives_map=deep_dives, parallel_map=parallel,
    )

    return final_pro_list, {
        "pro_discovered": len(pro_candidates),
        "pro_after_dedup": len(pro_clean),
        "pro_selected": len(pro_selected),
        "pro_final": len(final_pro_list),
        "pro_quizzes_ok": sum(1 for q in quizzes if q is not None),
    }


# ══════════════════════════════════════════════════════════════════
# SHARED — Build EventDetail
# ══════════════════════════════════════════════════════════════════
async def _build_event_details(
    selected_items: list,
    narratives_map: dict,
    quizzes: list,
    today: datetime,
    scraper: WikiScraper,
    is_pro: bool,
    deep_dives_map: dict | None = None,
    parallel_map: dict | None = None,
) -> list:
    tier_tag = "pro" if is_pro else "free"
    final_list = []

    for idx, item in enumerate(selected_items):
        slug = item.get("slug", "")
        year = item.get("year", 0)
        slug_display = slug.replace("_", " ")

        logger.info(f"🖼️ [{tier_tag.upper()}] Fetching images for: {slug_display}")

        hero_url = await scraper.fetch_pro_image(slug_display)
        wiki_urls = await scraper.fetch_gallery_urls(slug, limit=3)

        combined_sources = []
        seen_urls: set = set()

        if hero_url:
            combined_sources.append(hero_url)
            seen_urls.add(hero_url)

        for w_url in wiki_urls:
            if len(combined_sources) >= 3:
                break
            if w_url not in seen_urls and ".gif" not in w_url.lower():
                combined_sources.append(w_url)
                seen_urls.add(w_url)

        gallery = []
        for i, url in enumerate(combined_sources):
            if _is_wikipedia_url(url):
                gallery.append(url)
                logger.info(f"  → Wikipedia URL kept directly: {url[:70]}")
            else:
                img_url = await safe_upload(
                    scraper, url, f"{tier_tag}_ev_{year}_{slug[:20]}_{i}"
                )
                if img_url:
                    gallery.append(img_url)
                    logger.info(f"  → Uploaded via Cloudinary: {img_url[:70]}")
                await asyncio.sleep(0.5)

        try:
            ev_date = today.date().replace(year=year) if year > 0 else today.date()
        except ValueError:
            ev_date = today.date()

        narrative_data = narratives_map.get(f"EVENT_{idx}", {})
        titles = item.get("titles", {lang: "Historical Event" for lang in ["en", "ro", "es", "de", "fr"]})
        event_quiz = quizzes[idx] if idx < len(quizzes) else None

        # Per-language notification hooks stashed on the item by the narrative generator.
        # Missing → empty string, so the app falls back to its client-side template.
        notifs = item.get("notifications", {})
        notif_titles = {lang: (notifs.get(lang) or {}).get("title", "") for lang in ["en", "ro", "es", "de", "fr"]}
        notif_bodies = {lang: (notifs.get(lang) or {}).get("body", "") for lang in ["en", "ro", "es", "de", "fr"]}

        try:
            category_enum = EventCategory(item["category"].lower())
        except ValueError:
            logger.error(f"⚠️ Unknown category '{item['category']}' — defaulting to culture_arts")
            category_enum = EventCategory.CULTURE_ARTS

        final_list.append(
            EventDetail(
                category=category_enum,
                year=year,
                event_date=ev_date,
                source_url=f"https://en.wikipedia.org/wiki/{slug}",
                title_translations=Translations(**titles),
                narrative_translations=Translations(**narrative_data),
                notification_title_translations=Translations(**notif_titles),
                notification_body_translations=Translations(**notif_bodies),
                impact_score=float(item["final_score"]),
                page_views_30d=item["views"],
                gallery=gallery,
                quiz=event_quiz,
                is_pro=is_pro,
                location=item.get("location"),
                deep_dive=_deep_dive_for_index(deep_dives_map, idx),
                parallel=_parallel_for_index(parallel_map, idx),
            )
        )

    return final_list


def _deep_dive_for_index(deep_dives_map: dict | None, idx: int) -> DeepDiveTranslations | None:
    """Pick this event's long read out of the generator's output.

    Events whose generation failed are absent from the map, which is deliberate: the
    app shows no teaser at all rather than promising chapters that do not exist.
    """
    if not deep_dives_map:
        return None
    entry = deep_dives_map.get(f"EVENT_{idx}")
    if not entry:
        return None
    return DeepDiveTranslations(**{
        lang: DeepDive(
            chapters=[DeepDiveChapter(**c) for c in payload["chapters"]],
            highlights=[DeepDiveHighlight(**h) for h in payload.get("highlights", [])],
            timeline=payload["timeline"],
            misconception=payload["misconception"],
            aftermath=payload["aftermath"],
            sources=payload["sources"],
            teaser=payload["teaser"],
            word_count=payload["word_count"],
        )
        for lang, payload in entry.items()
    })


# ══════════════════════════════════════════════════════════════════
# CORE — run FREE + PRO for a single date and send to Java
# ══════════════════════════════════════════════════════════════════
async def run_pipeline_for_date(
    target_date: datetime,
    scraper: WikiScraper,
    processor: AIProcessor,
    quiz_gen: QuizGenerator,
    ranker: ScoringEngine,
    run_social: bool = False,
    refresh_mode: bool = False,
) -> bool:
    """
    Run the full FREE + PRO pipeline for `target_date`.
    refresh_mode=True  → date already has events; keep only new events scoring ≥60,
                         fill remaining slots with best existing DB events.
    refresh_mode=False → fresh date; generate full 6 FREE + 4 PRO.
    Creates fresh deduper/validator so each date gets a clean DB snapshot.
    Returns True if payload was sent successfully.
    """
    deduper = EventDeduplicator(similarity_threshold=0.85)
    date_validator = WikiDateValidator(fuzzy_slug_threshold=0.90)

    mode_label = "REFRESH" if refresh_mode else f"INITIAL ({TARGET_FREE_COUNT}+{TARGET_PRO_COUNT})"

    try:
        logger.info(f"⚡ Launching FREE + PRO pipelines in parallel — {mode_label}...")
        (free_events, free_meta), (pro_events, pro_meta) = await asyncio.gather(
            run_free_pipeline(
                target_date, processor, scraper, ranker, quiz_gen, deduper, date_validator,
                is_refresh=refresh_mode,
            ),
            run_pro_pipeline(
                target_date, processor, scraper, ranker, quiz_gen, deduper, date_validator,
                is_refresh=refresh_mode,
            ),
        )

        all_events = free_events + pro_events

        if not all_events:
            logger.error("❌ No events generated — aborting payload send")
            return False

        # Final cross-tier dedup — catches exact URL matches AND same-year
        # fuzzy title matches (same historical moment, different Wikipedia articles).
        # FREE events come first in all_events so PRO duplicates are dropped.
        all_events = deduper.filter_final_cross_tier(all_events)

        # Sort: FREE events first (by impact_score desc), then PRO (by impact_score desc).
        # Frontend uses events[0] = highest-impact FREE = Main/Home hero.
        free_sorted = sorted(
            [e for e in all_events if not e.is_pro],
            key=lambda e: e.impact_score,
            reverse=True,
        )
        pro_sorted = sorted(
            [e for e in all_events if e.is_pro],
            key=lambda e: e.impact_score,
            reverse=True,
        )
        all_events = free_sorted + pro_sorted

        combined_metadata = {
            **free_meta,
            **pro_meta,
            "target_date": str(target_date.date()),
            "pipeline": "ai_driven_v9_with_backfill",
            "total_events": len(all_events),
        }

        payload = DailyPayload(
            date_processed=target_date.date(),
            events=all_events,
            metadata=combined_metadata,
        )

        free_count = sum(1 for e in all_events if not e.is_pro)
        pro_count = sum(1 for e in all_events if e.is_pro)

        logger.info("━" * 60)
        logger.info(f"📊 FINAL PAYLOAD [{target_date.date()}]: {len(all_events)} events total")
        logger.info(f"   → FREE: {free_count} (target {TARGET_FREE_COUNT}) | "
                    f"PRO: {pro_count} (target {TARGET_PRO_COUNT})")
        logger.info("━" * 60)
        for i, ev in enumerate(all_events):
            tier = "💎 PRO" if ev.is_pro else "🆓 FREE"
            title = ev.title_translations.en[:55]
            logger.info(
                f"  {i+1}. {tier} [{ev.category.value:20s}] | "
                f"{ev.year} | {title}"
            )
        logger.info("━" * 60)

        if free_count < TARGET_FREE_COUNT:
            logger.warning(f"⚠️ FREE count below target: {free_count}/{TARGET_FREE_COUNT}")
        if pro_count < TARGET_PRO_COUNT:
            logger.warning(f"⚠️ PRO count below target: {pro_count}/{TARGET_PRO_COUNT}")

        await send_to_java(payload)

        if run_social:
            logger.info("📱 Running Social Media Agent (FREE events only)...")
            try:
                free_only = [e for e in all_events if not e.is_pro]
                if free_only:
                    social_agent = SocialMediaAgent()
                    await social_agent.generate_and_post(free_only, target_date)
                else:
                    logger.warning("⚠️ No FREE events — skipping social agent")
            except Exception as e:
                logger.error(f"⚠️ Social Media Agent failed (non-critical): {e}")

        return True

    except Exception as e:
        logger.error(f"🚨 Pipeline Crash for {target_date.date()}: {e}", exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════
# BACKFILL — long reads for the archive
# The archive predates the feature. This runs as its own job (never inside the
# daily cron) and writes only the two deep-dive columns, so a partial or failed
# backfill can never damage published content.
#
#   python main.py --backfill-deepdive --from 2026-01-01 --to 2026-08-26 --limit 20
# ══════════════════════════════════════════════════════════════════
async def backfill_deep_dives(from_date: str, to_date: str, limit: int) -> None:
    from datetime import date as _date

    deduper = EventDeduplicator()
    processor = AIProcessor()
    generator = DeepDiveGenerator(processor)

    rows = deduper.load_events_missing_deep_dive(
        _date.fromisoformat(from_date), _date.fromisoformat(to_date), limit
    )
    if not rows:
        logger.info("✅ Nothing to backfill in that range.")
        return

    ok = 0
    for row in rows:
        slug = str(row.get("source_url") or "").split("/wiki/")[-1]
        event_date = row.get("event_date")
        title = str(row.get("title_en") or "")
        narrative = str(row.get("narrative_en") or "")

        # `_generate_english` expects the discovery-shaped dict. The stored title plus
        # the published narrative is richer context than the original one-line text was.
        item = {
            "year": getattr(event_date, "year", 0),
            "text": f"{title}. {narrative[:600]}",
            "slug": slug,
            "location": row.get("location"),
        }

        logger.info(f"📚 Backfilling [{row['id']}] {title[:60]}")
        result = await generator.generate_deep_dives(
            [item], {"EVENT_0": {"en": narrative}}, datetime.combine(event_date, datetime.min.time())
        )
        entry = result.get("EVENT_0")
        if not entry:
            logger.warning(f"⚠️ Backfill failed for event {row['id']} — leaving it empty")
            continue

        deep_dive = _deep_dive_for_index(result, 0)
        if deduper.write_deep_dive(
            row["id"],
            _serialize_deep_dive(deep_dive),
            _serialize_deep_dive_teaser(deep_dive),
        ):
            ok += 1
            logger.info(f"✅ Backfilled event {row['id']}")

        # The daily run shares this provider quota. Pace the backfill so a large
        # range can never starve the job that actually has a deadline.
        await asyncio.sleep(3)

    logger.info(f"🏁 Backfill complete: {ok}/{len(rows)} events now have a long read")
    logger.info(
        "ℹ️ Backend caches daily content for 24h — backfilled days may serve the old "
        "payload until the entry expires."
    )


# ══════════════════════════════════════════════════════════════════
# MAIN
# Auto-detects which days need processing:
#   - If today AND tomorrow already have events → only process day+2
#   - Otherwise → process all 3 days (each in REFRESH or INITIAL as needed)
# DAY_OFFSET env var overrides auto-detection (0/1/2 = specific day only).
# SKIP_SOCIAL suppresses the social post, which otherwise fires on offset 0 and
# makes re-running today to pick up a prompt change publish it a second time.
# ══════════════════════════════════════════════════════════════════
async def main():
    import os

    today = datetime.now()
    day_labels = {0: "TODAY", 1: "TOMORROW", 2: "DAY AFTER"}

    # Any non-empty value turns off the social post for this run.
    skip_social = bool(os.environ.get("SKIP_SOCIAL"))
    if skip_social:
        logger.info("🔇 SKIP_SOCIAL set — the social agent will not run")

    day_offset_env = os.environ.get("DAY_OFFSET")
    if day_offset_env is not None:
        # Manual override via env var
        try:
            offset = int(day_offset_env)
            dates_to_process = [(offset, today + timedelta(days=offset))]
            logger.info(f"🚀 Pipeline — manual override DAY_OFFSET={offset}")
        except ValueError:
            logger.warning(f"⚠️ Invalid DAY_OFFSET='{day_offset_env}' — using auto-detect")
            day_offset_env = None

    if day_offset_env is None:
        # Auto-detect which of the next 3 days need processing, deciding
        # PER DAY (not all-or-nothing) so a hole on a single middle day
        # (e.g. a missed run left tomorrow empty) always gets backfilled.
        #   - Day+2 (farthest) is ALWAYS processed — refresh if it already has
        #     events, initial if not — so the rolling window keeps advancing.
        #   - Today / tomorrow are processed ONLY if they have NO events yet.
        #     Skipping already-covered earlier days avoids redundant work and,
        #     crucially, avoids re-firing the social agent for today every run.
        checker = EventDeduplicator()
        dates_to_process = []
        for i in range(3):
            d = today + timedelta(days=i)
            covered = checker.has_events_for_date(d.date())
            if i == 2 or not covered:
                dates_to_process.append((i, d))
            else:
                logger.info(
                    f"↩️ {day_labels[i]} ({d.date()}) already has events → skipping"
                )
        logger.info(
            "🚀 Pipeline — auto-detect will process: "
            + ", ".join(f"{day_labels[i]}({d.date()})" for i, d in dates_to_process)
        )

    scraper = WikiScraper()
    processor = AIProcessor()
    quiz_gen = QuizGenerator()
    ranker = ScoringEngine()

    for i, target_date in dates_to_process:
        label = day_labels.get(i, f"DAY +{i}")
        logger.info(f"\n{'═' * 60}")
        logger.info(f"📅 Processing {label}: {target_date.date()}")
        logger.info(f"{'═' * 60}")

        checker = EventDeduplicator()
        already_populated = checker.has_events_for_date(target_date.date())
        mode = "REFRESH" if already_populated else "INITIAL"
        logger.info(f"📋 {label} ({target_date.date()}) → {mode} mode")

        await run_pipeline_for_date(
            target_date=target_date,
            scraper=scraper,
            processor=processor,
            quiz_gen=quiz_gen,
            ranker=ranker,
            # Offset 0 is the only day that posts to social, and re-running today to
            # pick up a prompt change would post it a second time. SKIP_SOCIAL exists
            # so regenerating a day is never coupled to publishing it again.
            run_social=(i == 0 and not skip_social),
            refresh_mode=already_populated,
        )

    logger.info("\n✅ Pipeline complete.")


def _cli_arg(name: str, default: str | None = None) -> str | None:
    """Read `--name value` from argv. Kept tiny on purpose — the pipeline is a cron
    job configured by env vars; the backfill is the only thing with real arguments."""
    import sys
    if name in sys.argv:
        idx = sys.argv.index(name)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


def _print_cost_report() -> None:
    """What the run actually cost, per stage, most expensive first.

    Printed unconditionally: the bill is the one number nobody looks at until it
    is too big, and by then there is no record of which stage grew."""
    try:
        from core.llm import cost_report
        logger.info(cost_report())
    except Exception as e:  # never let accounting sink a finished run
        logger.warning(f"cost report unavailable: {e!r}")


if __name__ == "__main__":
    import sys
    import atexit

    atexit.register(_print_cost_report)

    if "--backfill-deepdive" in sys.argv:
        _from = _cli_arg("--from")
        _to = _cli_arg("--to")
        if not _from or not _to:
            logger.error("🚨 --backfill-deepdive needs --from YYYY-MM-DD --to YYYY-MM-DD")
            sys.exit(1)
        asyncio.run(backfill_deep_dives(_from, _to, int(_cli_arg("--limit", "20"))))
    else:
        asyncio.run(main())