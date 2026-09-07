"""Long-form PRO narratives ("The Long Read").

The free narrative sells the day; this sells the subscription. Every event gets a
second, longer piece — chapters, a timeline, the myth-correction, the aftermath and
real sources — that only PRO users ever receive in full.

Two rules shape the whole module:

1. **It must not repeat the free narrative.** A subscriber reading both should feel
   they got a second article, not a padded version of the first. `_overlap_ratio`
   enforces that mechanically; the prompt asks for it explicitly.

2. **English is generated, the rest is translated.** Generating 2,000 words natively
   in five languages would multiply the daily token bill by five and let the facts
   drift between languages. Instead English gets reasoning tokens (this is dense
   factual writing, unlike the creative short narrative) and the other four are
   translated from it — the same trade `AIProcessor._patch_from_english` already makes.
"""

import asyncio
import re

from core.llm import budget_allows
from core.text import strip_prose_dashes
from core.logger import setup_logger

logger = setup_logger("DeepDive")

LANGUAGES = ["en", "ro", "es", "de", "fr"]
TRANSLATION_LANGS = ["ro", "es", "de", "fr"]

LANG_NAMES = {
    "en": "English",
    "ro": "Romanian",
    "es": "Spanish",
    "de": "German",
    "fr": "French",
}

# ── Length ─────────────────────────────────────────────────────────────
# `_word_count` totals the chapters plus the misconception and aftermath sections, and
# the app divides exactly that by 200 to print the reading time. The long read is sold
# as a 7 to 8 minute read, so it has to land between 1300 and 1700 words, which leaves
# the chapters themselves carrying roughly 220 fewer.
#
# The floor was 700 until Sept 2026 and almost nothing was ever retried for length: the
# 09-07 run shipped long reads of 754, 754, 1018, 1037 and 1364 words, a 4 to 7 minute
# read against a subscription sold on 7 to 8. It now sits just under the ask, so a short
# piece costs a retry instead of passing silently.
#
# Being under the floor still never loses the piece. An earlier, stricter bar discarded
# whole finished articles for falling a paragraph short (three at 887, 909 and 981 words
# on 2026-09-02), and losing the long read costs the event far more than its length
# does. `_generate_english` keeps the longest of the failed attempts and ships it, so
# the floor buys retries without ever being able to throw the work away.
MIN_WORDS = 1300          # under this, retry for length — but never discard
MAX_WORDS = 1900          # over this it is padding, not prose → retry

# What the prompt asks of the chapters alone. The top of the band sits below MAX_WORDS
# by about what the misconception and aftermath sections add.
CHAPTER_WORDS_MIN = 1200
CHAPTER_WORDS_MAX = 1450

# Marks the one validation failure that is a matter of degree rather than of kind. A
# short article is still an article; a missing misconception or an invented URL is not.
TOO_SHORT = "Too short"
MIN_CHAPTERS = 4
MAX_CHAPTERS = 7
MIN_CHAPTER_WORDS = 80
# Points of interest shown above the article. Enough to be worth a glance, few enough
# that they stay points rather than becoming a second article.
MIN_HIGHLIGHTS = 3
MAX_HIGHLIGHTS = 5
MIN_SOURCES = 3
TEASER_WORDS = 70         # opening words shipped to free users as the pitch
MAX_OVERLAP = 0.12        # 8-gram overlap with the free narrative

BAD_MARKERS = [
    "narrative pending", "content pending", "error generating",
    "i apologize", "i'm sorry", "as an ai", "i cannot",
    "in this article", "in this chapter", "as mentioned above",
]


class DeepDiveGenerator:
    """Generates the PRO long read for a batch of events.

    Borrows the processor's client, model and `_safe_ai_call` rather than opening a
    second connection — provider config lives in exactly one place.
    """

    def __init__(self, processor):
        self.processor = processor
        self.thinking_budget = processor.thinking_budget

    # ══════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ══════════════════════════════════════════════════════════════════
    async def generate_deep_dives(
        self, top_events: list, narratives_map: dict, target_date
    ) -> dict:
        """Return {"EVENT_0": {"en": {...}, "ro": {...}, ...}, ...}.

        A failed event yields no key at all, so `_build_event_details` simply attaches
        nothing and the app shows no teaser. Partial output is never shipped.
        """
        date_str = target_date.strftime("%B %d")

        async def process_single(idx, item):
            # The long read is the richer half of an event, not the half that makes it
            # publishable. When the run is near its spend cap it is the first thing to
            # go, so the remaining dates still get narratives and translations.
            if not budget_allows(optional=True):
                logger.warning(
                    f"💸 DeepDive {idx} skipped — spend cap reached for optional stages"
                )
                return f"EVENT_{idx}", None
            short_narrative = narratives_map.get(f"EVENT_{idx}", {}).get("en", "")
            english = await self._generate_english(idx, item, date_str, short_narrative)
            if not english:
                logger.error(f"🚨 DeepDive {idx} — English failed, skipping this event")
                return f"EVENT_{idx}", None

            translations = await asyncio.gather(*[
                self._translate(idx, english, lang) for lang in TRANSLATION_LANGS
            ])

            out = {"en": english}
            for lang, payload in zip(TRANSLATION_LANGS, translations):
                # A failed translation falls back to English rather than leaving the
                # language empty — a readable English long read beats no long read.
                out[lang] = payload if payload else english
            return f"EVENT_{idx}", out

        results = dict(await asyncio.gather(*[
            process_single(i, item) for i, item in enumerate(top_events)
        ]))

        ok = sum(1 for v in results.values() if v)
        logger.info(f"📚 Deep dives: {ok}/{len(top_events)} events complete")
        return {k: v for k, v in results.items() if v}

    # ══════════════════════════════════════════════════════════════════
    # ENGLISH GENERATION
    # ══════════════════════════════════════════════════════════════════
    async def _generate_english(
        self, idx: int, item: dict, date_str: str, short_narrative: str
    ) -> dict | None:
        year = item.get("year", "")
        text = item.get("text", "")
        slug = item.get("slug", "")
        location = item.get("location") or "the location"

        best: dict | None = None
        for attempt in range(1, 4):
            prompt = self._build_prompt(
                year, text, slug, location, date_str, short_narrative
            )

            res = await self.processor._safe_ai_call(
                prompt,
                f"DeepDive {idx}:en (attempt {attempt})",
                {"chapters": []},
                temperature=0.7,
                max_tokens=8192,
                # The free article runs at thinking_budget=0; this one is longer and
                # denser — sequencing, causation, real sources across seven chapters —
                # and reasoning measurably reduces invented detail at that length.
                thinking_budget=self.thinking_budget,
            )

            payload = self._normalize(res)
            is_valid, reason = self._validate(payload, short_narrative, attempt)
            if is_valid:
                logger.info(
                    f"✅ DeepDive {idx}:en — {payload['word_count']} words, "
                    f"{len(payload['chapters'])} chapters (attempt {attempt})"
                )
                return payload

            # Length is the one failure worth keeping the loser of. Everything else the
            # validator catches — no misconception, a fabricated URL, a placeholder
            # phrase — makes the article wrong, and a wrong article should not ship. A
            # short one is merely shorter than we asked for, and on 2026-09-02 and again
            # on 09-05 that distinction cost real events their long read outright: three
            # complete articles thrown away at 887, 909 and 981 words, and another at 762
            # after three full generations. Keep the longest and ship it if nothing
            # clears the bar, so the retries push for length without being able to lose
            # the piece.
            if reason.startswith(TOO_SHORT) and (
                best is None or payload["word_count"] > best["word_count"]
            ):
                best = payload

            logger.warning(f"⚠️ DeepDive {idx}:en attempt {attempt}: {reason}")

        if best:
            logger.info(
                f"✅ DeepDive {idx}:en — {best['word_count']} words, "
                f"{len(best['chapters'])} chapters (short of {MIN_WORDS}, shipped anyway)"
            )
            return best
        return None

    def _build_prompt(
        self, year, text, slug, location, date_str, short_narrative
    ) -> str:
        # The free narrative is handed over purely as a "do not repeat this" reference.
        # Truncated because only its shape and angle matter, not its full text.
        avoid_block = ""
        if short_narrative:
            avoid_block = f"""
ALREADY PUBLISHED — DO NOT REPEAT THIS PIECE:
\"\"\"
{short_narrative[:1400]}
\"\"\"
The article above is what every reader already got for free. Yours is the second,
longer piece for paying subscribers. Open on a DIFFERENT hook. Take a DIFFERENT angle.
Do not reuse its opening image, its closing line, or its best fact. If the free piece
covered the moment itself, you cover the machinery behind it and what came after.
A subscriber reading both must feel they received two different articles.
"""

        return f"""
You are writing the long-form piece for subscribers of a history app — the kind of
article someone reads to the end on a Sunday morning and then tells someone about.
ONE event, {CHAPTER_WORDS_MIN} to {CHAPTER_WORDS_MAX} words across the chapters. With the
misconception and aftermath sections that comes to the 7 to 8 minute read the subscription
promises. Under {CHAPTER_WORDS_MIN} it is not a long read, over {CHAPTER_WORDS_MAX} it is
padding. Write in English.

EVENT: {year} — {text}
WIKIPEDIA: {slug}
DATE: {date_str}, {year}
LOCATION: {location}
{avoid_block}
WHAT TO PRODUCE:

1. CHAPTERS — {MIN_CHAPTERS} to {MAX_CHAPTERS} of them, each with a real title and at least
   {MIN_CHAPTER_WORDS} words. Titles are hooks, not labels: "The Order That Was Never Sent",
   not "Background". Each chapter does distinct work — set-up, mechanism, the moment,
   the people, the consequence. Never summarize the previous chapter.

2. HIGHLIGHTS — {MIN_HIGHLIGHTS} to {MAX_HIGHLIGHTS} points of interest, read BEFORE the article as
   the reason to bother with it. Each is {{"label": "...", "text": "..."}}: a 2-5 word label
   and 25-45 words under it. One concrete fact, number, name or scene each — never a
   summary of the chapter it came from.

   THE LABELS DEPEND ON WHAT KIND OF EVENT THIS IS. Choose them yourself to fit the
   subject; do not reuse a fixed set. What a reader wants to know about a person is not
   what they want to know about a treaty. For example:

   • A PERSON (born, died, or the subject) — what they actually did, in one line. What
     they are genuinely known for, as opposed to what they are misremembered for. The
     thing about them almost nobody knows. What was left behind.
   • A RELIGIOUS OR DOCTRINAL EVENT — what was actually decided or happened on the day.
     Who was in the room and who was excluded. What changed the following morning. What
     is still being argued about because of it.
   • A BATTLE, DISASTER OR CATASTROPHE — the scale in real numbers. The decision that
     made it go this way rather than another. The detail that makes it human. Who paid.
   • A TREATY, LAW OR FOUNDING — what problem it was supposed to solve. What it actually
     did. Who won and who lost, named. Whether it held.
   • A DISCOVERY, INVENTION OR FIRST — what existed before it and did not after. How it
     actually worked, in plain words. Who else nearly got there first.

   Those are illustrations, not a menu. A coronation, a premiere, a heist and an
   expedition each want their own labels. Write the labels this event deserves.

3. TIMELINE — 5 to 12 entries, each "MARKER — what happened". The marker is a real time,
   date or year ("14:32", "3 March 1848", "Spring 1919"). Tight, factual, sequential.
   This is the spine of the event, not a repeat of the chapters.

4. MISCONCEPTION — 80-150 words on what most people get wrong about this event. The
   popular version, then what actually happened, and why the wrong version stuck.
   If there is genuinely no popular misconception, write instead about the detail that
   is consistently left out of the retellings.

5. AFTERMATH — 3 to 4 entries tracing consequences forward in time. Each begins with a
   time marker ("Within a decade", "By 1961", "Two centuries later"). Concrete effects
   on real people, institutions or places — not "it changed history".

6. SOURCES — {MIN_SOURCES} to 5 real references as "Author, Title (Year)". Books, papers,
   archives, published collections. NEVER invent a source. NEVER output a URL. If you
   are not confident a specific work exists, name the archive or the primary document
   type instead ("the Admiralty logs held at Kew").

HOW TO WRITE IT:
- Real numbers as evidence, woven into sentences. "Of the 300 who went in, 11 walked out."
- Name real people. Quote them only when you know the actual words.
- Explain mechanisms in plain language — the engineering, the law, the politics.
  The explanation should be the most satisfying part, never a chore.
- Dry, intelligent irony where the material earns it. A genuinely funny line once or
  twice in the whole piece, never forced onto tragedy.
- Vary paragraph and sentence length. If a sentence is boring, cut it.
- No headers inside chapter bodies. Paragraphs separated by blank lines.

PUNCTUATION, and this one is not negotiable:
NEVER use a dash as punctuation. No em dash, no en dash, no " - " standing in for a
comma, a colon or a full stop. If a clause needs joining, use a comma. If it is a new
thought, start a new sentence. A dash is only allowed inside a hyphenated compound
(record-breaking) or a numeric range (1914-1918).
Write to inform. Say what happened, what it meant and why it mattered, in plain
language. No scene-setting for its own sake, no lyricism, no building atmosphere. If a
sentence carries no fact the reader did not have, delete it.

BANNED PHRASES:
"it is worth noting" / "history tells us" / "changed the course of history" /
"left an indelible mark" / "without a doubt" / "subsequently" / "in conclusion" /
"serves as a reminder" / "stands as a testament" / "little did they know" /
"on this day" / "fast forward" / "needless to say" / "in this article".

Return JSON:
{{
  "chapters": [
    {{"title": "chapter title", "body": "chapter text, blank lines between paragraphs"}}
  ],
  "highlights": [
    {{"label": "Why he is remembered", "text": "25-45 words, one concrete thing"}}
  ],
  "timeline": ["14:32 — the first signal reaches Lisbon", "..."],
  "misconception": "80-150 words",
  "aftermath": ["Within a decade — ...", "..."],
  "sources": ["Author, Title (Year)", "..."]
}}
"""

    # ══════════════════════════════════════════════════════════════════
    # TRANSLATION
    # ══════════════════════════════════════════════════════════════════
    async def _translate(self, idx: int, english: dict, lang: str) -> dict | None:
        lang_full = LANG_NAMES.get(lang, lang.upper())

        # Send the structure as JSON and ask for the same structure back. Translating
        # field by field would cost four times the requests for no gain in quality.
        payload = {
            "chapters": english["chapters"],
            "highlights": english.get("highlights", []),
            "timeline": english["timeline"],
            "misconception": english["misconception"],
            "aftermath": english["aftermath"],
        }

        prompt = f"""
Translate this long-form historical article into {lang_full}.

Keep the voice: the rhythm, the short punchy sentences, the dry irony. Do not smooth it
into academic prose. If the English uses a fragment for impact, keep the fragment.
All numbers stay as digits. Proper nouns take their standard {lang_full} form.
Chapter titles stay hooks, not labels — translate their punch, not just their words.
Highlight labels stay short — 2-5 words, the same promise the English one makes.
Timeline and aftermath markers keep their format ("14:32 — ...", "By 1961 — ...").
Blank lines between paragraphs are preserved exactly.
Output only {lang_full} — no English except proper nouns.

SOURCES ARE NOT TRANSLATED — they are omitted from the input and re-attached afterwards.

ARTICLE JSON:
{payload}

Return the SAME JSON structure, with every string translated into {lang_full}:
{{
  "chapters": [{{"title": "...", "body": "..."}}],
  "timeline": ["..."],
  "misconception": "...",
  "aftermath": ["..."]
}}
"""

        res = await self.processor._safe_ai_call(
            prompt,
            f"DeepDive {idx}:{lang}",
            {"chapters": []},
            temperature=0.3,
            max_tokens=8192,
            thinking_budget=0,  # mechanical work — reasoning buys nothing
        )

        translated = self._normalize(res)
        # Sources are language-neutral bibliography; carry the English ones across.
        translated["sources"] = english["sources"]
        translated["teaser"] = self._extract_teaser(translated["chapters"])

        if translated["word_count"] < MIN_WORDS * 0.6:
            logger.warning(
                f"⚠️ DeepDive {idx}:{lang} — only {translated['word_count']} words, "
                f"falling back to English"
            )
            return None

        if not self._is_target_language(translated, lang):
            logger.warning(f"⚠️ DeepDive {idx}:{lang} — looks like English, falling back")
            return None

        logger.info(f"✅ DeepDive {idx}:{lang} — {translated['word_count']} words")
        return translated

    # ══════════════════════════════════════════════════════════════════
    # NORMALIZE / VALIDATE
    # ══════════════════════════════════════════════════════════════════
    def _normalize(self, res: dict) -> dict:
        """Coerce a raw model response into the DeepDive shape.

        The model occasionally returns a chapter as a bare string or a timeline entry
        as an object; normalising here keeps the validator and the serializer simple.
        """
        chapters = []
        for ch in (res.get("chapters") or []):
            if isinstance(ch, dict):
                title = str(ch.get("title") or "").strip()
                body = str(ch.get("body") or "").strip()
            else:
                title, body = "", str(ch).strip()
            if body:
                chapters.append({
                    "title": strip_prose_dashes(title),
                    "body": strip_prose_dashes(body),
                })

        def _str_list(key: str) -> list:
            out = []
            for entry in (res.get(key) or []):
                if isinstance(entry, dict):
                    # e.g. {"marker": "14:32", "text": "..."} → "14:32 — ..."
                    marker = str(entry.get("marker") or entry.get("year") or "").strip()
                    body = str(entry.get("text") or entry.get("event") or "").strip()
                    entry = f"{marker} — {body}" if marker and body else (body or marker)
                entry = str(entry).strip()
                if entry:
                    out.append(entry)
            return out

        highlights = []
        for h in (res.get("highlights") or []):
            if isinstance(h, dict):
                label = str(h.get("label") or h.get("title") or "").strip()
                text = str(h.get("text") or h.get("body") or h.get("value") or "").strip()
            else:
                # A bare string: keep it as the text and let the app show it unlabelled
                # rather than throwing away a good point over its packaging.
                label, text = "", str(h).strip()
            if text:
                highlights.append({
                    "label": strip_prose_dashes(label),
                    "text": strip_prose_dashes(text),
                })

        payload = {
            "chapters": chapters,
            "highlights": highlights[:MAX_HIGHLIGHTS],
            "timeline": _str_list("timeline"),
            # Timeline and aftermath are deliberately NOT cleaned: their dash is the
            # separator in "14:32 — the first signal reaches Lisbon", which the app lays
            # out around, not punctuation the model reached for.
            "misconception": strip_prose_dashes(str(res.get("misconception") or "").strip()),
            "aftermath": _str_list("aftermath"),
            "sources": _str_list("sources"),
        }
        payload["word_count"] = self._word_count(payload)
        payload["teaser"] = self._extract_teaser(chapters)
        return payload

    @staticmethod
    def _word_count(payload: dict) -> int:
        parts = [c["body"] for c in payload["chapters"]]
        parts.append(payload.get("misconception", ""))
        parts.extend(payload.get("aftermath", []))
        return sum(len(p.split()) for p in parts)

    @staticmethod
    def _extract_teaser(chapters: list) -> str:
        """First ~70 words of chapter one — the only body text a free user receives."""
        if not chapters:
            return ""
        words = chapters[0]["body"].split()
        if len(words) <= TEASER_WORDS:
            return chapters[0]["body"]
        return " ".join(words[:TEASER_WORDS]).rstrip(".,;:—-") + "…"

    def _validate(self, payload: dict, short_narrative: str, attempt: int = 3) -> tuple:
        chapters = payload["chapters"]

        # Highlights are worth one retry and no more. Rejecting a finished article
        # because its points of interest came back thin would trade the thing readers
        # came for against the thing that introduces it — and a long read that never
        # ships helps nobody. So this gates the first attempt and is advisory after.
        if len(payload.get("highlights", [])) < MIN_HIGHLIGHTS and attempt == 1:
            return False, (
                f"{len(payload.get('highlights', []))} highlights "
                f"(want {MIN_HIGHLIGHTS}-{MAX_HIGHLIGHTS})"
            )

        if not chapters:
            return False, "No chapters"
        if not (MIN_CHAPTERS <= len(chapters) <= MAX_CHAPTERS):
            return False, f"{len(chapters)} chapters (want {MIN_CHAPTERS}-{MAX_CHAPTERS})"

        for i, ch in enumerate(chapters):
            if not ch["title"]:
                return False, f"Chapter {i} has no title"
            if len(ch["body"].split()) < MIN_CHAPTER_WORDS:
                return False, f"Chapter {i} only {len(ch['body'].split())} words"

        wc = payload["word_count"]
        if wc > MAX_WORDS:
            return False, f"Too long: {wc} words (max {MAX_WORDS})"

        if len(payload["timeline"]) < 5:
            return False, f"Timeline has {len(payload['timeline'])} entries (min 5)"
        if len(payload["aftermath"]) < 3:
            return False, f"Aftermath has {len(payload['aftermath'])} entries (min 3)"
        if not payload["misconception"]:
            return False, "Missing misconception section"
        if len(payload["sources"]) < MIN_SOURCES:
            return False, f"Only {len(payload['sources'])} sources (min {MIN_SOURCES})"

        # An invented URL is worse than no source at all — it is a checkable lie.
        for src in payload["sources"]:
            if "http://" in src or "https://" in src or "www." in src:
                return False, f"Source contains a URL: {src[:60]}"

        blob = " ".join(c["body"] for c in chapters).lower()
        for marker in BAD_MARKERS:
            if marker in blob:
                return False, f"Contains placeholder/AI text: '{marker}'"

        # Deliberately the LAST check. The caller keeps a short article and ships it when
        # nothing clears the bar, so "too short" has to mean "sound in every other way" —
        # if this ran earlier, a piece could be held back for length while quietly also
        # carrying an invented URL or a placeholder phrase, and then be shipped anyway.
        if wc < MIN_WORDS:
            return False, f"{TOO_SHORT}: {wc} words (min {MIN_WORDS})"
        if short_narrative:
            overlap = self._overlap_ratio(short_narrative, blob)
            if overlap > MAX_OVERLAP:
                return False, f"Repeats the free narrative ({overlap:.0%} 8-gram overlap)"

        return True, "OK"

    @staticmethod
    def _overlap_ratio(short_text: str, long_text: str) -> float:
        """Share of the free narrative's 8-grams that reappear in the long read.

        Eight words is long enough that a match means a recycled sentence rather than
        a shared proper noun or a common phrase.
        """
        def grams(text: str) -> set:
            words = re.findall(r"[a-z0-9']+", text.lower())
            return {tuple(words[i:i + 8]) for i in range(len(words) - 7)}

        short_grams = grams(short_text)
        if not short_grams:
            return 0.0
        return len(short_grams & grams(long_text)) / len(short_grams)

    @staticmethod
    def _is_target_language(payload: dict, lang: str) -> bool:
        """Rough guard against the model echoing English back — same heuristic the
        short-narrative validator uses."""
        if lang == "en":
            return True
        blob = " ".join(c["body"] for c in payload["chapters"]).lower()
        giveaways = ["the ", "and ", "was ", "were ", "this ", "that ", "with ", "from "]
        hits = sum(1 for w in giveaways if w in blob)
        return (hits / len(giveaways)) <= 0.8
