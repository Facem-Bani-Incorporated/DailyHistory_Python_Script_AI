import json
from groq import Groq
from core.config import config


class AIProcessor:
    def __init__(self, model: str = config.AI_MODEL):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = model

    async def batch_score_and_translate_titles(self, candidates: list):
        """Scorează relevanța și traduce titlurile pentru evenimentele secundare"""
        candidates_text = "\n".join(
            [f"ID {i}: ({item['year']}) {item['text'][:150]}" for i, item in enumerate(candidates)])

        prompt = f"""
        Act as a historian and polyglot. 
        1. Rate each event 0-100 based on global impact.
        2. Translate the titles into EN, RO, ES, DE, FR.
        Events:
        {candidates_text}

        RETURN JSON ONLY: 
        {{ "results": {{ "ID_0": {{ "score": 85, "titles": {{ "en": "..", "ro": "..", "es": "..", "de": "..", "fr": ".." }} }} }} }}
        """
        completion = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)

    async def generate_multilingual_main_event(self, text: str, year: int):
        """Generează titlul și narațiunea full în 5 limbi pentru evenimentul principal"""
        prompt = f"""
        Analyze event: {text} ({year}).
        Generate a 400-word narrative and a interesting title.
        Translate both into English, Romanian, Spanish, German, French.
        RETURN JSON ONLY:
        {{
            "titles": {{ "en": "..", "ro": "..", "es": "..", "de": "..", "fr": ".." }},
            "narratives": {{ "en": "..", "ro": "..", "es": "..", "de": "..", "fr": ".." }}
        }}
        """
        completion = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)