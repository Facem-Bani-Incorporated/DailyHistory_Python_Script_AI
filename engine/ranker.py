from typing import Dict


class ScoringEngine:
    def __init__(self):
        self.keywords = {
            "war": 50, "declared": 40, "independence": 45, "signed": 30,
            "born": 10, "died": 15, "assassinated": 45, "discovered": 35,
            "founded": 30, "revolution": 50, "constitution": 40
        }

    def heuristic_score(self, item: dict) -> float:
        """Calculul clasic (viteză maximă)"""
        score = 10.0
        text = item.get("text", "").lower()

        # Puncte pentru profunzime (pagini asociate)
        pages = item.get("pages", [])
        score += len(pages) * 5

        # Puncte pentru cuvinte cheie
        for word, bonus in self.keywords.items():
            if word in text:
                score += bonus

        return min(score, 100.0)

    def hybrid_calculate(self, heuristic: float, ai_score: float) -> float:
        """
        Formula cerută: 40% Algoritmul meu, 60% AI
        """
        final = (0.4 * heuristic) + (0.6 * ai_score)
        return round(final, 2)