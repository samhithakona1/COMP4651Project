# Origin: original
"""Skill taxonomy (src/spark/taxonomy.json): one combined case-insensitive pattern per skill."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from src.common import config

CATEGORIES = ("Languages", "Cloud/DevOps", "AI/ML", "Data", "Frameworks/Web")
# CLAUDE.md §1 Q2: the GenAI frameworks whose posting share and premium we track.
GENAI_FRAMEWORKS = ("vLLM", "LangChain", "LlamaIndex", "PyTorch", "Hugging Face", "Triton")
MODERN_SYSTEMS = ("Rust", "Go", "Kubernetes")
LEGACY_STACKS = ("PHP", "Ruby on Rails", "Java Spring")


@dataclass(frozen=True)
class Skill:
    name: str
    category: str
    emerging_ai: bool
    patterns: tuple[str, ...]

    @property
    def pattern(self) -> str:
        """Single pattern valid in both Java and Python regex."""
        return "(?i)(?:" + "|".join(self.patterns) + ")"


@lru_cache(maxsize=4)
def load_taxonomy(path: str | Path | None = None) -> tuple[Skill, ...]:
    raw = json.loads(Path(path or config.TAXONOMY_PATH).read_text(encoding="utf-8"))
    skills = []
    for name, spec in raw.items():
        if spec["category"] not in CATEGORIES:
            raise ValueError(f"{name}: unknown category {spec['category']}")
        skills.append(Skill(name, spec["category"], bool(spec.get("emerging_ai", False)),
                            tuple(spec["patterns"])))
    return tuple(skills)


@lru_cache(maxsize=4)
def compiled(path: str | Path | None = None) -> tuple[tuple[str, re.Pattern], ...]:
    return tuple((s.name, re.compile(s.pattern, re.ASCII)) for s in load_taxonomy(path))


def extract_skills(text: str | None, path: str | Path | None = None) -> list[str]:
    """Reference (Python) extractor: distinct skill names, in taxonomy order."""
    if not text:
        return []
    return [name for name, pat in compiled(path) if pat.search(text)]
