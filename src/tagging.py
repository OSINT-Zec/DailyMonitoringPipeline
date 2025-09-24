# src/tagging.py
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Set, Dict, List

# Heuristics
LONG_TOKEN_MIN = 6  # short tokens require word boundaries
SOFT_BOUNDARY_CHARS = (" ", "-", "/", ".", "’", "‘", "“", "”", "—", "–", "_")

__all__ = ["tag_item"]

# -----------------------
# Normalization
# -----------------------
def _norm(s: str) -> str:
    """
    Normalize across scripts/widths and lowercase. Also unify common punctuation
    so matching is stable across sources.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = (
        s.replace("—", "-")
         .replace("–", "-")
         .replace("’", "'")
         .replace("‘", "'")
         .replace("“", '"')
         .replace("”", '"')
    )
    return s.casefold().strip()


# -----------------------
# Matching helpers
# -----------------------
def _token_hit(text_norm: str, kw_norm: str) -> bool:
    """
    Safer keyword matching:

    - If the keyword includes obvious boundaries (space, hyphen, slash, etc.)
      OR the keyword is relatively long, do a substring check.
    - Otherwise require word boundaries to avoid false positives like
      'ss' inside 'class' or 'us' inside 'focus'.

    Assumes both inputs are already normalized with _norm().
    """
    if not kw_norm:
        return False

    if any(ch in kw_norm for ch in SOFT_BOUNDARY_CHARS) or len(kw_norm) >= LONG_TOKEN_MIN:
        return kw_norm in text_norm

    # Short tokens: use word boundaries
    pattern = rf"\b{re.escape(kw_norm)}\b"
    return re.search(pattern, text_norm, flags=re.IGNORECASE) is not None


def _excluded(text_norm: str, excludes_norm: Iterable[str]) -> bool:
    """Return True if any exclusion fragment is present in text_norm."""
    for ex in excludes_norm:
        if ex and ex in text_norm:
            return True
    return False


# -----------------------
# Public API
# -----------------------
def tag_item(text: str, cfg: Dict) -> List[str]:
    """
    Tag a document with topics defined in cfg['keywords'].

    Also respects per-topic negative filters in:
      cfg['filters'][<topic>]['exclude'] -> list[str]

    Returns a sorted list of topic names with at least one hit.
    """
    if not text or not isinstance(cfg, dict):
        return []

    text_norm = _norm(text)
    if not text_norm:
        return []

    keywords_cfg: Dict[str, List[str]] = cfg.get("keywords", {}) or {}
    filters_cfg: Dict[str, Dict] = cfg.get("filters", {}) or {}

    found: Set[str] = set()

    for topic, kws in keywords_cfg.items():
        if not isinstance(kws, (list, tuple)) or not kws:
            continue

        excludes = filters_cfg.get(topic, {}).get("exclude", []) or []
        excludes_norm = [_norm(x) for x in excludes if x]

        # Skip topic if any exclusion matches
        if _excluded(text_norm, excludes_norm):
            continue

        # One hit per topic is enough
        for kw in kws:
            if not kw:
                continue
            if _token_hit(text_norm, _norm(kw)):
                found.add(topic)
                break

    return sorted(found)

