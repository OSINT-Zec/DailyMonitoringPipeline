# src/enrich.py
# Enrich items with language + topic tags using a hybrid rule:
#   keywords  ⊕  embedding similarity to topic anchors  ⊖  negatives
#
# - Reads classification config from monitor.yaml (via settings.CFG)
# - Optional deps: langid / langdetect, sentence-transformers / numpy
# - Updates only rows where lang or topics are NULL
# - Writes Postgres TEXT[] safely (never NULL lists)
# - Tunables via env: ENRICH_METHOD, ENRICH_BATCH_LIMIT, ENRICH_TEXT_CAP, ENRICH_USE_EMBED, etc.

from __future__ import annotations
from .tagging import tag_item  # <-- use the shared tagger

import logging
import os
import re
import unicodedata
from typing import Dict, List, Tuple, Optional

from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.exc import SQLAlchemyError

# -----------------------
# Optional dependencies
# -----------------------
_HAS_LANGID = False
_HAS_LANGDETECT = False
try:
    import langid  # fast, offline
    _HAS_LANGID = True
except Exception:
    pass
try:
    from langdetect import detect, detect_langs  # slower
    _HAS_LANGDETECT = True
except Exception:
    pass

_HAS_EMBED = False
try:
    from sentence_transformers import SentenceTransformer  # type: ignore
    import numpy as np  # type: ignore
    _HAS_EMBED = True
except Exception:
    pass

# -----------------------
# Config / settings
# -----------------------
try:
    from .settings import CFG, POSTGRES_URL  # type: ignore
except Exception:
    CFG, POSTGRES_URL = {}, os.getenv("POSTGRES_URL", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")

ALL_TOPICS: List[str] = list((CFG or {}).get("topics", []))

KEYWORDS: Dict[str, List[str]] = (CFG or {}).get("keywords", {}) or {}
CLASSCFG = (CFG or {}).get("classification", {}) or {}

# Method precedence: env > yaml (default hybrid)
METHOD = (os.getenv("ENRICH_METHOD") or CLASSCFG.get("method") or "hybrid").lower()
THRESHOLDS = CLASSCFG.get("thresholds", {}) or {}
KW_MIN_HITS = int(os.getenv("ENRICH_KEYWORD_MIN", THRESHOLDS.get("keyword_min_hits", 1)))
EMBED_COS = float(os.getenv("ENRICH_EMBED_COS", THRESHOLDS.get("embed_cosine", 0.42)))

ANCHORS: Dict[str, List[str]] = CLASSCFG.get("anchors", {}) or {}
NEGATIVES: Dict[str, List[str]] = CLASSCFG.get("negatives", {}) or {}

# Safety: keep only anchors/negatives for known topics
ANCHORS = {t: ANCHORS.get(t, []) for t in ALL_TOPICS}
NEGATIVES = {t: NEGATIVES.get(t, []) for t in ALL_TOPICS}

BATCH_LIMIT = int(os.getenv("ENRICH_BATCH_LIMIT", "2000"))
TEXT_CAP = int(os.getenv("ENRICH_TEXT_CAP", "4000"))  # cap for embedding/lang detection
USE_EMBED = os.getenv("ENRICH_USE_EMBED", "").lower() or ("1" if METHOD in ("hybrid", "embedding") else "0")
USE_EMBED = (USE_EMBED == "1") and _HAS_EMBED

LANG_PREFS: List[str] = list((CFG or {}).get("languages_prefer", []))

# -----------------------
# Normalization helpers
# -----------------------
LEET_MAP = str.maketrans({
    "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "@": "a", "$": "s",
})

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.lower().translate(LEET_MAP)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def keyword_hits(text_norm: str, kws: List[str]) -> Tuple[int, List[str]]:
    if not text_norm or not kws:
        return 0, []
    matched: List[str] = []
    for term in kws:
        t = normalize_text(term)
        if t and t in text_norm:
            matched.append(term)
    return len(matched), matched

def negative_hits(text_norm: str, negatives: List[str]) -> int:
    if not text_norm or not negatives:
        return 0
    c = 0
    for term in negatives:
        t = normalize_text(term)
        if t and t in text_norm:
            c += 1
    return c

# -----------------------
# Language detection
# -----------------------
def detect_lang(text: str) -> Tuple[Optional[str], Optional[float]]:
    """Return (lang_code, confidence) where confidence is 0..1 if available."""
    text = (text or "").strip()
    if not text:
        return None, None
    # Soft cap to speed up
    sample = text[:TEXT_CAP]

    if _HAS_LANGID:
        try:
            code, conf = langid.classify(sample)
            return code, float(conf)
        except Exception:
            pass

    if _HAS_LANGDETECT:
        try:
            scores = detect_langs(sample)
            if scores:
                best = max(scores, key=lambda x: x.prob)
                return best.lang, float(best.prob)
        except Exception:
            try:
                return detect(sample), None
            except Exception:
                return None, None

    return None, None

# -----------------------
# Embedding model (optional)
# -----------------------
_EMBED_MODEL: Optional["SentenceTransformer"] = None
_ANCHOR_VECS: Dict[str, Optional["np.ndarray"]] = {}

def _maybe_load_model() -> None:
    global _EMBED_MODEL
    if not USE_EMBED:
        logging.info("Embeddings disabled (USE_EMBED=0 or method=%s).", METHOD)
        return
    if _EMBED_MODEL is None:
        try:
            _EMBED_MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
            logging.info("Embedding model loaded: all-MiniLM-L6-v2")
        except Exception as e:
            logging.warning("Failed to load embedding model, falling back to keywords only: %s", e)
            _EMBED_MODEL = None

def _precompute_anchor_vecs() -> None:
    if _EMBED_MODEL is None:
        for t in ALL_TOPICS:
            _ANCHOR_VECS[t] = None
        return
    for t in ALL_TOPICS:
        phrases = ANCHORS.get(t, [])
        if not phrases:
            _ANCHOR_VECS[t] = None
            continue
        try:
            vecs = _EMBED_MODEL.encode(phrases, normalize_embeddings=True)
            _ANCHOR_VECS[t] = np.asarray(vecs)
        except Exception as e:
            logging.warning("Anchor embedding failed for topic '%s': %s", t, e)
            _ANCHOR_VECS[t] = None

def embed_score(text: str, topic: str) -> float:
    """Max cosine similarity between text and the topic's anchor phrases."""
    if _EMBED_MODEL is None:
        return 0.0
    A = _ANCHOR_VECS.get(topic)
    if A is None or getattr(A, "size", 0) == 0:
        return 0.0
    try:
        v = _EMBED_MODEL.encode([text[:TEXT_CAP]], normalize_embeddings=True)  # (1, d)
        sims = v @ A.T  # (1, n)
        return float(sims.max())
    except Exception:
        return 0.0

# -----------------------
# DB operations
# -----------------------
SEL_SQL = sql_text("""
SELECT id, coalesce(title,''), coalesce(text,''), lang, topics
FROM items
WHERE (lang IS NULL OR topics IS NULL)
ORDER BY ts DESC
LIMIT :lim
""")

UPD_SQL = sql_text("""
UPDATE items
SET lang = :lang,
    lang_conf = :lang_conf,
    topics = :topics,
    keywords = :keywords
WHERE id = :id
""")

def enrich_row(row) -> Tuple[List[str], List[str], Optional[str], Optional[float]]:
    _id, title, body, lang_cur, topics_cur = row

    raw_text = f"{title}\n{body}".strip()
    text_norm = normalize_text(raw_text)

    lang_code, lang_conf = detect_lang(raw_text)

    if not text_norm:
        return [], [], lang_code, lang_conf

    # 1) Keywords (from tagging.py)
    kw_labels = set(tag_item(raw_text, CFG))

    # Optional: record which specific keywords hit (per topic)
    matched_kw_total: List[str] = []
    for topic in kw_labels:
        k_hits, k_matched = keyword_hits(text_norm, KEYWORDS.get(topic, []))
        matched_kw_total.extend(k_matched)

    labels = set(kw_labels)

    # 2) Embeddings (optional, adds topics not already present)
    if METHOD in ("embedding", "hybrid") and _EMBED_MODEL is not None:
        for topic in ALL_TOPICS:
            if topic in labels:
                continue
            if embed_score(raw_text[:4000], topic) >= EMBED_COS:
                labels.add(topic)

    # 3) Negatives (light damping: drop topics that look like false positives)
    for topic in list(labels):
        if negative_hits(text_norm, NEGATIVES.get(topic, [])) > 0:
            # keep if strongly supported by embeddings
            strong = (_EMBED_MODEL is not None and embed_score(raw_text[:4000], topic) >= EMBED_COS + 0.05)
            if not strong:
                labels.discard(topic)

    # De-dup matched keywords while preserving order
    seen = set()
    matched_kw_total = [k for k in matched_kw_total if not (k in seen or seen.add(k))]

    return sorted(labels), matched_kw_total, lang_code, lang_conf

def run() -> int:
    if not POSTGRES_URL:
        logging.error("POSTGRES_URL not set.")
        return 2

    logging.info(
        "Starting enrich (method=%s, embed=%s, embed_cos=%.2f, kw_min=%d, batch=%d)",
        METHOD, "on" if USE_EMBED else "off", EMBED_COS, KW_MIN_HITS, BATCH_LIMIT
    )

    if METHOD in ("embedding", "hybrid"):
        _maybe_load_model()
    if _EMBED_MODEL is not None:
        _precompute_anchor_vecs()
    else:
        if METHOD in ("embedding", "hybrid"):
            logging.info("Proceeding without embeddings (keywords-only).")

    try:
        eng = create_engine(POSTGRES_URL, pool_pre_ping=True, future=True)
    except Exception as e:
        logging.error("DB engine create failed: %s", e)
        return 2

    total = 0
    updated = 0

    try:
        with eng.begin() as con:
            rows = con.execute(SEL_SQL, {"lim": BATCH_LIMIT}).fetchall()
            total = len(rows)
            logging.info("Loaded %d rows needing enrichment.", total)

            for row in rows:
                labels, matched_kw, lang_code, lang_conf = enrich_row(row)
                _id = row[0]

                try:
                    con.execute(
                        UPD_SQL,
                        dict(
                            id=_id,
                            lang=lang_code,
                            lang_conf=lang_conf,
                            topics=labels or [],       # never None
                            keywords=matched_kw or [], # never None
                        ),
                    )
                    updated += 1
                except SQLAlchemyError as ex:
                    logging.error("Update failed for id=%s: %s", _id, ex)

        logging.info("Enrich complete. Updated %d/%d rows.", updated, total)
        return 0
    finally:
        try:
            eng.dispose()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(run())

