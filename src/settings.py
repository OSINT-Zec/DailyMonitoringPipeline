# src/settings.py
from __future__ import annotations

import os
import re
import logging
from pathlib import Path
from typing import Dict, List, Any

import yaml

logger = logging.getLogger(__name__)

# --- DB URL (env-driven; no hard-coded secrets) ---
POSTGRES_URL: str = os.getenv("POSTGRES_URL", "").strip()
if not POSTGRES_URL:
    # Keep a sensible, non-secret fallback for local dev only.
    # Prefer overriding via environment or .env loaded elsewhere.
    POSTGRES_URL = "postgresql+psycopg2://osint:osint@127.0.0.1:5432/osint"
    logger.warning("POSTGRES_URL not set; using local default for development.")

# --- Config path (can be overridden via MONITOR_YAML env) ---
MONITOR_YAML: str = os.getenv("MONITOR_YAML", "config/monitor.yaml")


# ----------------------------
# Small sanitization utilities
# ----------------------------
def _dedupe_keep_order(seq: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for s in seq:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _as_list_str(x: Any) -> List[str]:
    """Coerce to list[str], trimming whitespace and dropping empties."""
    if x is None:
        return []
    if isinstance(x, str):
        x = [x]
    if not isinstance(x, (list, tuple)):
        return []
    out: List[str] = []
    for v in x:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    return out


_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _only_http_urls(urls: List[str]) -> List[str]:
    out: List[str] = []
    for u in urls:
        if _URL_RE.match(u):
            out.append(u)
        else:
            logger.warning("Dropping non-http(s) source URL: %r", u)
    return out


def _sanitize_keywords_map(d: Any, topics: List[str], label: str) -> Dict[str, List[str]]:
    """Ensure mapping[str -> list[str]], keep only known topics, dedupe/trim lists."""
    if not isinstance(d, dict):
        if d is not None:
            logger.warning("%s must be a mapping; got %r. Using empty.", label, type(d).__name__)
        return {t: [] for t in topics}
    out: Dict[str, List[str]] = {t: [] for t in topics}
    for k, v in d.items():
        ks = str(k).strip()
        if ks not in topics:
            logger.warning("Ignoring %s for unknown topic %r", label, ks)
            continue
        lst = _dedupe_keep_order(_as_list_str(v))
        out[ks] = lst
    return out


# ----------------------------
# Main loader + sanitizer
# ----------------------------
def load_cfg(path: str | Path = MONITOR_YAML) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"monitor.yaml not found at: {p.resolve()}")

    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg: dict = {}

    # Topics
    topics = _dedupe_keep_order(_as_list_str(raw.get("topics")))
    if not topics:
        raise ValueError("monitor.yaml: 'topics' is required and must be a non-empty list of strings.")
    cfg["topics"] = topics

    # Keywords (per topic)
    cfg["keywords"] = _sanitize_keywords_map(raw.get("keywords"), topics, "keywords")

    # Languages preference (loose validation; keep as simple list[str])
    langs = _dedupe_keep_order(_as_list_str(raw.get("languages_prefer")))
    cfg["languages_prefer"] = langs or ["en", "de", "ru"]

    # Summaries block
    sums = dict(raw.get("summaries") or {})
    daily_n = int(sums.get("daily_top_clusters", 8) or 8)
    bullets_n = int(sums.get("bullets_per_cluster", 3) or 3)
    if daily_n <= 0:
        logger.warning("summaries.daily_top_clusters <= 0; defaulting to 8.")
        daily_n = 8
    if bullets_n <= 0:
        logger.warning("summaries.bullets_per_cluster <= 0; defaulting to 3.")
        bullets_n = 3
    cfg["summaries"] = {
        "daily_top_clusters": daily_n,
        "bullets_per_cluster": bullets_n,
    }

    # Optional: filters.exclude per topic (kept for forward-compat)
    filters = dict(raw.get("filters") or {})
    out_filters: Dict[str, dict] = {}
    for t in topics:
        tblock = dict(filters.get(t) or {})
        excl = _dedupe_keep_order(_as_list_str(tblock.get("exclude")))
        out_filters[t] = {"exclude": excl}
    cfg["filters"] = out_filters

    # Classification (hybrid / keywords / embedding)
    cls = dict(raw.get("classification") or {})
    method = str(cls.get("method", "hybrid")).strip().lower()
    if method not in {"hybrid", "keywords", "embedding"}:
        logger.warning("classification.method=%r invalid; defaulting to 'hybrid'.", method)
        method = "hybrid"

    th = dict(cls.get("thresholds") or {})
    try:
        kw_min = int(th.get("keyword_min_hits", 1))
    except Exception:
        logger.warning("thresholds.keyword_min_hits invalid; defaulting to 1.")
        kw_min = 1
    try:
        embed_cos = float(th.get("embed_cosine", 0.42))
    except Exception:
        logger.warning("thresholds.embed_cosine invalid; defaulting to 0.42.")
        embed_cos = 0.42

    anchors = _sanitize_keywords_map(cls.get("anchors"), topics, "anchors")
    negatives = _sanitize_keywords_map(cls.get("negatives"), topics, "negatives")

    cfg["classification"] = {
        "method": method,
        "thresholds": {"keyword_min_hits": kw_min, "embed_cosine": embed_cos},
        "anchors": anchors,
        "negatives": negatives,
    }

    # Sources
    sources = dict(raw.get("sources") or {})
    rss_list = _only_http_urls(_dedupe_keep_order(_as_list_str(sources.get("rss"))))
    if not rss_list:
        logger.warning("No valid RSS URLs under sources.rss; ingestion may do nothing.")
    # Preserve optional reddit subtree as-is (collect_rss knows how to read it),
    # but ensure it's at least a dict to avoid attribute errors.
    reddit_cfg = sources.get("reddit") if isinstance(sources.get("reddit"), dict) else {}
    cfg["sources"] = {
        "rss": rss_list,
        "reddit": reddit_cfg,
    }

    return cfg


# Export sanitized, ready-to-use config at import time
CFG: dict = load_cfg()

__all__ = ["POSTGRES_URL", "MONITOR_YAML", "CFG"]

