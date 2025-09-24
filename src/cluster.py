# src/cluster.py
from __future__ import annotations

import os
import math
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.exc import SQLAlchemyError

# ---------- Config / env ----------
def _get_postgres_url() -> str:
    # Prefer env; fall back to src.settings if available
    url = (os.getenv("POSTGRES_URL") or "").strip()
    if url:
        return url
    try:
        from .settings import POSTGRES_URL as SETTINGS_URL  # type: ignore
        return (SETTINGS_URL or "").strip()
    except Exception:
        return ""

POSTGRES_URL = _get_postgres_url()
LOOKBACK_HOURS = int(os.getenv("CLUSTER_LOOKBACK_HOURS", "36"))
LIGHT_CLUSTER = os.getenv("LIGHT_CLUSTER", "1") == "1"
MIN_CLUSTER_SIZE = int(os.getenv("MIN_CLUSTER_SIZE", "3"))
MAX_REP_ITEMS = int(os.getenv("MAX_REP_ITEMS", "5"))
TFIDF_MAX_DF = float(os.getenv("TFIDF_MAX_DF", "0.85"))
TFIDF_MIN_DF = int(os.getenv("TFIDF_MIN_DF", "2"))
AGGLO_DIST_THRESH = float(os.getenv("AGGLO_DISTANCE_THRESHOLD", "0.35"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------- Helpers ----------
def _choose_k(n_docs: int) -> int:
    """Heuristic for k in KMeans: clamp between 2 and 24."""
    if n_docs <= 8:
        return 2
    # sqrt-like growth; works decently for newsy blobs
    k = int(round(math.sqrt(n_docs / 2)))
    return max(2, min(k, 24))

def _stable_cluster_id(ids: List[str]) -> str:
    """Deterministic cluster id regardless of row order."""
    return hashlib.md5("|".join(sorted(ids)).encode("utf-8")).hexdigest()

def _topic_guess(topics_col) -> str:
    # items.topics is TEXT[] (may be empty list/None)
    try:
        if topics_col and isinstance(topics_col, (list, tuple)) and len(topics_col) > 0:
            return str(topics_col[0]) or "misc"
    except Exception:
        pass
    return "misc"

def _pick_representatives(rows: List[Tuple]) -> List[str]:
    """
    Pick representative item ids.
    Strategy: newest first; fall back to original order.
    Each row tuple has: (id, content, topics, ts)
    """
    try:
        sorted_rows = sorted(rows, key=lambda r: r.ts or datetime.now(timezone.utc), reverse=True)  # type: ignore
    except Exception:
        sorted_rows = rows
    return [r.id for r in sorted_rows[:MAX_REP_ITEMS]]  # type: ignore

# ---------- Main ----------
def run() -> int:
    if not POSTGRES_URL:
        logging.error("POSTGRES_URL not set.")
        return 2

    try:
        eng = create_engine(POSTGRES_URL, pool_pre_ping=True, future=True)
    except Exception as e:
        logging.error("Failed to create DB engine: %s", e)
        return 2

    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    # Pull recent items
    try:
        with eng.begin() as con:
            rows = con.execute(
                sql_text(
                    """
                    SELECT id,
                           COALESCE(title,'') || ' ' || COALESCE(text,'') AS content,
                           topics,
                           ts
                    FROM items
                    WHERE ts >= :since
                    """
                ),
                {"since": since},
            ).fetchall()
    except SQLAlchemyError as e:
        logging.error("DB error selecting items: %s", e)
        return 1

    if not rows:
        logging.info("No items to cluster.")
        return 0

    docs = [ (r.content or "")[:2000] for r in rows ]  # cap to keep features sane
    n_docs = len(docs)
    logging.info("Clustering %d recent items (lookback=%dh, light=%s)", n_docs, LOOKBACK_HOURS, LIGHT_CLUSTER)

    # ---- Assign labels ----
    try:
        if LIGHT_CLUSTER:
            # Fast path: TF-IDF + MiniBatchKMeans
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.cluster import MiniBatchKMeans
            except Exception as e:
                logging.error("scikit-learn not available for light clustering: %s", e)
                return 2

            min_df = TFIDF_MIN_DF if n_docs >= TFIDF_MIN_DF * 2 else 1
            vec = TfidfVectorizer(
                max_df=TFIDF_MAX_DF,
                min_df=min_df,
                ngram_range=(1, 2),
                stop_words="english",
            )
            X = vec.fit_transform(docs)

            k = _choose_k(n_docs)
            km = MiniBatchKMeans(n_clusters=k, random_state=0, batch_size=256, n_init="auto")
            labels = km.fit_predict(X)

        else:
            # Quality path: embeddings + agglomerative
            try:
                from sentence_transformers import SentenceTransformer
                from sklearn.cluster import AgglomerativeClustering
            except Exception as e:
                logging.error("sentence-transformers/scikit-learn missing: %s", e)
                return 2

            model_name = os.getenv("CLUSTER_EMBED_MODEL", "all-MiniLM-L6-v2")
            model = SentenceTransformer(model_name)
            emb = model.encode(docs, batch_size=64, show_progress_bar=False, normalize_embeddings=True)

            # sklearn API changed: metric vs affinity
            try:
                cl = AgglomerativeClustering(
                    n_clusters=None,
                    distance_threshold=AGGLO_DIST_THRESH,
                    linkage="average",
                    metric="cosine",
                )
            except TypeError:
                cl = AgglomerativeClustering(
                    n_clusters=None,
                    distance_threshold=AGGLO_DIST_THRESH,
                    linkage="average",
                    affinity="cosine",  # older sklearn
                )
            labels = cl.fit_predict(emb)
    except Exception as e:
        logging.error("Clustering failed: %s", e)
        return 2

    # ---- Group by label ----
    clusters: Dict[int, List] = {}
    for lab, row in zip(labels, rows):
        clusters.setdefault(int(lab), []).append(row)

    made = 0
    kept = 0

    # ---- Upsert clusters ----
    UPSERT = sql_text(
        """
        INSERT INTO clusters
          (cluster_id, topic, start_ts, end_ts, size, score, top_terms, rep_item_ids)
        VALUES
          (:cid, :topic, :start_ts, :end_ts, :size, :score, :terms, :rep)
        ON CONFLICT (cluster_id) DO UPDATE SET
          end_ts = EXCLUDED.end_ts,
          size   = EXCLUDED.size,
          score  = GREATEST(clusters.score, EXCLUDED.score),
          rep_item_ids = EXCLUDED.rep_item_ids
        """
    )

    try:
        with eng.begin() as con:
            for lab, members in clusters.items():
                if len(members) < MIN_CLUSTER_SIZE:
                    continue

                # window and topic
                try:
                    ts_vals = [m.ts for m in members if m.ts]  # type: ignore
                    start_ts = min(ts_vals) if ts_vals else since
                    end_ts = max(ts_vals) if ts_vals else datetime.now(timezone.utc)
                except Exception:
                    start_ts = since
                    end_ts = datetime.now(timezone.utc)

                topic = _topic_guess(members[0].topics)  # type: ignore
                rep_ids = _pick_representatives(members)
                cid = _stable_cluster_id([m.id for m in members])  # type: ignore

                size = len(members)
                # simple score: size with small recency boost
                hours_old = max(1.0, (datetime.now(timezone.utc) - end_ts).total_seconds() / 3600.0)
                score = float(size) + (2.0 / hours_old)

                con.execute(
                    UPSERT,
                    {
                        "cid": cid,
                        "topic": topic,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "size": size,
                        "score": score,
                        "terms": [],            # summarizer fills later
                        "rep": rep_ids,
                    },
                )
                kept += 1
                made += size

    except SQLAlchemyError as e:
        logging.error("DB error upserting clusters: %s", e)
        return 1
    finally:
        eng.dispose()

    logging.info("Clustered %d items into %d clusters (min_size=%d).", made, kept, MIN_CLUSTER_SIZE)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

