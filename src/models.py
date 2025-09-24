# src/models.py
from sqlalchemy import text

# --- Tables ---
DDL_ITEMS = """
CREATE TABLE IF NOT EXISTS items (
  id         TEXT PRIMARY KEY,
  src        TEXT NOT NULL DEFAULT 'rss',
  chan       TEXT,
  url        TEXT,
  ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  lang       TEXT,
  lang_conf  REAL CHECK (lang_conf BETWEEN 0 AND 1 OR lang_conf IS NULL),
  title      TEXT,
  text       TEXT,
  entities   JSONB  NOT NULL DEFAULT '{}'::jsonb,
  topics     TEXT[] NOT NULL DEFAULT '{}'::text[],
  keywords   TEXT[] NOT NULL DEFAULT '{}'::text[],
  hash_sim   TEXT
);
"""

DDL_CLUSTERS = """
CREATE TABLE IF NOT EXISTS clusters (
  cluster_id   TEXT PRIMARY KEY,
  topic        TEXT,
  start_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_ts       TIMESTAMPTZ,
  size         INT  NOT NULL DEFAULT 0,
  score        REAL NOT NULL DEFAULT 0,
  top_terms    TEXT[] NOT NULL DEFAULT '{}'::text[],
  rep_item_ids TEXT[] NOT NULL DEFAULT '{}'::text[]
);
"""

# --- Indexes (idempotent) ---
IDX_SQL = [
    # items
    "CREATE INDEX IF NOT EXISTS idx_items_ts ON items (ts);",
    "CREATE INDEX IF NOT EXISTS idx_items_topics_gin ON items USING GIN (topics);",
    "CREATE INDEX IF NOT EXISTS idx_items_keywords_gin ON items USING GIN (keywords);",
    "CREATE INDEX IF NOT EXISTS idx_items_entities_gin ON items USING GIN (entities);",
    # prevent dup URLs when present (still keep custom id as PK)
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_unique ON items (url) WHERE url IS NOT NULL;",

    # clusters
    "CREATE INDEX IF NOT EXISTS idx_clusters_topic_score ON clusters (topic, score);",
    "CREATE INDEX IF NOT EXISTS idx_clusters_end_ts ON clusters (end_ts);",
    "CREATE INDEX IF NOT EXISTS idx_clusters_start_ts ON clusters (start_ts);"
]

def ensure_schema(engine) -> None:
    """Create tables and indexes if they do not exist (idempotent)."""
    with engine.begin() as con:
        # tables
        con.execute(text(DDL_ITEMS))
        con.execute(text(DDL_CLUSTERS))
        # indexes
        for stmt in IDX_SQL:
            con.execute(text(stmt))

