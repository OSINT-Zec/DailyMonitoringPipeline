# 📰 Daily Monitoring Pipeline — v1.0

**Version:** 1.0  
**Last Updated:** 24 Sep 2025  

A lightweight pipeline for:  
- Collecting sources (RSS + research feeds)  
- Enriching content  
- Clustering similar items  
- Generating LLM-based summaries  
- Building a daily HTML digest  
- Optionally emailing it to you  

💻 **Runs on:** Single machine (laptop/VM) with **Postgres** + **Python**  

> ⚠️ **Disclaimer:** This repository may process sensitive or extremist content for research and monitoring purposes. Handle outputs responsibly.

---

## What it does

**Collect:** Pulls articles/posts from RSS (and optional Reddit RSS) into Postgres.

**Enrich:** Detects language and tags topics via keywords + (optional) embeddings.

**Cluster:** Groups related items into clusters by topic (TF-IDF or SBERT).

**Summarize:** Uses OpenAI to produce English, 3-bullet summaries + an Entities line.

**Digest:** Emits a clean HTML report with tone + links and mails it to you.

---

## Architecture

```
config/
  monitor.yaml        # Topics, keywords, RSS sources, classification thresholds

src/
  collect_rss.py      # Fetch feeds, extract main text, store in 'items' table
  enrich.py           # Language detection + topic tagging (keywords + hybrid rules)
  cluster.py          # Group items into clusters per topic (TF-IDF or embeddings)
  summarize.py        # Generate LLM-based summaries (3 bullets + Entities)
  tagging.py          # Additional tagging logic for entities, tone, or source metadata
  build_digest.py     # Render HTML digest with summaries, tone, and local timestamps
  models.py           # PostgreSQL schema definitions (items, clusters)
  settings.py         # Config loader + POSTGRES_URL environment handling

scripts/
  initdb.py           # Initialize or verify database schema

digest_job.sh         # End-to-end pipeline runner (collect → enrich → cluster → summarize → digest → email)
makefile              # Make targets for each stage: collect, enrich, cluster, summarize, digest

.env                  # Local environment variables (never commit)
requirements.txt      # Python dependencies for pipeline and optional extras
```

### Tables:

```
items(id, url, ts, title, text, lang, topics[], keywords[])
clusters(cluster_id, topic, start_ts, end_ts, size, score, top_terms[], rep_item_ids[])
```

---

## Requirements

* Python 3.10+ (3.11 recommended)
* PostgreSQL 13+
* Optional: sentence-transformers (for embedding-based tagging)
* Optional: msmtp + Proton Mail Bridge (or any SMTP) to send email

**Python deps (pip):**

```
feedparser trafilatura requests SQLAlchemy python-dotenv psycopg2-binary
langid langdetect sentence-transformers scikit-learn tzlocal openai pyyaml
```

---

## Setup

### Clone & create venv

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # or install packages listed above
```

### Database

```bash
createdb osint
# set a user/pass as you like
```

Put your URL in `.env`:

```
POSTGRES_URL=postgresql://<user>:<pass>@127.0.0.1:5432/osint
```

Initialize schema:

```bash
python scripts/initdb.py
```

---

## OpenAI (for summaries)

```
OPENAI_API_KEY=sk-...              # do not commit this
OPENAI_SUMMARY_MODEL=gpt-4o-mini   # default; change if desired
```

---

## Configuration

Edit `config/monitor.yaml` to tweak:

* **topics** — category names
* **keywords** — seed phrases (why: fast, explainable, low-compute)
* **classification** — hybrid thresholds, anchors, negatives
* **sources.rss** — feeds to crawl

---

## Running the pipeline (manually)

```bash
source .venv/bin/activate
set -a; source .env; set +a

make collect    # fetch feeds to items
make enrich     # language + topic tagging
make cluster    # group into clusters
make summarize  # OpenAI summaries
make digest     # render HTML digest to digests/digest_14Sep2025.html
```

The digest file is HTML and includes auto-detected local timezone in the header.

---

## Emailing the digest

Sample `~/.msmtprc` (use Proton Mail Bridge or your SMTP; do NOT commit secrets):

```
defaults
auth           on
tls            on
tls_starttls   on
tls_certcheck  on
tls_trust_file /etc/ssl/certs/ca-certificates.crt
logfile        ~/.msmtp.log

account proton
host 127.0.0.1
port 1025
from you@proton.me
user you@proton.me
password ********       # or use 'passwordeval' with a keychain/cmd
auth plain

account default : proton
```

Then use the provided job script:

```bash
bash scripts/digest_job.sh
```

It will:

* (optionally) reset tables (see the note below),
* run the pipeline,
* send the latest HTML via msmtp with proper MIME headers.

Tip: File naming is `digests/digest_<ddMonYYYY>.html` (e.g., `digest_14Sep2025.html`).

---

## Scheduling (cron)

Edit crontab:

```bash
crontab -e
```

Example: every day at 09:00 local time

```
0 9 * * * /bin/bash -lc '/path/to/repo/scripts/digest_job.sh' >> /path/to/repo/logs/cron.log 2>&1
```

Cron runs with a minimal env; `digest_job.sh` exports what’s needed.

If your script uses `sudo -u postgres ... TRUNCATE ...`, ensure your user’s sudoers allow passwordless for that command or avoid sudo and use a DB user with privileges.

---

## Daily reset vs rolling window

You don’t have to wipe data daily. Two patterns:

**A. Rolling window (recommended)**
Filter by `ts >= now() - interval '36 hours'` in collect/cluster and leave data intact.

**B. Daily reset (simple, destructive)**
Truncate `items` and `clusters` before each run. This avoids drift but loses history.

`digest_job.sh` contains the reset step as an example—comment it out if you prefer a rolling window.

---

## Customization

**Keywords vs Embeddings**
Keywords give explainability and zero model downloads; embeddings improve recall. The default hybrid method uses both with thresholds in `monitor.yaml`.

**Languages**
`enrich.py` detects language via `langid` (fast) or `langdetect` (fallback).

**Clustering**

* Light mode (default): TF-IDF + MiniBatchKMeans (no GPU, quick).
* SBERT mode: `LIGHT_CLUSTER=0` env var to enable semantic clustering (`sentence-transformers` required).

**Summaries**
Always 3 bullets + Entities. HTML entities are unescaped; summaries forced to English; tone heuristics shown.

---

## Troubleshooting

* **OpenAI key not picked up**
  Ensure `.env` is sourced in the shell or by the job script.

* **msmtp TLS errors**
  Verify `tls_certcheck on` and that your CA bundle is present; for Proton Bridge, its local cert is trusted.

* **“Peer authentication failed” (psql)**
  Adjust `pg_hba.conf` or use a proper DB user/password in `POSTGRES_URL`.

---

## Security & privacy

* Keep `.env` and `~/.msmtprc` `0600`.
* Review feed list; the pipeline may ingest extremist content for monitoring. Summaries paraphrase neutrally.
* If you store history, consider retention policies and encryption at rest.

---

## Common tasks

**Initialize DB schema**

```bash
python scripts/initdb.py
```

**Run end-to-end once**

```bash
make collect enrich cluster summarize digest
```

**Send the latest digest manually**

```bash
bash scripts/digest_job.sh
```

**Change date format or timezone label in digest**
Already auto-detects local zone. See `now_local_eu()` in `build_digest.py` to customize.

---

## Roles of key modules (quick)

* `collect_rss.py` — Fetch feeds, extract main text (Trafilatura + fallbacks), store to `items`.
* `enrich.py` — Language + topic tagging (keywords / hybrid / embeddings).
* `cluster.py` — Build topic clusters (light TF-IDF KMeans or SBERT + agglomerative).
* `summarize.py` — OpenAI summaries with strict format + fallback entity extraction.
* `build_digest.py` — HTML report (3 bullets, Entities, Tone, Links; local timezone).
* `scripts/digest_job.sh` — Scheduled job: (optional) reset, pipeline, email.
* `settings.py` — Loads `config/monitor.yaml` + env.
* `models.py` — DB schema.

---

## Versioning & license

* Current: **v0.1**
* License: MIT (or your preferred license).

---

## Credits

* RSS parsing: feedparser
* Content extraction: trafilatura
* Clustering/ML: scikit-learn, sentence-transformers (optional)
* Summarization: OpenAI API
* Email: msmtp (+ Proton Mail Bridge or SMTP of your choice)

---

**Happy monitoring!**
