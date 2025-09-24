#!/usr/bin/env bash
set -euo pipefail

# === Configurable Paths (adjust as needed) ===
BASE="$(dirname "$(realpath "$0")")/.."  # Base dir: script's parent folder
LOGDIR="$BASE/logs"
OUTDIR="$BASE/digests"
TS="$(date '+%d%b%Y %H:%M')"
SUBJECT="Daily Monitoring Digest — ${TS}"
HTML_FILE=""

mkdir -p "$LOGDIR" "$OUTDIR"

# === Minimal cron-safe environment ===
export PATH="/usr/local/bin:/usr/bin:/bin"
export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

cd "$BASE"

# === Load environment and venv ===
if [[ -f ".env" ]]; then
    set -a
    source .env
    set +a
else
    echo "Missing .env file!" | tee -a "$LOGDIR/pipeline.log"
    exit 1
fi

if [[ -d ".venv" ]]; then
    source .venv/bin/activate
else
    echo "Missing virtual environment (.venv)!" | tee -a "$LOGDIR/pipeline.log"
    exit 1
fi

# === Reset DB before running pipeline (uses POSTGRES_URL from .env) ===
echo "==== $(date) : Resetting DB ====" | tee -a "$LOGDIR/pipeline.log"
if [[ -n "${POSTGRES_URL:-}" ]]; then
    psql "$POSTGRES_URL" -c "TRUNCATE clusters, items RESTART IDENTITY CASCADE;" \
        | tee -a "$LOGDIR/pipeline.log"
else
    echo "POSTGRES_URL not set in .env" | tee -a "$LOGDIR/pipeline.log"
    exit 1
fi

# === Run the pipeline (capture logs) ===
{
  echo "==== $(date) : START ===="
  make collect enrich cluster summarize
  python -m src.build_digest
  echo "==== $(date) : END ===="
} | tee -a "$LOGDIR/pipeline.log"

# === Find the latest digest HTML ===
HTML_FILE=$(ls -1t "$OUTDIR"/digest_*.html 2>/dev/null | head -n 1)

# === Send the email (msmtp expects full MIME headers) ===
if [[ -n "${HTML_FILE:-}" && -f "$HTML_FILE" ]]; then
  {
    echo "From: ${DIGEST_FROM:-no-reply@example.com}"
    echo "To: ${DIGEST_TO:-recipient@example.com}"
    echo "Subject: ${SUBJECT}"
    echo "MIME-Version: 1.0"
    echo "Content-Type: text/html; charset=UTF-8"
    echo
    cat "$HTML_FILE"
  } | msmtp -a "${MSMTP_ACCOUNT:-default}" "${DIGEST_TO:-recipient@example.com}"
else
  echo "No digest HTML found to send" | tee -a "$LOGDIR/pipeline.log"
  exit 2
fi