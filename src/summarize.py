# src/summarize.py
from __future__ import annotations

import os
import re
import html
import logging
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text
from openai import OpenAI

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# -------------------
# Helpers
# -------------------
def clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\u200B-\u200F\uFEFF]", "", s)
    return s


def extract_entities_fallback(items) -> str:
    # Use hostnames and a few titles as a backup entity list
    hosts = []
    for _, u, _ in items:
        if not u:
            continue
        netloc = urlparse(u).netloc
        if not netloc:
            continue
        # take the registrable-ish part
        parts = [p for p in netloc.split(".") if p]
        if len(parts) >= 2:
            hosts.append(parts[-2].capitalize())
        else:
            hosts.append(parts[0].capitalize())

    titles = [clean_text(t) for (t, _, _) in items if clean_text(t)]
    combined = titles[:3] + hosts[:6]
    combined = [c for c in combined if c]
    if not combined:
        return "—"
    # de-dup but keep order
    seen = set()
    out = []
    for c in combined:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return ", ".join(out[:6]) or "—"


# -------------------
# OpenAI prompt
# -------------------
SYS = (
    "You are an OSINT analyst. Output must be English only.\n"
    "Summarize the cluster into EXACTLY 3 concise bullets (<=60 words each), then an 'Entities:' line.\n"
    "Do NOT reproduce slurs or slogans; paraphrase neutrally. No HTML entities; render proper characters.\n"
    "Format:\n"
    "- <bullet 1>\n- <bullet 2>\n- <bullet 3>\nEntities: <comma-separated list>"
)


def summarize_with_responses(client: OpenAI, content: str) -> str:
    resp = client.responses.create(model=MODEL, input=f"{SYS}\n\n---\nSummarize this cluster:\n\n{content}", temperature=0.2)
    return clean_text(getattr(resp, "output_text", "") or "")


def summarize_with_chat(client: OpenAI, content: str) -> str:
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "system", "content": SYS}, {"role": "user", "content": f"Summarize this cluster:\n\n{content}"}],
        temperature=0.2,
    )
    return clean_text(resp.choices[0].message.content or "")


# -------------------
# Formatting
# -------------------
def enforce_format(txt: str, items) -> str:
    """
    Guarantee:
      - exactly 3 lines starting with '- '
      - a single 'Entities:' line (fallback if missing)
    """
    txt = clean_text(txt or "")

    # Extract entities (case-insensitive)
    m = re.search(r"(?im)^entities\s*:\s*(.*)$", txt)
    ents = clean_text(m.group(1)) if m else extract_entities_fallback(items)
    body = re.sub(r"(?im)^entities\s*:.*$", "", txt).strip()

    # Try to collect explicit bullets
    raw_lines = [l.strip() for l in body.splitlines() if l.strip()]
    bullets = [re.sub(r"^[\-\u2022•]\s*", "", l).strip() for l in raw_lines if l.startswith(("-", "•", "•", "—"))]

    # Handle inline form: "foo - bar - baz"
    if len(bullets) <= 1 and body:
        inline = [seg.strip() for seg in re.split(r"\s-\s", body.lstrip("- •").strip()) if seg.strip()]
        if len(inline) >= 2:
            bullets = inline

    # Sentence fallback
    if not bullets and body:
        bullets = [p.strip() for p in re.split(r"(?<=[.?!])\s+", body) if p.strip()]

    # As an absolute fallback, use up to three titles
    if len(bullets) < 3:
        for t, _, _ in items:
            t = clean_text(t)
            if t and t not in bullets:
                bullets.append(t)
            if len(bullets) >= 3:
                break

    # Clean & clamp
    bullets = [clean_text(b) for b in bullets if b]
    if not bullets:
        bullets = ["No substantive content", "Source too short", "Will summarize later"]
    while len(bullets) < 3:
        bullets.append(bullets[-1])
    bullets = bullets[:3]

    return "- " + "\n- ".join(bullets) + f"\nEntities: {ents or '—'}"


def build_context(items) -> str:
    """
    Build a bounded context blob for the model out of (title, url, text) tuples.
    """
    parts = []
    for title, url, body in items:
        t = clean_text(title)
        u = (url or "").strip()
        b = clean_text(body)
        block = []
        if t:
            block.append(f"Title: {t}")
        if u:
            block.append(f"URL: {u}")
        if b:
            block.append(b)
        if block:
            parts.append("\n".join(block))
    # Keep within a safe bound
    return ("\n\n---\n\n".join(parts))[:6500]


# -------------------
# Main
# -------------------
def run():
    if not POSTGRES_URL or not OPENAI_API_KEY:
        print("Missing POSTGRES_URL or OPENAI_API_KEY.")
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    eng = create_engine(POSTGRES_URL, pool_pre_ping=True)

    with eng.begin() as con:
        clusters = con.execute(
            sql_text(
                """
                SELECT cluster_id, topic, size, rep_item_ids
                FROM clusters
                WHERE size >= 1
                ORDER BY score DESC NULLS LAST
                LIMIT 24
                """
            )
        ).fetchall()

    if not clusters:
        print("No clusters to summarize.")
        return

    with eng.begin() as con:
        for cid, topic, size, rep_ids in clusters:
            items = con.execute(
                sql_text("SELECT title, url, text FROM items WHERE id = ANY(:ids)"),
                {"ids": rep_ids or []},
            ).fetchall()

            blob = build_context(items)

            if not blob.strip():
                # Hard fallback without calling the model
                titles = [clean_text(t) for (t, _, _) in items if clean_text(t)]
                if not titles:
                    titles = ["No substantive content", "Source too short", "Will summarize later"]
                summary = "- " + "\n- ".join(titles[:3]) + f"\nEntities: {extract_entities_fallback(items)}"
            else:
                try:
                    s = summarize_with_responses(client, blob) or ""
                    summary = enforce_format(s, items)
                except Exception as e1:
                    # Fallback to chat completions
                    try:
                        s = summarize_with_chat(client, blob) or ""
                        summary = enforce_format(s, items)
                    except Exception as e2:
                        logging.error("Summarization failed for cluster %s: %s", cid, e2)
                        summary = "- Summarization failed\n- Summarization failed\n- Summarization failed\nEntities: —"

            con.execute(
                sql_text("UPDATE clusters SET top_terms = :terms WHERE cluster_id = :cid"),
                {"terms": [summary], "cid": cid},
            )

    print("Summaries added.")


if __name__ == "__main__":
    run()
