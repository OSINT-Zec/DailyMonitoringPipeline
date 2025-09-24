# src/build_digest.py
from __future__ import annotations
import os, html, re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tzlocal import get_localzone
from typing import List, Tuple
from sqlalchemy import create_engine, text as sql_text
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")

# ---------- Helpers ----------
def clean(s: str | None) -> str:
    if not s: return ""
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\u200B-\u200F\uFEFF]", "", s)
    return s

def now_local_eu() -> str:
    tz = get_localzone()
    local_now = datetime.now(tz)
    # e.g., "16.Sep.2025 17:26 (KST)"
    return local_now.strftime("%d.%b.%Y %H:%M") + f" ({local_now.strftime('%Z')})"

def guess_tone(text: str, urls: List[str]) -> str:
    if any(u for u in urls if "gov" in u or "europa.eu" in u or "nato.int" in u):
        return "Official"
    if text.isupper() or "!!!" in text:
        return "Propagandistic"
    return "Neutral/Reportage"

def split_summary_and_entities(summary_text: str):
    """
    Separate summary bullets from the final Entities line.
    Also handles the case where 'Entities:' appears inline at the end of a bullet.
    """
    s = html.unescape(summary_text or "").strip()
    s = re.sub(r"[\u200B-\u200F\uFEFF]", "", s)

    entities = ""
    bullets = []

    for ln in [l.strip() for l in s.splitlines() if l.strip()]:
        # If 'Entities:' appears anywhere on this line, capture it and strip from the line
        m = re.search(r'(?i)\bentities\s*:\s*(.+)$', ln)
        if m:
            if not entities:  # first one wins
                entities = m.group(1).strip()
            ln = ln[:m.start()].rstrip()
            if not ln:
                continue  # line was only entities

        # Keep bullets whether they begin with "-", "•", or plain text already split upstream
        if re.match(r'^[\-\*\u2022]\s+', ln):
            ln = re.sub(r'^[\-\*\u2022]\s*', '', ln).strip()

        if ln:
            bullets.append(ln)

    return bullets, entities  # return entities *without* the 'Entities:' prefix

# ---------- Main ----------
def run():
    if not POSTGRES_URL:
        print("POSTGRES_URL not set.")
        return

    eng = create_engine(POSTGRES_URL, pool_pre_ping=True)
    since = datetime.utcnow() - timedelta(hours=36)
    date_str = now_local_eu()

    with eng.begin() as con:
        counts = con.execute(sql_text("""
            SELECT topic, COALESCE(SUM(size),0) AS total
            FROM clusters
            WHERE (end_ts IS NULL OR end_ts >= :since)
            GROUP BY topic
        """), {"since": since}).fetchall()

    topic_counts = {t: int(c) for t, c in counts}
    topics = list(topic_counts.keys())

    # HTML header + style
    html_out = [
        "<!DOCTYPE html>",
        "<html lang='en'><head><meta charset='UTF-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>Daily Monitoring Digest</title>",
        "<style>",
        " body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; line-height: 1.5; color: #111; }",
        " h1 { margin: 0 0 0.5em; }",
        " h2 { margin: 1.2em 0 0.4em; border-bottom: 1px solid #eee; padding-bottom: 0.2em; }",
        " h3 { margin: 1em 0 0.4em; }",
        " ul { margin: 0.2em 0 0.6em 1.2em; }",
        " .entities { margin-top: 0.6em; }",
        " .links ul { margin-top: 0.2em; }",
        " .topic-counts { color: #444; }",
        " .cluster { margin: 0.8em 0 1.2em; }",
        "</style>",
        "</head><body>",
        f"<h1>Daily Monitoring Digest — {date_str}</h1>"
    ]

    if topic_counts:
        counts_str = ", ".join(f"{t}: {n}" for t, n in topic_counts.items())
        html_out.append(f"<p class='topic-counts'><b>Topic counts (last 24–36h buffer):</b> {counts_str}</p>")

    with eng.begin() as con:
        for topic in topics:
            rows = con.execute(sql_text("""
                SELECT cluster_id, size, score, top_terms, rep_item_ids
                FROM clusters
                WHERE topic = :topic
                ORDER BY score DESC NULLS LAST, size DESC NULLS LAST
                LIMIT 8
            """), {"topic": topic}).fetchall()

            if not rows:
                continue

            html_out.append(f"<h2>{topic} (up to 8 clusters)</h2>")

            for idx, (cid, size, score, top_terms, rep_ids) in enumerate(rows, 1):
                summary_text = (top_terms[0] if top_terms else "").strip()
                if not summary_text:
                    summary_text = "(no summary available)"

                bullets, entities_only = split_summary_and_entities(summary_text)

                # Items for links/tone
                items = con.execute(
                    sql_text("SELECT title, url FROM items WHERE id = ANY(:ids)"),
                    {"ids": rep_ids or []},
                ).fetchall()

                urls = [u for _, u in items if u]
                tone = guess_tone(summary_text, urls)

                html_out.append("<div class='cluster'>")
                html_out.append(f"<h3>{idx}) Summary</h3>")

                # Bullets list
                if bullets:
                    html_out.append("<ul>")
                    for b in bullets:
                        html_out.append(f"<li>{clean(b)}</li>")
                    html_out.append("</ul>")
                else:
                    html_out.append("<p>(no summary available)</p>")

                # Entities OUTSIDE the list
                if entities_only:
                    html_out.append(f"<p class='entities'><b>Entities:</b> {entities_only}</p>")

                # Tone
                html_out.append(f"<p><b>Tone:</b> {tone}</p>")

                # Links
                if items:
                    html_out.append("<div class='links'><p><b>Links:</b></p><ul>")
                    for title, url in items:
                        if url:
                            title_clean = clean(title) or url
                            html_out.append(
                                f"<li><a href='{url}' target='_blank' rel='noopener noreferrer'>{title_clean}</a></li>"
                            )
                    html_out.append("</ul></div>")
                else:
                    html_out.append("<p><b>Links:</b> (none)</p>")

                html_out.append("</div>")  # .cluster

    html_out.append("</body></html>")

    # Save to file like digest_16Sep2025.html (dayMonAbbrevYear)
    output_file = f"digests/digest_{datetime.now().strftime('%d%b%Y')}.html"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(html_out))

    print(f"Digest saved to {output_file}")

if __name__ == "__main__":
    run()
