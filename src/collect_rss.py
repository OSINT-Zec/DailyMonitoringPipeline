# src/collect_rss.py
from __future__ import annotations

import os
import hashlib
import time
import socket
import logging
import urllib.parse
import re
import html
from html import unescape
from datetime import datetime, timezone
from http.client import IncompleteRead
from email.utils import parsedate_to_datetime

import feedparser
import trafilatura
import requests
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import create_engine, text

from .settings import CFG, POSTGRES_URL

# ---------- Configurable knobs (env) ----------
RSS_TIMEOUT = int(os.getenv("RSS_TIMEOUT", "20"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_BACKOFF = float(os.getenv("HTTP_BACKOFF", "0.5"))
MIN_BODY_LEN_WARN = int(os.getenv("MIN_BODY_LEN_WARN", "80"))  # warn if shorter, but DO NOT discard

# ---------- Logging & timeouts ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
socket.setdefaulttimeout(RSS_TIMEOUT)

# Silence noisy internal logs from trafilatura/readability
for _name in ("trafilatura", "trafilatura.core", "trafilatura.utils",
              "readability", "readability.readability"):
    try:
        _logger = logging.getLogger(_name)
        _logger.setLevel(logging.CRITICAL)
        _logger.propagate = False
        if not _logger.handlers:
            _logger.addHandler(logging.NullHandler())
    except Exception:
        pass

# Realistic desktop UAs
UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
UA_FIREFOX = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) "
    "Gecko/20100101 Firefox/128.0"
)
FEED_UA = UA_CHROME

PAGE_HEADERS_PRIMARY = {
    "User-Agent": UA_CHROME,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
PAGE_HEADERS_FALLBACK = {
    **PAGE_HEADERS_PRIMARY,
    "User-Agent": UA_FIREFOX,
    "Referer": "https://news.google.com/",
}

# ---------- SQL (typed literals for optional fields) ----------
INSERT_SQL = text("""
INSERT INTO items (
  id, src, chan, url, ts, lang, lang_conf, title, text, entities, topics, keywords, hash_sim
) VALUES (
  :id, 'rss', NULL, :url, :ts, NULL, NULL, :title, :text,
  '{}'::jsonb, '{}'::text[], '{}'::text[], NULL
) ON CONFLICT (id) DO NOTHING
""")

# ---------- HTTP session with retries ----------
def make_session():
    s = requests.Session()
    retries = Retry(
        total=HTTP_RETRIES,
        backoff_factor=HTTP_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

SESSION = make_session()

def origin(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url)
        return f"{u.scheme}://{u.netloc}"
    except Exception:
        return ""

# ---------- Reddit URL expansion ----------
def reddit_urls_from_cfg(cfg) -> list[str]:
    urls: list[str] = []
    sources = (cfg.get("sources") or {})
    red = (sources.get("reddit") or {})

    subs = red.get("subs") or []
    if isinstance(subs, (list, tuple)):
        urls.extend(subs)

    search_rss = red.get("search_rss") or {}
    if isinstance(search_rss, dict):
        for lst in search_rss.values():
            if isinstance(lst, (list, tuple)):
                urls.extend(lst)

    per_sub = red.get("per_sub_search") or {}
    if isinstance(per_sub, dict):
        for topics in per_sub.values():
            if isinstance(topics, dict):
                for lst in topics.values():
                    if isinstance(lst, (list, tuple)):
                        urls.extend(lst)

    out, seen = [], set()
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out

# ---------- Feed helpers ----------
def safe_parse_feed(url: str):
    """Parse a feed without crashing the run."""
    try:
        fp = feedparser.parse(url, request_headers={"User-Agent": FEED_UA})
    except IncompleteRead as e:
        logging.warning("[WARN] Incomplete read on %s: %s", url, e)
        return None
    except Exception as e:
        logging.error("[ERROR] Failed to fetch %s: %s", url, e)
        return None

    if getattr(fp, "bozo", 0):
        logging.warning("[WARN] Bozo feed on %s: %s", url, getattr(fp, "bozo_exception", ""))
    return fp

def pick_url(entry) -> str | None:
    url = getattr(entry, "link", None) or getattr(entry, "id", None)
    if not url:
        links = getattr(entry, "links", None) or []
        for l in links:
            href = l.get("href")
            if href:
                url = href
                break
    return url

def parse_ts(entry) -> datetime:
    """Return an aware UTC datetime for the entry."""
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        st = getattr(entry, key, None)
        if st:
            return datetime(*st[:6], tzinfo=timezone.utc)
    for key in ("published", "updated", "created"):
        s = getattr(entry, key, None)
        if s:
            try:
                dt = parsedate_to_datetime(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)

# ---------- Page fetching / AMP fallback ----------
def amp_variants(url: str):
    variants = []
    u = urllib.parse.urlsplit(url)
    p = u.path + ("" if u.path.endswith("/") else "/")
    variants.append(urllib.parse.urlunsplit((u.scheme, u.netloc, p + "amp/", u.query, u.fragment)))
    variants.append(urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, "amp=1", u.fragment)))
    variants.append(urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, "outputType=amp", u.fragment)))
    return variants

def fetch_html(url: str) -> str | None:
    try:
        hdrs = {**PAGE_HEADERS_PRIMARY, "Referer": origin(url) or "https://www.google.com/"}
        r = SESSION.get(url, headers=hdrs, timeout=RSS_TIMEOUT, allow_redirects=True)
        if r.status_code == 403:
            r = SESSION.get(url, headers=PAGE_HEADERS_FALLBACK, timeout=RSS_TIMEOUT, allow_redirects=True)
        if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith(("text/html", "application/xhtml")):
            return r.text
        if r.status_code in (401, 403, 404):
            for amp in amp_variants(url):
                r2 = SESSION.get(amp, headers=hdrs, timeout=RSS_TIMEOUT, allow_redirects=True)
                if r2.status_code == 200 and r2.headers.get("content-type", "").lower().startswith(("text/html", "application/xhtml")):
                    logging.info("Using AMP variant for %s -> %s", url, amp)
                    return r2.text
        logging.warning("non-200 response: %s for URL %s", r.status_code, url)
        return None
    except Exception as e:
        logging.warning("[WARN] requests failed for %s: %s", url, e)
        return None

# ---------- Safe HTML → text handling ----------
TAG_RE = re.compile(r"<[^>]+>")

def extract_html_like(html_str: str, url: str | None) -> str | None:
    if not html_str or len(html_str) < 30:
        return None

    looks_like_html = "<" in html_str and ">" in html_str
    if looks_like_html:
        try:
            txt = trafilatura.extract(
                html_str,
                include_comments=False,
                include_tables=False,
                favor_recall=True,
                url=url,
            )
            if txt and txt.strip():
                return txt.strip()
        except Exception:
            pass
        try:
            stripped = TAG_RE.sub("", html_str)
            stripped = unescape(stripped).strip()
            return stripped or None
        except Exception:
            return None
    else:
        return unescape(html_str).strip() or None

def extract_from_entry_html(entry, url: str | None) -> str | None:
    best = None
    try:
        for c in (getattr(entry, "content", None) or []):
            html_val = c.get("value")
            if not html_val:
                continue
            txt = extract_html_like(html_val, url)
            if txt and (best is None or len(txt) > len(best)):
                best = txt
    except Exception:
        pass
    try:
        sd = getattr(entry, "summary_detail", None) or {}
        if sd.get("value"):
            txt = extract_html_like(sd["value"], url)
            if txt and (best is None or len(txt) > len(best)):
                best = txt
    except Exception:
        pass
    return best

def extract_text(entry, url: str | None) -> str:
    txt = extract_from_entry_html(entry, url)
    if txt:
        return txt

    if url:
        html_doc = fetch_html(url)
        if html_doc:
            txt2 = extract_html_like(html_doc, url)
            if txt2:
                return txt2

    return (getattr(entry, "summary", "") or getattr(entry, "description", "") or "").strip()

def make_doc_id(url: str | None, title: str | None, entry_id: str | None) -> str:
    base = "|".join([p for p in (url, title, entry_id) if p]) or str(time.time())
    return hashlib.sha256(base.encode("utf-8", "ignore")).hexdigest()

def clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[\u200B-\u200F\uFEFF]", "", s)
    return s

# ---------- Main ----------
def run():
    # 1) Start with RSS list
    rss_feeds = ((CFG.get("sources") or {}).get("rss")) or []
    if not isinstance(rss_feeds, (list, tuple)):
        logging.warning("[WARN] CFG['sources']['rss'] is not a list; skipping.")
        rss_feeds = []

    # 2) Merge in Reddit feeds/searches from config
    merged = list(rss_feeds) + reddit_urls_from_cfg(CFG)

    # 3) Deduplicate while preserving order
    feeds, seen = [], set()
    for u in merged:
        if u and u not in seen:
            seen.add(u)
            feeds.append(u)

    logging.info("Total feed URLs (rss + reddit): %d", len(feeds))

    eng = create_engine(POSTGRES_URL, pool_pre_ping=True)

    with eng.begin() as con:
        total_items = 0
        for feed_url in feeds:
            logging.info("Fetching feed: %s", feed_url)
            fp = safe_parse_feed(feed_url)
            if not fp or not getattr(fp, "entries", None):
                continue

            for entry in fp.entries:
                url = pick_url(entry)
                title = getattr(entry, "title", None)
                ts = parse_ts(entry)

                text_content = extract_text(entry, url)
                text_trunc = text_content[:8000]
                if len(text_trunc) < MIN_BODY_LEN_WARN:
                    logging.warning("short body (%d chars), keeping: %s", len(text_trunc), url or (title or ""))

                entry_id = getattr(entry, "id", None)
                docid = make_doc_id(url, title, entry_id)

                try:
                    con.execute(
                        INSERT_SQL,
                        {"id": docid, "url": url, "ts": ts, "title": title or "", "text": text_trunc},
                    )
                    total_items += 1
                except Exception as ex:
                    logging.error("[ERROR] DB insert failed for %s: %s", url, ex)

        logging.info("RSS collected. Inserted (or skipped on conflict): %d items.", total_items)

if __name__ == "__main__":
    run()

