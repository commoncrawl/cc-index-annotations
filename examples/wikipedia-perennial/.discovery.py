#!/usr/bin/env python3
"""Discover all Wikipedia source/reliability list pages.

find wikipedia perennial sources and likely urls
"""
from __future__ import annotations
import json, time
from urllib.request import urlopen, Request
from urllib.parse import quote, urlencode

UA = "WPSourceDiscovery/1.0 (research; contact@example.com)"

def api(params: dict) -> dict:
    params["format"] = "json"
    params["formatversion"] = "2"
    url = "https://en.wikipedia.org/w/api.php?" + urlencode(params)
    req = Request(url, headers={"User-Agent": UA})
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def search_pages(query: str, ns: int = 4, limit: int = 50) -> list:
    """Search Wikipedia namespace (4=Wikipedia:) for pages matching query."""
    data = api({"action": "query", "list": "search", "srsearch": query,
                "srnamespace": str(ns), "srlimit": str(limit)})
    return [r["title"] for r in data.get("query", {}).get("search", [])]

def cat_members(cat: str, limit: int = 200) -> list:
    """Get all pages in a category."""
    data = api({"action": "query", "list": "categorymembers", "cmtitle": cat,
                "cmlimit": str(limit)})
    return [r["title"] for r in data.get("query", {}).get("categorymembers", [])]

def page_exists(title: str) -> bool:
    data = api({"action": "query", "titles": title})
    pages = data.get("query", {}).get("pages", [])
    return pages and pages[0].get("missing") is not True

# ── Discovery ────────────────────────────────────────────────────────────────
found = set()

# 1) Category that indexes WikiProject source lists
print("=== Category:WikiProject lists of reliable sources ===")
for p in cat_members("Category:WikiProject lists of reliable sources"):
    found.add(p)
    print(f"  {p}")
time.sleep(1)

# 2) Search for source/reliability pages in Wikipedia: namespace
queries = [
    "reliable sources WikiProject",
    "Sources WikiProject reliable",
    "Perennial sources",
    "reliable sources noticeboard",
    "deprecated sources",
    "spam blacklist",
    "source guide patrol",
    "WikiProject sources list",
]
for q in queries:
    print(f"\n=== Search: {q} ===")
    for p in search_pages(q):
        if any(kw in p.lower() for kw in ["source", "reliable", "blacklist", "deprecated", "spam"]):
            found.add(p)
            print(f"  {p}")
    time.sleep(1)

# 3) Check specific known/guessed titles
print("\n=== Checking specific titles ===")
guesses = [
    "Wikipedia:Reliable_sources/Perennial_sources",
    "Wikipedia:Deprecated_sources",
    "Wikipedia:Deprecated_sources/Domains",
    "MediaWiki:Spam-blacklist",
    "MediaWiki:Spam-whitelist",
    "Wikipedia:WikiProject_Video_games/Sources",
    "Wikipedia:WikiProject_Film/Resources",
    "Wikipedia:WikiProject_AfroCine/Reliable_Sources",
    "Wikipedia:WikiProject_Albums/Sources",
    "Wikipedia:WikiProject_Korea/Reliable_sources",
    "Wikipedia:WikiProject_Japan/Reliable_sources",
    "Wikipedia:WikiProject_Africa/Sources",
    "Wikipedia:WikiProject_Africa/Reliable_sources",
    "Wikipedia:WikiProject_Philippines/Sources",
    "Wikipedia:WikiProject_Philippines/Reliable_sources",
    "Wikipedia:WikiProject_Medicine/Reliable_sources",
    "Wikipedia:WikiProject_Horror/Sources",
    "Wikipedia:WikiProject_Christian_music/Sources",
    "Wikipedia:WikiProject_Latin_music/Resources",
    "Wikipedia:WikiProject_Economics/Reliable_sources_and_weight",
    "Wikipedia:WikiProject_Television/Sources",
    "Wikipedia:WikiProject_Television/Reliable_sources",
    "Wikipedia:WikiProject_Westerns/Television/Sources",
    "Wikipedia:WikiProject_Anime_and_manga/Reliable_sources",
    "Wikipedia:WikiProject_Anime_and_manga/Sources",
    "Wikipedia:WikiProject_Comics/Reliable_sources",
    "Wikipedia:WikiProject_Cricket/Sources",
    "Wikipedia:WikiProject_Football/Reliable_sources",
    "Wikipedia:WikiProject_Martial_arts/Reliable_sources",
    "Wikipedia:WikiProject_Professional_wrestling/Sources",
    "Wikipedia:WikiProject_Motorcycling/Sources",
    "Wikipedia:WikiProject_Automobiles/Sources",
    "Wikipedia:WikiProject_China/Reliable_sources",
    "Wikipedia:WikiProject_India/Reliable_sources",
    "Wikipedia:WikiProject_Russia/Reliable_sources",
    "Wikipedia:WikiProject_Israel_Palestine_Collaboration/Sources",
    "Wikipedia:WikiProject_Military_history/Reliable_sources",
    "Wikipedia:New_pages_patrol/Source_guide",
    "Wikipedia:New_pages_patrol/Source_guide/List",
    "Wikipedia:Suggested_sources",
    "Wikipedia:Genealogy_sources",
    "Wikipedia:Potentially_unreliable_sources",
    "Wikipedia:WikiProject_Podcasts/Sources",
    "Wikipedia:WikiProject_Law/Sources",
    "Wikipedia:External_links/Perennial_websites",
    "Wikipedia:Reliable_sources/Perennial_sources/Further_classification",
    "Wikipedia:Reliable_sources/Perennial_sources/reliable",
    "Wikipedia:Reliable_sources/Perennial_sources/X",
]
for title in guesses:
    exists = page_exists(title)
    tag = "OK" if exists else "404"
    print(f"  [{tag}] {title}")
    if exists:
        found.add(title)
    time.sleep(0.5)

# 4) Check RSP subpages
print("\n=== RSP subpages ===")
for i in range(1, 12):
    t = f"Wikipedia:Reliable_sources/Perennial_sources/{i}"
    exists = page_exists(t)
    tag = "OK" if exists else "404"
    print(f"  [{tag}] {t}")
    if exists:
        found.add(t)
    time.sleep(0.5)

# ── Summary ──────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"TOTAL UNIQUE PAGES FOUND: {len(found)}")
print(f"{'='*60}")
for p in sorted(found):
    print(p)

