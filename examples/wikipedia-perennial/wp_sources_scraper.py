#!/usr/bin/env python3
"""Scrape Wikipedia's source reliability lists into structured data.

Outputs: .json (always), .parquet (if pyarrow available), .tsv (if DEBUG).
Requires network access to en.wikipedia.org.
"""
from __future__ import annotations

import json, re, csv, sys, time
from urllib.request import urlopen, Request
from urllib.parse import quote

UA = "WPSourcesScraper/1.0 (research; contact@example.com)"

# ── Pages to scrape ──────────────────────────────────────────────────────────
PAGES = {
    # ── Main RSP table (subpages 1-9 + X) ────────────────────────────────
    "rsp": [f"Wikipedia:Reliable_sources/Perennial_sources/{i}" for i in range(1, 10)],
    "rsp_x": ["Wikipedia:Reliable_sources/Perennial_sources/X"],
    "rsp_further": ["Wikipedia:Reliable_sources/Perennial_sources/Further_classification"],
    # ── Core Wikipedia-wide lists ─────────────────────────────────────────
    "deprecated": ["Wikipedia:Deprecated_sources"],
    "deprecated_domains": ["Wikipedia:Deprecated_sources/Domains"],
    "potentially_unreliable": ["Wikipedia:Potentially_unreliable_sources"],
    "ext_links": ["Wikipedia:External_links/Perennial_websites"],
    "nppsg": ["Wikipedia:New_pages_patrol_source_guide"],
    "suggested": ["Wikipedia:Suggested_sources"],
    "genealogy": ["Wikipedia:Genealogy_sources"],
    "free_online": ["Wikipedia:List_of_free_online_resources"],
    "record_charts": ["Wikipedia:Record_charts"],
    "sci_tech": ["Wikipedia:Current_science_and_technology_sources"],
    "south_african": ["Wikipedia:Reliable_South_African_Sources"],
    "vaccine": ["Wikipedia:Vaccine_safety/Sources"],
    "vaccine_perennial": ["Wikipedia:Vaccine_safety/Perennial_sources"],
    # ── Regional / country WikiProjects ───────────────────────────────────
    "korea": ["Wikipedia:WikiProject_Korea/Reliable_sources"],
    "japan": ["Wikipedia:WikiProject_Japan/Reliable_sources"],
    "africa": ["Wikipedia:WikiProject_Africa/Africa_Sources_List"],
    "nigeria": ["Wikipedia:WikiProject_Nigeria/Nigerian_sources"],
    "philippines": ["Wikipedia:Tambayan_Philippines/Sources"],
    "india": ["Wikipedia:WikiProject_India/Resources"],
    "malaysia": ["Wikipedia:WikiProject_Malaysia/Resources"],
    "guyana": ["Wikipedia:WikiProject_Guyana/Reliable_sources"],
    "venezuela": ["Wikipedia:WikiProject_Venezuela/Reliable_and_unreliable_sources"],
    "peru": ["Wikipedia:WikiProject_Peru/Reliable_and_unreliable_sources"],
    "mongols": ["Wikipedia:WikiProject_Mongols/Reliable_sources"],
    "oregon": ["Wikipedia:WikiProject_Oregon/Reference_desk"],
    # ── Entertainment / media WikiProjects ─────────────────────────────────
    "videogames": ["Wikipedia:WikiProject_Video_games/Sources"],
    "film": ["Wikipedia:WikiProject_Film/Resources"],
    "afrocine": ["Wikipedia:WikiProject_AfroCine/Reliable_Sources"],
    "music": ["Wikipedia:WikiProject_Albums/Sources"],
    "music_about": ["Wikipedia:WikiProject_Albums/Sources/About.com_Critics_Table"],
    "christianmusic": ["Wikipedia:WikiProject_Christian_music/Sources"],
    "latinmusic": ["Wikipedia:WikiProject_Latin_music/Resources"],
    "songcontests": ["Wikipedia:WikiProject_Song_Contests/Sources"],
    "horror": ["Wikipedia:WikiProject_Horror/Sources"],
    "television": ["Wikipedia:WikiProject_Television/Reliable_sources"],
    "westerns": ["Wikipedia:WikiProject_Westerns/Television/Sources"],
    "anime": ["Wikipedia:WikiProject_Anime_and_manga/Online_reliable_sources"],
    "comics": ["Wikipedia:WikiProject_Comics/References"],
    "scifi": ["Wikipedia:WikiProject_Science_Fiction/References"],
    "webcomics": ["Wikipedia:WikiProject_Webcomics/Sources"],
    "novels_fantasy": ["Wikipedia:WikiProject_Novels/Fantasy_task_force/References"],
    "wrestling": ["Wikipedia:WikiProject_Professional_wrestling/Sources"],
    "percussion": ["Wikipedia:WikiProject_Percussion/Resources"],
    "beauty_pageants": ["Wikipedia:WikiProject_Beauty_Pageants/Sources"],
    "dnd": ["Wikipedia:WikiProject_Dungeons_&_Dragons/References"],
    "board_games": ["Wikipedia:WikiProject_Board_and_table_games/Sources"],
    "conservatism": ["Wikipedia:WikiProject_Conservatism/References"],
    # ── Sports WikiProjects ───────────────────────────────────────────────
    "ice_hockey": ["Wikipedia:WikiProject_Ice_Hockey/Sources"],
    "cricket": ["Wikipedia:WikiProject_Cricket/Sources"],
    "college_football": ["Wikipedia:WikiProject_College_football/Reliable_sources"],
    "arena_football": ["Wikipedia:WikiProject_Arena_Football_League/Reliable_Sources"],
    "nba": ["Wikipedia:WikiProject_National_Basketball_Association/References"],
    "baseball": ["Wikipedia:WikiProject_Baseball/Resource_library"],
    "motorsport": ["Wikipedia:WikiProject_Motorsport/Sources"],
    "football_shef": ["Wikipedia:WikiProject_Football/Sheffield_Wednesday_task_force/Sources"],
    # ── Science / academic WikiProjects ────────────────────────────────────
    "medicine": ["Wikipedia:WikiProject_Medicine/Reliable_sources"],
    "economics": ["Wikipedia:WikiProject_Economics/Reliable_sources_and_weight"],
    "covid": ["Wikipedia:WikiProject_COVID-19/Reference_sources"],
    "math": ["Wikipedia:WikiProject_Mathematics/Reference_resources"],
    "aircraft_engines": ["Wikipedia:WikiProject_Aircraft/Engines/Reference_sources"],
    "weather": ["Wikipedia:WikiProject_Weather/Sources"],
    # ── Other WikiProjects ────────────────────────────────────────────────
    "dogs": ["Wikipedia:WikiProject_Dogs/Reliable_sources"],
    "birds": ["Wikipedia:WikiProject_Birds/References"],
    "lds": ["Wikipedia:WikiProject_Latter_Day_Saint_movement/Sources"],
    "foss": ["Wikipedia:WikiProject_Software/Free_and_open-source_software_task_force/List_of_reliable_sources"],
    "timeline": ["Wikipedia:WikiProject_Timeline_Tracer/Reliable_sources"],
}

# RSP CSS classes on |- rows
CSS_STATUS = {
    "s-gr": "generally_reliable",
    "s-nc": "no_consensus",
    "s-gu": "generally_unreliable",
    "s-d":  "deprecated",
    "s-b":  "blacklisted",
}
# {{WP:RSPSTATUS|XX}} values
TMPL_STATUS = {
    "gr": "generally_reliable", "nc": "no_consensus",
    "gu": "generally_unreliable", "d": "deprecated", "b": "blacklisted",
}


def fetch_raw(title: str) -> str:
    url = f"https://en.wikipedia.org/w/index.php?title={quote(title)}&action=raw"
    req = Request(url, headers={"User-Agent": UA})
    try:
        with urlopen(req, timeout=30) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  WARN: {title}: {e}", file=sys.stderr)
        return ""


def clean(s: str) -> str:
    s = re.sub(r"'''?", "", s)
    s = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]*)\]\]', r'\1', s)
    s = re.sub(r'\[https?://\S+\s*([^\]]*)\]', r'\1', s)
    s = re.sub(r'<ref[^>]*>.*?</ref>', '', s, flags=re.S)
    s = re.sub(r'<ref[^/]*/>', '', s)
    s = re.sub(r'<[^>]+>', '', s)
    s = re.sub(r'\{\{[^}]*\}\}', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def extract_domains(text: str) -> list:
    return list(set(re.findall(
        r'(?:https?://)?(?:www\.)?([a-z0-9][-a-z0-9]*\.[a-z]{2,}(?:\.[a-z]{2,})?)',
        text, re.I
    )))


# ── RSP parser ──────────────────────────────────────────────────────────────
# Actual row format (from WP:RSP/Instructions):
#   |- class="s-gr" id="Source_Name"
#   | data-sort-value="..." | Source Name {{small|(...)}}
#   |                          <-- status column (icon template)
#   | {{rsnl|...}} +N          <-- discussion links
#   | {{WP:RSPLAST|2025}}      <-- last discussed
#   | Summary of discussions.  <-- summary text

def parse_rsp(wikitext: str) -> list:
    rows = []
    # Split on row delimiters. Each chunk starts after \n|-
    raw_rows = re.split(r'\n\|-', wikitext)

    for raw in raw_rows:
        # 1) Status from CSS class
        class_m = re.search(r'class="([^"]*)"', raw)
        status = "unknown"
        if class_m:
            for cls in class_m.group(1).split():
                if cls in CSS_STATUS:
                    status = CSS_STATUS[cls]
                    break
        # Fallback: {{WP:RSPSTATUS|XX}} or {{RSPSTATUS|XX}}
        if status == "unknown":
            st_m = re.search(r'\{\{(?:WP:)?RSPSTATUS\|(\w+)', raw, re.I)
            if st_m:
                status = TMPL_STATUS.get(st_m.group(1).lower(), "unknown")
        if status == "unknown":
            continue  # not a data row (header, footer, etc.)

        # 2) Source name from id="..." attribute
        id_m = re.search(r'id="([^"]+)"', raw)
        source_id = id_m.group(1).replace("_", " ") if id_m else ""

        # 3) Split into cells on \n| (newline then pipe, not ||)
        cells = re.split(r'\n\s*\|(?!\|)', raw)

        # First data cell has the source name, possibly with data-sort-value
        source_name = ""
        if len(cells) > 1:
            c = cells[1]
            c = re.sub(r'data-sort-value="[^"]*"\s*\|', '', c)
            source_name = clean(c)

        if not source_name or len(source_name) < 2:
            source_name = source_id
        if not source_name or len(source_name) < 2:
            continue

        # 4) Summary is the last cell (typically cell index 5)
        summary = ""
        if len(cells) > 3:
            summary = clean(cells[-1])[:500]

        # 5) Domains from the full row text
        domains = extract_domains(raw)

        rows.append({
            "source": source_name,
            "status": status,
            "summary": summary,
            "domains": domains,
        })
    return rows


# ── Generic wiki table parser ────────────────────────────────────────────────
def parse_wiki_table(wikitext: str) -> list:
    rows = []
    tables = re.findall(r'\{\|.*?\|\}', wikitext, re.S)
    for table in tables:
        raw_rows = re.split(r'\n\|-', table)
        for raw in raw_rows[1:]:
            cells = re.split(r'\s*\|\|\s*', raw)
            if len(cells) < 2:
                cells = re.split(r'\n\s*\|(?!\|)', raw)
            cells = [c.strip() for c in cells if c.strip()]
            if len(cells) < 2:
                continue
            source = clean(cells[0]).lstrip('|').lstrip('!').strip()
            if not source or len(source) < 2 or source.startswith('!'):
                continue
            full = " ".join(cells).lower()
            status = "unknown"
            for kw, val in [
                ("generally unreliable", "generally_unreliable"),
                ("not reliable", "generally_unreliable"),
                ("unreliable", "generally_unreliable"),
                ("deprecated", "deprecated"),
                ("blacklist", "blacklisted"),
                ("situational", "no_consensus"),
                ("no consensus", "no_consensus"),
                ("generally reliable", "generally_reliable"),
                ("reliable", "generally_reliable"),
                ("usable", "generally_reliable"),
            ]:
                if kw in full:
                    status = val
                    break
            rows.append({
                "source": source,
                "status": status,
                "summary": clean(cells[-1])[:500] if len(cells) > 2 else "",
                "domains": extract_domains(raw),
            })
    return rows


# ── Bullet list parser ───────────────────────────────────────────────────────
def parse_bullets(wikitext: str, default_status: str = "unknown") -> list:
    rows = []
    for m in re.finditer(r'^\*+\s*(.+)', wikitext, re.M):
        text = m.group(1)
        source = clean(text.split('\u2013')[0].split('\u2014')[0])[:200]
        if not source or len(source) < 2:
            continue
        low = text.lower()
        status = default_status
        for kw, val in [
            ("unreliable", "generally_unreliable"),
            ("deprecated", "deprecated"),
            ("reliable", "generally_reliable"),
            ("situational", "no_consensus"),
        ]:
            if kw in low:
                status = val
                break
        rows.append({
            "source": source,
            "status": status,
            "summary": clean(text)[:500],
            "domains": extract_domains(text),
        })
    return rows


# ── SURT conversion ──────────────────────────────────────────────────────────
def to_surt(host: str) -> str:
    """Convert host to SURT format: 'www.dailymail.co.uk' -> 'uk,co,dailymail'."""
    if not host:
        return ""
    host = host.lower().strip(".")
    # Strip www. prefix
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    parts.reverse()
    return ",".join(parts)


# ── Status columns ───────────────────────────────────────────────────────────
STATUS_COLS = [
    "wikipedia_generally_reliable",
    "wikipedia_no_consensus",
    "wikipedia_generally_unreliable",
    "wikipedia_deprecated",
    "wikipedia_blacklisted",
]
STATUS_TO_COL = {
    "generally_reliable": "wikipedia_generally_reliable",
    "no_consensus": "wikipedia_no_consensus",
    "generally_unreliable": "wikipedia_generally_unreliable",
    "deprecated": "wikipedia_deprecated",
    "blacklisted": "wikipedia_blacklisted",
}

# Build list-id column names: wikipedia_list_rsp, wikipedia_list_spam, etc.
# Prefix with 'list_' to avoid collision with status columns (e.g. wikipedia_deprecated)
LIST_COLS = [f"wikipedia_list_{lid}" for lid in PAGES.keys()]


# ── Main ─────────────────────────────────────────────────────────────────────
def scrape_all() -> list:
    """Fetch and parse all pages, return raw entry dicts."""
    all_rows = []
    for list_id, pages in PAGES.items():
        print(f"[{list_id}]", file=sys.stderr)
        for page in pages:
            print(f"  {page}", file=sys.stderr)
            wt = fetch_raw(page)
            if not wt:
                continue
            time.sleep(1)

            if list_id in ("rsp", "rsp_x", "rsp_further"):
                rows = parse_rsp(wt)
            else:
                rows = parse_wiki_table(wt)
                if not rows:
                    rows = parse_bullets(wt)

            for r in rows:
                r["list_id"] = list_id
                r["page"] = page
            all_rows.extend(rows)
            print(f"    -> {len(rows)} entries", file=sys.stderr)

    # Dedupe by (source, list_id)
    seen = set()
    deduped = []
    for r in all_rows:
        key = (r["source"].lower(), r["list_id"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    print(f"\nRaw entries: {len(all_rows)}, deduped: {len(deduped)}", file=sys.stderr)
    return deduped


def explode_to_domain_rows(entries: list) -> list:
    """Explode entries to one row per domain. No-domain entries get one row with empty host.
    Then merge rows sharing the same host_name across lists."""

    # Step 1: Build (domain -> merged row data) mapping
    domain_data = {}  # key = host_name (lowercase, no www)

    for entry in entries:
        domains = entry.get("domains", [])
        if not domains:
            domains = [""]  # keep no-domain entries

        for raw_domain in domains:
            host = raw_domain.lower().strip(".") if raw_domain else ""
            if host.startswith("www."):
                host = host[4:]

            if host not in domain_data:
                domain_data[host] = {
                    "host_name": host,
                    "surt_host_name": to_surt(host),
                    "url": f"https://{host}" if host else "",
                    "wikipedia_source": [],
                    "wikipedia_source_name": [],
                    "wikipedia_status": set(),
                    "_status_flags": set(),
                    "_list_flags": set(),
                }

            d = domain_data[host]
            page = entry.get("page", "")
            if page and page not in d["wikipedia_source"]:
                d["wikipedia_source"].append(page)
            src = entry.get("source", "")
            if src and src not in d["wikipedia_source_name"]:
                d["wikipedia_source_name"].append(src)

            status = entry.get("status", "unknown")
            if status != "unknown":
                d["wikipedia_status"].add(status)
                col = STATUS_TO_COL.get(status)
                if col:
                    d["_status_flags"].add(col)

            list_id = entry.get("list_id", "")
            if list_id:
                d["_list_flags"].add(f"wikipedia_list_{list_id}")

    # Step 2: Flatten to final row dicts
    out = []
    for host, d in domain_data.items():
        row = {
            "surt_host_name": d["surt_host_name"],
            "host_name": d["host_name"],
            "url": d["url"],
            "wikipedia_source": "; ".join(d["wikipedia_source"]),
            "wikipedia_source_name": "; ".join(d["wikipedia_source_name"]),
            "wikipedia_status": "; ".join(sorted(d["wikipedia_status"])) if d["wikipedia_status"] else "",
        }
        # Status boolean columns
        for col in STATUS_COLS:
            row[col] = col in d["_status_flags"]
        # List boolean columns
        for col in LIST_COLS:
            row[col] = col in d["_list_flags"]
        out.append(row)

    # Sort by SURT for nice output
    out.sort(key=lambda r: (r["surt_host_name"], r["host_name"]))
    return out


def save(rows: list, prefix: str = "wp_sources"):
    """Save as .json, .parquet, and .tsv."""
    # Column order
    cols = (
        ["surt_host_name", "host_name", "url",
         "wikipedia_source", "wikipedia_source_name", "wikipedia_status"]
        + STATUS_COLS
        + LIST_COLS
    )

    # Ensure every row has all columns with False for missing booleans
    for r in rows:
        for c in cols:
            if c not in r:
                if c.startswith("wikipedia_") and c not in (
                    "wikipedia_source", "wikipedia_source_name", "wikipedia_status"
                ):
                    r[c] = False
                else:
                    r[c] = ""

    # JSON
    with open(f"{prefix}.json", "w") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    print(f"Wrote {prefix}.json ({len(rows)} rows)", file=sys.stderr)

    # Parquet
    try:
        import pandas as pd
        df = pd.DataFrame(rows, columns=cols)
        # Ensure bool columns are actual bools
        bool_cols = STATUS_COLS + LIST_COLS
        for c in bool_cols:
            df[c] = df[c].astype(bool)
        df.to_parquet(f"{prefix}.parquet", index=False)
        print(f"Wrote {prefix}.parquet", file=sys.stderr)
    except ImportError:
        print("WARN: pip install pyarrow for .parquet output", file=sys.stderr)
    except Exception as e:
        print(f"WARN: parquet failed: {e}", file=sys.stderr)

    # TSV (always, for debugging)
    with open(f"{prefix}.tsv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote {prefix}.tsv", file=sys.stderr)

    # Stats
    filled = sum(1 for r in rows if r["host_name"])
    empty = len(rows) - filled
    multi = sum(1 for r in rows if r.get("wikipedia_source", "").count(";") > 0)
    print(f"\nStats: {len(rows)} total rows, {filled} with domains, "
          f"{empty} without, {multi} appearing in multiple lists", file=sys.stderr)
    for col in STATUS_COLS:
        n = sum(1 for r in rows if r.get(col))
        print(f"  {col}: {n}", file=sys.stderr)


if __name__ == "__main__":
    entries = scrape_all()
    rows = explode_to_domain_rows(entries)
    save(rows)

