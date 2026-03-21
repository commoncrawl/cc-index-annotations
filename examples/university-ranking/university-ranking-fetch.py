#!/usr/bin/env python3
import json
import os
import random
import re
import sys
import time
from html.parser import HTMLParser
from urllib.parse import urlparse, quote
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.robotparser import RobotFileParser

import pandas as pd
import surt

debugging = True

UA = "university-ranking-fetcher/1.0 (Common Crawl Foundation; https://github.com/commoncrawl/cc-index-annotations)"

CWUR_BASE = "https://cwur.org"
CWUR_LIST = f"{CWUR_BASE}/2025.php"

HIPO_URL = "https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json"

CACHE_DIR = ".cache"
SLEEP_BETWEEN = 1.5


def check_robots(url):
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(UA, url)
    except Exception:
        return True


def fetch(url, max_retries=5, initial_delay=2.0, respect_robots=True):
    if respect_robots and not check_robots(url):
        print(f"  SKIP (robots.txt): {url}", file=sys.stderr)
        return None

    delay = initial_delay
    for attempt in range(max_retries + 1):
        try:
            req = Request(url, headers={"User-Agent": UA})
            with urlopen(req, timeout=60) as r:
                return r.read()
        except (URLError, HTTPError, TimeoutError, OSError) as e:
            if attempt == max_retries:
                print(f"  FAIL after {max_retries + 1} attempts: {url}: {e}", file=sys.stderr)
                return None
            jitter = random.uniform(0, delay * 0.5)
            wait = delay + jitter
            print(f"  RETRY {attempt + 1}/{max_retries} ({e}), waiting {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            delay *= 2


def fetch_cached(url, filename, sleep_after=SLEEP_BETWEEN):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        print(f"  cached: {filename}", file=sys.stderr)
        with open(path, "rb") as f:
            return f.read()
    data = fetch(url)
    if data:
        with open(path, "wb") as f:
            f.write(data)
        time.sleep(sleep_after + random.uniform(0, 1))
    return data


def to_surt(domain):
    try:
        full = surt.surt(f"http://{domain}")
        return full.split(")/")[0]
    except Exception:
        return None


class CWURListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.in_td = False
        self.in_a = False
        self.current_href = None

    def handle_starttag(self, tag, attrs):
        if tag == "td":
            self.in_td = True
        if tag == "a" and self.in_td:
            d = dict(attrs)
            href = d.get("href", "")
            if href.startswith("2025/") and href.endswith(".php"):
                self.in_a = True
                self.current_href = href

    def handle_endtag(self, tag):
        if tag == "td":
            self.in_td = False
        if tag == "a":
            self.in_a = False

    def handle_data(self, data):
        if self.in_a and self.current_href:
            self.links.append((data.strip(), self.current_href))
            self.current_href = None


class CWURProfileParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}
        self.in_td = False
        self.tds = []
        self.current_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "td":
            self.in_td = True
            self.current_text = ""

    def handle_endtag(self, tag):
        if tag == "td":
            self.in_td = False
            self.tds.append(self.current_text.strip())
        if tag == "tr" and len(self.tds) == 2:
            label, val = self.tds[0], self.tds[1]
            if label == "Domain":
                self.fields["domain"] = val.lower().strip()
            for key_label in ["World Rank", "National Rank", "Education Rank",
                              "Employability Rank", "Faculty Rank", "Research Rank",
                              "Score"]:
                if label == key_label:
                    clean = val.split()[0].replace(",", "") if val else ""
                    key = key_label.lower().replace(" ", "_")
                    self.fields[key] = clean
            self.tds = []
        elif tag == "tr":
            self.tds = []

    def handle_data(self, data):
        if self.in_td:
            self.current_text += data


# HIPO
def fetch_hipo():
    print("[hipo] fetching university domains list", file=sys.stderr)
    data = fetch_cached(HIPO_URL, "hipo_universities.json")
    if not data:
        return {}
    entries = json.loads(data)
    domain_map = {}
    for entry in entries:
        country = entry.get("country", "")
        name = entry.get("name", "")
        for d in entry.get("domains", []):
            d = d.lower().strip(".")
            if d and "." in d:
                domain_map[d] = {"name": name, "country": country}
    print(f"  -> {len(domain_map)} domains", file=sys.stderr)
    return domain_map


# CWUR
def fetch_cwur_list():
    print("[cwur] fetching ranking list", file=sys.stderr)
    data = fetch_cached(CWUR_LIST, "cwur_2025_list.html")
    if not data:
        return []
    parser = CWURListParser()
    parser.feed(data.decode("utf-8", errors="replace"))
    print(f"  -> {len(parser.links)} universities found", file=sys.stderr)
    return parser.links


def fetch_cwur_profile(name, href):
    slug = href.replace("2025/", "").replace(".php", "")
    safe_slug = re.sub(r'[^\w\-.]', '_', slug)
    filename = f"cwur_profile_{safe_slug}.html"
    url = f"{CWUR_BASE}/{quote(href, safe='/')}"
    data = fetch_cached(url, filename, sleep_after=SLEEP_BETWEEN)
    if not data:
        return None
    parser = CWURProfileParser()
    parser.feed(data.decode("utf-8", errors="replace"))
    return parser.fields


def fetch_cwur_profiles(uni_links):
    print(f"[cwur] fetching {len(uni_links)} profiles", file=sys.stderr)
    profiles = {}
    for i, (name, href) in enumerate(uni_links):
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(uni_links)}", file=sys.stderr)
        fields = fetch_cwur_profile(name, href)
        if fields and "domain" in fields:
            print(f"  + {fields['domain']} ({name})", file=sys.stderr)
            fields["cwur_name"] = name
            profiles[fields["domain"]] = fields
    print(f"  -> {len(profiles)} profiles with domains", file=sys.stderr)
    return profiles


def build_dataframe(hipo, cwur):
    all_domains = set(hipo.keys()) | set(cwur.keys())
    print(f"\nTotal unique domains: {len(all_domains)}", file=sys.stderr)

    rows = []
    for domain in sorted(all_domains):
        s = to_surt(domain)
        if not s:
            continue

        in_hipo = domain in hipo
        in_cwur = domain in cwur

        row = {
            "surt_host_name": s,
            "domain": domain,
            "is_university": True,
            "in_hipo": in_hipo,
            "in_cwur": in_cwur,
            "country": hipo[domain]["country"] if in_hipo else "",
            "university_name": cwur[domain].get("cwur_name", "") if in_cwur else hipo.get(domain, {}).get("name", ""),
        }

        if in_cwur:
            p = cwur[domain]
            for field in ["world_rank", "national_rank", "education_rank",
                          "employability_rank", "faculty_rank", "research_rank"]:
                val = p.get(field, "")
                row[f"cwur_{field}"] = int(val) if val and val.isdigit() else None
            score = p.get("score", "")
            try:
                row["cwur_score"] = float(score)
            except (ValueError, TypeError):
                row["cwur_score"] = None
        else:
            for field in ["world_rank", "national_rank", "education_rank",
                          "employability_rank", "faculty_rank", "research_rank"]:
                row[f"cwur_{field}"] = None
            row["cwur_score"] = None

        rows.append(row)

    df = pd.DataFrame(rows)
    int_cols = [c for c in df.columns if c.startswith("cwur_") and c != "cwur_score"]
    for c in int_cols:
        df[c] = df[c].astype("Int64")
    df = df.sort_values("surt_host_name").reset_index(drop=True)
    return df


if __name__ == "__main__":
    include_cwur = "--include-cwur" in sys.argv or "--get-ranking" in sys.argv

    hipo = fetch_hipo()

    cwur = {}
    if include_cwur:
        uni_links = fetch_cwur_list()
        cwur = fetch_cwur_profiles(uni_links)

    if not hipo and not cwur:
        print("ERROR: no sources fetched", file=sys.stderr)
        sys.exit(1)

    df = build_dataframe(hipo, cwur)

    print(f"\nRows: {len(df)}", file=sys.stderr)
    print(f"  in_hipo: {df['in_hipo'].sum()}", file=sys.stderr)
    print(f"  in_cwur: {df['in_cwur'].sum()}", file=sys.stderr)
    print(f"  both: {(df['in_hipo'] & df['in_cwur']).sum()}", file=sys.stderr)

    df.to_parquet("university-ranking.parquet", index=False)
    print("Wrote university-ranking.parquet", file=sys.stderr)

    if debugging:
        df.to_csv("university-ranking.tsv", sep="\t", index=False)
        print("Wrote university-ranking.tsv", file=sys.stderr)
