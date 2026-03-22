#!/usr/bin/env python3
import bz2
import csv
import io
import os
import random
import sys
import time
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.robotparser import RobotFileParser

import pandas as pd
import surt

debugging = False

UA = "spam-abuse-fetcher/1.0 (Common Crawl Foundation; https://github.com/commoncrawl/cc-index-annotations)"

URLHAUS_CSV = "https://urlhaus.abuse.ch/downloads/csv_online/"
PHISHTANK_CSV = "https://data.phishtank.com/data/online-valid.csv.bz2"
# OpenPhish excluded: robots.txt disallows /feed.txt
# Feodo Tracker excluded: datasets currently empty, IP-only (no domains)
UT1_BASE = "https://raw.githubusercontent.com/olbat/ut1-blacklists/master/blacklists"
UT1_CATEGORIES = ["malware", "phishing", "ddos", "cryptojacking"]

CACHE_DIR = ".cache"


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


def fetch_cached(url, filename, sleep_after=2.0):
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


def domain_from_url(url):
    try:
        host = urlparse(url).hostname
        if host:
            return host.lower().strip(".")
    except Exception:
        pass
    return None


def to_surt(domain):
    try:
        full = surt.surt(f"http://{domain}")
        return full.split(")/")[0]
    except Exception:
        return None


# URLHAUS
def parse_urlhaus(data):
    print("[urlhaus]", file=sys.stderr)
    domains = set()
    text = data.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row or row[0].startswith("#"):
            continue
        if len(row) >= 3:
            d = domain_from_url(row[2])
            if d:
                domains.add(d)
    print(f"  -> {len(domains)} domains", file=sys.stderr)
    return domains


# PHISHTANK
def parse_phishtank(data):
    print("[phishtank]", file=sys.stderr)
    try:
        text = bz2.decompress(data).decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  WARN: phishtank decompress failed: {e}", file=sys.stderr)
        return set()
    domains = set()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        d = domain_from_url(row.get("url", ""))
        if d:
            domains.add(d)
    print(f"  -> {len(domains)} domains", file=sys.stderr)
    return domains


# UT1
def parse_ut1_category(category, data):
    print(f"[ut1/{category}]", file=sys.stderr)
    domains = set()
    for line in data.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        d = line.lower().strip(".")
        if "." in d:
            domains.add(d)
    print(f"  -> {len(domains)} domains", file=sys.stderr)
    return domains


def collect_all():
    sources = {}

    data = fetch_cached(URLHAUS_CSV, "urlhaus_online.csv")
    if data:
        sources["urlhaus_malware"] = parse_urlhaus(data)

    data = fetch_cached(PHISHTANK_CSV, "phishtank_online.csv.bz2")
    if data:
        sources["phishtank_phishing"] = parse_phishtank(data)

    for cat in UT1_CATEGORIES:
        url = f"{UT1_BASE}/{cat}/domains"
        data = fetch_cached(url, f"ut1_{cat}_domains.txt")
        if data:
            sources[f"ut1_{cat}"] = parse_ut1_category(cat, data)

    return sources


def build_dataframe(sources):
    all_domains = set()
    for domains in sources.values():
        all_domains |= domains

    print(f"\nTotal unique domains: {len(all_domains)}", file=sys.stderr)

    rows = []
    for domain in sorted(all_domains):
        s = to_surt(domain)
        if not s:
            continue
        row = {"surt_host_name": s, "domain": domain}
        for source_name in sorted(sources.keys()):
            row[f"abuse_{source_name}"] = domain in sources[source_name]
        rows.append(row)

    df = pd.DataFrame(rows)
    bool_cols = [c for c in df.columns if c.startswith("abuse_")]
    for c in bool_cols:
        df[c] = df[c].astype(bool)
    df = df.sort_values("surt_host_name").reset_index(drop=True)
    return df


if __name__ == "__main__":
    sources = collect_all()
    if not sources:
        print("ERROR: no sources fetched", file=sys.stderr)
        sys.exit(1)

    df = build_dataframe(sources)

    print(f"\nRows: {len(df)}", file=sys.stderr)
    for col in [c for c in df.columns if c.startswith("abuse_")]:
        print(f"  {col}: {df[col].sum()}", file=sys.stderr)

    df.to_parquet("spam-abuse.parquet", index=False)
    print("Wrote spam-abuse.parquet", file=sys.stderr)

    if debugging:
        df.to_csv("spam-abuse.tsv", sep="\t", index=False)
        print("Wrote spam-abuse.tsv", file=sys.stderr)
