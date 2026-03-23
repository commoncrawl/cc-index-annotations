#!/usr/bin/env python3
import sys, os, json, duckdb, surt as surt_lib
from urllib.parse import urlparse
from urllib.request import urlopen, Request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import utils

DEBUG = '-d' in sys.argv or '--debug' in sys.argv
CACHE_DIR = '.cache'
SOURCE_URL = 'https://raw.githubusercontent.com/blekko/slashtag-data/master/slastag.json'
UA = 'cc-index-annotations/1.0 (https://github.com/commoncrawl/cc-index-annotations)'

os.makedirs(CACHE_DIR, exist_ok=True)

import time, random
def fetch_with_retry(url, retries=5):
    for attempt in range(retries):
        try:
            req = Request(url, headers={'User-Agent': UA})
            return urlopen(req, timeout=60).read()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f'  retry {attempt+1}/{retries} ({e}), waiting {wait:.1f}s')
            time.sleep(wait)

# SLASHTAG DATA (from blekko)
cache_file = os.path.join(CACHE_DIR, 'slastag.json')
if os.path.exists(cache_file):
    print(f'  cached: {cache_file}')
    data = json.load(open(cache_file))
else:
    print(f'[slashtag] fetching slashtag data')
    raw = fetch_with_retry(SOURCE_URL,10)
    data = json.loads(raw)
    with open(cache_file, 'wb') as f:
        f.write(raw)
print(f'  -> {len(data)} tags, {sum(len(v.get("urls", [])) for v in data.values())} URLs')

# EXTRACT — one row per (url, tag), compute surt
print('Extracting domains and computing SURTs...')
rows = []
skipped = 0
for tag, info in data.items():
    category = tag.split('/')[-1]
    for url in info.get('urls', []):
        if '*' in url:
            skipped += 1
            continue
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower().lstrip('www.')
            if not domain or '.' not in domain:
                continue
            surt_host = utils.thing_to_surt_host_name(domain)
            url_surtkey = surt_lib.surt(url)
            if surt_host:
                rows.append((surt_host, url_surtkey, domain, url, category))
        except (ValueError, TypeError):
            continue

print(f'  {len(rows)} rows ({skipped} wildcards skipped)')

# AGGREGATE — per-URL categories as LIST
con = duckdb.connect()
con.execute("CREATE TABLE raw (surt_host_name VARCHAR, surt_url_key VARCHAR, domain VARCHAR, url VARCHAR, category VARCHAR)")
con.executemany("INSERT INTO raw VALUES (?, ?, ?, ?, ?)", rows)

# URL-level: one row per (domain, url), categories as list
con.sql("""
CREATE TABLE slashtag_urls AS
SELECT surt_host_name, surt_url_key, domain, url,
  list(DISTINCT category ORDER BY category) as categories
FROM raw
GROUP BY surt_host_name, surt_url_key, domain, url
ORDER BY surt_host_name
""")

url_rows = con.sql("SELECT count(*) FROM slashtag_urls").fetchone()[0]
print(f'URL-level: {url_rows} rows')

con.sql("COPY slashtag_urls TO 'slashtag.parquet' (FORMAT PARQUET)")
print('Wrote slashtag.parquet')

# HOST-level: one row per domain, union of all URL categories
con.sql("""
CREATE TABLE slashtag_hosts AS
SELECT surt_host_name, domain,
  list(DISTINCT unnested ORDER BY unnested) as categories
FROM (SELECT surt_host_name, domain, unnest(categories) as unnested FROM slashtag_urls)
GROUP BY surt_host_name, domain
ORDER BY surt_host_name
""")

host_rows = con.sql("SELECT count(*) FROM slashtag_hosts").fetchone()[0]
print(f'Host-level: {host_rows} rows')

con.sql("COPY slashtag_hosts TO 'slashtag-hosts.parquet' (FORMAT PARQUET)")
print('Wrote slashtag-hosts.parquet')

if DEBUG:
    con.sql("COPY slashtag_urls TO 'slashtag.csv' (FORMAT CSV, HEADER)")
    con.sql("COPY slashtag_hosts TO 'slashtag-hosts.csv' (FORMAT CSV, HEADER)")
    print('Wrote slashtag.csv, slashtag-hosts.csv')
