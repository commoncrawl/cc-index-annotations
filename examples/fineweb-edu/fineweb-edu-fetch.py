import os
import sys
import time
import random
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
import utils

DEBUG_CSV = False
CACHE_DIR = '.cache'
HF_BASE = 'https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu-llama3-annotations/resolve/refs%2Fconvert%2Fparquet/default/train'
NUM_FILES = 9
SLEEP_BETWEEN = 5
USER_AGENT = 'cc-index-annotations/1.0 (https://github.com/commoncrawl/cc-index-annotations)'
HF_TOKEN = os.environ.get('HF_TOKEN', '')


os.makedirs(CACHE_DIR, exist_ok=True)


def fetch_cached(url, filename):
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        print(f'  cached: {filename}')
        return path
    print(f'  fetching: {filename}')
    headers = {'User-Agent': USER_AGENT}
    if HF_TOKEN:
        headers['Authorization'] = f'Bearer {HF_TOKEN}'
    req = Request(url, headers=headers)
    retries = 5
    for attempt in range(retries):
        try:
            with urlopen(req, timeout=120) as r:
                data = r.read()
            with open(path, 'wb') as f:
                f.write(data)
            time.sleep(SLEEP_BETWEEN + random.uniform(0, 1))
            return path
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            print(f'    retry {attempt+1}/{retries}: {e}, waiting {wait:.1f}s')
            time.sleep(wait)
    print(f'    FAILED: {filename}')
    return None


def url_to_domain(url):
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return None


def domain_to_surt(domain):
    if not domain:
        return None
    try:
        return utils.thing_to_surt_host_name('http://' + domain + '/')
    except Exception:
        return None


# FINEWEB-EDU LLAMA ANOTATIONS
print('[fineweb-edu] downloading annotation files from HuggingFace')
parquet_files = []
for i in range(NUM_FILES):
    filename = f'annotations-{i:04d}.parquet'
    url = f'{HF_BASE}/{i:04d}.parquet'
    path = fetch_cached(url, filename)
    if path:
        parquet_files.append(path)

print(f'  -> {len(parquet_files)} files downloaded')

# AGGREGATE TO DOMAIN LEVEL
print('[fineweb-edu] rollup to domain-level scores')
con = duckdb.connect()

file_list = ', '.join(f"'{f}'" for f in parquet_files)
con.sql(f"""
    CREATE TABLE raw AS
    SELECT
        metadata.url as url,
        metadata.dump as dump,
        metadata.language_score as language_score,
        score as edu_score
    FROM read_parquet([{file_list}])
""")

total = con.sql("SELECT count(*) FROM raw").fetchone()[0]
print(f'  -> {total} total annotations')

con.sql("""
    CREATE TABLE domain_agg AS
    SELECT
        split_part(split_part(url, '://', 2), '/', 1) as domain,
        count(*) as fineweb_edu_pages,
        round(avg(edu_score), 2) as fineweb_edu_avg_score,
        max(edu_score) as fineweb_edu_max_score,
        round(avg(language_score), 3) as fineweb_edu_avg_lang_score
    FROM raw
    WHERE url IS NOT NULL
    GROUP BY domain
""")

domains = con.sql("SELECT count(*) FROM domain_agg").fetchone()[0]
print(f'  -> {domains} unique domains')

# ADD SURT
print('[fineweb-edu] computing SURT host names')
rows = con.sql("SELECT domain, fineweb_edu_pages, fineweb_edu_avg_score, fineweb_edu_max_score, fineweb_edu_avg_lang_score FROM domain_agg").fetchall()

surt_names = []
out_domains = []
out_pages = []
out_avg = []
out_max = []
out_lang = []

for domain, pages, avg_score, max_score, lang_score in rows:
    s = domain_to_surt(domain)
    if s:
        surt_names.append(s)
        out_domains.append(domain)
        out_pages.append(pages)
        out_avg.append(avg_score)
        out_max.append(max_score)
        out_lang.append(lang_score)

table = pa.table({
    'surt_host_name': pa.array(surt_names, type=pa.string()),
    'domain': pa.array(out_domains, type=pa.string()),
    'fineweb_edu_pages': pa.array(out_pages, type=pa.int32()),
    'fineweb_edu_avg_score': pa.array(out_avg, type=pa.float32()),
    'fineweb_edu_max_score': pa.array(out_max, type=pa.int32()),
    'fineweb_edu_avg_lang_score': pa.array(out_lang, type=pa.float32()),
})

print(f'\nRows: {table.num_rows}')

pq.write_table(table, 'fineweb-edu.parquet')
print('Wrote fineweb-edu.parquet')

if DEBUG_CSV:
    import pyarrow.csv as csv
    csv.write_csv(table, 'fineweb-edu.csv')
    print('Wrote fineweb-edu.csv')
