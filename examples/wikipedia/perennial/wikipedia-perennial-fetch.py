#!/usr/bin/env python3
"""Fetch source reliability lists from multiple Wikipedia languages.

Reads languages.yaml for page URLs and parser types.
English (en) is handled by the existing wp_sources_scraper.py (standalone: true).
"""
import json, os, random, re, sys, time, yaml
from urllib.parse import urlparse, quote
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import utils

UA = 'CCIndexAnnotations/1.0 (https://github.com/commoncrawl/cc-index-annotations)'
SLEEP_BETWEEN = 1.5
CACHE_DIR = '.cache'
DEBUG = '--debug' in sys.argv or '-d' in sys.argv
DOMAIN_RE = re.compile(r'(?:https?://)?(?:www\.)?([a-z0-9][-a-z0-9]*(?:\.[a-z0-9][-a-z0-9]*)+)', re.IGNORECASE)


def fetch(url):
    req = Request(url, headers={'User-Agent': UA})
    for attempt in range(5):
        try:
            return urlopen(req, timeout=60).read()
        except (HTTPError, URLError, TimeoutError) as e:
            wait = (2 ** attempt) + random.random()
            print(f'  retry {attempt+1}/5 ({e}), waiting {wait:.1f}s')
            time.sleep(wait)
    return None


def fetch_cached(filename, url):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        print(f'  cached: {filename}')
        with open(path, 'rb') as f:
            return f.read()
    time.sleep(SLEEP_BETWEEN)
    data = fetch(url)
    if data:
        with open(path, 'wb') as f:
            f.write(data)
    return data


def fetch_wikitext(wiki, title):
    safe_title = quote(title, safe='/:')
    url = f'https://{wiki}/w/index.php?title={safe_title}&action=raw'
    safe = re.sub(r'[^\w.-]', '_', title)
    filename = f'{wiki}_{safe}.txt'
    data = fetch_cached(filename, url)
    return data.decode('utf-8', errors='replace') if data else ''


def extract_domains_generic(text):
    return list(set(m.lower() for m in DOMAIN_RE.findall(text) if '.' in m and len(m) > 4))


def clean_domain(d):
    d = d.lower().strip().rstrip('/')
    if d.startswith('www.'):
        d = d[4:]
    return d if '.' in d and len(d) > 3 else None


# FRENCH: {{Source ODS|nom=...|url={{Utilisations domaine|domain}}|résumé=...}}
def parse_source_ods(text, lang_config):
    pattern = lang_config.get('domain_pattern', r'Utilisations domaine\|([^}]+)')
    entries = re.split(r'\{\{Source ODS', text)[1:]
    rows = []
    for entry in entries:
        name_m = re.search(r'\|nom\s*=\s*(.+?)(?:\n|\|)', entry)
        name = name_m.group(1).strip() if name_m else 'unknown'
        domains = re.findall(pattern, entry)
        domains = [clean_domain(d) for d in domains]
        domains = [d for d in domains if d]
        for domain in domains:
            rows.append({'source_name': name, 'domain': domain})
    return rows


# GENERIC: extract domains from wiki table rows
def parse_wikitext_table(text, lang_config):
    rows = []
    domains_found = extract_domains_generic(text)
    sections = re.split(r'^==\s*(.+?)\s*==\s*$', text, flags=re.MULTILINE)
    current_section = ''
    for i, section in enumerate(sections):
        if i % 2 == 1:
            current_section = section.strip()
            continue
        for domain in extract_domains_generic(section):
            domain = clean_domain(domain)
            if domain:
                rows.append({'source_name': current_section or 'general', 'domain': domain})
    seen = set()
    deduped = []
    for r in rows:
        key = r['domain']
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


PARSERS = {
    'source_ods': parse_source_ods,
    'wikitext_table': parse_wikitext_table,
}


def main():
    lang_filter = None
    for arg in sys.argv[1:]:
        if arg.startswith('--lang='):
            lang_filter = arg.split('=', 1)[1]

    with open('languages.yaml', 'r', encoding='utf-8') as f:
        languages = yaml.safe_load(f)

    all_rows = []
    for lang, config in languages.items():
        if config.get('standalone'):
            print(f'[{lang}] standalone — skipping (use wp_sources_scraper.py)')
            continue
        if lang_filter and lang != lang_filter:
            continue

        wiki = config['wiki']
        parser_name = config.get('parser', 'wikitext_table')
        parser = PARSERS.get(parser_name)
        if not parser:
            print(f'[{lang}] unknown parser: {parser_name}, skipping')
            continue

        print(f'[{lang}] fetching from {wiki}')
        for page_info in config.get('pages', []):
            title = page_info if isinstance(page_info, str) else page_info['title']
            print(f'  page: {title}')
            text = fetch_wikitext(wiki, title)
            if not text:
                print(f'  -> empty/failed')
                continue
            rows = parser(text, config)
            for r in rows:
                r['wiki_lang'] = lang
                r['wiki_page'] = title
            print(f'  -> {len(rows)} domains')
            all_rows.extend(rows)

    for r in all_rows:
        r['surt_host_name'] = utils.thing_to_surt_host_name(r['domain'])
    all_rows = [r for r in all_rows if r['surt_host_name']]

    seen = {}
    for r in all_rows:
        key = (r['surt_host_name'], r['wiki_lang'])
        if key not in seen:
            seen[key] = r
        else:
            existing = seen[key]
            if r['source_name'] not in existing['source_name']:
                existing['source_name'] += '; ' + r['source_name']

    rows = sorted(seen.values(), key=lambda r: (r['wiki_lang'], r['surt_host_name']))
    print(f'\nTotal: {len(rows)} unique (domain, lang) pairs')
    by_lang = {}
    for r in rows:
        by_lang.setdefault(r['wiki_lang'], 0)
        by_lang[r['wiki_lang']] += 1
    for lang, cnt in sorted(by_lang.items()):
        print(f'  {lang}: {cnt}')

    import pyarrow as pa, pyarrow.parquet as pq
    schema = pa.schema([
        ('surt_host_name', pa.string()),
        ('domain', pa.string()),
        ('wiki_lang', pa.string()),
        ('wiki_page', pa.string()),
        ('source_name', pa.string()),
    ])
    table = pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)
    pq.write_table(table, 'wikipedia-perennial.parquet')
    print(f'Wrote wikipedia-perennial.parquet')

    if DEBUG:
        import pyarrow.csv as csv
        csv.write_csv(table, 'wikipedia-perennial.csv')
        print(f'Wrote wikipedia-perennial.csv')


if __name__ == '__main__':
    main()
