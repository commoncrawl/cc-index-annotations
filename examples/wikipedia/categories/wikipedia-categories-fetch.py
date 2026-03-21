#!/usr/bin/env python3
"""Fetch domains from Wikipedia category-based website lists via Wikidata P856."""
import json, os, random, re, sys, time
from urllib.parse import urlparse, quote
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import utils

UA = 'CCIndexAnnotations/1.0 (https://github.com/commoncrawl/cc-index-annotations)'
SLEEP_BETWEEN = 1.5
CACHE_DIR = '.cache'
DEBUG = '--debug' in sys.argv or '-d' in sys.argv

CATEGORIES = {
    'fake_news': 'Category:Fake news websites',
    'fact_checking': 'Category:Fact-checking websites',
    'satirical': 'Category:Satirical websites',
    'holocaust_denial': 'Category:Holocaust-denying websites',
    'alt_right': 'Category:Alt-right websites',
    'disinformation': 'Category:Disinformation operations',
}


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


def fetch_json(url):
    data = fetch(url)
    return json.loads(data) if data else None


def fetch_cached(filename, url):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, filename)
    if os.path.exists(path):
        print(f'  cached: {filename}')
        with open(path, 'rb') as f:
            return json.loads(f.read())
    time.sleep(SLEEP_BETWEEN)
    data = fetch_json(url)
    if data:
        with open(path, 'wb') as f:
            f.write(json.dumps(data).encode('utf-8'))
    return data


# CATEGORY MEMBERS
def get_category_members(category):
    members = []
    cmcontinue = ''
    page = 0
    while True:
        cont = f'&cmcontinue={cmcontinue}' if cmcontinue else ''
        url = (f'https://en.wikipedia.org/w/api.php?action=query'
               f'&list=categorymembers&cmtitle={quote(category)}'
               f'&cmtype=page&cmlimit=50&format=json{cont}')
        filename = f'cat_{quote(category, safe="")}_{page}.json'
        data = fetch_cached(filename, url)
        if not data:
            break
        for m in data.get('query', {}).get('categorymembers', []):
            if m['ns'] == 0:
                members.append(m['title'])
        cmcontinue = data.get('continue', {}).get('cmcontinue', '')
        if not cmcontinue:
            break
        page += 1
    return members


# WIKIDATA P856 (official website)
def get_wikidata_urls(titles):
    results = {}
    batch_size = 50
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        titles_str = '|'.join(quote(t, safe='') for t in batch)
        url = (f'https://en.wikipedia.org/w/api.php?action=query'
               f'&titles={titles_str}&prop=pageprops&ppprop=wikibase_item&format=json')
        filename = f'qids_{i}.json'
        data = fetch_cached(filename, url)
        if not data:
            continue

        qid_map = {}
        for pid, page in data.get('query', {}).get('pages', {}).items():
            qid = page.get('pageprops', {}).get('wikibase_item')
            if qid:
                qid_map[qid] = page['title']

        if not qid_map:
            continue

        qids = '|'.join(qid_map.keys())
        wd_url = (f'https://www.wikidata.org/w/api.php?action=wbgetentities'
                   f'&ids={qids}&props=claims&format=json')
        filename2 = f'wikidata_{i}.json'
        wd_data = fetch_cached(filename2, wd_url)
        if not wd_data:
            continue

        for qid, title in qid_map.items():
            entity = wd_data.get('entities', {}).get(qid, {})
            p856 = entity.get('claims', {}).get('P856', [])
            urls = []
            for claim in p856:
                try:
                    urls.append(claim['mainsnak']['datavalue']['value'])
                except (KeyError, TypeError):
                    pass
            if urls:
                results[title] = urls

    return results


def url_to_domain(url):
    try:
        parsed = urlparse(url if '://' in url else f'https://{url}')
        host = parsed.hostname or ''
        host = host.lower()
        if host.startswith('www.'):
            host = host[4:]
        return host if '.' in host else None
    except Exception:
        return None


def main():
    print('[categories] fetching category members')
    all_articles = {}
    for cat_key, cat_title in CATEGORIES.items():
        members = get_category_members(cat_title)
        print(f'  {cat_key}: {len(members)} articles')
        for title in members:
            if title not in all_articles:
                all_articles[title] = set()
            all_articles[title].add(cat_key)

    titles = sorted(all_articles.keys())
    print(f'  -> {len(titles)} unique articles')

    print('[wikidata] fetching P856 official website URLs')
    wikidata_urls = get_wikidata_urls(titles)
    print(f'  -> {len(wikidata_urls)} articles with URLs')

    rows = []
    seen_domains = {}
    for title, urls in wikidata_urls.items():
        cats = all_articles.get(title, set())
        for url in urls:
            domain = url_to_domain(url)
            if not domain:
                continue
            surt = utils.thing_to_surt_host_name(domain)
            if surt in seen_domains:
                for cat in cats:
                    seen_domains[surt]['categories'].add(cat)
                continue
            row = {
                'surt_host_name': surt,
                'domain': domain,
                'wikipedia_article': title,
                'categories': cats,
            }
            seen_domains[surt] = row
            rows.append(row)

    for row in rows:
        for cat_key in CATEGORIES:
            row[f'wikipedia_cat_{cat_key}'] = cat_key in row['categories']
        row['categories'] = ';'.join(sorted(row['categories']))

    rows.sort(key=lambda r: r['surt_host_name'])
    print(f'\nTotal unique domains: {len(rows)}')
    for cat_key in CATEGORIES:
        col = f'wikipedia_cat_{cat_key}'
        cnt = sum(1 for r in rows if r[col])
        print(f'  {col}: {cnt}')

    import duckdb, pyarrow as pa
    schema = pa.schema([
        ('surt_host_name', pa.string()),
        ('domain', pa.string()),
        ('wikipedia_article', pa.string()),
        ('categories', pa.string()),
    ] + [
        (f'wikipedia_cat_{k}', pa.bool_()) for k in CATEGORIES
    ])
    table = pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)
    import pyarrow.parquet as pq
    pq.write_table(table, 'wikipedia-categories.parquet')
    print(f'Wrote wikipedia-categories.parquet')

    if DEBUG:
        import pyarrow.csv as csv
        csv.write_csv(table, 'wikipedia-categories.csv')
        print(f'Wrote wikipedia-categories.csv')


if __name__ == '__main__':
    main()
