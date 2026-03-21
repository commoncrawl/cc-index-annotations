#!/usr/bin/env python3
"""Fetch domains from Wikipedia category-based website lists via Wikidata P856.

Default mode: 6 curated categories (fast, ~160 domains)
With --deep: recursively walks 80+ topic categories (slow, 10K-50K domains)
"""
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
DEEP = '--deep' in sys.argv
NO_SKIP = '--no-skip' in sys.argv
CI = '--ci' in sys.argv

# CURATED CATEGORIES (default mode)
CURATED_CATEGORIES = {
    'fake_news': 'Category:Fake news websites',
    'fact_checking': 'Category:Fact-checking websites',
    'satirical': 'Category:Satirical websites',
    'holocaust_denial': 'Category:Holocaust-denying websites',
    'alt_right': 'Category:Alt-right websites',
    'disinformation': 'Category:Disinformation operations',
}

# TOPIC CATEGORIES (--deep mode, recursive)
TOPIC_ROOT = 'Category:Websites by topic'
MAX_DEPTH = 4
SKIP_CATEGORIES = {
    'Category:Blogs by subject',
    'Category:Wikis by topic',
    'Category:Video hosting',
    'Category:Webmail',
    'Category:Web directories',
    'Category:Digital marketing companies',
    'Category:Internet streaming services',
    'Category:Online marketplaces',
    'Category:Social networking websites',
    'Category:Online dating services',
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
        return json.loads(open(path, 'rb').read())
    time.sleep(SLEEP_BETWEEN)
    data = fetch_json(url)
    if data:
        with open(path, 'wb') as f:
            f.write(json.dumps(data).encode('utf-8'))
    return data


# CATEGORY MEMBERS (pages + optional subcats)
def get_category_members(category, include_subcats=False):
    pages, subcats = [], []
    cmcontinue = ''
    page_num = 0
    cmtype = 'page|subcat' if include_subcats else 'page'
    while True:
        cont = f'&cmcontinue={cmcontinue}' if cmcontinue else ''
        url = (f'https://en.wikipedia.org/w/api.php?action=query'
               f'&list=categorymembers&cmtitle={quote(category)}'
               f'&cmtype={cmtype}&cmlimit=500&format=json{cont}')
        safe_cat = re.sub(r'[^\w\-.]', '_', category)
        filename = f'cat_{safe_cat}_{page_num}.json'
        data = fetch_cached(filename, url)
        if not data:
            break
        for m in data.get('query', {}).get('categorymembers', []):
            if m['ns'] == 0:
                pages.append(m['title'])
            elif m['ns'] == 14:
                subcats.append(m['title'])
        cmcontinue = data.get('continue', {}).get('cmcontinue', '')
        if not cmcontinue:
            break
        page_num += 1
    return pages, subcats


def walk_category_tree(root, max_depth=MAX_DEPTH):
    visited = set()
    all_pages = {}

    def walk(category, topic, depth):
        skip = set() if NO_SKIP else SKIP_CATEGORIES
        if category in visited or category in skip or depth > max_depth:
            return
        visited.add(category)
        pages, subcats = get_category_members(category, include_subcats=True)
        for title in pages:
            if title not in all_pages:
                all_pages[title] = set()
            all_pages[title].add(topic)
        for sub in subcats:
            walk(sub, topic, depth + 1)

    _, top_subcats = get_category_members(root, include_subcats=True)
    print(f'[deep] {len(top_subcats)} topic categories under {root}')
    skip = set() if NO_SKIP else SKIP_CATEGORIES
    for subcat in sorted(top_subcats):
        if subcat in skip:
            print(f'  skip: {subcat}')
            continue
        topic = subcat.replace('Category:', '').replace(' websites', '').replace(' ', '_').lower()
        print(f'  {topic}: {subcat}')
        walk(subcat, topic, 1)
        print(f'    -> {sum(1 for t, cats in all_pages.items() if topic in cats)} articles so far')

    return all_pages


# WIKIDATA P856 (official website)
def get_wikidata_urls(titles):
    results = {}
    batch_size = 50
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        if i % 500 == 0 and i > 0:
            print(f'  {i}/{len(titles)}')
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
    cats_to_fetch = CURATED_CATEGORIES
    if CI:
        cats_to_fetch = {'fact_checking': CURATED_CATEGORIES['fact_checking']}
        print('[ci] limited run: 1 category, max 50 articles')

    if DEEP and not CI:
        print('[deep] recursively walking Wikipedia topic categories')
        all_articles = walk_category_tree(TOPIC_ROOT)
        for cat_key, cat_title in cats_to_fetch.items():
            pages, _ = get_category_members(cat_title)
            print(f'  + curated {cat_key}: {len(pages)} articles')
            for title in pages:
                if title not in all_articles:
                    all_articles[title] = set()
                all_articles[title].add(cat_key)
        all_topics = set()
        for cats in all_articles.values():
            all_topics.update(cats)
    else:
        print('[categories] fetching curated category members')
        all_articles = {}
        for cat_key, cat_title in cats_to_fetch.items():
            pages, _ = get_category_members(cat_title)
            print(f'  {cat_key}: {len(pages)} articles')
            for title in pages:
                if title not in all_articles:
                    all_articles[title] = set()
                all_articles[title].add(cat_key)
        all_topics = set(CURATED_CATEGORIES.keys())

    titles = sorted(all_articles.keys())
    if CI:
        titles = titles[:50]
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

    sorted_topics = sorted(all_topics)
    for row in rows:
        for topic in sorted_topics:
            row[f'wikipedia_cat_{topic}'] = topic in row['categories']
        row['categories'] = ';'.join(sorted(row['categories']))

    rows.sort(key=lambda r: r['surt_host_name'])
    print(f'\nTotal unique domains: {len(rows)}')
    for topic in sorted_topics:
        col = f'wikipedia_cat_{topic}'
        cnt = sum(1 for r in rows if r[col])
        if cnt > 0:
            print(f'  {col}: {cnt}')

    import pyarrow as pa, pyarrow.parquet as pq
    schema = pa.schema([
        ('surt_host_name', pa.string()),
        ('domain', pa.string()),
        ('wikipedia_article', pa.string()),
        ('categories', pa.string()),
    ] + [
        (f'wikipedia_cat_{t}', pa.bool_()) for t in sorted_topics
    ])
    table = pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)
    pq.write_table(table, 'wikipedia-categories.parquet')
    print(f'Wrote wikipedia-categories.parquet')

    if DEBUG:
        import pyarrow.csv as csv
        csv.write_csv(table, 'wikipedia-categories.csv')
        print(f'Wrote wikipedia-categories.csv')


if __name__ == '__main__':
    main()
