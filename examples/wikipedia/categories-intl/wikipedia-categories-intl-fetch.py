#!/usr/bin/env python3
"""Fetch domains from Wikipedia category-based website lists across all languages.

Uses langlinks API to auto-discover international equivalents of English categories.

Default mode: 6 curated categories across all available languages (~500-2000 domains)
With --deep: recursively walks 80+ topic categories internationally (slow, 5K-20K domains)
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

CURATED_CATEGORIES = {
    'fake_news': 'Category:Fake news websites',
    'fact_checking': 'Category:Fact-checking websites',
    'satirical': 'Category:Satirical websites',
    'holocaust_denial': 'Category:Holocaust-denying websites',
    'alt_right': 'Category:Alt-right websites',
    'disinformation': 'Category:Disinformation operations',
}

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


# LANGLINKS — discover international equivalents of an English category
def get_langlinks(en_category):
    url = (f'https://en.wikipedia.org/w/api.php?action=query'
           f'&titles={quote(en_category)}&prop=langlinks&lllimit=500&format=json')
    safe = re.sub(r'[^\w\-.]', '_', en_category)
    data = fetch_cached(f'langlinks_{safe}.json', url)
    if not data:
        return {}
    pages = data.get('query', {}).get('pages', {})
    result = {}
    for page in pages.values():
        for ll in page.get('langlinks', []):
            result[ll['lang']] = ll['*']
    return result


# CATEGORY MEMBERS on any wiki
def get_category_members(wiki, category, include_subcats=False):
    pages, subcats = [], []
    cmcontinue = ''
    page_num = 0
    cmtype = 'page|subcat' if include_subcats else 'page'
    while True:
        cont = f'&cmcontinue={cmcontinue}' if cmcontinue else ''
        url = (f'https://{wiki}/w/api.php?action=query'
               f'&list=categorymembers&cmtitle={quote(category)}'
               f'&cmtype={cmtype}&cmlimit=500&format=json{cont}')
        safe_cat = re.sub(r'[^\w\-.]', '_', f'{wiki}_{category}')
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


def walk_foreign_category(wiki, category, topic, max_depth=MAX_DEPTH):
    visited = set()
    all_pages = []

    def walk(cat, depth):
        if cat in visited or depth > max_depth:
            return
        visited.add(cat)
        pages, subcats = get_category_members(wiki, cat, include_subcats=True)
        all_pages.extend(pages)
        for sub in subcats:
            walk(sub, depth + 1)

    walk(category, 1)
    return list(set(all_pages))


# WIKIDATA P856 — resolve article titles to domains (on any wiki)
def get_wikidata_urls(wiki, titles):
    results = {}
    batch_size = 50
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i+batch_size]
        titles_str = '|'.join(quote(t, safe='') for t in batch)
        url = (f'https://{wiki}/w/api.php?action=query'
               f'&titles={titles_str}&prop=pageprops&ppprop=wikibase_item&format=json')
        safe_wiki = wiki.replace('.', '_')
        filename = f'qids_{safe_wiki}_{i}.json'
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
        filename2 = f'wikidata_{safe_wiki}_{i}.json'
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


def discover_categories(en_categories):
    """Given {key: en_category_title}, return {key: [(lang, wiki, localized_title), ...]}."""
    intl = {}
    for key, en_title in en_categories.items():
        langlinks = get_langlinks(en_title)
        pairs = [(lang, f'{lang}.wikipedia.org', title) for lang, title in langlinks.items()]
        if pairs:
            print(f'  {key}: {len(pairs)} languages')
        else:
            print(f'  {key}: no international equivalents')
        intl[key] = pairs
    return intl


def main():
    # Step 1: discover international equivalents
    if DEEP:
        print('[deep] discovering international equivalents for topic categories')
        _, top_subcats = get_category_members('en.wikipedia.org', TOPIC_ROOT, include_subcats=True)
        skip = set() if NO_SKIP else SKIP_CATEGORIES
        en_cats = dict(CURATED_CATEGORIES)
        for subcat in sorted(top_subcats):
            if subcat in skip:
                continue
            key = subcat.replace('Category:', '').replace(' websites', '').replace(' ', '_').lower()
            en_cats[key] = subcat
        print(f'  {len(en_cats)} English categories to map internationally')
    else:
        en_cats = dict(CURATED_CATEGORIES)

    print('[langlinks] discovering international equivalents')
    intl_map = discover_categories(en_cats)

    # Count total (lang, category) pairs
    total_pairs = sum(len(v) for v in intl_map.values())
    total_langs = set()
    for pairs in intl_map.values():
        for lang, _, _ in pairs:
            total_langs.add(lang)
    print(f'  -> {total_pairs} (lang, category) pairs across {len(total_langs)} languages')

    # Step 2: fetch category members per (lang, category)
    print('[fetch] fetching category members internationally')
    # article_title -> {(lang, cat_key), ...}
    all_articles = {}  # key: (wiki, title) -> set of cat_keys
    all_langs = {}     # key: (wiki, title) -> lang

    for cat_key, pairs in intl_map.items():
        for lang, wiki, localized_title in pairs:
            if DEEP:
                members = walk_foreign_category(wiki, localized_title, cat_key)
            else:
                members, _ = get_category_members(wiki, localized_title)
            if members:
                print(f'  {lang}/{cat_key}: {len(members)} articles')
            for title in members:
                k = (wiki, title)
                if k not in all_articles:
                    all_articles[k] = set()
                    all_langs[k] = lang
                all_articles[k].add(cat_key)

    print(f'  -> {len(all_articles)} unique (wiki, article) pairs')

    # Step 3: resolve domains via Wikidata P856
    print('[wikidata] resolving domains via P856')
    # Group by wiki for batched lookups
    by_wiki = {}
    for (wiki, title) in all_articles:
        by_wiki.setdefault(wiki, []).append(title)

    wiki_urls = {}  # (wiki, title) -> [urls]
    for wiki, titles in sorted(by_wiki.items()):
        print(f'  {wiki}: {len(titles)} articles')
        urls = get_wikidata_urls(wiki, titles)
        for title, article_urls in urls.items():
            wiki_urls[(wiki, title)] = article_urls

    print(f'  -> {len(wiki_urls)} articles with URLs')

    # Step 4: build rows
    rows = []
    seen_domains = {}
    for (wiki, title), urls in wiki_urls.items():
        cat_keys = all_articles.get((wiki, title), set())
        lang = all_langs.get((wiki, title), '?')
        for url in urls:
            domain = url_to_domain(url)
            if not domain:
                continue
            surt = utils.thing_to_surt_host_name(domain)
            if surt in seen_domains:
                seen_domains[surt]['_cats'].update(cat_keys)
                seen_domains[surt]['_langs'].add(lang)
                continue
            row = {
                'surt_host_name': surt,
                'domain': domain,
                'wikipedia_article': title,
                '_cats': set(cat_keys),
                '_langs': {lang},
            }
            seen_domains[surt] = row
            rows.append(row)

    all_cat_keys = sorted(en_cats.keys())
    for row in rows:
        row['wiki_langs'] = ';'.join(sorted(row['_langs']))
        row['categories'] = ';'.join(sorted(row['_cats']))
        for key in all_cat_keys:
            row[f'wikipedia_cat_{key}'] = key in row['_cats']
        del row['_cats'], row['_langs']

    rows.sort(key=lambda r: r['surt_host_name'])
    print(f'\nTotal unique domains: {len(rows)}')
    print(f'Languages represented: {len(total_langs)}')
    for key in all_cat_keys:
        col = f'wikipedia_cat_{key}'
        cnt = sum(1 for r in rows if r[col])
        if cnt > 0:
            print(f'  {col}: {cnt}')

    import pyarrow as pa, pyarrow.parquet as pq
    schema = pa.schema([
        ('surt_host_name', pa.string()),
        ('domain', pa.string()),
        ('wiki_langs', pa.string()),
        ('wikipedia_article', pa.string()),
        ('categories', pa.string()),
    ] + [
        (f'wikipedia_cat_{k}', pa.bool_()) for k in all_cat_keys
    ])
    table = pa.table({col.name: [r[col.name] for r in rows] for col in schema}, schema=schema)
    pq.write_table(table, 'wikipedia-categories-intl.parquet')
    print(f'Wrote wikipedia-categories-intl.parquet')

    if DEBUG:
        import pyarrow.csv as csv
        csv.write_csv(table, 'wikipedia-categories-intl.csv')
        print(f'Wrote wikipedia-categories-intl.csv')


if __name__ == '__main__':
    main()
