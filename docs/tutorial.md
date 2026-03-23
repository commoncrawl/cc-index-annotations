# Tutorial: Build Your Own Annotation

This tutorial walks through creating an annotation from scratch, using the [Curlie](https://curlie.org/) web directory as an example. By the end, you'll know how to take any dataset of websites and overlay it onto Common Crawl's index.

Already familiar with the concepts? See the [quickstart guide](creating-annotations.md) for a condensed reference, or the [YAML reference](yaml-reference.md) for all configuration options.

## What's an annotation?

Common Crawl is a huge archive of web pages — billions of them. The index tells you *what's in the archive*, but not *what kind of site each page came from*.

An annotation adds that missing context. It's a file that says things like:
- "this domain is a cooking website"
- "this domain is in German"
- "this URL is listed in a human-curated directory"

You build it once, then join it against the Common Crawl index to filter or enrich the crawl data.

## The simplest possible annotation

In its most basic form, an annotation is just **a single YAML file** that points at a public dataset:

```yaml
# join_majestic_million.yaml — that's it, that's the whole annotation
table:
  source:
    url: https://downloads.majestic.com/majestic_million.csv
    format: csv
right_columns:
  - GlobalRank
  - RefSubNets
join_columns:
  left: url_host_registered_domain
  right: Domain
```

This YAML says: "grab the Majestic Million CSV from the internet and join it against Common Crawl's index on the domain column." No download scripts, no conversion — just a YAML file describing where the data lives and how it connects.

You can find ready-made examples like this in the [`examples/external-data/`](../examples/external-data/) directory. Drop one into a query and you instantly have extra columns on every domain in a crawl.

For datasets that aren't a clean CSV with domain columns (most of them!), you'll need a bit more work. That's what the rest of this tutorial covers.

## The three ingredients

To run an annotation query, you need three YAML files:

```
python annotate.py  left.yaml  join.yaml  action.yaml
                      │           │          │
                      │           │          └─ What to output (columns, filters)
                      │           └─ Your dataset (the annotation)
                      └─ Common Crawl's index (left side of the join)
```

Think of it like a database query: LEFT is the big table, JOIN adds your columns, ACTION is the SELECT/WHERE.

## Step 1: Find a dataset

You need a dataset that has **website domains or URLs** in it. It can be:
- A CSV or TSV file
- A JSON API
- An HTML page you can scrape
- A database dump
- ...

For this tutorial, we'll use [Curlie.org](https://curlie.org/) — the largest human-edited web directory with 2.9 million entries. It's a free download as a tar.gz of TSV files.

**What to look for in a dataset:**
- Does it have a column with domains or URLs? (required)
- What extra info does it provide? (categories, ratings, flags, scores)
- What license is it under? (respect the terms!)
- Is it freely downloadable?

**Before you scrape or download, always check robots.txt!** Open [https://curlie.org/robots.txt](https://curlie.org/robots.txt) in your browser. This file tells automated tools what they're allowed to access (see [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html) for the spec). Curlie's robots.txt allows access to their download — so we're good. If a site's robots.txt blocks your user agent or the paths you need, respect that and look for an alternative data source.

## Step 2: Download and explore

```bash
# Download (~170MB compressed)
curl -L -o curlie-rdf-all.tar.gz https://curlie.org/directory-dl
tar xzf curlie-rdf-all.tar.gz
```

Peek at what's inside:
```bash
head -3 curlie-rdf/rdf-Arts-c.tsv
```
```
http://www.awn.com/	Animation World Network	Provides information resources...	423945
http://www.toonhound.com/	Toonhound	British cartoon, animation...	423945
```

Four tab-separated columns: **URL**, title, description, category ID. That URL column is what we need.

The category files (`*-s.tsv`) map those IDs to human-readable paths:
```
423945	Arts/Animation	4343	Sites primarily related to...
```

## Step 3: Convert to Parquet

Common Crawl's index uses **SURT keys** to identify hosts. A SURT (Sort-friendly URI Rewriting Transform) reverses the domain:
- `www.example.com` → `com,example`
- `cooking.allrecipes.com` → `com,allrecipes,cooking`

This lets the database do fast prefix lookups. Your annotation needs a `surt_host_name` column to join with the index.

Here's the conversion script ([curlie-convert.py](../examples/curlie/curlie-convert.py)):

```python
import duckdb, surt as surt_lib, utils

con = duckdb.connect()

# Step 3a: Read TSV files and join sites with categories
con.sql("""
CREATE TABLE sites AS
WITH raw_sites AS (
    SELECT url, category_id
    FROM read_csv('curlie-rdf/rdf-*-c.tsv', sep='\t', header=false,
      columns={'url': 'VARCHAR', 'title': 'VARCHAR',
               'description': 'VARCHAR', 'category_id': 'BIGINT'},
      ignore_errors=true, null_padding=true, strict_mode=false, parallel=false)
),
raw_cats AS (
    SELECT category_id, category_path
    FROM read_csv('curlie-rdf/rdf-*-s.tsv', sep='\t', header=false,
      columns={'category_id': 'BIGINT', 'category_path': 'VARCHAR',
               'site_count': 'INTEGER', 'description': 'VARCHAR',
               'geo1': 'VARCHAR', 'geo2': 'VARCHAR'},
      ignore_errors=true, null_padding=true, strict_mode=false, parallel=false)
)
SELECT s.url, c.category_path
FROM raw_sites s LEFT JOIN raw_cats c ON s.category_id = c.category_id
WHERE s.url LIKE 'http%'
""")

# Step 3b: Extract domains and compute SURTs
rows = con.sql("""
    SELECT DISTINCT url, split_part(split_part(url, '://', 2), '/', 1) as domain
    FROM sites
""").fetchall()

surt_data = []
for url, domain in rows:
    try:
        surt_host = utils.thing_to_surt_host_name(domain)
        url_surtkey = surt_lib.surt(url)
        surt_data.append((url, surt_host, url_surtkey))
    except (ValueError, TypeError):
        pass  # skip unparseable URLs

# Step 3c: Write the parquet
con.execute("CREATE TABLE surt_lookup (url VARCHAR, surt_host_name VARCHAR, url_surtkey VARCHAR)")
con.executemany("INSERT INTO surt_lookup VALUES (?, ?, ?)", surt_data)

con.sql("""
    COPY (
        SELECT s.surt_host_name, s.url_surtkey, e.category_path as category
        FROM sites e JOIN surt_lookup s ON e.url = s.url
        ORDER BY s.surt_host_name
    ) TO 'curlie.parquet' (FORMAT PARQUET)
""")
```

The real script also extracts languages, validates domains, and handles edge cases — but this is the core idea: **read the source data → add SURT columns → write parquet**.

## Step 4: Write the YAML files

### The Join YAML — describes your dataset

This tells the system where your parquet is and how to join it:

```yaml
# join_curlie.yaml
table:
  local: ./curlie.parquet

right_columns:
  - domain
  - lang
  - category
join_columns:
  - surt_host_name
```

- `local` points at your parquet file
- `right_columns` lists the columns you want available in queries
- `join_columns` says what to join on — `surt_host_name` matches the host index

### The Left YAML — points at Common Crawl's index

This is usually the same across all annotations and points to either the Common Crawl's host, or page, index:

```yaml
# left_host_index.yaml
table:
  web_prefix: https://data.commoncrawl.org/
  paths: host-index-paths.gz

limits:
  grep: [CC-MAIN-2024-51]    # which crawl(s) to query
  #count: 1                   # uncomment to test with just 1 file
```

### The Action YAML — your query

This is the fun part — what do you want to find?

```yaml
# action_cooking.yaml — find cooking sites in all languages
sql: "SELECT DISTINCT {columns} FROM joined WHERE {where}"
columns: "surt_host_name, domain, lang, category"
where: >
  category LIKE '%Cooking%'
  OR category LIKE '%Kochen%'
  OR category LIKE '%Cuisine%'
  OR category LIKE '%Cucina%'
  OR category LIKE '%料理%'
```

Or a simple "show me everything" action:

```yaml
# action_star.yaml
sql: "SELECT {columns} FROM joined WHERE {where}"
columns: "*"
where: "category IS NOT NULL"
limits:
  count: 100
```

## Step 5: Run it!

Clone the repository and set up a Python environment:

```bash
git clone https://github.com/commoncrawl/cc-index-annotations.git
cd cc-index-annotations

python -m venv .venv
source .venv/bin/activate        # on Windows WSL: same command
pip install -r requirements.txt
```

Navigate to the Curlie example and download the host index path list:

```bash
cd examples/curlie

# Download the list of host index parquet files (~2KB)
curl -L -o host-index-paths.gz \
  "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"

# Symlink the core scripts (or copy them)
ln -s ../../annotate.py ../../duck_utils.py ../../utils.py .
```

Now run the conversion and query:

```bash
# Convert Curlie TSV → parquet (takes ~2 minutes)
python curlie-convert.py

# Query: find cooking sites across all languages in a crawl
python annotate.py left_host_index.yaml join_curlie.yaml action_cooking.yaml
```

This outputs a CSV of every cooking-related domain that appears in the CC-MAIN-2024-51 crawl, with its Curlie category and language. 🎉

## Step 6: Share your annotation

To share your annotation with the world, you need:

1. **The parquet file** — host it somewhere accessible (S3, a web server, GitHub Releases)
2. **A join YAML** — so others can plug it into their queries
3. **A README** — what the data is, where it came from, what license it's under

See [creating-annotations.md](creating-annotations.md) for the full spec, and [yaml-reference.md](yaml-reference.md) for all YAML options.

## Quick reference

| What | File | Purpose |
|------|------|---------|
| Common Crawl index | `left_*.yaml` | The big table (billions of hosts/URLs) |
| Your dataset | `join_*.yaml` | Your annotation data as parquet |
| Your query | `action_*.yaml` | What columns to output, what to filter |

| Join type | Use when |
|-----------|----------|
| `surt_host_name` | Your data is per-domain (e.g. "example.com is a news site") |
| `url_surtkey` | Your data is per-URL (e.g. "this specific page is about cooking") |
| `url_host_registered_domain` | Joining URL index with domain-level data (asymmetric join) |

## Tips

- **Start small**: use `count: 1` in limits to test with one parquet file before querying all crawls
- **Debug with CSV**: add `-d` to your fetch script to also write a CSV you can open in a spreadsheet
- **Stack annotations**: pass multiple join YAMLs to combine datasets in one query
- **Use `prefix`**: when stacking, add `prefix: curlie_` to avoid column name collisions
- **Check `examples/`**: there are over 20 working examples covering spam lists, university rankings, fact-checkers, popularity rankings, and more

## Built with

This project stands on the shoulders of some great open source tools:

- [DuckDB](https://duckdb.org/) — the in-process analytical database that makes all the SQL magic work, including reading remote parquet files over HTTP
- [SURT](https://pypi.org/project/surt/) — Internet Archive's Sort-friendly URI Rewriting Transform library, used to convert domains and URLs into the key format Common Crawl's index uses
- [Apache Arrow / PyArrow](https://arrow.apache.org/) — the columnar memory format and Parquet I/O that keeps everything fast
- [Curlie.org](https://curlie.org/) — the largest human-edited web directory, used as the example throughout this tutorial. Originally the Open Directory Project (DMOZ), kept alive by volunteer editors since 2017
