# Creating Your Own Annotation

Have a dataset that classifies, scores, or labels web domains or URLs? You can distribute it as a Common Crawl annotation — a parquet file that anyone can join against Common Crawl's indexes to selectively extract crawl data.

This guide walks you through packaging your data and providing the YAML templates that let others use it.

## What makes a good annotation?

Anything that expresses an opinion, classification, or measurement about web hosts or URLs:

- Domain reputation scores or trust ratings
- Topic or category classifications
- Language identification
- License detection (Creative Commons, etc.)
- Spam, phishing, or abuse flags
- Academic or research relevance indicators
- Industry or sector labels
- Content quality signals

## Step 1: Prepare your parquet file

Your data needs one thing: a **join key** that matches Common Crawl's index format.

### Host-level annotations

For data about domains/hosts, use `surt_host_name` as the join key. SURT (Sort-friendly URI Rewriting Transform) reverses the hostname and drops `www.`:

| Original | SURT |
|----------|------|
| `example.com` | `com,example` |
| `www.example.com` | `com,example` |
| `blog.example.co.uk` | `uk,co,example,blog` |

In Python, using the `surt` library:

```python
import surt

full = surt.surt("http://example.com")  # "com,example)/"
surt_host_name = full.split(")/")[0]    # "com,example"
```

### URL-level annotations

For data about specific URLs, you need three join keys: `url_surtkey`, `crawl`, and `fetch_time`. These must match the Common Crawl URL index exactly.

### Minimal parquet schema

```
surt_host_name  (string)    — join key
your_column_a   (bool/int/float/string)
your_column_b   (bool/int/float/string)
...
```

Sort by `surt_host_name` before writing — this significantly improves join performance.

### Writing the parquet

```python
import pandas as pd

df = df.sort_values("surt_host_name").reset_index(drop=True)
df.to_parquet("my-annotation.parquet", index=False)
```

For boolean columns, ensure they are actual bools, not objects:

```python
for col in bool_columns:
    df[col] = df[col].astype(bool)
```

## Step 2: Provide YAML templates

Include three YAML files so users can immediately query your annotation against Common Crawl's index. These are templates — users will adjust paths and filters to their needs.

### Option A: Self-contained source YAML (simplest)

If your data is hosted at a public URL (HTTP, S3, or even a raw CSV), you can create a single YAML that works as both data definition and join config. Users don't need to download anything — DuckDB fetches the data directly.

```yaml
# join_my_annotation.yaml — all a user needs
table:
  source:
    url: https://your-server.com/my-annotation.parquet
right_columns:
  - your_column_a
  - your_column_b
join_columns:
  - surt_host_name
```

This also works with CSV files:

```yaml
table:
  source:
    url: https://your-server.com/my-data.csv
    format: csv
    options:
      header: true
right_columns:
  - score
join_columns:
  left: url_host_registered_domain
  right: domain
```

### Option B: Local parquet with paths (traditional)

For larger datasets or when users want local copies:

### left_host_index.yaml

```yaml
table:
  web_prefix: https://data.commoncrawl.org/
  paths: host-index-paths.gz
limits:
  grep: [CC-MAIN-2024-33]
```

### join_my_annotation.yaml

```yaml
table:
  local: ./
right_columns:
  - your_column_a
  - your_column_b
join_columns:
  - surt_host_name
```

### action_my_query.yaml

```yaml
sql: "SELECT {columns} FROM joined WHERE {where}"
columns: "surt_host_name, crawl, your_column_a, your_column_b"
where: "your_column_a = 1 AND crawl = 'CC-MAIN-2024-33'"
```

## Step 3: Document your annotation

At minimum, document:

- **What each column means** — name, type, value range, and a one-line description
- **Where the data comes from** — your methodology, data sources, update frequency
- **License** — under what terms others may use the data
- **Attribution** — the classifications are your opinions, not Common Crawl's

That last point matters: Common Crawl hosts the index, but your annotation represents your organization's assessment. Make this clear so users direct questions and disputes to you.

## Step 4: Distribute

Options for distribution, from simplest to most integrated:

- **Share a YAML file** — host your data anywhere (HTTP server, GitHub releases, S3) and distribute a single `table.source` YAML. Users join it immediately without downloading anything
- **Publish on GitHub** — for smaller datasets, include the parquet directly in a repository with YAML templates
- **Host it yourself** — put the parquet on your own HTTP server or S3 bucket. Users can point `table.source`, `web_prefix`, or `s3_prefix` at your location
- **Send it to Common Crawl** — contact Common Crawl to have your annotation hosted alongside the index for easy joining

## Column naming conventions

Prefix your columns with a short identifier to avoid collisions when users combine multiple annotations:

| Provider | Prefix | Example columns |
|----------|--------|-----------------|
| Web Graph | `webgraph_` | `webgraph_outdegree`, `webgraph_indegree` |
| GneissWeb | `gneissweb_` | `gneissweb_medical`, `gneissweb_science` |
| Wikipedia | `wikipedia_` | `wikipedia_spam`, `wikipedia_deprecated` |
| Your org | `yourorg_` | `yourorg_quality_score`, `yourorg_topic` |

## Existing examples

See the `examples/` directory for complete working annotations:

- `web-graph/` — host-level link metrics (outdegree, indegree)
- `gneissweb/` — host and URL-level topic classification scores
- `wikipedia/spam/` — spam and shortener domain flags from Wikipedia
- `wikipedia/perennial/` — source reliability ratings from 10 language Wikipedias
- `wikipedia/categories/` — website classification from Wikipedia categories (fact-checking, fake news, etc.)
- `wikipedia-perennial/` — English Wikipedia perennial sources (detailed RSP ratings)
- `spam-abuse/` — combined malware, phishing, and abuse flags from public blocklists
- `external-data/` — 6 ready-to-use external datasets via `table.source` (Tranco, Majestic, CISA, IFCN, etc.)
- `university-ranking/` — university identification and world rankings
- `fineweb-edu/` — educational quality scores from HuggingFace FineWeb-Edu

For a complete reference of all YAML options, see [yaml-reference.md](yaml-reference.md).
