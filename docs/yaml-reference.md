# YAML Configuration Reference

The annotation system uses three types of YAML configuration files, passed as arguments to `annotate.py`:

```
python annotate.py <left.yaml> [join.yaml ...] <action.yaml> [args ...]
```

All YAML files after the first and before the action are treated as join configurations. The last YAML file is always the action. Any arguments after the action YAML are passthrough arguments available as template variables.

---

## Left (Database) YAML

Defines the "left" table — typically a Common Crawl host index or URL index.

### Keys

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `table.local` | string | one of four | Path to a local `.parquet` file, a directory containing `.parquet` files, or a glob pattern (e.g. `./data-*.parquet`) |
| `table.web_prefix` | string | one of four | HTTP(S) URL prefix (e.g. `https://data.commoncrawl.org/`) |
| `table.s3_prefix` | string | one of four | S3 URI prefix (e.g. `s3://commoncrawl/`) |
| `table.source` | dict | one of four | Direct URL to a CSV, parquet, or JSON file (see [Source YAML](#source-yaml) below) |
| `table.paths` | string | required for web/s3 | Path to a file listing relative parquet paths. Supports `.gz` compressed files |
| `limits.grep` | list of strings | optional | Keep only paths containing any of these substrings (OR logic) |
| `limits.count` | integer | optional | Keep only the first N paths after grep filtering |

Exactly one of `local`, `web_prefix`, `s3_prefix`, or `source` must be specified.

When using `local` without `paths`:
- If it points to a **file**, that file is used directly
- If it points to a **directory**, all `.parquet` files in it (and subdirectories) are auto-discovered
- If it contains **glob characters** (`*`, `?`), the pattern is expanded

### Examples

```yaml
# Local directory — auto-discovers all .parquet files
table:
  local: /home/cc-pds/commoncrawl/projects/host-index-testing/v2/
limits:
  grep: ["CC-MAIN-2025", "CC-MAIN-2024"]
  count: 1
```

```yaml
# Local file — single parquet
table:
  local: ./curlie.parquet
```

```yaml
# Local glob pattern
table:
  local: ./data-*.parquet
```

```yaml
# HTTPS
table:
  web_prefix: https://data.commoncrawl.org/
  paths: host-index-paths.gz
limits:
  grep: [CC-MAIN-2021-49]
```

```yaml
# S3
table:
  s3_prefix: s3://commoncrawl/
  paths: host-index-paths.gz
limits:
  grep: ["CC-MAIN-2025", "CC-MAIN-2024"]
  count: 1
```

---

## Join YAML

Defines a right-side table to join against the left table (or a previous join result). Multiple join YAMLs can be chained — they are processed in order, each joining against the result of the previous.

### Keys

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `table.*` | | yes | Same `table` and `limits` options as Left YAML (see above) |
| `limits.*` | | optional | Same `limits` options as Left YAML |
| `right_columns` | list of strings | yes | Columns to select from the right table into the joined result |
| `join_columns` | list or dict | yes | Column(s) to join on. A list when names match in both tables, or a dict with `left`/`right` keys when column names differ (see examples) |
| `prefix` | string | optional | Prefix added to all `right_columns` in the result (e.g. `tranco_` turns `rank` into `tranco_rank`). Useful when stacking multiple joins to avoid column name collisions |
| `join_type` | string | optional | `OUTER` (default) or `INNER` |

`OUTER` produces a `LEFT OUTER JOIN` — rows in the left table without a match in the right table are kept, with NULLs for the right columns. `INNER` drops unmatched rows from both sides.

### Examples

```yaml
# Host-level join on surt_host_name only
table:
  local: ./
right_columns:
  - wikipedia_spam
  - wikipedia_shortener
join_columns:
  - surt_host_name
```

```yaml
# Host-level join on surt_host_name + crawl, from S3
table:
  s3_prefix: s3://commoncrawl/
  paths: web-graph-outin-paths.gz
right_columns:
  - webgraph_outdegree
  - webgraph_indegree
join_columns:
  - surt_host_name
  - crawl
```

```yaml
# URL-level join
table:
  web_prefix: https://data.commoncrawl.org/
  paths: paths.urls.txt.gz
limits:
  grep: [CC-MAIN-2020-05]
right_columns:
  - gneissweb_technology
  - gneissweb_science
  - gneissweb_education
  - gneissweb_medical
join_columns:
  - url_surtkey
  - crawl
  - fetch_time
```

```yaml
# Asymmetric join — column names differ between left and right (single column)
table:
  local: ./
right_columns:
  - is_university
  - country
  - university_name
join_columns:
  left: url_host_registered_domain
  right: domain
```

```yaml
# Asymmetric join — multiple columns with different names
table:
  local: ./
right_columns:
  - score
join_columns:
  left: [url_host_registered_domain, crawl]
  right: [domain, crawl_id]
```

### join_columns formats

`join_columns` supports two formats:

- **List** — when column names are the same in both tables:
  ```yaml
  join_columns:
    - surt_host_name
    - crawl
  ```

- **Dict with left/right** — when column names differ. Each side accepts a single string or a list. When using lists, columns are paired positionally:
  ```yaml
  join_columns:
    left: [url_host_registered_domain, crawl]
    right: [domain, crawl_id]
  # produces: left.url_host_registered_domain = right.domain
  #       AND left.crawl = right.crawl_id
  ```

```yaml
# Prefix to avoid column collisions when stacking multiple joins
table:
  source:
    url: https://downloads.majestic.com/majestic_million.csv
    format: csv
prefix: majestic_
right_columns:
  - GlobalRank
  - RefSubNets
join_columns:
  left: url_host_registered_domain
  right: Domain
# Result columns: majestic_GlobalRank, majestic_RefSubNets
```

```yaml
# INNER join (only keep matched rows)
table:
  web_prefix: https://data.commoncrawl.org/
  paths: ../web-graph/web-graph-outin-paths.gz
limits:
  grep: [CC-MAIN-2024-33]
join_type: INNER
right_columns:
  - webgraph_outdegree
  - webgraph_indegree
join_columns:
  - surt_host_name
  - crawl
```

### Chaining multiple joins

When multiple join YAMLs are provided, they are applied sequentially. Intermediate views are created as `join_step_0`, `join_step_1`, etc. The final join result is always named `joined`.

```
python annotate.py left.yaml join_webgraph.yaml join_wikipedia.yaml action.yaml
```

This produces: `left` → join with webgraph → join with wikipedia → `joined` view → action query.

---

## Action YAML

Defines the SQL query to run against the final `joined` view.

### Keys

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `sql` | string | yes | SQL template. Use `{columns}` and `{where}` as placeholders |
| `columns` | string | yes | Column list substituted into `{columns}` |
| `where` | string | yes | WHERE clause substituted into `{where}`. Supports template variables (see below) |
| `limits.count` | integer | optional | Maximum number of rows in the output. Appends `LIMIT N` to the query |
| `argv.surt_host_name` | boolean | optional | If `true`, passthrough arguments are converted to SURT format before substitution |

### Template variables in `where`

| Variable | Description |
|----------|-------------|
| `{argv}` | The current passthrough argument (optionally SURT-converted) |
| `{and_tld}` | Auto-generated TLD filter, e.g. ` AND url_host_tld = 'org'`. Only set when `argv.surt_host_name: true` |
| `{arg1}`, `{arg2}`, ... | Individual passthrough arguments by position |

When passthrough arguments are provided, the action runs once per argument. Output CSV files are named after the argument value (e.g. `org,commoncrawl.csv`). When no arguments are given, output goes to `output.csv`.

### Examples

```yaml
# Exact hostname lookup with SURT conversion
sql: "SELECT {columns} FROM joined WHERE {where} ORDER BY crawl ASC"
columns: "surt_host_name, crawl, hcrank10, webgraph_outdegree, webgraph_indegree"
where: "surt_host_name = '{argv}'{and_tld}"
argv:
  surt_host_name: true
```

Usage: `python annotate.py left.yaml join.yaml action.yaml commoncrawl.org`

The argument `commoncrawl.org` is converted to SURT `org,commoncrawl` and `{and_tld}` becomes ` AND url_host_tld = 'org'`.

```yaml
# LIKE query for hostname prefix matching
sql: "SELECT {columns} FROM joined WHERE {where} ORDER BY hcrank10 DESC"
columns: "surt_host_name, crawl, hcrank10, webgraph_outdegree, webgraph_indegree"
where: "surt_host_name LIKE '{argv}%'{and_tld}"
argv:
  surt_host_name: true
```

Usage: `python annotate.py left.yaml join.yaml action.yaml .commoncrawl.org`

```yaml
# Fixed query, no arguments
sql: "SELECT {columns} FROM joined WHERE {where}"
columns: "surt_host_name, crawl, wikipedia_spam"
where: "wikipedia_spam = 1 AND crawl = 'CC-MAIN-2021-49'"
```

```yaml
# Multi-condition filter
sql: "SELECT {columns} FROM joined WHERE {where}"
columns: "surt_host_name, crawl, abuse_urlhaus_malware, abuse_ut1_malware"
where: "(abuse_urlhaus_malware = 1 OR abuse_ut1_malware = 1) AND crawl = 'CC-MAIN-2021-49'"
```

---

## Source YAML

`table.source` is the simplest way to use an external dataset — point directly at a URL. The format (CSV, parquet, JSON) is auto-detected from the file extension, or set explicitly. Works in both left and join position based on argument order.

This is the recommended method for distributing standalone annotations. A data provider can share a single YAML file and users can immediately join it against any Common Crawl index.

### Keys

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `table.source.url` | string | one of two | URL or local path to a single file |
| `table.source.urls` | list | one of two | List of URLs/paths (loaded as a single table) |
| `table.source.format` | string | optional | `csv`, `json`, or `parquet`. Auto-detected from extension if omitted |
| `table.source.options` | dict | optional | Passed directly to DuckDB's `read_csv()`, `read_json()`, or `read_parquet()` |

Auto-detection: `.csv`/`.tsv`/`.csv.gz` → csv, `.json`/`.jsonl`/`.ndjson` → json, everything else → parquet.

### Examples

```yaml
# Tranco top-1M domain ranking (CSV without headers)
table:
  source:
    url: https://tranco-list.eu/download/L76X4/full
    format: csv
    options:
      header: false
      columns:
        rank: INTEGER
        domain: VARCHAR

right_columns:
  - rank
join_columns:
  left: url_host_registered_domain
  right: domain
```

```yaml
# Local parquet file via source
table:
  source:
    url: ./my-annotation.parquet

right_columns:
  - my_score
join_columns:
  - surt_host_name
```

```yaml
# Multiple parquet files
table:
  source:
    urls:
      - https://example.com/data-part1.parquet
      - https://example.com/data-part2.parquet
```

### Hive partitioning

- Local and S3 globs work (`source.url: ./crawl=*/*.parquet`)
- HTTP URLs cannot glob — use `source.urls` with explicit paths, or use `web_prefix` + `paths` for partitioned data
- `table.source` is best for single-file datasets; use `web_prefix`/`s3_prefix` for multi-partition CC infrastructure data

---

## Common join columns

| Index type | Join columns | Description |
|------------|-------------|-------------|
| Host index | `surt_host_name` | Reversed hostname (e.g. `com,example`) |
| Host index | `surt_host_name`, `crawl` | Hostname + crawl ID (e.g. `CC-MAIN-2024-33`) |
| URL index | `url_surtkey`, `crawl`, `fetch_time` | Full SURT URL + crawl + timestamp |

---

## File naming conventions

File names are purely conventional — the system does not require any specific naming. The project uses this convention for clarity:

| Prefix | Purpose |
|--------|---------|
| `left_` | Left table (index) configuration |
| `join_` | Right table (annotation) join configuration |
| `action_` | Query/action configuration |

Suffixes like `_local`, `_web`, `_s3` indicate the data source type.

---

## Paths file format

The `table.paths` key points to a file listing parquet file paths, one per line. These are relative to the prefix (`web_prefix` or `s3_prefix`). Lines are stripped of trailing whitespace.

Gzip-compressed files (`.gz`) are automatically decompressed. For local sources, glob wildcards (`*`) are supported in the paths value itself.

---

## DuckDB configuration

The system automatically configures DuckDB with:
- HTTP retries: 10 attempts, 2000ms wait between retries
- AWS credentials: auto-detected from credential chain, falls back to anonymous S3 access
- Object caching: enabled for HTTP sources
- Hive partitioning: enabled for all parquet reads
- Progress bar: enabled (disabled in CI)
