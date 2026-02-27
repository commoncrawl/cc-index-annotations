# parquet_join

Join arbitrary parquet files into a single output parquet file.

## Install

```
pip install pyarrow pandas
```

## Usage

```
python parquet_join.py [options] input1 [input2 ...] -o OUTPUT
```

Each input uses the format `path[:prefix[:join_cols]]`:

| Segment     | Description                                   | Required |
|-------------|-----------------------------------------------|----------|
| `path`      | Path to a `.parquet` file                     | yes      |
| `prefix`    | String prepended to all non-join column names | no       |
| `join_cols` | Comma-separated join key(s) for this file     | no       |

### Options

| Flag                | Description                                        |
|---------------------|----------------------------------------------------|
| `-o`, `--output`    | Output parquet file path (required)                |
| `-j`, `--join-cols` | Default join columns applied to all inputs         |
| `--how`             | Join type: `inner` (default), `outer`, `left`, `right`, `cross` |

Prefixes are applied to every column **except** the join columns.

## Examples

### Basic: inner join with per-file prefixes

```bash
python parquet_join.py \
  -o merged.parquet \
  users.parquet:u_:id \
  orders.parquet:o_:id
```

### Default join columns, outer join

```bash
python parquet_join.py \
  -o merged.parquet \
  -j id,date --how outer \
  a.parquet:a_ \
  b.parquet:b_ \
  c.parquet:c_
```

### Per-file column override, empty prefix slot

```bash
python parquet_join.py \
  -o merged.parquet \
  a.parquet::id,date \
  b.parquet:pfx_:id
```

---

## Real-world example: Common Crawl host index + annotations

The [cc-index-annotations](https://github.com/commoncrawl/cc-index-annotations) repo
demonstrates joining Common Crawl's **host index** with annotation tables (web graph
metrics, GneissWeb classifiers, Wikipedia reliability scores, etc.) on the shared key
`surt_host_name`.

The host index and annotation parquet files live on S3:

| Dataset            | S3 path                                                                      |
|--------------------|------------------------------------------------------------------------------|
| Host index         | `s3://commoncrawl/projects/host-index-testing/v2/crawl=CC-MAIN-2024-30/*.parquet`     |
| Web graph          | `s3://commoncrawl/projects/webgraph-annotation-testing-v1/hosts/crawl=CC-MAIN-2024-30/*.parquet` |
| GneissWeb          | `s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=CC-MAIN-2021-49/*.parquet` |

For comparison, here's the equivalent DuckDB query from the
[GneissWeb blog post](https://commoncrawl.org/blog/gneissweb-annotations-examples):

```sql
SELECT h.surt_host_name, h.hcrank, h.hcrank10,
       g.gneissweb_education, g.gneissweb_medical,
       g.gneissweb_science, g.gneissweb_technology
FROM read_parquet('s3://...host-index.../crawl=CC-MAIN-2021-49/*.parquet') AS h
JOIN read_parquet('s3://...gneissweb.../hosts/crawl=CC-MAIN-2021-49/*.parquet') AS g
  ON h.surt_host_name = g.surt_host_name
WHERE g.gneissweb_medical > 0.5
ORDER BY h.hcrank DESC LIMIT 10;
```

With `parquet_join.py`, the same outer join on local copies of those files looks like:

### Host index + web graph annotation

```bash
# outer join host index with web graph metrics on surt_host_name
python parquet_join.py \
  -o host_webgraph.parquet \
  --how outer \
  host_index_2024-30.parquet:h_:surt_host_name \
  webgraph_2024-30.parquet:wg_:surt_host_name
```

Result columns: `surt_host_name`, `h_hcrank10`, `h_fetch_200`, …, `wg_webgraph_outdegree`, `wg_webgraph_indegree`, …

### Host index + GneissWeb annotation

```bash
python parquet_join.py \
  -o host_gneissweb.parquet \
  --how outer \
  host_index_2021-49.parquet:h_:surt_host_name \
  gneissweb_2021-49.parquet:gw_:surt_host_name
```

### Three-way join: host index + web graph + GneissWeb

```bash
python parquet_join.py \
  -o host_combined.parquet \
  -j surt_host_name --how outer \
  host_index.parquet:h_ \
  webgraph.parquet:wg_ \
  gneissweb.parquet:gw_
```

### Downloading the data first

The parquet files are on S3 and require AWS credentials. To grab a single crawl's
host-level data:

```bash
aws s3 sync \
  s3://commoncrawl/projects/host-index-testing/v2/crawl=CC-MAIN-2024-30/ \
  ./host_index_2024-30/

aws s3 sync \
  s3://commoncrawl/projects/webgraph-annotation-testing-v1/hosts/crawl=CC-MAIN-2024-30/ \
  ./webgraph_2024-30/
```

Then point `parquet_join.py` at the local directories (or use a glob/concat step to
merge shards into a single file first).
