# parquet_join

Join arbitrary annotation  parquet files into a single annotation.

## Install

```
pip install -r requirements.txt
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

The host index and annotation parquet files live on S3 as Hive-partitioned datasets:

| Dataset            | S3 path                                                                     |
|--------------------|-----------------------------------------------------------------------------|
| Host index         | `s3://commoncrawl/projects/host-index-testing/v2/crawl=*/*.parquet`         |
| Web graph          | `s3://commoncrawl/projects/webgraph-annotation-testing-v1/hosts/crawl=*/*.parquet` |
| GneissWeb          | `s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=*/*.parquet` |

### Host index + web graph (with prefixes)

```bash
python parquet_join.py \
  -o host_webgraph.parquet \
  --how outer \
  host_index.parquet:h_:surt_host_name \
  webgraph.parquet:wg_:surt_host_name
```

Result columns: `surt_host_name`, `h_hcrank10`, `h_fetch_200`, …, `wg_webgraph_outdegree`, `wg_webgraph_indegree`, …

### Host index + GneissWeb (no prefixes)

```bash
python parquet_join.py \
  -o host_gneissweb.parquet \
  --how outer \
  host_index.parquet::surt_host_name \
  gneissweb.parquet::surt_host_name
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

