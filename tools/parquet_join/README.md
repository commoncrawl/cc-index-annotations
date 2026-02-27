# parquet_join

Join annotation files into a single annotation parquet.

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
| `path`      | A `.parquet` file, glob, or directory         | yes      |
| `prefix`    | String prepended to all non-join column names | no       |
| `join_cols` | Comma-separated join key(s) for this file     | no       |

### Options

| Flag                | Description                                        |
|---------------------|----------------------------------------------------|
| `-o`, `--output`    | Output parquet file path (required)                |
| `-j`, `--join-cols` | Default join columns applied to all inputs         |
| `--how`             | Join type: `inner` (default), `outer`, `left`, `right`, `cross` |

Prefixes are applied to every column **except** the join columns.
When a path is a directory, all `.parquet` files underneath it are concatenated
before joining. Duplicate non-join column names that remain after prefixing get
automatic `_leftN`/`_rightN` suffixes.

## Examples

The host index and annotation parquet files live on S3 as Hive-partitioned datasets:

| Dataset            | S3 path                                                                     |
|--------------------|-----------------------------------------------------------------------------|
| Host index         | `s3://commoncrawl/projects/host-index-testing/v2/crawl=*/*.parquet`         |
| Web graph          | `s3://commoncrawl/projects/webgraph-annotation-testing-v1/hosts/crawl=*/*.parquet` |
| GneissWeb          | `s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=*/*.parquet` |

All examples below join on `surt_host_name` and assume you have local copies of
the parquet files for the crawl(s) of interest (see [Downloading the data](#downloading-the-data)).

### Downloading the example data

The parquet files are on S3 and require AWS credentials. To grab a single crawl's
host-level data:

```bash
aws s3 sync \
  s3://commoncrawl/projects/webgraph-annotation-testing-v1/hosts/crawl=CC-MAIN-2024-30/ \
  ./webgraph/

aws s3 sync \
  s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=CC-MAIN-2024-30/ \
  ./gneissweb/
```

Then join the two directories directly — all parquet shards in each directory are
concatenated automatically. Without prefixes, any columns that overlap (other than
the join key) get `_leftN`/`_rightN` suffixes:

```bash
python parquet_join.py \
  -o webgraph_gneissweb_2024-30.parquet \
  -j surt_host_name --how outer \
  ./webgraph/ \
  ./gneissweb/
```

