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

The annotation parquet files live on S3 as Hive-partitioned datasets:

| Dataset   | S3 path                                                                            |
|-----------|------------------------------------------------------------------------------------|
| Web graph | `s3://commoncrawl/projects/webgraph-outin-testing/v2/crawl=*/*.parquet` |
| GneissWeb | `s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=*/*.parquet` |

The examples below assume local copies of the parquet files for a crawl of interest,

### Downloading the example data

The parquet files are on S3 and require AWS credentials. To grab a single crawl's
host-level data:

```bash
aws s3 sync \
   s3://commoncrawl/projects/webgraph-outin-testing/v2/crawl=CC-MAIN-2024-18/ \
  ./webgraph/

aws s3 sync \
  s3://commoncrawl/projects/gneissweb-annotation-testing-v1/hosts/crawl=CC-MAIN-2024-18/ \
  ./gneissweb/
```

### Join two annotation directories

When joining the two directories directly — all parquet shards in each directory are
concatenated automatically. Without prefixes, any columns that overlap (other than
the join key) get `_leftN`/`_rightN` suffixes:


```bash
python parquet_join.py \
  -o webgraph_gneissweb.parquet \
  -j surt_host_name --how inner \
  ./webgraph/ \
  ./gneissweb/
```

All parquet shards in each directory are concatenated automatically.
Any overlapping non-join column names get `_leftN`/`_rightN` suffixes.

Above annotation join result:
```
====================================================================================================
File: webgraph_gneissweb.parquet
Total rows: 7512652
====================================================================================================

Schema:
root
 |-- surt_host_name: string (nullable = true)
 |-- webgraph_outdegree: long (nullable = true)
 |-- webgraph_indegree: long (nullable = true)
 |-- host_rev: string (nullable = true)
 |-- gneissweb_technology: double (nullable = true)
 |-- gneissweb_science: double (nullable = true)
 |-- gneissweb_education: double (nullable = true)
 |-- gneissweb_medical: double (nullable = true)
 |-- in_gneissweb: boolean (nullable = true)


────────────────────────────────────────────────────────────────────────────────────────────────────
ROW 1
────────────────────────────────────────────────────────────────────────────────────────────────────
gneissweb_education: 0.5419631389280161
gneissweb_medical: 0.06258945873317619
gneissweb_science: 0.033660448951801904
gneissweb_technology: 0.3591726745168368
host_rev: abb.careers
in_gneissweb: True
surt_host_name: abb,careers
webgraph_indegree: 665
webgraph_outdegree: 19

────────────────────────────────────────────────────────────────────────────────────────────────────
ROW 2
────────────────────────────────────────────────────────────────────────────────────────────────────
gneissweb_education: 0.0035266188271634745
gneissweb_medical: 0.10797339590644697
gneissweb_science: 0.13446958106760576
gneissweb_technology: 0.24355749068599836
host_rev: abb.global
in_gneissweb: True
surt_host_name: abb,global
webgraph_indegree: 2843
webgraph_outdegree: 104

────────────────────────────────────────────────────────────────────────────────────────────────────
ROW 3
────────────────────────────────────────────────────────────────────────────────────────────────────
gneissweb_education: 0.03020171052776277
gneissweb_medical: 0.13582982309162617
[...]
```


### Join with prefixes

```bash
python parquet_join.py \
  -o webgraph_gneissweb.parquet \
  -j surt_host_name --how outer \
  ./webgraph/:wg_ \
  ./gneissweb/:gw_
```

Result columns: `surt_host_name`, `wg_webgraph_outdegree`, `wg_webgraph_indegree`, …,
`gw_gneissweb_education`, `gw_gneissweb_medical`, …
