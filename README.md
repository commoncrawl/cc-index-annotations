# cc-index-annotations

**Join your own data against Common Crawl's web index, and query just the slice you need.**

[Common Crawl](https://commoncrawl.org/)'s datasets are all random
access — you can use an index to efficiently process a subset of
the data, if you don't want all of it. Index annotations are a
mechanism for one organization to create a database table that can
be joined to Common Crawl's columnar url index or host index. Then
they or another organization can use the resulting joined index.

As an example, perhaps you're interested in content on the web that
has Creative Commons licenses — that's currently less than 0.1% of
all of the content on the web. In this scenario, you'd like to use a
list of webpages — labeled by someone else — to extract just the CC
licensed content. That is an annotation table, joined against the
url index, followed by an extraction of content.

Or you might be interested in which web hosts have a lot of Creative
Commons-licensed data. That is an aggregation of url-level
information to a host level, and then a join against the host index.
You could then use additional information from the host index, such
as host ranks or language percentages, to examine which hosts have
CC content in a particular language.

## More examples of annotations

- URL quality indications from the ClueWeb22 dataset, Nemotron-CC,
  FineWeb
- robots.txt information, such as how often robots.txt files are
  changed on web hosts
- Alternative language identifications (the standard index only has
  CLD2)

## Understanding the "join" column(s)

A database join needs to use one or more keys. For better or worse,
the usual practice in web archiving is a little unusual. The URL is
usually indexed in the SURT form, which drops the leading www and
reverses the order of the parts of the hostname:

- example.com/README -> com,example)/README
- www.example.com/README -> com,example)/README

For the host index, the primary key is the hostname part of the
SURT:

- example.com -> com,example
- www.example.com -> com,example
- www.com -> com,www

In both cases (url index, host index) the data tables are Hive
sharded by crawl, e.g. CC-MAIN-2025-18.

If you only have a list of urls (e.g. FineWeb), then you'll have to
decide for yourself what time range you'd like the annotation to
apply to.

## 'left join' jargon

These tools consider the url index or host index the "left"
database, and the annotation database is joined to the left database
using a LEFT OUTER JOIN. This means that any row in the annotation
that does not match the "left" database will not appear in the
result. The choice "left" instead of "right" is totally arbitrary.

## Annotation tool

This repo contains tools that join an index with an annotation,
run a query, and save the output to a csv file. The configuration
of the index, annotation, and query are all contained in yaml
files. The index and annotation can be on local disk or on AWS.

```
,-----------------------.    ,----------------------.
|  Common Crawl Index   |    |   Your Annotation    |
|  (host or URL index)  |    |  (parquet/csv file)  |
'-----------------------'    '----------------------'
           |                             |
           |   ,---------------------.   |
           '-->|  DuckDB LEFT JOIN   |<--'
               |(configured by YAML) |
               '---------------------'
                         |
                         v
              ,----------------------.
              |    Query results     |
              |    (.csv output)     |
              '----------------------'
```

The configuration is split across three yaml files, passed as
arguments to `annotate.py`:

- **Left** — the Common Crawl index to query (local, HTTP, or S3)
- **Join** — your annotation data and the columns to join on
- **Action** — the SQL query to run on the joined result

```
python annotate.py <left.yaml> [join.yaml ...] <action.yaml> [args]
```

You can chain multiple join files to combine several annotations
in one query.

For a complete reference of all YAML configuration options, see
[docs/yaml-reference.md](docs/yaml-reference.md).
New to annotations? Follow the
[in-depth tutorial](docs/tutorial.md) to build one from scratch.
For a quick overview of creating and distributing your own
annotation, see the
[quickstart guide](docs/creating-annotations.md).

To run the python code, you'll need to install a few things in
your virtual environment:

```
pip install -r requirements.txt
```

> **Windows users**: This project is developed and tested on macOS
> and Linux. On Windows, we recommend using
> [WSL (Windows Subsystem for Linux)](https://learn.microsoft.com/en-us/windows/wsl/install).
> Native Windows support is untested — path handling, symlinks,
> and Make may behave differently.

## AWS Configuration

Please ensure you can access AWS S3 before running the s3 examples.
Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
tools and ensure you can list our s3 data:
```
aws s3 ls s3://commoncrawl/projects/gneissweb-annotation-testing-v1/paths.hosts.txt.gz
```

If you have multiple AWS profiles defined on your system (eg,
multiple sections are defined in `~/.aws/config`) it can be that
the system takes the wrong profile. To combat this, please set an
`AWS_PROFILE` environment variable before executing.
```
export AWS_PROFILE=<profilename>
```

## Quick start

Please note that to run the "web" or "s3" examples, you'll need
to download some necessary 'path.gz' files first:

```
make web-graph
```

Then, generate a .csv of `crawl`, `hcrank10`, `webgraph_outdegree`,
and `webgraph_indegree` for hosts matching 'commoncrawl.org':
```
cd examples/web-graph
python annotate.py left_web_host_index.yaml join_web_outin.yaml \
    action_surt_host_name.yaml commoncrawl.org
cd -
```

Example output:

```csv
| surt_host_name | crawl | hcrank10 | webgraph_outdegree | webgraph_indegree |
| org | commoncrawl | CC-MAIN-2024-30 | 5.085 | 291 | 1746 |
| org | commoncrawl | CC-MAIN-2024-42 | 5.067 | 294 | 1624 |
| org | commoncrawl | CC-MAIN-2025-08 | 4.973 | 330 | 1682 |
| org | commoncrawl | CC-MAIN-2025-18 | 4.845 | 310 | 1721 |
| [...] |
```

## Examples

See [examples/README.md](examples/README.md) for a full directory
listing of all available examples. A few highlights:

| Example | Level | What it does |
|---------|-------|-------------|
| [web-graph](examples/web-graph) | host | Outdegree and indegree from Common Crawl's [web graphs](https://commoncrawl.org/web-graphs) |
| [fineweb-edu](examples/fineweb-edu) | host | Educational quality scores from [HuggingFace FineWeb-Edu](https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu) |
| [wikipedia/spam](examples/wikipedia/spam) | host | Spam and URL shortener flags from [Wikipedia's spam list](https://en.wikipedia.org/wiki/MediaWiki:Spam-blacklist) |
| [university-ranking](examples/university-ranking) | host | University identification and world rankings from [Hipo](https://github.com/hipo/university-domains-list) and [CWUR](https://cwur.org/2025.php) |
| [external-data](examples/external-data) | host | Ready-to-use external datasets (Tranco, Majestic, CISA, and more) — no fetch scripts needed |
