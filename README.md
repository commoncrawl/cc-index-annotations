# cc-index-annotations

Common Crawl's datasets are all random access -- you can use an index
to efficiently process a subset of the data, if you don't want all of
it.

Index annotations are a mechanism for one organization to create a
database table that can be joined to Common Crawl's columnar url index
or host index. Then they or another organization can use the
resulting joined index.

As an example, perhaps you're interested in content on the web that
has Creative Commons licenses -- that's currently less than 0.1% of
all of the content on the web. In this scenario, you'd like to use a
list of webpages -- labeled by someone else -- to extract just the CC
licensed content. That is a an annotation table, joined against our
url index, followed by an extraction of content.

Alternately, you might be interested in investigating which web hosts
have a lot of Creative Commons-licensed data. That is an aggregation
of url-level information to a host level, and then a join against our
host index. Then you could use additional information from the host
index, such as host ranks or the percentage of languages, to examine
which web hosts have a lot of Creative Commons licensed content in a
particular language.

## More examples of annotations

- URL quality indications from the ClueWeb22 dataset, Nemotron-CC, FineWeb
- robots.txt information, such as how often robots.txt files are changed on web hosts
- Alternative language identifications (the standard index only has CLD2)

(None of these are currently available in a form that can easily be joined.)

## Understanding the "join" column(s)

A database join needs to use one or more keys. For better or worse,
the usual practice in web archiving is little unusual. For
applications like the Wayback Machine, the important fields in the url
index are the URL, and the time it was crawled. There is a unique ID
named WARC-Record-ID, but it is not traditionally included in indexes.
Also, the URL is usually indexed in the SURT form, which drops the
leading www and reverses the order of the parts of the hostname:

- example.com/README -> com,example)/README
- www.exmaple.com/README -> com,example)/README
- www.com -> com,www)/

For the host index, the primary key is the hostname part of the SURT:

- example.com -> com,example
- www.example.com -> com,example
- www.com -> com,www

In both cases (url index, host index) the data tables are Hive sharded
by crawl, e.g. CC-MAIN-2025-18.

If you only have a list of urls (e.g. FineWeb), then you'll have to
decide for yourself what time range you'd like the annotation to apply
to.

## 'left join' jargon

These tools consider the url index or host index the "left" database,
and the annotation database is joined to the left database using a
LEFT OUTER JOIN. This means that any row in the annotation that does
not match the "left" database will not appear in the result. The
choice "left" instead of "right" is totally arbitrary.

## Annotation tool

This repo contains tools that join an index with an annotation,
runs a query, and saves the output to a csv file. The configuration
of the index, annotation, and query are all contained in yaml
files. The index and annotation can be on local disk or on AWS.

In the following example, the index is our host index, and the
annotation is taken from our web graph, and contains the columns
`surt_host_name`, `webgraph_outdegree`, and `webgraph_indegree`.

The example YAML configuration files of the webgraph example are:

- `left_local_host_index.yaml`
- `left_web_host_index.yaml`
- `left_s3_host_index.yaml`
- `join_local_outin.yaml`
- `join_web_outin.yaml`
- `join_s3_outin.yaml`
- `action_surt_host_name.yaml`
- `action_like_surt_host_name.yaml`

The gneissweb YAML configuration files are similarly named, this naming 
convention is purely for convenience, the system does not require a 'join'
YAML to be named 'join_' explicitedly to work.

To run the python code, you'll need to install a few things in your
virtual environment:

```
pip install -r requirements.txt
```

If you want to use "web" or "s3", you'll need to download some necessary
'path.gz' files:

```
make webgraph
make gneissweb

```

Additionally, for "s3", it is advisable to install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) tools and ensure you are logged in via it before running the examples.


Here are example command lines:

- ```cd examples/webgraph
python ../../annotate.py left_local_host_index.yaml join_local_outin.yaml action_surt_host_name.yaml commoncrawl.org
```
- `cd examples/webgraph; python ../../annotate.py left_web_host_index.yaml join_web_outin.yaml action_surt_host_name.yaml commoncrawl.org`
- `cd examples/webgraph; python ../../annotate.py left_local_host_index.yaml join_local_outin.yaml action_like_surt_host_name.yaml .commoncrawl.org`
- `cd examples/webgraph; python ../../annotate.py left_web_host_index.yaml join_web_outin.yaml action_like_surt_host_name.yaml .commoncrawl.org`
- `cd examples/gneissweb; python ../../annotate.py left_web_host_index.yaml join_local_gneissweb_host.yaml action_gneissweb_medical.yaml`
- `cd examples/gneissweb; python ../../annotate.py left_local_page_index.yaml join_local_gneissweb_page.yaml action_gneissweb_medical_pages_like_surt_host_name.yaml .stanford.edu`



And example csv output:

```
"surt_host_name","crawl","hcrank10","webgraph_outdegree","webgraph_indegree"
"org,commoncrawl","CC-MAIN-2021-49",4.718,,
"org,commoncrawl","CC-MAIN-2022-05",4.718,,
"org,commoncrawl","CC-MAIN-2022-21",4.86,,
"org,commoncrawl","CC-MAIN-2022-27",4.86,,
"org,commoncrawl","CC-MAIN-2022-33",4.86,,
"org,commoncrawl","CC-MAIN-2022-40",4.847,,
"org,commoncrawl","CC-MAIN-2022-49",4.847,,
"org,commoncrawl","CC-MAIN-2023-06",4.847,,
"org,commoncrawl","CC-MAIN-2023-14",5.003,,
"org,commoncrawl","CC-MAIN-2023-23",5.003,,
"org,commoncrawl","CC-MAIN-2023-40",5.003,,
"org,commoncrawl","CC-MAIN-2023-50",4.773,,
"org,commoncrawl","CC-MAIN-2024-10",4.954,,
"org,commoncrawl","CC-MAIN-2024-18",4.879,,
"org,commoncrawl","CC-MAIN-2024-22",4.872,,
"org,commoncrawl","CC-MAIN-2024-26",4.982,,
"org,commoncrawl","CC-MAIN-2024-30",5.085,291,1746
"org,commoncrawl","CC-MAIN-2024-33",4.928,274,1654
"org,commoncrawl","CC-MAIN-2024-38",5.101,288,1608
"org,commoncrawl","CC-MAIN-2024-42",5.067,294,1624
"org,commoncrawl","CC-MAIN-2024-46",4.974,307,1710
"org,commoncrawl","CC-MAIN-2024-51",4.83,329,1588
"org,commoncrawl","CC-MAIN-2025-05",4.967,,
"org,commoncrawl","CC-MAIN-2025-08",4.973,330,1682
"org,commoncrawl","CC-MAIN-2025-13",4.962,,
"org,commoncrawl","CC-MAIN-2025-18",4.845,310,1721
```
## TODOS

- copy script that joins an index and annotation and outputs the result to local disk
