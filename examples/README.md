# Examples

## Annotation examples

| Directory | Level | Description |
|-----------|-------|-------------|
| `external-data/` | host | Ready-to-use external datasets — no fetch scripts needed (see below) |
| `fineweb-edu/` | host | Educational quality scores from HuggingFace FineWeb-Edu (Llama3-70B rated, 0-5 scale) |
| `gneissweb/` | host | Topic classification scores (technology, science, education, medical) |
| `gneissweb-url/` | url | Same as gneissweb but at URL granularity |
| `spam-abuse/` | host | Malware, phishing, and abuse flags from URLhaus, PhishTank, OpenPhish, and UT1 |
| `tranco-top1m/` | host | Domain popularity ranking from Tranco (streams CSV via `table.source`) |
| `university-ranking/` | host | University identification (Hipo) and world rankings (CWUR 2025) |
| `university-ranking-url/` | url | Same as university-ranking but at URL granularity |
| `web-graph/` | host | Link metrics (outdegree, indegree) from Common Crawl's web graph |
| `web-graph-wikipedia/` | host | Multi-join example combining web-graph + wikipedia-spam |
| `wikipedia/categories/` | host | Website classification from Wikipedia categories (fact-checking, fake news, satirical, etc.) |
| `wikipedia/perennial/` | host | Source reliability ratings from 10+ language Wikipedias |
| `wikipedia/spam/` | host | Spam and URL shortener flags from Wikipedia's blacklist |

## External data sources (`external-data/`)

Pre-built YAML files that stream external datasets directly at query time — no local downloads needed. Can be stacked as extra columns on any query.

| YAML | Source | Domains | License |
|------|--------|---------|---------|
| `join_tranco.yaml` | [Tranco](https://tranco-list.eu/) top sites ranking | ~5.4M | CC BY-SA/BY-NC 4.0 |
| `join_majestic_million.yaml` | [Majestic](https://majestic.com/) top 1M by referring subnets | 1M | CC BY 3.0 |
| `join_cisa_gov_domains.yaml` | [CISA](https://github.com/cisagov/dotgov-data) US .gov domain registry | ~12.6K | Public domain |
| `join_gsa_nongov_federal.yaml` | [GSA](https://github.com/GSA/govt-urls) US federal non-.gov domains | ~400 | Public domain |
| `join_ifcn_factcheckers.yaml` | [IFCN](https://github.com/IFCN/verified-signatories) verified fact-checkers | ~167 | Public |
| `join_misinfo_domains.yaml` | [Lasser et al.](https://github.com/JanaLasser/misinformation_domains) misinformation domains | ~4.8K | CC BY-SA 4.0 |

## Quick start

From the project root, fetch dependencies for an example:
```
make web-graph
```

Then run a query:
```
cd examples/web-graph
python annotate.py left_web_host_index.yaml join_web_outin.yaml action_surt_host_name.yaml commoncrawl.org
```

All examples follow the same pattern: `python annotate.py <left.yaml> <join.yaml> [join.yaml ...] <action.yaml> [args]`. See [docs/yaml-reference.md](../docs/yaml-reference.md) for the full YAML spec.
