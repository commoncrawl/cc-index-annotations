# Examples

| Directory | Level | Description |
|-----------|-------|-------------|
| `gneissweb/` | host | Topic classification scores (technology, science, education, medical) |
| `gneissweb-url/` | url | Same as gneissweb but at URL granularity |
| `spam-abuse/` | host | Malware, phishing, and abuse flags from URLhaus, PhishTank, and UT1 |
| `university-ranking/` | host | University identification (Hipo) and world rankings (CWUR 2025) |
| `web-graph/` | host | Link metrics (outdegree, indegree) from Common Crawl's web graph |
| `web-graph-wikipedia/` | host | Multi-join example combining web-graph + wikipedia-spam |
| `wikipedia-perennial/` | host | Source reliability ratings from Wikipedia's perennial sources lists |
| `wikipedia-spam/` | host | Spam and URL shortener flags from Wikipedia's blacklist |

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
