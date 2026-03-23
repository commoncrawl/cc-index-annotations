# Slashtag Data (URL-level)

URL-level version of the slashtag annotations. Joins against Common Crawl's URL index on `url_surtkey` for per-URL topic classification.

See `../slashtag/` for full details on the dataset and data attribution.

## Usage

```bash
make slashtag            # from project root (builds both host and URL examples)

cd examples/slashtag-url
python annotate.py left_web_url_index.yaml join_slashtag_url.yaml action_star.yaml
```
