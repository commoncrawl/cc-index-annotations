.PHONY: examples web-graph gneissweb wikipedia-spam wikipedia-perennial spam-abuse university-ranking university-ranking-url tranco-top1m external-data fineweb-edu wikipedia-categories wikipedia-categories-intl

examples: web-graph gneissweb wikipedia-spam web-graph-wikipedia wikipedia-perennial university-ranking

web-graph: examples/web-graph/host-index-paths.gz examples/web-graph/web-graph-outin-paths.gz examples/web-graph/annotate.py

gneissweb: examples/gneissweb/host-index-paths.gz examples/gneissweb/paths.hosts.txt.gz examples/gneissweb/paths.urls.txt.gz examples/gneissweb/cc-index-table.paths.gz examples/gneissweb/annotate.py examples/gneissweb-url/annotate.py

wikipedia-spam: examples/wikipedia/spam/wikipedia-spam.txt examples/wikipedia/spam/wikipedia-spam.parquet examples/wikipedia/spam/annotate.py examples/wikipedia/spam/host-index-paths.gz

web-graph-wikipedia: web-graph wikipedia-spam examples/web-graph-wikipedia/annotate.py

spam-abuse: examples/spam-abuse/spam-abuse.parquet examples/spam-abuse/annotate.py examples/spam-abuse/host-index-paths.gz

wikipedia-perennial: examples/wikipedia/perennial/wp_sources.parquet examples/wikipedia/perennial/wikipedia-perennial.parquet examples/wikipedia/perennial/annotate.py examples/wikipedia/perennial/host-index-paths.gz

examples/wikipedia/perennial/wp_sources.parquet:
	cd examples/wikipedia/perennial; python3 wp_sources_scraper.py

examples/web-graph-wikipedia/annotate.py:
	cd examples/web-graph-wikipedia/; ln -s ../../*.py .

examples/wikipedia/spam/wikipedia-spam.txt:
	curl -L -o examples/wikipedia/spam/wikipedia-spam.txt --retry 1000 --retry-all-errors --retry-delay 1 https://meta.wikimedia.org/wiki/Spam_blacklist?action=raw
examples/wikipedia/spam/wikipedia-spam.parquet: examples/wikipedia/spam/wikipedia-spam.txt
	cd examples/wikipedia/spam; python .convert.py; cd -
examples/wikipedia/spam/host-index-paths.gz:
	curl -L -o examples/wikipedia/spam/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia/spam/annotate.py:
	cd examples/wikipedia/spam/; ln -s ../../../*.py .

examples/web-graph/host-index-paths.gz:
	curl -L -o examples/web-graph/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/web-graph/web-graph-outin-paths.gz:
	curl -L -o examples/web-graph/web-graph-outin-paths.gz  --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/webgraph-outin-testing/v2.paths.gz"
examples/web-graph/annotate.py:
	cd examples/web-graph/; ln -s ../../*.py .

examples/gneissweb/host-index-paths.gz:
	curl -L -o examples/gneissweb/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
	cd examples/gneissweb-url/; ln -s ../gneissweb/host-index-paths.gz .
examples/gneissweb/paths.hosts.txt.gz:
	curl -L -o examples/gneissweb/paths.hosts.txt.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/gneissweb-annotation-testing-v1/paths.hosts.txt.gz"
	cd examples/gneissweb-url/; ln -s ../gneissweb/paths.hosts.txt.gz .
examples/gneissweb/paths.urls.txt.gz:
	curl -L -o examples/gneissweb/paths.urls.txt.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/gneissweb-annotation-testing-v1/paths.urls.txt.gz"
	cd examples/gneissweb-url/; ln -s ../gneissweb/paths.urls.txt.gz .
examples/gneissweb/cc-index-table.paths.gz:
	curl -L -o examples/gneissweb/cc-index-table.paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/crawl-data/CC-MAIN-2020-05/cc-index-table.paths.gz"
	cd examples/gneissweb-url/; ln -s ../gneissweb/cc-index-table.paths.gz .
examples/gneissweb/annotate.py:
	cd examples/gneissweb/; ln -s ../../*.py .
examples/gneissweb-url/annotate.py:
	cd examples/gneissweb-url/; ln -s ../../*.py .

examples/spam-abuse/spam-abuse.parquet:
	cd examples/spam-abuse; python spam-abuse-fetch.py; cd -
examples/spam-abuse/host-index-paths.gz:
	curl -L -o examples/spam-abuse/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/spam-abuse/annotate.py:
	cd examples/spam-abuse/; ln -s ../../*.py .

fineweb-edu: examples/fineweb-edu/fineweb-edu.parquet examples/fineweb-edu/annotate.py examples/fineweb-edu/host-index-paths.gz

examples/fineweb-edu/fineweb-edu.parquet:
	cd examples/fineweb-edu; python fineweb-edu-fetch.py
examples/fineweb-edu/host-index-paths.gz:
	curl -L -o examples/fineweb-edu/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/fineweb-edu/annotate.py:
	cd examples/fineweb-edu/; ln -s ../../*.py .

wikipedia-categories: examples/wikipedia/categories/wikipedia-categories.parquet examples/wikipedia/categories/annotate.py examples/wikipedia/categories/host-index-paths.gz

examples/wikipedia/categories/wikipedia-categories.parquet:
	cd examples/wikipedia/categories; python3 wikipedia-categories-fetch.py
examples/wikipedia/categories/host-index-paths.gz:
	curl -L -o examples/wikipedia/categories/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia/categories/annotate.py:
	cd examples/wikipedia/categories/; ln -s ../../../*.py .

examples/wikipedia/perennial/wikipedia-perennial.parquet:
	cd examples/wikipedia/perennial; python3 wikipedia-perennial-fetch.py
examples/wikipedia/perennial/host-index-paths.gz:
	curl -L -o examples/wikipedia/perennial/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia/perennial/annotate.py:
	cd examples/wikipedia/perennial/; ln -s ../../../*.py .

wikipedia-categories-intl: examples/wikipedia/categories-intl/wikipedia-categories-intl.parquet examples/wikipedia/categories-intl/annotate.py examples/wikipedia/categories-intl/host-index-paths.gz

examples/wikipedia/categories-intl/wikipedia-categories-intl.parquet:
	cd examples/wikipedia/categories-intl; python3 wikipedia-categories-intl-fetch.py
examples/wikipedia/categories-intl/host-index-paths.gz:
	curl -L -o examples/wikipedia/categories-intl/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia/categories-intl/annotate.py:
	cd examples/wikipedia/categories-intl/; ln -s ../../../*.py .

external-data: examples/external-data/annotate.py examples/external-data/host-index-paths.gz

examples/external-data/host-index-paths.gz:
	curl -L -o examples/external-data/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/external-data/annotate.py:
	cd examples/external-data/; ln -s ../../*.py .

tranco-top1m: examples/tranco-top1m/join_tranco.yaml examples/tranco-top1m/annotate.py examples/tranco-top1m/host-index-paths.gz

examples/tranco-top1m/join_tranco.yaml:
	cd examples/tranco-top1m; python tranco-fetch.py
examples/tranco-top1m/host-index-paths.gz:
	curl -L -o examples/tranco-top1m/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/tranco-top1m/annotate.py:
	cd examples/tranco-top1m/; ln -s ../../*.py .

university-ranking: examples/university-ranking/university-ranking.parquet examples/university-ranking/annotate.py examples/university-ranking/host-index-paths.gz examples/university-ranking-url/annotate.py examples/university-ranking-url/cc-index-table.paths.gz

examples/university-ranking/university-ranking.parquet:
	cd examples/university-ranking; python university-ranking-fetch.py; cd -
	cd examples/university-ranking-url/; ln -sf ../university-ranking/university-ranking.parquet .
examples/university-ranking/host-index-paths.gz:
	curl -L -o examples/university-ranking/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
	cd examples/university-ranking-url/; ln -sf ../university-ranking/host-index-paths.gz .
examples/university-ranking/annotate.py:
	cd examples/university-ranking/; ln -s ../../*.py .
examples/university-ranking-url/annotate.py:
	cd examples/university-ranking-url/; ln -s ../../*.py .
examples/university-ranking-url/cc-index-table.paths.gz:
	curl -L -o examples/university-ranking-url/cc-index-table.paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/cc-index-table.paths.gz"
