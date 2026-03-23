.PHONY: examples web-graph gneissweb wikipedia-spam wikipedia-perennial spam-abuse university-ranking university-ranking-url tranco-top1m external-data fineweb-edu wikipedia-categories wikipedia-categories-intl curlie slashtag

examples: web-graph gneissweb wikipedia-spam web-graph-wikipedia wikipedia-perennial university-ranking

# --- Shared downloads (downloaded once, symlinked into examples) ---

shared/host-index-paths.gz:
	mkdir -p shared
	curl -L -o shared/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"

shared/cc-index-table.paths.gz:
	mkdir -p shared
	curl -L -o shared/cc-index-table.paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/cc-index-table.paths.gz"

shared/web-graph-outin-paths.gz:
	mkdir -p shared
	curl -L -o shared/web-graph-outin-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/webgraph-outin-testing/v2.paths.gz"

# --- Helper: symlink shared files + python into an example dir ---
# Usage: $(call link-example,examples/slashtag)
#   depth=2 for examples/foo, depth=3 for examples/wikipedia/foo
define link-example
	cd $(1); ln -sf $(2)/*.py .
	cd $(1); ln -sf $(2)/shared/host-index-paths.gz .
endef

# --- Examples ---

web-graph: shared/host-index-paths.gz shared/web-graph-outin-paths.gz examples/web-graph/annotate.py
examples/web-graph/annotate.py:
	cd examples/web-graph/; ln -s ../../*.py .
	cd examples/web-graph/; ln -sf ../../shared/host-index-paths.gz .
	cd examples/web-graph/; ln -sf ../../shared/web-graph-outin-paths.gz .

gneissweb: shared/host-index-paths.gz examples/gneissweb/paths.hosts.txt.gz examples/gneissweb/paths.urls.txt.gz examples/gneissweb/cc-index-table.paths.gz examples/gneissweb/annotate.py examples/gneissweb-url/annotate.py
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
	cd examples/gneissweb/; ln -sf ../../shared/host-index-paths.gz .
	cd examples/gneissweb-url/; ln -s ../gneissweb/host-index-paths.gz .
examples/gneissweb-url/annotate.py:
	cd examples/gneissweb-url/; ln -s ../../*.py .

wikipedia-spam: examples/wikipedia/spam/wikipedia-spam.txt examples/wikipedia/spam/wikipedia-spam.parquet examples/wikipedia/spam/annotate.py
examples/wikipedia/spam/wikipedia-spam.txt:
	curl -L -o examples/wikipedia/spam/wikipedia-spam.txt --retry 1000 --retry-all-errors --retry-delay 1 https://meta.wikimedia.org/wiki/Spam_blacklist?action=raw
examples/wikipedia/spam/wikipedia-spam.parquet: examples/wikipedia/spam/wikipedia-spam.txt
	cd examples/wikipedia/spam; python .convert.py; cd -
examples/wikipedia/spam/annotate.py: shared/host-index-paths.gz
	cd examples/wikipedia/spam/; ln -s ../../../*.py .
	cd examples/wikipedia/spam/; ln -sf ../../../shared/host-index-paths.gz .

web-graph-wikipedia: web-graph wikipedia-spam examples/web-graph-wikipedia/annotate.py
examples/web-graph-wikipedia/annotate.py:
	cd examples/web-graph-wikipedia/; ln -s ../../*.py .

spam-abuse: examples/spam-abuse/spam-abuse.parquet examples/spam-abuse/annotate.py
examples/spam-abuse/spam-abuse.parquet:
	cd examples/spam-abuse; python spam-abuse-fetch.py; cd -
examples/spam-abuse/annotate.py: shared/host-index-paths.gz
	cd examples/spam-abuse/; ln -s ../../*.py .
	cd examples/spam-abuse/; ln -sf ../../shared/host-index-paths.gz .

wikipedia-perennial: examples/wikipedia/perennial/wp_sources.parquet examples/wikipedia/perennial/wikipedia-perennial.parquet examples/wikipedia/perennial/annotate.py
examples/wikipedia/perennial/wp_sources.parquet:
	cd examples/wikipedia/perennial; python3 wp_sources_scraper.py
examples/wikipedia/perennial/wikipedia-perennial.parquet:
	cd examples/wikipedia/perennial; python3 wikipedia-perennial-fetch.py
examples/wikipedia/perennial/annotate.py: shared/host-index-paths.gz
	cd examples/wikipedia/perennial/; ln -s ../../../*.py .
	cd examples/wikipedia/perennial/; ln -sf ../../../shared/host-index-paths.gz .

curlie: examples/curlie/curlie.parquet examples/curlie/annotate.py
examples/curlie/curlie.parquet: examples/curlie/curlie-rdf-all.tar.gz
	cd examples/curlie; tar xzf curlie-rdf-all.tar.gz; python3 curlie-convert.py; rm -rf curlie-rdf
examples/curlie/curlie-rdf-all.tar.gz:
	curl -L -o examples/curlie/curlie-rdf-all.tar.gz https://curlie.org/directory-dl
examples/curlie/annotate.py: shared/host-index-paths.gz
	cd examples/curlie/; ln -s ../../*.py .
	cd examples/curlie/; ln -sf ../../shared/host-index-paths.gz .

slashtag: examples/slashtag/slashtag.parquet examples/slashtag/annotate.py
examples/slashtag/slashtag.parquet:
	cd examples/slashtag; python3 slashtag-convert.py
	cd examples/slashtag-url/; ln -sf ../slashtag/slashtag.parquet .
	cd examples/slashtag-url/; ln -sf ../slashtag/slashtag-hosts.parquet .
examples/slashtag/annotate.py: shared/host-index-paths.gz
	cd examples/slashtag/; ln -s ../../*.py .
	cd examples/slashtag/; ln -sf ../../shared/host-index-paths.gz .
	cd examples/slashtag-url/; ln -s ../../*.py .
	cd examples/slashtag-url/; ln -sf ../../shared/host-index-paths.gz .
examples/slashtag-url/cc-index-table.paths.gz: shared/cc-index-table.paths.gz
	cd examples/slashtag-url/; ln -sf ../../shared/cc-index-table.paths.gz .

fineweb-edu: examples/fineweb-edu/fineweb-edu.parquet examples/fineweb-edu/annotate.py
examples/fineweb-edu/fineweb-edu.parquet:
	cd examples/fineweb-edu; python fineweb-edu-fetch.py
examples/fineweb-edu/annotate.py: shared/host-index-paths.gz
	cd examples/fineweb-edu/; ln -s ../../*.py .
	cd examples/fineweb-edu/; ln -sf ../../shared/host-index-paths.gz .

wikipedia-categories: examples/wikipedia/categories/wikipedia-categories.parquet examples/wikipedia/categories/annotate.py
examples/wikipedia/categories/wikipedia-categories.parquet:
	cd examples/wikipedia/categories; python3 wikipedia-categories-fetch.py
examples/wikipedia/categories/annotate.py: shared/host-index-paths.gz
	cd examples/wikipedia/categories/; ln -s ../../../*.py .
	cd examples/wikipedia/categories/; ln -sf ../../../shared/host-index-paths.gz .

wikipedia-categories-intl: examples/wikipedia/categories-intl/wikipedia-categories-intl.parquet examples/wikipedia/categories-intl/annotate.py
examples/wikipedia/categories-intl/wikipedia-categories-intl.parquet:
	cd examples/wikipedia/categories-intl; python3 wikipedia-categories-intl-fetch.py
examples/wikipedia/categories-intl/annotate.py: shared/host-index-paths.gz
	cd examples/wikipedia/categories-intl/; ln -s ../../../*.py .
	cd examples/wikipedia/categories-intl/; ln -sf ../../../shared/host-index-paths.gz .

external-data: examples/external-data/annotate.py
examples/external-data/annotate.py: shared/host-index-paths.gz
	cd examples/external-data/; ln -s ../../*.py .
	cd examples/external-data/; ln -sf ../../shared/host-index-paths.gz .

tranco-top1m: examples/tranco-top1m/join_tranco.yaml examples/tranco-top1m/annotate.py
examples/tranco-top1m/join_tranco.yaml:
	cd examples/tranco-top1m; python tranco-fetch.py
examples/tranco-top1m/annotate.py: shared/host-index-paths.gz
	cd examples/tranco-top1m/; ln -s ../../*.py .
	cd examples/tranco-top1m/; ln -sf ../../shared/host-index-paths.gz .

university-ranking: examples/university-ranking/university-ranking.parquet examples/university-ranking/annotate.py
university-ranking-url: university-ranking examples/university-ranking-url/annotate.py examples/university-ranking-url/cc-index-table.paths.gz
examples/university-ranking/university-ranking.parquet:
	cd examples/university-ranking; python university-ranking-fetch.py; cd -
	cd examples/university-ranking-url/; ln -sf ../university-ranking/university-ranking.parquet .
examples/university-ranking/annotate.py: shared/host-index-paths.gz
	cd examples/university-ranking/; ln -s ../../*.py .
	cd examples/university-ranking/; ln -sf ../../shared/host-index-paths.gz .
	cd examples/university-ranking-url/; ln -sf ../university-ranking/host-index-paths.gz .
examples/university-ranking-url/annotate.py:
	cd examples/university-ranking-url/; ln -s ../../*.py .
examples/university-ranking-url/cc-index-table.paths.gz: shared/cc-index-table.paths.gz
	cd examples/university-ranking-url/; ln -sf ../../shared/cc-index-table.paths.gz .
