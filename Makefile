.PHONY: examples web-graph gneissweb wikipedia

examples: web-graph gneissweb wikipedia web-graph-wikipedia wikipedia-perennial

web-graph: examples/web-graph/host-index-paths.gz examples/web-graph/web-graph-outin-paths.gz examples/web-graph/annotate.py

gneissweb: examples/gneissweb/host-index-paths.gz examples/gneissweb/paths.hosts.txt.gz examples/gneissweb/paths.urls.txt.gz examples/gneissweb/cc-index-table.paths.gz examples/gneissweb/annotate.py examples/gneissweb-url/annotate.py

wikipedia: examples/wikipedia/wikipedia-spam.txt examples/wikipedia/wikipedia-domains.parquet examples/wikipedia/wikipedia-perennial.json.txt examples/wikipedia/annotate.py examples/wikipedia/host-index-paths.gz

web-graph-wikipedia: web-graph wikipedia examples/web-graph-wikipedia/annotate.py

wikipedia-perennial: examples/wikipedia-perennial/wp_sources.parquet examples/wikipedia-perennial/annotate.py examples/wikipedia-perennial/host-index-paths.gz

examples/wikipedia-perennial/host-index-paths.gz:
	curl -L -o examples/wikipedia-perennial/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia-perennial/annotate.py:
	cd examples/wikipedia-perennial/; ln -s ../../*.py .
examples/wikipedia-perennial/wp_sources.parquet:
	cd examples/wikipedia-perennial/; python wp_sources_scraper.py

examples/web-graph-wikipedia/annotate.py:
	cd examples/web-graph-wikipedia/; ln -s ../../*.py .

examples/wikipedia/wikipedia-spam.txt:
	curl -L -o examples/wikipedia/wikipedia-spam.txt --retry 1000 --retry-all-errors --retry-delay 1 https://meta.wikimedia.org/wiki/Spam_blacklist?action=raw
examples/wikipedia/wikipedia-perennial.json.txt:
	curl -L -o examples/wikipedia/wikipedia-perennial.json.txt --retry 1000 --retry-all-errors --retry-delay 1 "https://en.wikipedia.org/w/api.php?action=parse&page=Wikipedia:Reliable_sources/Perennial_sources&format=json"
examples/wikipedia/wikipedia-domains.parquet: examples/wikipedia/wikipedia-spam.txt examples/wikipedia/wikipedia-perennial.json.txt 
	cd examples/wikipedia; python .convert.py; cd -
examples/wikipedia/host-index-paths.gz:
	curl -L -o examples/wikipedia/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/wikipedia/annotate.py:
	cd examples/wikipedia/; ln -s ../../*.py .

examples/web-graph/host-index-paths.gz:
	curl -L -o examples/web-graph/host-index-paths.gz --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz"
examples/web-graph/web-graph-outin-paths.gz:
	curl -L -o examples/web-graph/web-graph-outin-paths.gz  --retry 1000 --retry-all-errors --retry-delay 1 "https://data.commoncrawl.org/projects/webgraph-outin-testing/v1.paths.gz"
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
