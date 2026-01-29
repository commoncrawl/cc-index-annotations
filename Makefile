.PHONY: examples

examples: web-graph gneissweb wikipedia

web-graph: examples/web-graph/host-index-paths.gz examples/web-graph/web-graph-outin-paths.gz examples/web-graph/annotate.py

gneissweb: examples/gneissweb/host-index-paths.gz examples/gneissweb/paths.hosts.txt.gz examples/gneissweb/paths.urls.txt.gz examples/gneissweb/cc-index-table.paths.gz examples/gneissweb/annotate.py

wikipedia: examples/wikipedia/wikipedia-spam.txt examples/wikipedia/wikipedia-spam.parquet

examples/wikipedia/wikipedia-spam.parquet: examples/wikipedia/wikipedia-spam.txt
	cd examples/wikipedia; python .convert.py; cd -
examples/wikipedia/wikipedia-spam.txt:
	curl -L -o examples/wikipedia/wikipedia-spam.txt https://meta.wikimedia.org/wiki/Spam_blacklist?action=raw

examples/web-graph/host-index-paths.gz:
	curl  https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz > examples/web-graph/host-index-paths.gz
examples/web-graph/web-graph-outin-paths.gz:
	curl  https://data.commoncrawl.org/projects/web-graph-outin-testing/v1.paths.gz > examples/web-graph/web-graph-outin-paths.gz
examples/web-graph/annotate.py:
	cp annotate.py examples/web-graph/

examples/gneissweb/host-index-paths.gz:
	curl  https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz > examples/gneissweb/host-index-paths.gz
examples/gneissweb/paths.hosts.txt.gz:
	curl  https://data.commoncrawl.org/projects/gneissweb-annotation-testing-v1/paths.hosts.txt.gz > examples/gneissweb/paths.hosts.txt.gz
examples/gneissweb/paths.urls.txt.gz:
	curl  https://data.commoncrawl.org/projects/gneissweb-annotation-testing-v1/paths.urls.txt.gz > examples/gneissweb/paths.urls.txt.gz
examples/gneissweb/cc-index-table.paths.gz:
	curl https://data.commoncrawl.org/crawl-data/CC-MAIN-2020-05/cc-index-table.paths.gz > examples/gneissweb/cc-index-table.paths.gz
examples/gneissweb/annotate.py:
	cp annotate.py examples/gneissweb/
