.PHONY: webgraph gneissweb

webgraph: examples/webgraph/host-index-paths.gz examples/webgraph/webgraph-outin-paths.gz examples/webgraph/annotate.py

gneissweb: examples/gneissweb/host-index-paths.gz examples/gneissweb/paths.hosts.txt.gz examples/gneissweb/paths.urls.txt.gz examples/gneissweb/cc-index-table.paths.gz examples/gneissweb/annotate.py

wikipedia-blacklist: examples/wikipedia-blacklist/blacklist.txt examples/wikipedia-blacklist/blacklist.parquet

examples/wikipedia-blacklist/blacklist.parquet: examples/wikipedia-blacklist/blacklist.txt
	cd examples/wikipedia-blacklist; python .convert.py; cd -

examples/wikipedia-blacklist/blacklist.txt:
	curl -L -o examples/wikipedia-blacklist/blacklist.txt https://meta.wikimedia.org/wiki/Spam_blacklist?action=raw

examples/webgraph/host-index-paths.gz:
	curl  https://data.commoncrawl.org/projects/host-index-testing/v2.paths.gz > examples/webgraph/host-index-paths.gz
examples/webgraph/webgraph-outin-paths.gz:
	curl  https://data.commoncrawl.org/projects/webgraph-outin-testing/v1.paths.gz > examples/webgraph/webgraph-outin-paths.gz
examples/webgraph/annotate.py:
	cp annotate.py examples/webgraph/
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
