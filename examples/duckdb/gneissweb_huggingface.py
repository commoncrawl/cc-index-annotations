import duckdb
from pprint import pprint

con = duckdb.connect()

con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    CALL load_aws_credentials();
    SET s3_region='us-east-1';
""")

result = con.execute("""
    SELECT h.url_host_registered_domain, h.crawl, h.surt_host_name, g.*
    FROM 's3://commoncrawl/projects/host-index-testing/v2/crawl=CC-MAIN-2021-49/*.parquet' h
    JOIN 'hf://datasets/commoncrawl/gneissweb-annotation-testing-v1/hosts/crawl=CC-MAIN-2021-49/*.parquet' g
    ON h.surt_host_name = g.surt_host_name AND h.crawl = g.crawl
    WHERE g.gneissweb_medical > .5
    ORDER BY h.surt_host_name
    LIMIT 2
""")

cols = [desc[0] for desc in result.description]

for row in result.fetchall():
    pprint(dict(zip(cols, row)))
