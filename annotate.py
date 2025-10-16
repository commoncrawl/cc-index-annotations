import sys
import os.path
import glob
import gzip

import yaml
import duckdb
import pyarrow.csv as csv

import utils
import duck_utils

verbose = 1

duck_utils.init_duckdb_httpfs(verbose=verbose)

database_yaml = sys.argv[1]
join_yaml = sys.argv[2]
action_yaml = sys.argv[3]

with open(database_yaml, 'r', encoding='utf8') as fd:
    config = yaml.safe_load(fd)

left_db = duck_utils.db_config(config, verbose=verbose)

with open(join_yaml, 'r', encoding='utf8') as fd:
    config = yaml.safe_load(fd)

right_db = duck_utils.db_config(config, verbose=verbose)

rcols = ', '.join(config['right_columns'])
jcols = ' AND '.join(f'left_db.{jc} = right_db.{jc}' for jc in config['join_columns'])

view_sql = '''\
CREATE VIEW joined AS
SELECT *,
  {rcols}
FROM left_db
LEFT OUTER JOIN right_db
  ON ({jcols})
'''

sql = view_sql.format(rcols=rcols, jcols=jcols)
if verbose:
    print('view sql is:\n')
    print(sql)

joined = duckdb.sql(sql)

with open(action_yaml, 'r', encoding='utf8') as fd:
    action = yaml.safe_load(fd)

# optional argv
# optional argv transformation

argvs = sys.argv[4:]
if not argvs:
    if 'argv' in action:
        raise ValueError('configuration requires an argument')
    argvs = ['']

for argv in argvs:
    and_tld = ''
    if 'argv' in action and 'surt_host_name' in action['argv']:
        argv = utils.thing_to_surt_host_name(argv.rstrip())
        tld = argv.split(',', 1)[0]
        and_tld = f" AND url_host_tld = '{tld}'"
    sql = action['sql'].format(
        columns=action['columns'],
        where=action['where'].format(argv=argv, and_tld=and_tld),
    )
    if verbose:
        print('action sql is:\n')
        print(sql)

    table = duckdb.sql(sql).fetch_arrow_table()
    if verbose:
        print('result is', table.num_rows, 'rows')

    if isinstance(argv, str):
        filename = argv + '.csv' if argv else 'output.csv'
        print('writing', filename)
        with open(filename, 'wb') as fd:
            csv.write_csv(table, fd)
    else:
        csv.write_csv(table, sys.stdout.buffer)
