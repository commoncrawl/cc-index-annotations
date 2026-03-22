import sys
import os.path
import glob
import gzip
import re

import yaml
import duckdb
import pyarrow.csv as csv

import utils
import duck_utils

verbose = 1

duck_utils.init_duckdb_httpfs(verbose=verbose)

# Parse command line arguments
if len(sys.argv) < 2:
    raise ValueError("At least database_yaml argument is required")

database_yaml = sys.argv[1]

# Validate database_yaml path exists
if not os.path.exists(database_yaml):
    raise FileNotFoundError(f"Database YAML not found: {database_yaml}")

# Load database configuration
with open(database_yaml, 'r', encoding='utf8') as fd:
    config = yaml.safe_load(fd)

left_db = duck_utils.db_config(config, verbose=verbose)

# Process remaining arguments: find valid YAML paths for joins and action
join_yamls = []
action_yaml = None
passthrough_args = []

for i, arg in enumerate(sys.argv[2:], start=2):
    if os.path.exists(arg) and arg.endswith(('.yaml', '.yml')):
        if action_yaml is None:
            join_yamls.append(arg)
        else:
            # Already found action_yaml, so this and rest are passthrough
            passthrough_args.append(arg)
            passthrough_args.extend(sys.argv[i+1:])
            break
    else:
        # First non-existent/non-YAML path: previous valid path becomes action_yaml
        if join_yamls:
            action_yaml = join_yamls.pop()
        passthrough_args.append(arg)
        passthrough_args.extend(sys.argv[i+1:])
        break
else:
    # All remaining args were valid YAML paths, last one is action_yaml
    if join_yamls:
        action_yaml = join_yamls.pop()

# Backward compatibility: if we have exactly 2 YAML args after database (old behavior)
# treat first as join_yaml and second as action_yaml
if not join_yamls and not action_yaml and len(sys.argv) >= 4:
    # Old style: database_yaml join_yaml action_yaml [args...]
    if os.path.exists(sys.argv[2]) and os.path.exists(sys.argv[3]):
        join_yamls = [sys.argv[2]]
        action_yaml = sys.argv[3]
        passthrough_args = sys.argv[4:]

# Load all join table YAMLs and perform joins
current_view = 'left_db'
for idx, join_yaml in enumerate(join_yamls):
    with open(join_yaml, 'r', encoding='utf8') as fd:
        config = yaml.safe_load(fd)
    
    if verbose:
        print(f'right db config: {config}')

    # Use a unique name for each right table
    right_view = f'right_db_{idx}'
    exec(f'{right_view} = duck_utils.db_config(config, verbose=verbose)')
    
    prefix = config.get('prefix', '')
    def q(col):
        return f'"{col}"'
    if prefix:
        rcols = ', '.join(f'{right_view}.{q(col)} AS {q(prefix + col)}' for col in config['right_columns'])
    else:
        rcols = ', '.join(f'{right_view}.{q(col)}' for col in config['right_columns'])
    join_columns = config['join_columns']
    if isinstance(join_columns, dict):
        left_cols = join_columns['left'] if isinstance(join_columns['left'], list) else [join_columns['left']]
        right_cols = join_columns['right'] if isinstance(join_columns['right'], list) else [join_columns['right']]
        jcols = ' AND '.join(f'{current_view}.{q(lc)} = {right_view}.{q(rc)}' for lc, rc in zip(left_cols, right_cols))
    else:
        jcols = ' AND '.join(f'{current_view}.{q(jc)} = {right_view}.{q(jc)}' for jc in join_columns)
    
    # Determine join type from YAML (default to LEFT OUTER for backward compatibility)
    join_type = config.get('join_type', 'OUTER').upper()
    if join_type == 'INNER':
        join_clause = 'INNER JOIN'
    elif join_type == 'OUTER':
        join_clause = 'LEFT OUTER JOIN'
    else:
        raise ValueError(f"Invalid join_type '{join_type}'. Must be 'INNER' or 'OUTER'")

    # Create intermediate view for chaining
    next_view = 'joined' if idx == len(join_yamls) - 1 else f'join_step_{idx}'
    
    view_sql = '''\
CREATE OR REPLACE VIEW {next_view} AS
SELECT {current_view}.*,
  {rcols}
FROM {current_view}
{join_clause} {right_view}
  ON ({jcols})
'''

    sql = view_sql.format(next_view=next_view, current_view=current_view, right_view=right_view, rcols=rcols, jcols=jcols, join_clause=join_clause)
    if verbose:
        print('view sql is:\n')
        print(sql)

    duckdb.sql(sql)
    
    # Update current_view for next iteration
    current_view = next_view

# Load action configuration (if exists)
if action_yaml:
    with open(action_yaml, 'r', encoding='utf8') as fd:
        action = yaml.safe_load(fd)
else:
    raise ValueError("No action YAML file provided")

# Handle passthrough arguments
argvs = passthrough_args if passthrough_args else ['']

# Validate argv requirement
if not passthrough_args and 'argv' in action:
    raise ValueError('configuration requires an argument')

for argv in argvs:
    and_tld = ''
    
    # Create arg1, arg2, arg3, ... variables for use in action yaml
    arg_vars = {f'arg{i+1}': arg for i, arg in enumerate(argvs)}
    
    if 'argv' in action and 'surt_host_name' in action['argv']:
        argv = utils.thing_to_surt_host_name(argv.rstrip())
        tld = argv.split(',', 1)[0]
        and_tld = f" AND url_host_tld = '{tld}'"
    
    limits = action.get('limits', {}) or {}
    limit_count = limits.get('count')

    sql = action['sql'].format(
        columns=action['columns'],
        where=action['where'].format(argv=argv, and_tld=and_tld, **arg_vars),
    )
    if limit_count:
        sql = re.sub(r'(?i)\s+LIMIT\s+\d+\s*;?\s*$', '', sql)
        sql = sql.rstrip().rstrip(';')
        sql += f'\nLIMIT {int(limit_count)}'
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
