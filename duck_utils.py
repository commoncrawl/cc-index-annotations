import sys
import os
import os.path
import glob
import gzip

import duckdb
import yaml


def init_duckdb_httpfs(verbose=0):
    # default is 3, 10 should be 1024 * wait
    # I used to have 1000 retries and that definitely wasn't happening
    duckdb.sql('SET http_retries = 10')

    duckdb.sql('SET http_retry_wait_ms = 2000')

    # this defaults off, but seems like a good idea for httpfs
    duckdb.sql('SET enable_object_cache = true')

    if verbose > 1:
        duckdb.sql('SET enable_http_logging = true')
        print('writing duckdb_http log to duckdb_http.log', file=sys.stderr)
        duckdb.sql("SET http_logging_output = './duckdb_http.log'")


def db_config(config, verbose=0):
    table = config['table']

    count = ['local' in table, 'web_prefix' in table, 's3_prefix' in table].count(True)
    if count > 1:
        raise ValueError('can only have 1 of local, web_prefix, and s3_prefix')
    if count == 0:
        raise ValueError('config needs 1 of local, web_prefix, or s3_prefix')

    paths = None
    if 'paths' in table:
        path_file = table['paths']
        if '*' in path_file:
            if 'local' not in table:
                raise NotImplementedError('wildcards only supported for local')
            paths = glob.glob(path_file)
        elif path_file.endswith('.gz'):
            paths = gzip.open(path_file, mode='rt').readlines()
        else:
            paths = open(path_file, mode='rt').readlines()

    if 'local' in table:
        if not paths:
            path = table['local'].rstrip('/')
            if os.path.isdir(path):
                paths = glob.glob(path + '/*.parquet') + glob.glob(path + '/**/*.parquet')
            else:
                raise NotImplementedError('local needs to be a directory if you do not specify paths')
    elif 'web_prefix' in table:
        paths = [(table['web_prefix'].rstrip('/') + '/' + p.rstrip()) for p in paths]
    elif 's3_prefix' in table:
        paths = [(table['s3_prefix'].rstrip('/') + '/' + p.rstrip()) for p in paths]
    else:
        raise NotImplementedError('must specify one of local, web_prefix, s3_prefix')

    if not paths:
        raise NotImplementedError('must specify paths')
    paths = list(sorted(paths))
    if verbose:
        print(len(paths), 'paths')
        print('', '\n '.join(paths[0:5]))

    # limit file list by limits
    limits = config.get('limits')
    if limits:
        if 'grep' in limits:
            # grep is a list, and it is an OR
            paths = [p for p in paths if any(g in p for g in limits['grep'])]
            if verbose:
                print(len(paths), 'paths after grep')
        if 'count' in limits:
            count = limits['count']
            if len(paths) > count:
                paths = paths[0:count]
            if verbose:
                print(len(paths), 'paths after count')

    # load the database into duckdb
    try:
        db = duckdb.read_parquet(paths, hive_partitioning=True)
    except Exception as e:
        print('exception seen: '+str(e))
        print('configuration:', yaml.safe_dump(config))
        print('paths')
        for path in paths:
            print(' ', path)
        raise

    return db
