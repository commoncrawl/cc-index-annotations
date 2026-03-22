#!/usr/bin/env python3
import yaml
from urllib.request import urlopen, Request

UA = 'cc-index-annotations/1.0 (https://github.com/commoncrawl/cc-index-annotations)'
BASE = 'https://tranco-list.eu'

# TRANCO
print('[tranco] resolving latest list URL')
req = Request(f'{BASE}/latest_list', headers={'User-Agent': UA})
resp = urlopen(req, timeout=30)
list_url = resp.url
list_id = list_url.rstrip('/').split('/')[-2]
download_url = f'{BASE}/download/{list_id}/full'
print(f'  list {list_id}: {download_url}')

join = {
    'table': {
        'source': {
            'url': download_url,
            'format': 'csv',
            'options': {
                'header': False,
                'columns': {'rank': 'INTEGER', 'domain': 'VARCHAR'},
            },
        },
    },
    'right_columns': ['rank'],
    'join_columns': {
        'left': 'url_host_registered_domain',
        'right': 'domain',
    },
}
with open('join_tranco.yaml', 'w') as f:
    yaml.safe_dump(join, f, sort_keys=False)
print(f'wrote join_tranco.yaml (list {list_id})')
