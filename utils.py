import datetime
import os

import surt


def thing_to_surt_host_name(thing, verbose=0):
    '''convert a thing, probably an url of some kind, to a surt host name'''
    orig = thing
    if '/' not in thing and '.' not in thing:
        if thing.endswith(',*'):
            thing = thing[:-1]
        if '*' in thing:
            raise ValueError('unexpected * in '+thing)
        if ',,' in thing:
            raise ValueError('unexpected ,, in '+thing)
        if ',' not in thing:
            # assume it is a tld wildcard
            thing += ','
        return thing
    surt_host_name, extra = surt.surt(thing).split(')/', 1)
    if extra:
        if verbose:
            print(f'skipping {orig} because extra is {extra}')
        return
    if surt_host_name.endswith(',*'):
        surt_host_name = surt_host_name[:-1]
    if ',,' in  surt_host_name:
        raise ValueError('unexpected ,, in '+thing)
    return surt_host_name


def dated_output_dir(base_dir, cache_ref=None):
    '''fetched=YYYY-MM-DD/ subdir under base_dir; date from cache_ref's mtime if given, else today (UTC)'''
    if cache_ref and os.path.exists(cache_ref):
        date = datetime.datetime.utcfromtimestamp(os.path.getmtime(cache_ref)).date()
    else:
        date = datetime.datetime.utcnow().date()
    path = os.path.join(base_dir, f'fetched={date.isoformat()}')
    os.makedirs(path, exist_ok=True)
    return path


def refresh_latest_symlink(dated_path):
    '''(re)create <base_dir>/<name> -> fetched=DATE/<name> so flat-path consumers keep working'''
    base_dir, fetched_dir = os.path.split(os.path.dirname(dated_path))
    name = os.path.basename(dated_path)
    link = os.path.join(base_dir, name)
    if os.path.islink(link) or os.path.exists(link):
        os.remove(link)
    os.symlink(os.path.join(fetched_dir, name), link)
