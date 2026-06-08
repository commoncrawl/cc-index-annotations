#!/usr/bin/env python3
"""
Build a single combined parquet from ALL cc-index-annotations sources.

Sources:
  9 local    — parquets produced by `make` (curlie, slashtag, spam-abuse, …)
  2 remote   — S3/HTTP parquets rolled up on the fly (gneissweb, web-graph)
  6 external — CSVs downloaded and converted (tranco, majestic, cisa, …)

Sources with multiple rows per surt_host_name (e.g. per-crawl data) are
automatically rolled up:
  - numeric columns  → <col>_avg and <col>_max
  - boolean columns  → OR'd (true if any row is true)
  - string columns   → distinct values joined with '; '
  - list columns     → flattened and deduplicated

Resumable: re-run the same command after an interruption and it picks up
where it left off.  Use --force to rebuild from scratch.
"""

import argparse
import glob
import gzip
import os
import random
import subprocess
import sys
import textwrap
import time

import duckdb
import surt as surt_lib
import yaml


def _find_repo_root():
    """
    Find the cc-index-annotations repo root.

    The Makefile symlinks *.py into every example dir, so this script
    may be invoked via a symlink.  We resolve that, then validate.
    """
    markers = ("Makefile", "examples", "annotate.py")

    # Resolve symlinks — handles `cd examples/foo && python build_combined_parquet.py`
    candidate = os.path.dirname(os.path.realpath(__file__))
    if all(os.path.exists(os.path.join(candidate, m)) for m in markers):
        return candidate

    # Fallback: walk up from cwd
    candidate = os.path.abspath(".")
    for _ in range(10):
        if all(os.path.exists(os.path.join(candidate, m)) for m in markers):
            return candidate
        parent = os.path.dirname(candidate)
        if parent == candidate:
            break
        candidate = parent

    sys.exit(
        "Cannot find the cc-index-annotations repo root.\n"
        "Run this script from the repo root:\n"
        "  cd /path/to/cc-index-annotations\n"
        "  python build_combined_parquet.py -o combined.parquet"
    )


ROOT = _find_repo_root()

# ═══════════════════════════════════════════════════════════════════════
# Source registries
# ═══════════════════════════════════════════════════════════════════════

LOCAL_SOURCES = {
    "curlie": {
        "dir": "examples/curlie",
        "parquet": "curlie.parquet",
        "make_target": "curlie",
        "col_prefix": "curlie",
    },
    "fineweb_edu": {
        "dir": "examples/fineweb-edu",
        "parquet": "fineweb-edu.parquet",
        "make_target": "fineweb-edu",
        "fetch": "python3 fineweb-edu-fetch.py",
        "col_prefix": "fwedu",
    },
    "slashtag": {
        "dir": "examples/slashtag",
        "parquet": "slashtag-hosts.parquet",
        "make_target": "slashtag",
        "fetch": "python3 slashtag-convert.py",
        "col_prefix": "slashtag",
    },
    "spam_abuse": {
        "dir": "examples/spam-abuse",
        "parquet": "spam-abuse.parquet",
        "make_target": "spam-abuse",
        "fetch": "python3 spam-abuse-fetch.py",
        "col_prefix": "spam",
    },
    "university_ranking": {
        "dir": "examples/university-ranking",
        "parquet": "university-ranking.parquet",
        "make_target": "university-ranking",
        "fetch": "python3 university-ranking-fetch.py",
        "extra_flags": {"include_cwur": "--include-cwur"},
        "col_prefix": "unirank",
    },
    "wikipedia_spam": {
        "dir": "examples/wikipedia/spam",
        "parquet": "wikipedia-spam.parquet",
        "make_target": "wikipedia-spam",
        "col_prefix": "wikispam",
    },
    "wikipedia_perennial": {
        "dir": "examples/wikipedia/perennial",
        "parquet": "wikipedia-perennial*.parquet",
        "make_target": "wikipedia-perennial",
        "fetch": "python3 wikipedia-perennial-fetch.py",
        "col_prefix": "wikiperl",
    },
    "wikipedia_categories": {
        "dir": "examples/wikipedia/categories",
        "parquet": "wikipedia-categories.parquet",
        "make_target": "wikipedia-categories",
        "fetch": "python3 wikipedia-categories-fetch.py",
        "extra_flags": {"deep": "--deep --no-skip"},
        "col_prefix": "wikicat",
    },
    "wikipedia_categories_intl": {
        "dir": "examples/wikipedia/categories-intl",
        "parquet": "wikipedia-categories-intl.parquet",
        "make_target": "wikipedia-categories-intl",
        "fetch": "python3 wikipedia-categories-intl-fetch.py",
        "extra_flags": {"deep": "--deep --no-skip"},
        "col_prefix": "wikicat_intl",
    },
}

REMOTE_SOURCES = {
    "gneissweb": {
        "make_target": "gneissweb",
        "paths_file": "examples/gneissweb/paths.hosts.txt.gz",
        "web_prefix": "https://data.commoncrawl.org/",
        "col_prefix": "gneiss",
        "batch_size": 10,
    },
    "web_graph": {
        "make_target": "web-graph",
        "paths_file": "shared/web-graph-outin-paths.gz",
        "web_prefix": "https://data.commoncrawl.org/",
        "col_prefix": "webgraph",
        "passthrough": True,  # download each file locally, rollup from disk
    },
}

EXTERNAL_SOURCES = {
    "tranco": {
        "yaml": "examples/external-data/join_tranco.yaml",
        "col_prefix": "tranco",
    },
    "majestic_million": {
        "yaml": "examples/external-data/join_majestic_million.yaml",
        "col_prefix": "majestic",
    },
    "cisa_gov_domains": {
        "yaml": "examples/external-data/join_cisa_gov_domains.yaml",
        "col_prefix": "cisa",
    },
    "gsa_nongov_federal": {
        "yaml": "examples/external-data/join_gsa_nongov_federal.yaml",
        "col_prefix": "gsa",
    },
    "ifcn_factcheckers": {
        "yaml": "examples/external-data/join_ifcn_factcheckers.yaml",
        "col_prefix": "ifcn",
    },
    "misinfo_domains": {
        "yaml": "examples/external-data/join_misinfo_domains.yaml",
        "col_prefix": "misinfo",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def to_surt(domain):
    try:
        return surt_lib.surt(f"http://{domain}").split(")/")[0]
    except Exception:
        return None


def run(cmd, cwd=None, check=True):
    print(f"  $ {cmd}" + (f"  (in {cwd})" if cwd else ""), file=sys.stderr, flush=True)
    # Stream output to terminal in real-time so the user sees progress
    # from long-running fetch scripts (university-ranking, wikipedia, etc.)
    r = subprocess.run(cmd, shell=True, cwd=cwd)
    if r.returncode != 0:
        if check:
            sys.exit(f"Command failed (exit {r.returncode}): {cmd}")
        return False
    return True


def parquet_is_valid(path_or_glob):
    if not glob.glob(path_or_glob):
        return False
    try:
        c = duckdb.connect()
        c.execute(f"SELECT count(*) FROM read_parquet('{path_or_glob}', union_by_name=true)")
        c.close()
        return True
    except Exception:
        return False


# ── Polite HTTP fetching ─────────────────────────────────────────────
# Matches the pattern used by the existing fetch scripts in examples/.

UA = (
    "cc-index-annotations/1.0 "
    "(Common Crawl Foundation; "
    "https://github.com/commoncrawl/cc-index-annotations)"
)
SLEEP_BETWEEN_DOWNLOADS = 2.0  # seconds between sequential downloads


def polite_download(url, dest, max_retries=5, initial_delay=2.0):
    """
    Download a URL to a local file with exponential backoff + jitter.

    Matches the retry pattern from the existing fetch scripts:
      - exponential backoff (delay doubles each retry)
      - random jitter (0 to 50% of current delay)
      - Retry-After header respected if present
      - User-Agent identifies the project
      - atomic write (temp file + rename)
    """
    from urllib.request import urlopen, Request
    from urllib.error import URLError, HTTPError

    delay = initial_delay
    for attempt in range(max_retries + 1):
        try:
            req = Request(url, headers={"User-Agent": UA})
            with urlopen(req, timeout=120) as resp:
                data = resp.read()
            # Atomic write
            tmp = dest + ".tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, dest)
            return len(data)
        except HTTPError as e:
            if attempt == max_retries:
                raise
            # Respect Retry-After header
            retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
            if retry_after:
                try:
                    wait = float(retry_after)
                except ValueError:
                    wait = delay
                print(
                    f"    HTTP {e.code}, Retry-After: {retry_after}s "
                    f"(attempt {attempt + 1}/{max_retries})",
                    file=sys.stderr,
                )
            else:
                jitter = random.uniform(0, delay * 0.5)
                wait = delay + jitter
                print(
                    f"    HTTP {e.code}, retrying in {wait:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries})",
                    file=sys.stderr,
                )
            time.sleep(wait)
            delay *= 2
        except (URLError, TimeoutError, OSError) as e:
            if attempt == max_retries:
                raise
            jitter = random.uniform(0, delay * 0.5)
            wait = delay + jitter
            print(
                f"    {e}, retrying in {wait:.1f}s "
                f"(attempt {attempt + 1}/{max_retries})",
                file=sys.stderr,
            )
            time.sleep(wait)
            delay *= 2


def init_httpfs(con):
    """
    Configure httpfs + S3 credentials on a DuckDB connection.

    Uses conservative retry settings:
      - 5 retries (not 10+) with 3s base wait
      - DuckDB does exponential backoff internally
      - Object cache enabled to avoid re-fetching metadata
    """
    con.execute("SET http_retries = 5")
    con.execute("SET http_retry_wait_ms = 3000")
    con.execute("SET enable_object_cache = true")
    try:
        con.execute("""CREATE OR REPLACE SECRET secret (
            TYPE s3, PROVIDER credential_chain
        );""")
    except Exception:
        con.execute("""CREATE OR REPLACE SECRET secret (
            TYPE S3, PROVIDER CONFIG, REGION 'us-east-1'
        );""")


def safe_duckdb(staging_dir=None):
    """
    Create a DuckDB connection that spills to disk instead of OOM-killing.

    Sets memory_limit to ~60% of available RAM (or 2GB floor) and uses
    staging/ as a temp directory for spill files.
    """
    con = duckdb.connect()

    # Figure out a reasonable memory limit
    try:
        import shutil
        total_ram = shutil.disk_usage("/").total  # not great but fallback
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        avail_kb = int(line.split()[1])
                        mem_limit = max(int(avail_kb * 0.5 / 1024), 2048)  # MB
                        break
                else:
                    mem_limit = 2048
        except (FileNotFoundError, ValueError):
            mem_limit = 2048
    except Exception:
        mem_limit = 2048

    temp_dir = os.path.join(staging_dir or ".", ".duckdb_tmp")
    os.makedirs(temp_dir, exist_ok=True)

    con.execute(f"SET memory_limit = '{mem_limit}MB'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute("SET enable_progress_bar = true")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET threads = 4")

    return con


def classify_col(col_type):
    """Classify a DuckDB column type for rollup aggregation."""
    t = col_type.upper()
    if any(k in t for k in [
        "INT", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
        "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL",
    ]):
        return "numeric"
    if "BOOL" in t:
        return "boolean"
    if "LIST" in t or "[]" in t:
        return "list"
    return "string"


def build_rollup_select(columns):
    """
    Build SELECT parts for GROUP BY surt_host_name rollup.

    columns: list of (col_name, col_type) from DESCRIBE.
    Returns: list of SQL expressions.
    """
    parts = ["surt_host_name"]
    for col_name, col_type in columns:
        if col_name == "surt_host_name":
            continue
        kind = classify_col(col_type)
        q = f'"{col_name}"'
        if kind == "numeric":
            parts.append(f'AVG({q}) AS "{col_name}_avg"')
            parts.append(f'MAX({q}) AS "{col_name}_max"')
        elif kind == "boolean":
            parts.append(f'BOOL_OR({q}) AS {q}')
        elif kind == "list":
            parts.append(
                f'LIST_SORT(LIST_DISTINCT(FLATTEN(LIST({q})))) AS {q}'
            )
        else:
            parts.append(
                f"STRING_AGG(DISTINCT CAST({q} AS VARCHAR), '; '"
                f" ORDER BY CAST({q} AS VARCHAR)) AS {q}"
            )
    return parts


# --- Batched rollup for large remote sources ---
# You can't AVG(AVG(...)) across batches, so the intermediate pass stores
# SUM + COUNT + MAX.  The final pass reconstructs AVG = SUM(sums)/SUM(counts).

BATCH_SIZE = 10  # remote parquet files per batch


def _batch_intermediate_select(columns):
    """SELECT parts for intermediate batch rollup (SUM/COUNT/MAX for numerics)."""
    parts = ["surt_host_name"]
    for col_name, col_type in columns:
        if col_name == "surt_host_name":
            continue
        kind = classify_col(col_type)
        q = f'"{col_name}"'
        if kind == "numeric":
            parts.append(f'SUM(CAST({q} AS DOUBLE)) AS "{col_name}__sum"')
            parts.append(f'COUNT({q}) AS "{col_name}__count"')
            parts.append(f'MAX({q}) AS "{col_name}__max"')
        elif kind == "boolean":
            parts.append(f'BOOL_OR({q}) AS {q}')
        elif kind == "list":
            parts.append(
                f'LIST_SORT(LIST_DISTINCT(FLATTEN(LIST({q})))) AS {q}'
            )
        else:
            parts.append(
                f"STRING_AGG(DISTINCT CAST({q} AS VARCHAR), '; '"
                f" ORDER BY CAST({q} AS VARCHAR)) AS {q}"
            )
    return parts


def _batch_final_select(columns):
    """SELECT parts for final merge across batch outputs → _avg and _max."""
    parts = ["surt_host_name"]
    for col_name, col_type in columns:
        if col_name == "surt_host_name":
            continue
        kind = classify_col(col_type)
        if kind == "numeric":
            s = f'"{col_name}__sum"'
            c = f'"{col_name}__count"'
            m = f'"{col_name}__max"'
            parts.append(
                f'CASE WHEN SUM({c}) > 0 THEN SUM({s}) / SUM({c}) '
                f'ELSE NULL END AS "{col_name}_avg"'
            )
            parts.append(f'MAX({m}) AS "{col_name}_max"')
        elif kind == "boolean":
            parts.append(f'BOOL_OR("{col_name}") AS "{col_name}"')
        elif kind == "list":
            parts.append(
                f'LIST_SORT(LIST_DISTINCT(FLATTEN(LIST("{col_name}")))) '
                f'AS "{col_name}"'
            )
        else:
            # Re-split '; '-joined strings, flatten, deduplicate, rejoin
            parts.append(
                f"ARRAY_TO_STRING(LIST_SORT(LIST_DISTINCT(FLATTEN("
                f"LIST(STRING_SPLIT(\"{col_name}\", '; '))))), '; ') "
                f'AS "{col_name}"'
            )
    return parts


def ensure_rolled_up(parquet_path, source_name, staging_dir, force=False):
    """
    If a source has multiple rows per surt_host_name, create a rolled-up
    version in staging/ and return that path.  Otherwise return as-is.
    """
    rollup_path = os.path.join(staging_dir, f"rollup_{source_name}.parquet")

    if not force and os.path.exists(rollup_path) and parquet_is_valid(rollup_path):
        print(f"  [rollup cached] {source_name}", file=sys.stderr)
        return rollup_path

    con = safe_duckdb(staging_dir)
    src = f"read_parquet('{parquet_path}', union_by_name=true)"

    dup_count = con.execute(f"""
        SELECT count(*) FROM (
            SELECT surt_host_name FROM {src}
            GROUP BY surt_host_name HAVING count(*) > 1
        )
    """).fetchone()[0]

    if dup_count == 0:
        con.close()
        return parquet_path

    total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    unique = con.execute(
        f"SELECT count(DISTINCT surt_host_name) FROM {src}"
    ).fetchone()[0]
    print(
        f"  [rollup] {source_name}: {total:,} rows → {unique:,} hosts "
        f"({dup_count:,} with duplicates)",
        file=sys.stderr,
    )

    cols = [
        (row[0], row[1])
        for row in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    ]
    select_parts = build_rollup_select(cols)

    sql = f"SELECT {', '.join(select_parts)} FROM {src} GROUP BY surt_host_name"
    tmp = rollup_path + ".tmp"
    con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT PARQUET)")
    os.replace(tmp, rollup_path)

    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{rollup_path}')"
    ).fetchone()[0]
    print(f"  [rollup] wrote {rollup_path}: {n:,} rows", file=sys.stderr)
    con.close()
    return rollup_path


# ═══════════════════════════════════════════════════════════════════════
# Source builders
# ═══════════════════════════════════════════════════════════════════════

def build_local_source(name, cfg, force=False, active_flags=None):
    active_flags = active_flags or set()
    src_dir = os.path.join(ROOT, cfg["dir"])
    parquet_pattern = os.path.join(src_dir, cfg["parquet"])

    extra_map = cfg.get("extra_flags", {})
    needs_extra = any(f in active_flags for f in extra_map)

    # Always check cache first — if the parquet exists and we're not
    # forcing a rebuild, use it.  --deep/--include-cwur only matter
    # when building from scratch.  Use --force to rebuild with new flags.
    if not force and parquet_is_valid(parquet_pattern):
        n = len(glob.glob(parquet_pattern))
        print(f"  [cached] {name}: {n} parquet(s)", file=sys.stderr)
        return parquet_pattern

    make_target = cfg.get("make_target")
    if make_target:
        print(f"\n[make] {name}", file=sys.stderr)
        run(f"make {make_target}", cwd=ROOT, check=False)

    if not needs_extra and parquet_is_valid(parquet_pattern):
        print(f"  [built] {name}: ready", file=sys.stderr)
        return parquet_pattern

    if needs_extra:
        fetch_cmd = cfg.get("fetch")
        if not fetch_cmd:
            if parquet_is_valid(parquet_pattern):
                return parquet_pattern
            print(f"  [warn] {name}: no fetch command for re-run", file=sys.stderr)
            return None

        # Don't delete the existing parquet — the fetch script will
        # overwrite it on success, and we keep the old one if it fails.
        for flag_name, flag_args in extra_map.items():
            if flag_name in active_flags:
                fetch_cmd += f" {flag_args}"
                print(f"  [flag] adding {flag_args}", file=sys.stderr)
        print(f"  [fetch] {name}", file=sys.stderr)
        if not run(fetch_cmd, cwd=src_dir, check=False):
            print(f"  [warn] {name}: fetch failed", file=sys.stderr)
            # Fall through: if old parquet still exists, use it
            if parquet_is_valid(parquet_pattern):
                print(f"  [fallback] using previous parquet", file=sys.stderr)
                return parquet_pattern
            return None
        if parquet_is_valid(parquet_pattern):
            return parquet_pattern

    print(f"  [warn] {name}: no parquet produced", file=sys.stderr)
    return None


def build_remote_source(name, cfg, staging_dir, force=False, crawl_filter=None):
    """
    Read remote parquets via path file, rollup to one row per
    surt_host_name, save to staging/.

    Processes files in batches of BATCH_SIZE to avoid OOM on large sources
    (gneissweb has 96 crawl-partitioned parquets).

    Flow:
      1. Split remote paths into batches
      2. Each batch: GROUP BY surt_host_name → intermediate parquet
         (stores SUM/COUNT/MAX for numerics so final AVG is correct)
      3. Final pass: read all batch outputs, GROUP BY again → output
      4. Clean up batch intermediates
    """
    # Use different cache filename when crawl_filter is active so
    # a preview build doesn't collide with the full version
    if crawl_filter:
        suffix = f"_{len(crawl_filter)}crawls"
    else:
        suffix = ""
    out_path = os.path.join(staging_dir, f"remote_{name}{suffix}.parquet")
    full_path = os.path.join(staging_dir, f"remote_{name}.parquet")

    # If the full (unfiltered) version already exists, use it even when
    # --crawl-filter is set — no point re-processing a completed source
    if not force and crawl_filter and os.path.exists(full_path) and parquet_is_valid(full_path):
        print(f"  [cached] {name}: {full_path} (full version)", file=sys.stderr)
        return full_path

    if not force and os.path.exists(out_path) and parquet_is_valid(out_path):
        print(f"  [cached] {name}: {out_path}", file=sys.stderr)
        return out_path

    # Ensure path file is downloaded
    make_target = cfg.get("make_target")
    if make_target:
        run(f"make {make_target}", cwd=ROOT, check=False)

    paths_file = os.path.join(ROOT, cfg["paths_file"])
    if not os.path.exists(paths_file):
        print(
            f"  [warn] {name}: path file not found: {paths_file}",
            file=sys.stderr,
        )
        return None

    # Read path list
    if paths_file.endswith(".gz"):
        lines = gzip.open(paths_file, "rt").readlines()
    else:
        lines = open(paths_file, "rt").readlines()

    prefix = cfg.get("web_prefix", "")
    paths = [prefix + p.strip() for p in lines if p.strip()]

    # Filter by crawl if requested
    if crawl_filter:
        before = len(paths)
        paths = [
            p for p in paths
            if any(f"crawl={c}" in p for c in crawl_filter)
        ]
        print(
            f"\n[remote] {name}: {len(paths)}/{before} files "
            f"(filtered to {len(crawl_filter)} crawls)",
            file=sys.stderr,
        )
    else:
        print(
            f"\n[remote] {name}: {len(paths)} remote parquet(s)",
            file=sys.stderr,
        )

    con = safe_duckdb(staging_dir)
    try:
        init_httpfs(con)
    except Exception as e:
        print(f"  [warn] {name}: httpfs init failed: {e}", file=sys.stderr)
        con.close()
        return None

    # Get schema from first file
    try:
        cols = [
            (row[0], row[1])
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{paths[0]}', "
                f"hive_partitioning=true)"
            ).fetchall()
        ]
    except Exception as e:
        print(f"  [warn] {name}: can't read schema: {e}", file=sys.stderr)
        con.close()
        return None

    print(
        f"  columns: {', '.join(c[0] for c in cols)}",
        file=sys.stderr,
    )

    # --- Download / batch processing ---
    passthrough = cfg.get("passthrough", False)
    batch_size = cfg.get("batch_size", BATCH_SIZE)
    batch_dir = os.path.join(staging_dir, f".batches_{name}{suffix}")
    os.makedirs(batch_dir, exist_ok=True)

    if passthrough:
        # --- Passthrough mode: download each file locally, rollup later ---
        # Used for huge datasets (web_graph) where even a small-batch
        # GROUP BY OOMs.  Download raw data to local parquets, then do
        # the rollup from local files where DuckDB can spill to disk.
        non_join = [c for c in [col[0] for col in cols] if c != "crawl"]
        col_list = ", ".join(f'"{c}"' for c in non_join)

        batch_outputs = []
        for fi, file_path in enumerate(paths):
            local_out = os.path.join(batch_dir, f"file_{fi:03d}.parquet")

            if not force and os.path.exists(local_out) and parquet_is_valid(local_out):
                print(f"  file {fi+1}/{len(paths)}: cached", file=sys.stderr)
                batch_outputs.append(local_out)
                continue

            tmp = local_out + ".tmp"
            try:
                print(
                    f"  file {fi+1}/{len(paths)}: downloading...",
                    file=sys.stderr, flush=True,
                )
                sql = (
                    f"SELECT {col_list} FROM read_parquet('{file_path}', "
                    f"hive_partitioning=true)"
                )
                con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT PARQUET)")
                os.replace(tmp, local_out)
                n = con.execute(
                    f"SELECT count(*) FROM read_parquet('{local_out}')"
                ).fetchone()[0]
                print(f"    → {n:,} rows", file=sys.stderr)
                batch_outputs.append(local_out)
            except Exception as e:
                print(f"    FAILED: {e}", file=sys.stderr)
                if os.path.exists(tmp):
                    os.unlink(tmp)

        if not batch_outputs:
            print(f"  [warn] {name}: all downloads failed", file=sys.stderr)
            con.close()
            return None

        # Rollup via hash-partitioning — the GROUP BY hash table for
        # 350M+ unique hosts doesn't fit in RAM.  Strategy:
        #   1. Hash-partition each local file into N buckets (streaming)
        #   2. GROUP BY each bucket separately (~11M hosts each, fits)
        #   3. Concatenate bucket results
        con.close()
        con = safe_duckdb(staging_dir)

        NUM_BUCKETS = 32
        rollup_cols = [
            (c[0], c[1]) for c in cols if c[0] != "crawl"
        ]
        rollup_parts = build_rollup_select(rollup_cols)
        all_local_glob = os.path.join(batch_dir, "file_*.parquet")
        partition_dir = os.path.join(batch_dir, ".partitions")
        os.makedirs(partition_dir, exist_ok=True)

        # Step 1: hash-partition each downloaded file
        # DuckDB reads each file once and writes to bucket subdirectories
        non_join_cols = [c[0] for c in rollup_cols]
        select_cols = ", ".join(f'"{c}"' for c in non_join_cols)

        for fi, local_file in enumerate(batch_outputs):
            marker = os.path.join(partition_dir, f".done_{fi:03d}")
            if not force and os.path.exists(marker):
                print(
                    f"  partition {fi+1}/{len(batch_outputs)}: cached",
                    file=sys.stderr,
                )
                continue

            print(
                f"  partition {fi+1}/{len(batch_outputs)}: "
                f"splitting into {NUM_BUCKETS} buckets...",
                file=sys.stderr, flush=True,
            )
            part_sql = (
                f"COPY ("
                f"  SELECT {select_cols}, "
                f"  hash(surt_host_name) % {NUM_BUCKETS} AS _bucket "
                f"  FROM read_parquet('{local_file}')"
                f") TO '{partition_dir}/from_{fi:03d}/' "
                f"(FORMAT PARQUET, PARTITION_BY (_bucket))"
            )
            try:
                con.execute(part_sql)
                # Write marker so we don't redo on restart
                with open(marker, "w") as f:
                    f.write("done\n")
            except Exception as e:
                print(f"    FAILED: {e}", file=sys.stderr)

        # Step 2: GROUP BY each bucket (small hash table per bucket)
        print(
            f"  rolling up {NUM_BUCKETS} buckets...",
            file=sys.stderr, flush=True,
        )
        rolled_parts = []
        for bi in range(NUM_BUCKETS):
            rolled_out = os.path.join(partition_dir, f"rolled_{bi:02d}.parquet")

            if not force and os.path.exists(rolled_out) and parquet_is_valid(rolled_out):
                rolled_parts.append(rolled_out)
                continue

            # Glob all files for this bucket across all source files
            bucket_glob = os.path.join(
                partition_dir, f"from_*/_bucket={bi}/*.parquet"
            )
            bucket_files = glob.glob(bucket_glob)
            if not bucket_files:
                continue

            bucket_sql = (
                f"SELECT {', '.join(rollup_parts)} "
                f"FROM read_parquet('{bucket_glob}', union_by_name=true) "
                f"GROUP BY surt_host_name"
            )
            tmp = rolled_out + ".tmp"
            try:
                con.execute(f"COPY ({bucket_sql}) TO '{tmp}' (FORMAT PARQUET)")
                os.replace(tmp, rolled_out)
                rolled_parts.append(rolled_out)
                print(f"    bucket {bi+1}/{NUM_BUCKETS} ✓",
                      file=sys.stderr, flush=True)
            except Exception as e:
                print(f"    bucket {bi+1}/{NUM_BUCKETS} FAILED: {e}",
                      file=sys.stderr)
                if os.path.exists(tmp):
                    os.unlink(tmp)

        if not rolled_parts:
            print(f"  [warn] {name}: all buckets failed", file=sys.stderr)
            con.close()
            return None

        # Step 3: concatenate bucket results (no GROUP BY needed)
        print(
            f"  concatenating {len(rolled_parts)} buckets → {out_path}",
            file=sys.stderr, flush=True,
        )
        rolled_glob = os.path.join(partition_dir, "rolled_*.parquet")
        rollup_sql = (
            f"SELECT * FROM read_parquet('{rolled_glob}', "
            f"union_by_name=true)"
        )

    else:
        # --- Standard batched mode: GROUP BY per batch, then merge ---
        batches = [
            paths[i:i + batch_size]
            for i in range(0, len(paths), batch_size)
        ]

        intermediate_select = _batch_intermediate_select(cols)
        batch_outputs = []

        for bi, batch_paths in enumerate(batches):
            batch_out = os.path.join(batch_dir, f"batch_{bi:03d}.parquet")

            if not force and os.path.exists(batch_out) and parquet_is_valid(batch_out):
                print(
                    f"  batch {bi+1}/{len(batches)}: cached",
                    file=sys.stderr,
                )
                batch_outputs.append(batch_out)
                continue

            batch_sql_paths = (
                "[" + ", ".join(f"'{p}'" for p in batch_paths) + "]"
            )
            src = (
                f"read_parquet({batch_sql_paths}, union_by_name=true, "
                f"hive_partitioning=true)"
            )
            sql = (
                f"SELECT {', '.join(intermediate_select)} "
                f"FROM {src} GROUP BY surt_host_name"
            )

            tmp = batch_out + ".tmp"
            try:
                print(
                    f"  batch {bi+1}/{len(batches)}: "
                    f"{len(batch_paths)} files...",
                    file=sys.stderr, flush=True,
                )
                con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT PARQUET)")
                os.replace(tmp, batch_out)
                n = con.execute(
                    f"SELECT count(*) FROM read_parquet('{batch_out}')"
                ).fetchone()[0]
                print(f"    → {n:,} rows", file=sys.stderr)
                batch_outputs.append(batch_out)
            except Exception as e:
                print(f"    FAILED: {e}", file=sys.stderr)
                if os.path.exists(tmp):
                    os.unlink(tmp)

        if not batch_outputs:
            print(f"  [warn] {name}: all batches failed", file=sys.stderr)
            con.close()
            return None

        print(
            f"  final merge: {len(batch_outputs)} batches → {out_path}",
            file=sys.stderr, flush=True,
        )

        final_select = _batch_final_select(cols)
        all_batches_glob = os.path.join(batch_dir, "batch_*.parquet")
        rollup_sql = (
            f"SELECT {', '.join(final_select)} "
            f"FROM read_parquet('{all_batches_glob}', union_by_name=true) "
            f"GROUP BY surt_host_name"
        )

    # --- Write final output (shared by both strategies) ---
    tmp = out_path + ".tmp"
    try:
        con.execute(f"COPY ({rollup_sql}) TO '{tmp}' (FORMAT PARQUET)")
        os.replace(tmp, out_path)
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        print(f"  wrote {out_path}: {n:,} rows", file=sys.stderr)
    except Exception as e:
        print(f"  [warn] {name}: final rollup failed: {e}", file=sys.stderr)
        if os.path.exists(tmp):
            os.unlink(tmp)
        con.close()
        return None

    con.close()
    return out_path


def build_external_source(name, cfg, staging_dir, force=False):
    """Download external CSV, convert to parquet with surt_host_name."""
    out_path = os.path.join(staging_dir, f"external_{name}.parquet")

    if not force and os.path.exists(out_path) and parquet_is_valid(out_path):
        print(f"  [cached] {name}: {out_path}", file=sys.stderr)
        return out_path

    yaml_path = os.path.join(ROOT, cfg["yaml"])
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    src = config["table"]["source"]
    url = src["url"]
    fmt = src.get("format", "csv")
    opts = src.get("options", {}) or {}
    right_cols = config.get("right_columns", [])
    join_cfg = config.get("join_columns", {})
    prefix = config.get("prefix", "")

    if isinstance(join_cfg, dict):
        domain_col = join_cfg["right"]
    elif isinstance(join_cfg, list):
        domain_col = join_cfg[0]
    else:
        domain_col = join_cfg

    print(f"\n[external] {name}: {url}", file=sys.stderr)

    # Download via polite_download (retries, backoff, jitter, User-Agent)
    local_cache = os.path.join(staging_dir, f".cache_{name}.{fmt}")
    if not force and os.path.exists(local_cache):
        print(f"  using cached download", file=sys.stderr)
    else:
        try:
            print(f"  downloading...", file=sys.stderr)
            nbytes = polite_download(url, local_cache)
            print(f"  {nbytes:,} bytes", file=sys.stderr)
            # Be polite: sleep before next download
            sleep = SLEEP_BETWEEN_DOWNLOADS + random.uniform(0, 1)
            time.sleep(sleep)
        except Exception as e:
            print(f"  [warn] {name}: download failed: {e}", file=sys.stderr)
            return None

    con = safe_duckdb(staging_dir)
    try:
        if fmt == "csv":
            csv_opts = []
            if "header" in opts:
                csv_opts.append(f"header={str(opts['header']).lower()}")
            if "columns" in opts:
                col_defs = ", ".join(
                    f"'{k}': '{v}'" for k, v in opts["columns"].items()
                )
                csv_opts.append(f"columns={{{col_defs}}}")
            opt_str = ", " + ", ".join(csv_opts) if csv_opts else ""
            con.execute(
                f"CREATE TABLE src AS SELECT * FROM read_csv('{local_cache}'{opt_str})"
            )
        else:
            con.execute(
                f"CREATE TABLE src AS SELECT * FROM read_parquet('{local_cache}')"
            )
    except Exception as e:
        print(f"  [warn] {name}: failed to read: {e}", file=sys.stderr)
        con.close()
        return None

    row_count = con.execute("SELECT count(*) FROM src").fetchone()[0]
    print(f"  {row_count:,} rows, domain column: {domain_col}", file=sys.stderr)

    # Map domains → SURT
    domains = con.execute(
        f'SELECT DISTINCT "{domain_col}" FROM src WHERE "{domain_col}" IS NOT NULL'
    ).fetchall()
    surt_map = {}
    skipped = 0
    for (d,) in domains:
        s = to_surt(str(d).strip().lower().rstrip("."))
        if s:
            surt_map[d] = s
        else:
            skipped += 1
    if skipped:
        print(f"  skipped {skipped} unconvertible domains", file=sys.stderr)

    con.execute(
        "CREATE TABLE surt_map (domain_orig VARCHAR, surt_host_name VARCHAR)"
    )
    con.executemany("INSERT INTO surt_map VALUES (?, ?)", list(surt_map.items()))

    select_parts = ["m.surt_host_name"]
    for col in right_cols:
        out_name = f"{prefix}{col}" if prefix else col
        select_parts.append(f's."{col}" AS "{out_name}"')
    select_parts.append(f'true AS "in_{name}"')

    join_sql = f"""
        SELECT {", ".join(select_parts)}
        FROM src s
        INNER JOIN surt_map m ON s."{domain_col}" = m.domain_orig
        WHERE m.surt_host_name IS NOT NULL
    """

    tmp = out_path + ".tmp"
    try:
        con.execute(f"COPY ({join_sql}) TO '{tmp}' (FORMAT PARQUET)")
        os.replace(tmp, out_path)
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        print(f"  wrote {out_path}: {n:,} rows", file=sys.stderr)
    except Exception as e:
        print(f"  [warn] {name}: write failed: {e}", file=sys.stderr)
        if os.path.exists(tmp):
            os.unlink(tmp)
        con.close()
        return None

    con.close()
    return out_path


# ═══════════════════════════════════════════════════════════════════════
# Merge
# ═══════════════════════════════════════════════════════════════════════

def get_parquet_columns(con, path):
    return [
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=true)"
        ).fetchall()
    ]


def _prefixed_col(col, col_prefix, source_name):
    """
    Prefix a column with the source's short col_prefix.

    Every column gets prefixed unconditionally — so after the first
    underscore you always know the original column name.
    Exceptions:
      - surt_host_name  (join key)
      - in_<source_name> (our auto-generated boolean, stays unprefixed)
    """
    if col == "surt_host_name":
        return col
    if col == f"in_{source_name}":
        return col
    return f"{col_prefix}_{col}"


def merge_all(parquet_paths, source_names, col_prefixes, output_path,
              staging_dir=None):
    con = safe_duckdb(staging_dir)

    print(f"\n{'='*70}", file=sys.stderr)
    print(f"MERGING {len(parquet_paths)} sources → {output_path}", file=sys.stderr)
    print(f"{'='*70}\n", file=sys.stderr)

    if not parquet_paths:
        sys.exit("No parquet files to merge!")

    if len(parquet_paths) == 1:
        path, name, prefix = (
            parquet_paths[0], source_names[0], col_prefixes[0]
        )
        cols = get_parquet_columns(con, path)
        in_col = f"in_{name}"
        has_in = in_col in cols

        selects = []
        for c in cols:
            pc = _prefixed_col(c, prefix, name)
            if pc == c:
                selects.append(f'"{c}"')
            else:
                selects.append(f'"{c}" AS "{pc}"')
        if not has_in:
            selects.append(f'true AS "{in_col}"')

        sql = (
            f"SELECT {', '.join(selects)} "
            f"FROM read_parquet('{path}', union_by_name=true)"
        )
        con.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
        print(f"Wrote {output_path}: {n:,} rows", file=sys.stderr)
        _write_preview_tsv(con, output_path)
        con.close()
        return

    # Incremental FULL OUTER JOIN with prefixed columns
    path0, name0, prefix0 = (
        parquet_paths[0], source_names[0], col_prefixes[0]
    )
    cols0 = get_parquet_columns(con, path0)
    in_col0 = f"in_{name0}"
    has_in0 = in_col0 in cols0

    selects0 = []
    for c in cols0:
        pc = _prefixed_col(c, prefix0, name0)
        if pc == c:
            selects0.append(f'"{c}"')
        else:
            selects0.append(f'"{c}" AS "{pc}"')
    if not has_in0:
        selects0.append(f'true AS "{in_col0}"')

    con.execute(
        f"CREATE VIEW base AS SELECT {', '.join(selects0)} "
        f"FROM read_parquet('{path0}', union_by_name=true)"
    )
    print(f"  [0] {name0} ({prefix0}_*): {len(cols0)} cols", file=sys.stderr)

    current_view = "base"
    for i, (path, name, prefix) in enumerate(
        zip(parquet_paths[1:], source_names[1:], col_prefixes[1:]), 1
    ):
        cols = get_parquet_columns(con, path)
        in_col = f"in_{name}"
        has_in = in_col in cols

        non_join = [c for c in cols if c != "surt_host_name"]
        rsel = []
        for c in non_join:
            pc = _prefixed_col(c, prefix, name)
            if pc == c:
                rsel.append(f'r."{c}"')
            else:
                rsel.append(f'r."{c}" AS "{pc}"')
        if not has_in:
            rsel.append(
                f'CASE WHEN r.surt_host_name IS NOT NULL '
                f'THEN true ELSE NULL END AS "{in_col}"'
            )

        next_view = f"step_{i}"
        sql = f"""
            CREATE VIEW {next_view} AS
            SELECT
              COALESCE({current_view}.surt_host_name, r.surt_host_name)
                AS surt_host_name,
              {current_view}.* EXCLUDE (surt_host_name),
              {", ".join(rsel)}
            FROM {current_view}
            FULL OUTER JOIN read_parquet('{path}', union_by_name=true) AS r
              ON {current_view}.surt_host_name = r.surt_host_name
        """
        try:
            con.execute(sql)
            print(f"  [{i}] {name} ({prefix}_*): +{len(non_join)} cols",
                  file=sys.stderr)
        except Exception as e:
            print(f"  [{i}] {name}: FAILED ({e}), skipping", file=sys.stderr)
            continue
        current_view = next_view

    tmp = output_path + ".tmp"
    print(f"\nWriting {output_path}...", file=sys.stderr)
    con.execute(
        f"COPY (SELECT * FROM {current_view}) TO '{tmp}' (FORMAT PARQUET)"
    )
    os.replace(tmp, output_path)

    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]
    final_cols = get_parquet_columns(con, output_path)
    print(f"\nDone: {output_path}", file=sys.stderr)
    print(f"  {n:,} rows x {len(final_cols)} columns\n", file=sys.stderr)
    for c in final_cols:
        print(f"  - {c}", file=sys.stderr)

    # Save first 10 rows as TSV for easy eyeballing
    _write_preview_tsv(con, output_path)

    con.close()


def _write_preview_tsv(con, output_path):
    tsv_path = output_path.rsplit(".", 1)[0] + "_preview.tsv"
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{output_path}') LIMIT 10) "
        f"TO '{tsv_path}' (FORMAT CSV, DELIMITER '\t', HEADER true)"
    )
    print(f"  Preview: {tsv_path}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(
        description="Build a combined annotation parquet from all sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              %(prog)s -o combined.parquet
              %(prog)s -o combined.parquet --deep --include-cwur
              %(prog)s -o combined.parquet --exclude gneissweb,web_graph
              %(prog)s -o combined.parquet --only curlie,spam_abuse,tranco
              %(prog)s -o combined.parquet --force
              %(prog)s --list
        """),
    )
    p.add_argument("-o", "--output", default="combined_annotations.parquet")
    p.add_argument("--only", help="Comma-separated source names to include")
    p.add_argument("--exclude", help="Comma-separated source names to skip")
    p.add_argument("--force", action="store_true", help="Rebuild everything")
    p.add_argument("--list", action="store_true", help="List sources and exit")
    p.add_argument("--staging-dir", default=None)
    p.add_argument(
        "--deep", action="store_true",
        help="Wikipedia: recursive 80+ topic categories across all languages",
    )
    p.add_argument(
        "--include-cwur", action="store_true",
        help="University: add CWUR world/national rank (scrapes cwur.org)",
    )
    p.add_argument(
        "--crawl-filter",
        help="Comma-separated crawl IDs for remote sources "
             "(e.g. CC-MAIN-2020-10,CC-MAIN-2023-06,CC-MAIN-2026-04)",
    )
    args = p.parse_args()

    all_names = (
        sorted(LOCAL_SOURCES)
        + sorted(REMOTE_SOURCES)
        + sorted(EXTERNAL_SOURCES)
    )
    if args.list:
        print("Local sources (parquets built via `make`):")
        print(f"  {'NAME':30s}  {'PREFIX':15s}  {'DIR':40s}  EXTRA FLAGS")
        for n in sorted(LOCAL_SOURCES):
            c = LOCAL_SOURCES[n]
            ex = ", ".join(
                f"--{k.replace('_', '-')}" for k in c.get("extra_flags", {})
            )
            print(f"  {n:30s}  {c['col_prefix']+'_*':15s}  {c['dir']:40s}  {ex}")
        print("\nRemote sources (S3/HTTP, rolled up on the fly):")
        for n in sorted(REMOTE_SOURCES):
            c = REMOTE_SOURCES[n]
            print(f"  {n:30s}  {c['col_prefix']+'_*':15s}  {c['paths_file']}")
        print("\nExternal CSV sources:")
        for n in sorted(EXTERNAL_SOURCES):
            c = EXTERNAL_SOURCES[n]
            print(f"  {n:30s}  {c['col_prefix']+'_*':15s}  {c['yaml']}")
        print(f"\nTotal: {len(all_names)} sources")
        print(f"\nNote: in_<source_name> booleans use the full source name, not the prefix.")
        return

    staging_dir = args.staging_dir or os.path.join(ROOT, "staging")
    os.makedirs(staging_dir, exist_ok=True)

    active_flags = set()
    if args.deep:
        active_flags.add("deep")
    if args.include_cwur:
        active_flags.add("include_cwur")

    only = set(args.only.split(",")) if args.only else None
    exclude = set(args.exclude.split(",")) if args.exclude else set()

    def wanted(name):
        if only and name not in only:
            return False
        return name not in exclude

    parquet_paths = []
    source_names = []
    col_prefixes = []

    def _get_prefix(name):
        """Look up col_prefix from whichever source dict contains this name."""
        for registry in (LOCAL_SOURCES, REMOTE_SOURCES, EXTERNAL_SOURCES):
            if name in registry:
                return registry[name].get("col_prefix", name)
        return name

    crawl_filter = (
        args.crawl_filter.split(",") if args.crawl_filter else None
    )

    # --- Local sources ---
    for name in sorted(LOCAL_SOURCES):
        if not wanted(name):
            continue
        path = build_local_source(
            name, LOCAL_SOURCES[name], force=args.force,
            active_flags=active_flags,
        )
        if path:
            path = ensure_rolled_up(path, name, staging_dir, force=args.force)
            parquet_paths.append(path)
            source_names.append(name)
            col_prefixes.append(_get_prefix(name))

    # --- Remote sources (rollup happens inside build_remote_source) ---
    for name in sorted(REMOTE_SOURCES):
        if not wanted(name):
            continue
        path = build_remote_source(
            name, REMOTE_SOURCES[name], staging_dir, force=args.force,
            crawl_filter=crawl_filter,
        )
        if path:
            parquet_paths.append(path)
            source_names.append(name)
            col_prefixes.append(_get_prefix(name))

    # --- External CSV sources ---
    for name in sorted(EXTERNAL_SOURCES):
        if not wanted(name):
            continue
        path = build_external_source(
            name, EXTERNAL_SOURCES[name], staging_dir, force=args.force,
        )
        if path:
            path = ensure_rolled_up(path, name, staging_dir, force=args.force)
            parquet_paths.append(path)
            source_names.append(name)
            col_prefixes.append(_get_prefix(name))

    if not parquet_paths:
        sys.exit("No annotation parquets were built. Nothing to merge.")

    print(f"\n{'='*70}", file=sys.stderr)
    print(f"Ready to merge {len(parquet_paths)} source(s):", file=sys.stderr)
    for name, prefix, path in zip(source_names, col_prefixes, parquet_paths):
        print(f"  {name:30s}  {prefix:15s}  {path}", file=sys.stderr)

    merge_all(parquet_paths, source_names, col_prefixes, args.output,
              staging_dir=staging_dir)


if __name__ == "__main__":
    main()
