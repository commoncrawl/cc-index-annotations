import argparse
import glob
import os
import sys
import duckdb


def parse_input(spec: str) -> tuple[str, str | None, list[str] | None]:
    parts = spec.split(":")
    path = parts[0]
    prefix = parts[1] if len(parts) > 1 and parts[1] else None
    keys = parts[2].split(",") if len(parts) > 2 and parts[2] else None
    return path, prefix, keys


def resolve_path(path: str) -> str:
    if os.path.isdir(path):
        pattern = os.path.join(path, "**/*.parquet")
        if not glob.glob(pattern, recursive=True):
            sys.exit(f"No .parquet files found in {path}")
        return pattern
    return path


def get_columns(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    return [row[0] for row in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=true)"
    ).fetchall()]


def build_select(columns: list[str], alias: str, prefix: str | None, join_cols: list[str]) -> list[str]:
    parts = []
    for col in columns:
        if col in join_cols:
            continue
        out_name = f"{prefix}{col}" if prefix else col
        parts.append(f'{alias}."{col}" AS "{out_name}"')
    return parts


def build_join_key_select(join_cols: list[str], num_tables: int, how: str) -> list[str]:
    if how == "outer":
        return [f'COALESCE({", ".join(f"t{i}.\"{c}\"" for i in range(num_tables))}) AS "{c}"'
                for c in join_cols]
    return [f't0."{c}"' for c in join_cols]


def main():
    p = argparse.ArgumentParser(
        description="Join parquet files/dirs using DuckDB. Each input is path[:prefix[:join_cols]]",
        epilog="Example: %(prog)s -o out.parquet ./left_dir/:l_:id ./right_dir/:r_:id",
    )
    p.add_argument("inputs", nargs="+", help="path[:prefix[:col1,col2,...]] — path can be a file, glob, or directory")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("-j", "--join-cols", help="default join columns (comma-sep)")
    p.add_argument("--how", default="inner", choices=["inner", "outer", "left", "right", "cross"])
    args = p.parse_args()

    default_keys = args.join_cols.split(",") if args.join_cols else None
    con = duckdb.connect()

    inputs = []
    for spec in args.inputs:
        path, prefix, keys = parse_input(spec)
        keys = keys or default_keys
        pattern = resolve_path(path)
        inputs.append((pattern, prefix, keys))

    if len(inputs) < 2:
        sys.exit("Need at least 2 input files")

    join_cols = inputs[0][2]
    if not join_cols:
        sys.exit("No join columns specified")

    # start from first input
    pattern0, prefix0, _ = inputs[0]
    cols0 = get_columns(con, pattern0)
    select_parts = build_join_key_select(join_cols, len(inputs), args.how)
    select_parts += build_select(cols0, "t0", prefix0, join_cols)
    from_clause = f"read_parquet('{pattern0}', union_by_name=true) AS t0"
    join_clauses = []

    how_sql = {"inner": "INNER", "outer": "FULL OUTER", "left": "LEFT", "right": "RIGHT", "cross": "CROSS"}
    join_type = how_sql[args.how]

    for i, (pattern, prefix, keys) in enumerate(inputs[1:], 1):
        alias = f"t{i}"
        cols = get_columns(con, pattern)
        on_cols = keys or default_keys
        if not on_cols:
            sys.exit("No join columns specified")
        select_parts += build_select(cols, alias, prefix, on_cols)
        on_clause = " AND ".join(f't0."{c}" = {alias}."{c}"' for c in on_cols)
        join_clauses.append(f"{join_type} JOIN read_parquet('{pattern}', union_by_name=true) AS {alias} ON {on_clause}")

    sql = f"COPY (\n  SELECT {',\n         '.join(select_parts)}\n  FROM {from_clause}\n  {'  '.join(join_clauses)}\n) TO '{args.output}' (FORMAT PARQUET)"

    print(f"Executing join...", file=sys.stderr)
    con.execute(sql)

    count = con.execute(f"SELECT count(*) FROM read_parquet('{args.output}')").fetchone()[0]
    ncols = len(select_parts)
    print(f"Wrote {args.output}: {count} rows x {ncols} cols")


if __name__ == "__main__":
    main()

