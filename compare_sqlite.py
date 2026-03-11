import sqlite3, hashlib, json, sys, os

DB_A = r"C:\Users\AHMED\Downloads\AFTER CLEAN.db"
DB_B = r"D:\locked app\downloaded fbm\instance\ahmed_cement.db"


def connect_ro(path):
    uri = f"file:{path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def list_tables(conn):
    cur = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
    return cur.fetchall()


def table_info(conn, table):
    cur = conn.execute(f"PRAGMA table_info('{table.replace("'", "''")}')")
    cols = cur.fetchall()  # cid, name, type, notnull, dflt_value, pk
    return cols


def pk_order(cols):
    pk_cols = [c[1] for c in cols if c[5] > 0]
    if pk_cols:
        return ", ".join([f"\"{c}\"" for c in pk_cols])
    return "rowid"


def row_checksum(conn, table):
    cols = table_info(conn, table)
    col_names = [c[1] for c in cols]
    order_by = pk_order(cols)
    # Fetch rows in deterministic order
    query = f"SELECT * FROM \"{table}\" ORDER BY {order_by}"
    cur = conn.execute(query)
    h = hashlib.sha256()
    for row in cur:
        for v in row:
            if v is None:
                h.update(b"N;")
            elif isinstance(v, bytes):
                h.update(b"B")
                h.update(str(len(v)).encode("utf-8"))
                h.update(b":")
                h.update(v)
                h.update(b";")
            else:
                s = str(v)
                h.update(b"T")
                h.update(str(len(s)).encode("utf-8"))
                h.update(b":")
                h.update(s.encode("utf-8"))
                h.update(b";")
    return h.hexdigest()


def summarize(conn):
    tables = list_tables(conn)
    return tables


def main():
    if not os.path.exists(DB_A) or not os.path.exists(DB_B):
        print("ERROR: One or both DB paths do not exist.")
        sys.exit(2)

    a = connect_ro(DB_A)
    b = connect_ro(DB_B)

    a_tables = list_tables(a)
    b_tables = list_tables(b)

    a_map = {name: sql for name, sql in a_tables}
    b_map = {name: sql for name, sql in b_tables}

    all_tables = sorted(set(a_map) | set(b_map))

    diffs = {
        "missing_in_a": [],
        "missing_in_b": [],
        "schema_diff": [],
        "row_count_diff": [],
        "checksum_diff": []
    }

    for t in all_tables:
        if t not in a_map:
            diffs["missing_in_a"].append(t)
            continue
        if t not in b_map:
            diffs["missing_in_b"].append(t)
            continue
        if (a_map[t] or "").strip() != (b_map[t] or "").strip():
            diffs["schema_diff"].append(t)

        a_count = a.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]
        b_count = b.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]
        if a_count != b_count:
            diffs["row_count_diff"].append({"table": t, "a": a_count, "b": b_count})

        # Only checksum if counts match to avoid heavy work on clearly different tables
        if a_count == b_count:
            a_sum = row_checksum(a, t)
            b_sum = row_checksum(b, t)
            if a_sum != b_sum:
                diffs["checksum_diff"].append({"table": t, "a": a_sum, "b": b_sum})

    print(json.dumps(diffs, indent=2))

    a.close()
    b.close()


if __name__ == "__main__":
    main()
