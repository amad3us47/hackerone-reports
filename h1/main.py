"""
main.py - sweep HackerOne report IDs and store every PUBLIC/disclosed one in a
SQLite database, then export that database to CSV.

For each id it fetches https://hackerone.com/reports/{id}.json and, if the report
is public, UPSERTS a row keyed by the report id (so re-runs never duplicate).
The resume pointer (last processed id) lives inside the DB's `meta` table, so the
sweep is fully resumable: stop anytime (Ctrl-C / CI timeout), run again, it
continues. Only *publicly disclosed* reports are recorded; private ids are skipped.

Usage:
    pip install requests
    python3 main.py --end 3782701                       # scrape (resumable)
    python3 main.py --start 3700000 --end 3782701        # a recent range only
    python3 main.py --export-only                        # just (re)write CSV from DB

Reality check: most ids are private and return nothing, so a full sweep from 1 is
millions of requests over many days. Run it in chunks, or point --start at a
recent range you care about. Be a good citizen: keep --sleep reasonable so you
don't hammer HackerOne, and respect their terms of service.
"""
import argparse
import csv
import signal
import sqlite3
import time

import requests

FIELDNAMES = ["id", "program", "title", "link", "upvotes", "bounty", "vuln_type", "date"]

# --- graceful shutdown -------------------------------------------------------
# CI timeouts send SIGTERM; Ctrl-C sends SIGINT. We finish the current id, commit,
# and exit cleanly so the DB is never left mid-transaction.
_STOP = False


def _handle_stop(signum, _frame):
    global _STOP
    _STOP = True
    print(f"  received signal {signum}; will stop after the current id")


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


# --- database ----------------------------------------------------------------
def init_db(path):
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS reports (
               id        INTEGER PRIMARY KEY,
               program   TEXT,
               title     TEXT,
               link      TEXT,
               upvotes   INTEGER,
               bounty    REAL,
               vuln_type TEXT,
               date      TEXT
           )"""
    )
    con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    con.commit()
    return con


def get_last_done(con):
    row = con.execute("SELECT value FROM meta WHERE key='last_done'").fetchone()
    return int(row[0]) if row and row[0] else 0


def set_last_done(con, rid):
    con.execute(
        "INSERT INTO meta(key, value) VALUES('last_done', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(rid),),
    )


def upsert(con, row):
    con.execute(
        """INSERT INTO reports (id, program, title, link, upvotes, bounty, vuln_type, date)
               VALUES (:id, :program, :title, :link, :upvotes, :bounty, :vuln_type, :date)
           ON CONFLICT(id) DO UPDATE SET
               program=excluded.program, title=excluded.title, link=excluded.link,
               upvotes=excluded.upvotes, bounty=excluded.bounty,
               vuln_type=excluded.vuln_type, date=excluded.date""",
        row,
    )


def export_csv(con, csv_path):
    rows = con.execute(
        "SELECT id, program, title, link, upvotes, bounty, vuln_type, date "
        "FROM reports ORDER BY id"
    ).fetchall()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(FIELDNAMES)
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {csv_path}")


# --- fetch / parse -----------------------------------------------------------
def is_public_report(j):
    """Disclosed/public reports carry these; private ones return errors/minimal json."""
    if not isinstance(j, dict):
        return False
    return bool(j.get("public")) and bool(j.get("disclosed_at")) and bool(j.get("title"))


def extract(j, rid):
    url = j.get("url", "")
    link = url.replace("https://", "") if url else ""
    ts = j.get("disclosed_at") or ""
    bounty = 0.0
    if j.get("has_bounty?"):
        try:
            bounty = float(j.get("bounty_amount") or 0)
        except (TypeError, ValueError):
            bounty = 0.0
    return {
        "id":        int(j.get("id") or rid),
        "program":   (j.get("team") or {}).get("profile", {}).get("name", ""),
        "title":     j.get("title", ""),
        "link":      link,
        "upvotes":   int(j.get("vote_count") or 0),
        "bounty":    bounty,
        "vuln_type": (j.get("weakness") or {}).get("name", "") if j.get("weakness") else "",
        "date":      ts[:10] if ts else "",
    }


def fetch(session, rid):
    """Return (status, json_or_None): 'ok','private','missing','ratelimited','error'."""
    url = f"https://hackerone.com/reports/{rid}.json"
    try:
        r = session.get(url, timeout=20)
    except Exception:
        return "error", None
    if r.status_code == 429:
        return "ratelimited", None
    if r.status_code in (401, 403):
        return "private", None
    if r.status_code == 404:
        return "missing", None
    if r.status_code != 200:
        return "error", None
    try:
        return "ok", r.json()
    except ValueError:
        return "error", None


# --- sweep -------------------------------------------------------------------
def scrape(a, con):
    session = requests.Session()
    session.headers.update(
        {"User-Agent": "personal-research-script", "Accept": "application/json"}
    )
    last_done = get_last_done(con)
    start = max(a.start, last_done + 1) if last_done >= a.start else a.start
    print(f"Sweeping ids {start}..{a.end} (resume point {last_done})")

    found = 0
    processed = start - 1
    for rid in range(start, a.end + 1):
        if _STOP:
            break

        # retry the SAME id on rate-limit so nothing is skipped
        status, j = "error", None
        while True:
            status, j = fetch(session, rid)
            if status == "ratelimited":
                print(f"  {rid}: rate limited, backing off 60s")
                for _ in range(60):          # interruptible backoff
                    if _STOP:
                        break
                    time.sleep(1)
                if _STOP:
                    break
                continue
            break
        if status == "ratelimited":
            # interrupted mid-backoff before a real answer: stop WITHOUT marking rid,
            # so it gets retried on the next run.
            break

        hit = status == "ok" and is_public_report(j)
        if hit:
            row = extract(j, rid)
            upsert(con, row)
            found += 1
            print(f"  {rid}: {row['date']}  {row['title'][:55]}  (found {found})")

        set_last_done(con, rid)
        processed = rid
        # commit on every hit and every 200 ids, so a hard kill loses almost nothing
        if hit or rid % 200 == 0:
            con.commit()
        time.sleep(a.sleep)

    con.commit()
    print(f"Stopped at id {processed}. Public reports found this run: {found}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, default=1)
    p.add_argument("--end", type=int, default=3782701)
    p.add_argument("--sleep", type=float, default=0.5)
    p.add_argument("--db", default="reports.db")
    p.add_argument("--csv", default="reports.csv")
    p.add_argument("--export-only", action="store_true",
                   help="skip scraping; just (re)write the CSV from the DB")
    a = p.parse_args()

    con = init_db(a.db)
    try:
        if not a.export_only:
            scrape(a, con)
    finally:
        # always refresh the CSV so it matches the DB, even after Ctrl-C / timeout
        export_csv(con, a.csv)
        con.commit()
        con.close()


if __name__ == "__main__":
    main()
