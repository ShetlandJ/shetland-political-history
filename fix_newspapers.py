#!/usr/bin/env python3
"""
Corrections to shetland.db from newspaper reports. Runs after fix_minute_book.py and is
idempotent: re-running does nothing once the corrections are in place.

1. November 1884 Lerwick Town Council election. Shetland Times, 1 Nov 1884: "Tuesday first,
   the 4th instant, is the date for the annual election ... The four gentlemen falling to
   retire this year are Bailies Robertson and Hay and Messrs John Robertson, jr., and William
   Duncan." So:
   - the general election was on 4 November 1884, not 4 December;
   - the William Duncan standing was the sitting councillor elected in 1881, William Duncan (i),
     the Lerwick grocer, not William Duncan (ii) of Scalloway. The same goes for the
     by-election co-option that followed.
   Arthur Hay stays elected: he topped the poll (87) and then declined to take office, which is
   recorded as an election note. The co-option filling his seat was on 22 November 1884
   (newspaper, 22 Nov 1884, per CLAUDE.md), not the 11th.
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

GENERAL = 'Lerwick Town Council Election November 1884'
BY_ELECTION = 'Lerwick Town Council By-Election November 1884'

HAY_NOTE = (
    "Arthur J. Hay topped the poll but declined to take office (letter, 8 November 1884). "
    "The vacancy was filled by co-option: see Lerwick Town Council By-Election November 1884."
)


def one(c, sql, args):
    c.execute(sql, args)
    rows = c.fetchall()
    if len(rows) != 1:
        raise SystemExit(f"expected 1 row, got {len(rows)}: {sql} {args}")
    return rows[0]


def set_date(c, wiki_title, wrong, right):
    row = one(c, "SELECT id, election_date FROM elections WHERE wiki_page_title = ?", (wiki_title,))
    if row['election_date'] == right:
        print(f"  {wiki_title}: already {right}")
    elif row['election_date'] == wrong:
        c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (right, row['id']))
        print(f"  {wiki_title} (id {row['id']}): {wrong} -> {right}")
    else:
        raise SystemExit(f"'{wiki_title}' has unexpected date {row['election_date']}")
    return row['id']


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    print("=== 1. November 1884 LTC election ===")
    general_id = set_date(c, GENERAL, '1884-12-04', '1884-11-04')
    by_id = set_date(c, BY_ELECTION, '1884-11-11', '1884-11-22')

    duncan_i = one(c, "SELECT id, name FROM people WHERE slug = 'william-duncan-i'", ())
    duncan_ii = one(c, "SELECT id FROM people WHERE slug = 'william-duncan-ii'", ())
    c.execute("""
        UPDATE candidacies SET person_id = ?
        WHERE person_id = ? AND candidate_name = 'William Duncan' AND election_id IN (?, ?)
    """, (duncan_i['id'], duncan_ii['id'], general_id, by_id))
    print(f"  William Duncan candidacies re-pointed to {duncan_i['name']}: {c.rowcount} row(s)")
    c.execute("""
        SELECT COUNT(*) FROM candidacies
        WHERE person_id = ? AND candidate_name = 'William Duncan' AND election_id IN (?, ?)
    """, (duncan_i['id'], general_id, by_id))
    if c.fetchone()[0] != 2:
        raise SystemExit("expected 2 William Duncan candidacies on the 1884 elections")

    row = one(c, "SELECT notes FROM elections WHERE id = ?", (general_id,))
    if row['notes'] == HAY_NOTE:
        print("  Hay note: already set")
    elif not row['notes']:
        c.execute("UPDATE elections SET notes = ? WHERE id = ?", (HAY_NOTE, general_id))
        print("  Hay note: added")
    else:
        raise SystemExit(f"{GENERAL} already has notes, not overwriting: {row['notes']}")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
