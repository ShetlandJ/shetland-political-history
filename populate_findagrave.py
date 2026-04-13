#!/usr/bin/env python3
"""
Apply Find A Grave memorial IDs to the people table from findagrave_ids.csv.
Run after parse_wiki.py since the parser recreates the DB from scratch.
"""
import csv
import sqlite3
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'shetland.db')
CSV_PATH = os.path.join(HERE, 'findagrave_ids.csv')


def main():
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # Make sure the column exists (parse_wiki.py creates it via schema.sql)
    cols = [row[1] for row in cur.execute("PRAGMA table_info(people)")]
    if 'findagrave_id' not in cols:
        cur.execute("ALTER TABLE people ADD COLUMN findagrave_id INTEGER")

    applied = 0
    missing = []
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row['slug']
            fg_id = int(row['findagrave_id'])
            cur.execute("UPDATE people SET findagrave_id = ? WHERE slug = ?", (fg_id, slug))
            if cur.rowcount == 0:
                missing.append(slug)
            else:
                applied += 1

    db.commit()
    print(f"Applied {applied} Find A Grave IDs")
    if missing:
        print(f"  Skipped {len(missing)} (slug not found): {missing[:5]}")
    db.close()


if __name__ == '__main__':
    main()
