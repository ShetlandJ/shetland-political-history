#!/usr/bin/env python3
"""
Fetch birth/death place data from Bayanne and add born_in_shetland/died_in_shetland columns.
Uses the JSON-LD structured data embedded in each Bayanne person page.
"""
import sqlite3
import urllib.request
import json
import re
import time
import sys
import ssl

DB_PATH = 'shetland.db'

def fetch_bayanne_places(person_id):
    """Fetch birthPlace and deathPlace from Bayanne JSON-LD."""
    url = f'http://www.bayanne.info/Shetland/getperson.php?personID={person_id}&tree=ID1'
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"  ERROR fetching {person_id}: {e}", file=sys.stderr)
        return None, None

    # Extract JSON-LD
    m = re.search(r'<script type="application/ld\+json">\s*({.*?})\s*</script>', html, re.DOTALL)
    if not m:
        return None, None

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None, None

    return data.get('birthPlace', ''), data.get('deathPlace', '')


def is_shetland(place_str):
    """Check if a place string contains SHI (Shetland Islands) code."""
    if not place_str:
        return None  # unknown
    # SHI = Shetland Islands (the standard code used by Bayanne)
    if ', SHI,' in place_str or place_str.endswith(', SHI'):
        return True
    return False


def main():
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # Add columns if they don't exist
    cols = [row[1] for row in cur.execute("PRAGMA table_info(people)")]
    if 'born_in_shetland' not in cols:
        cur.execute("ALTER TABLE people ADD COLUMN born_in_shetland INTEGER")
        print("Added born_in_shetland column")
    if 'died_in_shetland' not in cols:
        cur.execute("ALTER TABLE people ADD COLUMN died_in_shetland INTEGER")
        print("Added died_in_shetland column")
    db.commit()

    # Get all people with bayanne_id who haven't been checked yet
    rows = cur.execute("""
        SELECT id, name, bayanne_id FROM people
        WHERE bayanne_id IS NOT NULL
        AND born_in_shetland IS NULL AND died_in_shetland IS NULL
        ORDER BY id
    """).fetchall()

    print(f"Fetching Bayanne data for {len(rows)} people...")

    for i, (pid, name, bayanne_id) in enumerate(rows):
        birth_place, death_place = fetch_bayanne_places(bayanne_id)

        born_shi = is_shetland(birth_place)
        died_shi = is_shetland(death_place)

        # Store: 1 = yes, 0 = no, NULL = unknown (no Bayanne data)
        born_val = 1 if born_shi is True else (0 if born_shi is False else None)
        died_val = 1 if died_shi is True else (0 if died_shi is False else None)

        cur.execute("UPDATE people SET born_in_shetland = ?, died_in_shetland = ? WHERE id = ?",
                    (born_val, died_val, pid))

        status = f"born={'SHI' if born_shi else birth_place or '?':30s} died={'SHI' if died_shi else death_place or '?':30s}"
        print(f"  [{i+1}/{len(rows)}] {name:30s} {status}")

        if (i + 1) % 10 == 0:
            db.commit()

        # Be polite to the server
        time.sleep(0.3)

    db.commit()

    # Print summary
    stats = cur.execute("""
        SELECT
            SUM(CASE WHEN born_in_shetland = 1 THEN 1 ELSE 0 END) as born_shi,
            SUM(CASE WHEN born_in_shetland = 0 THEN 1 ELSE 0 END) as born_other,
            SUM(CASE WHEN born_in_shetland IS NULL THEN 1 ELSE 0 END) as born_unknown,
            SUM(CASE WHEN died_in_shetland = 1 THEN 1 ELSE 0 END) as died_shi,
            SUM(CASE WHEN died_in_shetland = 0 THEN 1 ELSE 0 END) as died_other,
            SUM(CASE WHEN died_in_shetland IS NULL THEN 1 ELSE 0 END) as died_unknown
        FROM people
    """).fetchone()
    print(f"\nSummary:")
    print(f"  Born in Shetland: {stats[0]} | Born elsewhere: {stats[1]} | Unknown: {stats[2]}")
    print(f"  Died in Shetland: {stats[3]} | Died elsewhere: {stats[4]} | Unknown: {stats[5]}")

    db.close()


if __name__ == '__main__':
    main()
