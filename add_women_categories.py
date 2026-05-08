#!/usr/bin/env python3
"""
Ensure every winning SIC candidate has a person record, link unlinked
candidacies, and tag women councillors with the 'Female' category.

Steps:
1. Update existing person records that are missing 'Female' (and other) cats.
2. Link unlinked SIC winner candidacies to existing people where the name
   matches (exact or via a known-variant map).
3. Create minimal person records for SIC winners with no existing record,
   with appropriate categories ('Shetland Islands Councillors', plus
   'Female' for women).
4. Print final gender breakdown for the whole people table.

Idempotent — safe to re-run.
"""
import json
import re
import sqlite3
import sys

DB_PATH = "shetland.db"

# Existing person IDs -> categories that must be present (added if missing).
EXISTING_TO_FLAG = {
    274: ["Female"],  # Jemima Walterson
    536: ["Female", "Shetland Islands Councillors"],  # Beatrice Wishart
}

# Candidate-name -> existing person id (variant spellings of same person).
# Found via election-date overlap with existing candidacies.
VARIANT_TO_PERSON_ID = {
    "Davie Sandison": 118,           # David Sandison
    "George Smith": 168,             # George Smith (iii)
    "Theo Smith": 459,               # Theodore Smith
    "Cecil Burgess Eunson": 81,      # Cecil B. Eunson (Cecil Eunson (i))
}

# New women SIC winners needing person records.
# canonical_name -> [candidate_name spellings to link]
NEW_WOMEN = {
    "Allison Duncan": ["Allison Duncan"],
    "Amanda Westlake": ["Amanda Westlake"],
    "Andrea Manson": ["Andrea Manson"],
    "Catherine Hughson": ["Catherine Hughson"],
    "Emma MacDonald": ["Emma MacDonald", "Emma Macdonald"],
    "Liz Boxwell": ["Liz Boxwell"],
    "Moraig Lyall": ["Moraig Lyall"],
    "Roberta Clubb": ["Roberta Clubb"],
}

# New male SIC winners needing person records.
NEW_MEN = [
    "Alec Priest",
    "Alex Armitage",
    "Andrew Hall",
    "Arwed Wenger",
    "Bryan Peterson",
    "Dennis Leask",
    "Duncan Anderson",
    "Duncan Simpson",
    "Ian Scott",
    "John Fraser",
    "John Leask",
    "Mark Robinson",
    "Neil Pearson",
    "Robbie McGregor",
    "Robert Thomson",
    "Robert Tulloch",
    "Ryan Thomson",
    "Stephen Flaws",
    "Stephen Leask",
    "Tom Morton",
]


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def add_categories_to_existing(conn, pid, required_cats):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT name, categories FROM people WHERE id = ?", (pid,)
    ).fetchone()
    if not row:
        print(f"  ! id={pid} not found")
        return False
    name, cats_json = row
    cats = json.loads(cats_json) if cats_json else []
    added = [c for c in required_cats if c not in cats]
    if not added:
        return False
    cats.extend(added)
    cur.execute(
        "UPDATE people SET categories = ? WHERE id = ?",
        (json.dumps(cats), pid),
    )
    print(f"  + {name} (id={pid}): added {added}")
    return True


def link_unlinked_candidacies(conn, sic_council_id, candidate_name, person_id):
    cur = conn.cursor()
    res = cur.execute(
        """
        UPDATE candidacies
        SET person_id = ?
        WHERE candidate_name = ?
          AND person_id IS NULL
          AND election_id IN (SELECT id FROM elections WHERE council_id = ?)
        """,
        (person_id, candidate_name, sic_council_id),
    )
    return res.rowcount


def upsert_person(conn, canonical, categories):
    """Find or create a person by slug. Add any missing categories."""
    cur = conn.cursor()
    slug = slugify(canonical)
    row = cur.execute(
        "SELECT id, categories FROM people WHERE slug = ?", (slug,)
    ).fetchone()
    if row:
        pid, cats_json = row
        cats = json.loads(cats_json) if cats_json else []
        added = [c for c in categories if c not in cats]
        if added:
            cats.extend(added)
            cur.execute(
                "UPDATE people SET categories = ? WHERE id = ?",
                (json.dumps(cats), pid),
            )
            print(f"  ~ {canonical} (id={pid}): added {added}")
        return pid, False
    cur.execute(
        "INSERT INTO people (name, slug, categories) VALUES (?, ?, ?)",
        (canonical, slug, json.dumps(categories)),
    )
    pid = cur.lastrowid
    print(f"  + created {canonical} (id={pid}) cats={categories}")
    return pid, True


def gender_breakdown(conn):
    cur = conn.cursor()
    rows = cur.execute("SELECT id, categories FROM people").fetchall()
    total = len(rows)
    women = 0
    for _, cats_json in rows:
        if not cats_json:
            continue
        cats = json.loads(cats_json)
        if any(c.lower().strip() == "female" for c in cats):
            women += 1
    men = total - women
    return total, women, men


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        sic_council_id = conn.execute(
            "SELECT id FROM councils WHERE name = 'Shetland Islands Council'"
        ).fetchone()[0]

        print("== 1. Update existing people with required categories ==")
        for pid, cats in EXISTING_TO_FLAG.items():
            add_categories_to_existing(conn, pid, cats)

        print("\n== 2. Link variant-name candidacies to existing people ==")
        for cand_name, pid in VARIANT_TO_PERSON_ID.items():
            n = link_unlinked_candidacies(conn, sic_council_id, cand_name, pid)
            if n:
                print(f"  + linked {n} '{cand_name}' candidacies -> id={pid}")

        print("\n== 3. Women SIC winners (create/update + link) ==")
        for canonical, spellings in NEW_WOMEN.items():
            pid, _ = upsert_person(
                conn, canonical, ["Shetland Islands Councillors", "Female"]
            )
            for spelling in spellings:
                n = link_unlinked_candidacies(conn, sic_council_id, spelling, pid)
                if n:
                    print(f"      linked {n} '{spelling}' -> id={pid}")

        print("\n== 4. Men SIC winners (create/update + link) ==")
        for canonical in NEW_MEN:
            pid, _ = upsert_person(conn, canonical, ["Shetland Islands Councillors"])
            n = link_unlinked_candidacies(conn, sic_council_id, canonical, pid)
            if n:
                print(f"      linked {n} '{canonical}' -> id={pid}")

        print("\n== 5. Link any remaining exact-name SIC winner matches ==")
        # For names that exact-match an existing person but weren't linked
        # (e.g. Alastair Cooper, Gary Robinson, etc.).
        rows = conn.execute(
            """
            SELECT DISTINCT c.candidate_name, p.id
            FROM candidacies c
            JOIN elections e ON e.id = c.election_id
            JOIN people p ON LOWER(p.name) = LOWER(c.candidate_name)
            WHERE e.council_id = ?
              AND c.person_id IS NULL
              AND c.elected = 1
            """,
            (sic_council_id,),
        ).fetchall()
        for cand_name, pid in rows:
            n = link_unlinked_candidacies(conn, sic_council_id, cand_name, pid)
            if n:
                print(f"  + linked {n} '{cand_name}' -> id={pid}")

        conn.commit()

        print("\n== Final gender breakdown (all people in DB) ==")
        total, women, men = gender_breakdown(conn)
        if total:
            print(f"  Total: {total}")
            print(f"  Women: {women} ({women / total * 100:.1f}%)")
            print(f"  Men:   {men} ({men / total * 100:.1f}%)")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
