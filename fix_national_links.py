#!/usr/bin/env python3
"""
Link newly added national election candidacies to existing people,
and create person records for those who don't have one yet.
"""

import sqlite3

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'


def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    # === Link Alistair Carmichael (person_id=10) to his new candidacies ===
    print("=== Linking Alistair Carmichael ===")
    c.execute("""UPDATE candidacies SET person_id = 10
                 WHERE candidate_name = 'Alistair Carmichael' AND person_id IS NULL""")
    print(f"  Linked {c.rowcount} candidacies")

    # === Link Tavish Scott (person_id=457) to Scottish Parliament candidacies ===
    print("=== Linking Tavish Scott ===")
    c.execute("""UPDATE candidacies SET person_id = 457
                 WHERE candidate_name = 'Tavish Scott' AND person_id IS NULL""")
    print(f"  Linked {c.rowcount} candidacies")

    # === Create Beatrice Wishart ===
    print("=== Creating Beatrice Wishart ===")
    c.execute("SELECT id FROM people WHERE slug = 'beatrice-wishart'")
    if c.fetchone():
        print("  Already exists")
    else:
        c.execute("""INSERT INTO people (name, slug, intro)
                     VALUES (?, ?, ?)""",
                  ("Beatrice Wishart",
                   "beatrice-wishart",
                   "Beatrice Wishart is a Liberal Democrat politician who has served as the Member of the Scottish Parliament (MSP) for Shetland since 2019. She won the Shetland by-election in August 2019 following the resignation of Tavish Scott, and was re-elected in the 2021 Scottish Parliament election. Before entering national politics, Wishart served as a Shetland Islands Council councillor for Lerwick South from 2017."))
        wishart_id = c.lastrowid
        print(f"  Created (id={wishart_id})")

        # Link all her candidacies
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Beatrice Wishart' AND person_id IS NULL""",
                  (wishart_id,))
        print(f"  Linked {c.rowcount} candidacies")

    # === Also link other candidates who appear in both SIC and national elections ===
    # Check for Ryan Thomson (SIC councillor who stood in 2019 SP by-election)
    c.execute("SELECT id FROM people WHERE name = 'Ryan Thomson'")
    row = c.fetchone()
    if row:
        rt_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Ryan Thomson' AND person_id IS NULL""", (rt_id,))
        if c.rowcount:
            print(f"=== Linked Ryan Thomson: {c.rowcount} candidacies ===")

    # Check for Debra Nicolson
    c.execute("SELECT id FROM people WHERE name = 'Debra Nicolson'")
    row = c.fetchone()
    if row:
        dn_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Debra Nicolson' AND person_id IS NULL""", (dn_id,))
        if c.rowcount:
            print(f"=== Linked Debra Nicolson: {c.rowcount} candidacies ===")

    # Check for Gary Robinson
    c.execute("SELECT id FROM people WHERE name = 'Gary Robinson'")
    row = c.fetchone()
    if row:
        gr_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Gary Robinson' AND person_id IS NULL""", (gr_id,))
        if c.rowcount:
            print(f"=== Linked Gary Robinson: {c.rowcount} candidacies ===")

    # Check for Alex Armitage
    c.execute("SELECT id FROM people WHERE name = 'Alex Armitage'")
    row = c.fetchone()
    if row:
        aa_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Alex Armitage' AND person_id IS NULL""", (aa_id,))
        if c.rowcount:
            print(f"=== Linked Alex Armitage: {c.rowcount} candidacies ===")

    # Check for Ian Scott
    c.execute("SELECT id FROM people WHERE name = 'Ian Scott'")
    row = c.fetchone()
    if row:
        is_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Ian Scott' AND person_id IS NULL""", (is_id,))
        if c.rowcount:
            print(f"=== Linked Ian Scott: {c.rowcount} candidacies ===")

    # Check for Johan Adamson
    c.execute("SELECT id FROM people WHERE name = 'Johan Adamson'")
    row = c.fetchone()
    if row:
        ja_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Johan Adamson' AND person_id IS NULL""", (ja_id,))
        if c.rowcount:
            print(f"=== Linked Johan Adamson: {c.rowcount} candidacies ===")

    # Check for Robina Barton (stood in both SP and UK elections)
    c.execute("SELECT id FROM people WHERE name = 'Robina Barton'")
    row = c.fetchone()
    if row:
        rb_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Robina Barton' AND person_id IS NULL""", (rb_id,))
        if c.rowcount:
            print(f"=== Linked Robina Barton: {c.rowcount} candidacies ===")

    # Check for Danus Skene
    c.execute("SELECT id FROM people WHERE name = 'Danus Skene'")
    row = c.fetchone()
    if row:
        ds_id = row['id']
        c.execute("""UPDATE candidacies SET person_id = ?
                     WHERE candidate_name = 'Danus Skene' AND person_id IS NULL""", (ds_id,))
        if c.rowcount:
            print(f"=== Linked Danus Skene: {c.rowcount} candidacies ===")

    db.commit()

    # Verify
    print("\n=== Verification ===")
    c.execute("""SELECT ca.candidate_name, e.wiki_page_title, ca.person_id
                 FROM candidacies ca
                 JOIN elections e ON ca.election_id = e.id
                 JOIN councils co ON e.council_id = co.id
                 WHERE co.slug IN ('parliament-uk', 'scottish-parliament')
                 AND ca.elected = 1
                 ORDER BY e.election_date""")
    for row in c.fetchall():
        linked = "LINKED" if row['person_id'] else "UNLINKED"
        print(f"  [{linked}] {row['candidate_name']} - {row['wiki_page_title']}")

    db.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
