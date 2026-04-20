#!/usr/bin/env python3
"""
Add Scottish Parliament election results for Shetland constituency (1999-2021)
to the existing SQLite database.

Data sourced from Wikipedia (Shetland (Scottish Parliament constituency))
and official Scottish Parliament election results.

Constituency was named "Shetland" 1999-2007, renamed to "Shetland Islands" from 2011.
"""

import sqlite3

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'

def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    # ── Council ──────────────────────────────────────────────────────────────
    c.execute("SELECT id FROM councils WHERE slug = 'scottish-parliament'")
    row = c.fetchone()
    if row:
        sp_id = row['id']
        print(f"Scottish Parliament council already exists (id={sp_id})")
    else:
        c.execute("INSERT INTO councils (name, slug, level) VALUES (?, ?, ?)",
                  ("Scottish Parliament", "scottish-parliament", "devolved"))
        sp_id = c.lastrowid
        print(f"Created Scottish Parliament council (id={sp_id})")

    # ── Constituency ─────────────────────────────────────────────────────────
    # Use "Shetland Islands" as the canonical name (post-2011 name).
    # Elections 1999–2007 use constituency_display_name = "Shetland".
    def get_or_create_constituency(name):
        slug = name.lower().replace(' ', '-')
        c.execute("SELECT id FROM constituencies WHERE slug = ? AND council_id = ?", (slug, sp_id))
        row = c.fetchone()
        if row:
            return row['id']
        c.execute("INSERT INTO constituencies (council_id, name, slug) VALUES (?, ?, ?)",
                  (sp_id, name, slug))
        return c.lastrowid

    con_shetland_islands = get_or_create_constituency("Shetland Islands")
    print(f"Shetland Islands constituency id={con_shetland_islands}")

    # ── Person: Beatrice Wishart ──────────────────────────────────────────────
    c.execute("SELECT id FROM people WHERE slug = 'beatrice-wishart'")
    row = c.fetchone()
    if row:
        beatrice_id = row['id']
        print(f"Beatrice Wishart person record already exists (id={beatrice_id})")
    else:
        c.execute("""INSERT INTO people (name, slug, intro)
                     VALUES (?, ?, ?)""",
                  ("Beatrice Wishart",
                   "beatrice-wishart",
                   "Beatrice Wishart is the Scottish Liberal Democrat Member of the Scottish Parliament "
                   "for Shetland Islands. She won the seat at the 2019 by-election following the "
                   "resignation of Tavish Scott, and was re-elected in 2021. She previously served "
                   "as a Shetland Islands councillor for Lerwick South from 2017."))
        beatrice_id = c.lastrowid
        print(f"Created Beatrice Wishart person record (id={beatrice_id})")

    # Link her SIC candidacy to her person record
    c.execute("""UPDATE candidacies SET person_id = ?
                 WHERE candidate_name = 'Beatrice Wishart' AND person_id IS NULL""", (beatrice_id,))
    if c.rowcount:
        print(f"Linked {c.rowcount} existing SIC candidacy/candidacies to Beatrice Wishart")

    tavish_id = 457  # confirmed from DB

    # ── Helpers ───────────────────────────────────────────────────────────────
    def add_election(wiki_title, date, etype, constituency_id, display_name=None,
                     electorate=None, turnout=None, turnout_pct=None, notes=None, replaced_person=None):
        c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (wiki_title,))
        if c.fetchone():
            print(f"  Skipping (exists): {wiki_title}")
            return None
        c.execute("""INSERT INTO elections
                     (council_id, constituency_id, constituency_display_name, election_date,
                      election_type, electorate, turnout, turnout_pct, notes, replaced_person, wiki_page_title)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (sp_id, constituency_id, display_name, date, etype,
                   electorate, turnout, turnout_pct, notes, replaced_person, wiki_title))
        return c.lastrowid

    def add_candidacy(election_id, name, party, votes=None, votes_text=None,
                      elected=False, position=None, person_id=None):
        if election_id is None:
            return
        c.execute("""INSERT INTO candidacies
                     (election_id, person_id, candidate_name, party, votes, votes_text, elected, position)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                  (election_id, person_id, name, party, votes, votes_text, 1 if elected else 0, position))

    # ── Elections ─────────────────────────────────────────────────────────────

    print("\n=== 1999 Scottish Parliament Election, Shetland ===")
    eid = add_election(
        "1999 Scottish Parliament Election, Shetland Result",
        "1999-05-06", "general", con_shetland_islands,
        display_name="Shetland",
        electorate=17130, turnout=9998, turnout_pct=58.3,
    )
    if eid:
        add_candidacy(eid, "Tavish Scott",   "Liberal Democrats", 5455, elected=True,  position=1, person_id=tavish_id)
        add_candidacy(eid, "Jonathan Wills", "Labour",            2241, elected=False, position=2)
        add_candidacy(eid, "Willie Ross",    "SNP",               1430, elected=False, position=3)
        add_candidacy(eid, "Gary Robinson",  "Conservative",       872, elected=False, position=4)

    print("=== 2003 Scottish Parliament Election, Shetland ===")
    eid = add_election(
        "2003 Scottish Parliament Election, Shetland Result",
        "2003-05-01", "general", con_shetland_islands,
        display_name="Shetland",
        electorate=16691, turnout=8645, turnout_pct=51.8,
    )
    if eid:
        add_candidacy(eid, "Tavish Scott",    "Liberal Democrats",   3989, elected=True,  position=1, person_id=tavish_id)
        add_candidacy(eid, "Willie Ross",     "SNP",                 1729, elected=False, position=2)
        add_candidacy(eid, "John Firth",      "Conservative",        1281, elected=False, position=3)
        add_candidacy(eid, "Peter Hamilton",  "Labour",               880, elected=False, position=4)
        add_candidacy(eid, "Peter Andrews",   "Scottish Socialist",   766, elected=False, position=5)

    print("=== 2007 Scottish Parliament Election, Shetland ===")
    eid = add_election(
        "2007 Scottish Parliament Election, Shetland Result",
        "2007-05-03", "general", con_shetland_islands,
        display_name="Shetland",
        electorate=17073, turnout=9795, turnout_pct=57.3,
    )
    if eid:
        add_candidacy(eid, "Tavish Scott",  "Liberal Democrats", 6531, elected=True,  position=1, person_id=tavish_id)
        add_candidacy(eid, "Val Simpson",   "SNP",               1622, elected=False, position=2)
        add_candidacy(eid, "Mark Jones",    "Conservative",       972, elected=False, position=3)
        add_candidacy(eid, "Scott Burnett", "Labour",             670, elected=False, position=4)

    print("=== 2011 Scottish Parliament Election, Shetland Islands ===")
    eid = add_election(
        "2011 Scottish Parliament Election, Shetland Islands Result",
        "2011-05-05", "general", con_shetland_islands,
        electorate=17480, turnout=9428, turnout_pct=53.9,
    )
    if eid:
        add_candidacy(eid, "Tavish Scott",  "Liberal Democrats", 4462, elected=True,  position=1, person_id=tavish_id)
        add_candidacy(eid, "Billy Fox",     "Independent",       2845, elected=False, position=2)
        add_candidacy(eid, "Jean Urquhart", "SNP",               1134, elected=False, position=3)
        add_candidacy(eid, "Jamie Kerr",    "Labour",             620, elected=False, position=4)
        add_candidacy(eid, "Sandy Cross",   "Conservative",       330, elected=False, position=5)

    print("=== 2016 Scottish Parliament Election, Shetland Islands ===")
    eid = add_election(
        "2016 Scottish Parliament Election, Shetland Islands Result",
        "2016-05-05", "general", con_shetland_islands,
        electorate=17792, turnout=11087, turnout_pct=62.3,
    )
    if eid:
        add_candidacy(eid, "Tavish Scott",  "Liberal Democrats", 7440, elected=True,  position=1, person_id=tavish_id)
        add_candidacy(eid, "Danus Skene",   "SNP",               2545, elected=False, position=2)
        add_candidacy(eid, "Robina Barton", "Labour",             651, elected=False, position=3)
        add_candidacy(eid, "Cameron Smith", "Conservative",       405, elected=False, position=4)

    print("=== 2019 Shetland Islands By-Election ===")
    eid = add_election(
        "Shetland Islands By-Election September 2019",
        "2019-09-19", "by-election", con_shetland_islands,
        electorate=17779, turnout=11835, turnout_pct=66.5,
        replaced_person="Tavish Scott",
    )
    if eid:
        add_candidacy(eid, "Beatrice Wishart", "Liberal Democrats", 5659, elected=True,  position=1, person_id=beatrice_id)
        add_candidacy(eid, "Tom Wills",        "SNP",               3822, elected=False, position=2)
        add_candidacy(eid, "Ryan Thomson",     "Independent",       1286, elected=False, position=3)
        add_candidacy(eid, "Brydon Goodlad",   "Conservative",       425, elected=False, position=4)

    print("=== 2021 Scottish Parliament Election, Shetland Islands ===")
    eid = add_election(
        "2021 Scottish Parliament Election, Shetland Islands Result",
        "2021-05-06", "general", con_shetland_islands,
        electorate=18120, turnout=11968, turnout_pct=66.0,
    )
    if eid:
        add_candidacy(eid, "Beatrice Wishart", "Liberal Democrats", 5803, elected=True,  position=1, person_id=beatrice_id)
        add_candidacy(eid, "Tom Wills",        "SNP",               4997, elected=False, position=2)
        add_candidacy(eid, "Nick Tulloch",     "Conservative",       503, elected=False, position=3)
        add_candidacy(eid, "Martin Kerr",      "Labour",             424, elected=False, position=4)

    db.commit()

    c.execute("SELECT COUNT(*) FROM elections WHERE council_id = ?", (sp_id,))
    print(f"\nTotal Scottish Parliament elections added: {c.fetchone()[0]}")
    c.execute("SELECT COUNT(*) FROM candidacies ca JOIN elections e ON ca.election_id = e.id WHERE e.council_id = ?", (sp_id,))
    print(f"Total candidacies added: {c.fetchone()[0]}")

    db.close()
    print("Done!")

if __name__ == '__main__':
    main()
