#!/usr/bin/env python3
"""
Add modern SIC elections (2017, 2022) and by-elections (2019, 2022, 2025)
to the existing SQLite database.
"""

import sqlite3
import json

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'

def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    # Get SIC council ID
    c.execute("SELECT id FROM councils WHERE slug = 'shetland-islands-council'")
    sic_id = c.fetchone()['id']

    # Build constituency lookup - need to handle new 2017+ ward names
    # The 2007/2012 wards were different from the 2017+ wards
    # For 2017+, the wards are:
    # 1. North Isles, 2. Shetland North, 3. Shetland West, 4. Shetland Central,
    # 5. Shetland South, 6. Lerwick North (became "Lerwick North and Bressay" in 2022), 7. Lerwick South

    def get_or_create_constituency(name):
        slug = name.lower().replace(' ', '-').replace(',', '')
        c.execute("SELECT id FROM constituencies WHERE slug = ? AND council_id = ?", (slug, sic_id))
        row = c.fetchone()
        if row:
            return row['id']
        c.execute("INSERT INTO constituencies (council_id, name, slug) VALUES (?, ?, ?)", (sic_id, name, slug))
        return c.lastrowid

    def add_election(wiki_title, date, etype, constituency_name, electorate=None, turnout=None, turnout_pct=None, notes=None):
        con_id = get_or_create_constituency(constituency_name) if constituency_name else None
        # Check if already exists
        c.execute("SELECT id FROM elections WHERE wiki_page_title = ? AND constituency_id IS ?", (wiki_title, con_id))
        if c.fetchone():
            print(f"  Skipping (exists): {wiki_title} - {constituency_name}")
            return None
        c.execute("""INSERT INTO elections (council_id, constituency_id, election_date, election_type,
                     electorate, turnout, turnout_pct, notes, wiki_page_title)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (sic_id, con_id, date, etype, electorate, turnout, turnout_pct, notes, wiki_title))
        return c.lastrowid

    def add_candidacy(election_id, name, party=None, votes=None, votes_text=None, elected=False, position=None):
        if election_id is None:
            return
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, party, votes, votes_text, elected, position)
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
                  (election_id, name, party, votes, votes_text, 1 if elected else 0, position))

    print("=== Adding 2017 SIC Election ===")
    wiki_2017 = "Shetland Islands Council Election May 2017"

    # North Isles
    eid = add_election(wiki_2017, "2017-05-04", "general", "North Isles", turnout_pct=63.7)
    if eid:
        add_candidacy(eid, "Duncan Simpson", "Independent", 453, elected=True, position=1)
        add_candidacy(eid, "Ryan Thomson", "Independent", 372, elected=True, position=2)
        add_candidacy(eid, "Alec Priest", "Independent", 327, elected=True, position=3)
        add_candidacy(eid, "Cecil Hughson", "Independent", 185, elected=False, position=4)
        add_candidacy(eid, "Lynsay Cunningham", "Independent", 76, elected=False, position=5)

    # Shetland North
    eid = add_election(wiki_2017, "2017-05-04", "general", "Shetland North", turnout_pct=60.4)
    if eid:
        add_candidacy(eid, "Andrea Manson", "Independent", 509, elected=True, position=1)
        add_candidacy(eid, "Alastair Cooper", "Independent", 395, elected=True, position=2)
        add_candidacy(eid, "Emma Macdonald", "Independent", 240, elected=True, position=3)
        add_candidacy(eid, "Isobel Johnson", "Conservative", 48, elected=False, position=4)

    # Shetland West
    eid = add_election(wiki_2017, "2017-05-04", "general", "Shetland West", turnout_pct=40.9)
    if eid:
        add_candidacy(eid, "Theo Smith", "Independent", 521, elected=True, position=1)
        add_candidacy(eid, "Catherine Hughson", "Independent", 194, elected=True, position=2)
        add_candidacy(eid, "Steven Coutts", "Independent", 175, elected=True, position=3)
        add_candidacy(eid, "Gary Robinson", "Independent", 179, elected=False, position=4)
        add_candidacy(eid, "Debra Nicolson", "Independent", 81, elected=False, position=5)
        add_candidacy(eid, "Ian Tinkler", "Independent", 75, elected=False, position=6)

    # Shetland Central
    eid = add_election(wiki_2017, "2017-05-04", "general", "Shetland Central", turnout_pct=40.9)
    if eid:
        add_candidacy(eid, "Davie Sandison", "Independent", 320, elected=True, position=1)
        add_candidacy(eid, "Mark Burgess", "Independent", 260, elected=True, position=2)
        add_candidacy(eid, "Ian Scott", "Independent", 191, elected=True, position=3)
        add_candidacy(eid, "Julie Buchan", "Independent", 118, elected=False, position=4)
        add_candidacy(eid, "Brian Nugent", "Independent Nationalist", 84, elected=False, position=5)

    # Shetland South (uncontested)
    eid = add_election(wiki_2017, "2017-05-04", "general", "Shetland South", notes="Uncontested")
    if eid:
        add_candidacy(eid, "Allison Duncan", "Independent", votes_text="Unopposed", elected=True, position=1)
        add_candidacy(eid, "Robbie McGregor", "SNP", votes_text="Unopposed", elected=True, position=2)
        add_candidacy(eid, "George Smith", "Independent", votes_text="Unopposed", elected=True, position=3)

    # Lerwick North
    eid = add_election(wiki_2017, "2017-05-04", "general", "Lerwick North")
    if eid:
        add_candidacy(eid, "Malcolm Bell", "Independent", 715, elected=True, position=1)
        add_candidacy(eid, "Stephen Leask", "Independent", 130, elected=True, position=2)
        add_candidacy(eid, "John Fraser", "Independent", 124, elected=True, position=3)
        add_candidacy(eid, "Thomas Williamson", "Conservative", 26, elected=False, position=4)

    # Lerwick South
    eid = add_election(wiki_2017, "2017-05-04", "general", "Lerwick South")
    if eid:
        add_candidacy(eid, "Cecil Smith", "Independent", 429, elected=True, position=1)
        add_candidacy(eid, "Beatrice Wishart", "Independent", 394, elected=True, position=2)
        add_candidacy(eid, "Peter Campbell", "Independent", 299, elected=True, position=3)
        add_candidacy(eid, "Amanda Westlake", "Independent", 190, elected=True, position=4)
        add_candidacy(eid, "Frances Valente", "Independent", 180, elected=False, position=5)

    print("=== Adding 2019 By-Elections ===")

    # Shetland Central By-Election November 2019
    eid = add_election("Shetland Central By-Election November 2019", "2019-11-07", "by-election",
                       "Shetland Central", turnout_pct=31.0)
    if eid:
        add_candidacy(eid, "Moraig Lyall", "Independent", 344, elected=True, position=1)
        add_candidacy(eid, "Julie Buchan", "Independent", 116, elected=False, position=2)
        add_candidacy(eid, "Stewart Douglas", "SNP", 111, elected=False, position=3)
        add_candidacy(eid, "Gordon Laverie", "Independent", 84, elected=False, position=4)
        add_candidacy(eid, "Johan Adamson", "Independent", 77, elected=False, position=5)

    # Lerwick South By-Election November 2019
    eid = add_election("Lerwick South By-Election November 2019", "2019-11-07", "by-election",
                       "Lerwick South", turnout_pct=31.2)
    if eid:
        add_candidacy(eid, "Gary Robinson", "Independent", 374, elected=False, position=1)
        add_candidacy(eid, "Stephen Flaws", "Independent", 350, elected=True, position=2)
        add_candidacy(eid, "Frances Valente", "Independent", 154, elected=False, position=3)
        add_candidacy(eid, "Arwed Wenger", "Independent", 116, elected=False, position=4)
        add_candidacy(eid, "Caroline Henderson", "Independent", 73, elected=False, position=5)

    print("=== Adding 2022 SIC Election ===")
    wiki_2022 = "Shetland Islands Council Election May 2022"

    # North Isles (uncontested, only 2 candidates for 3 seats)
    eid = add_election(wiki_2022, "2022-05-05", "general", "North Isles",
                       notes="Uncontested. Only 2 candidates for 3 seats; by-election held August 2022 for vacant seat.")
    if eid:
        add_candidacy(eid, "Duncan Anderson", "Independent", votes_text="Unopposed", elected=True, position=1)
        add_candidacy(eid, "Ryan Thomson", "Independent", votes_text="Unopposed", elected=True, position=2)

    # Shetland North (uncontested)
    eid = add_election(wiki_2022, "2022-05-05", "general", "Shetland North",
                       notes="Uncontested. Exactly 3 candidates for 3 seats.")
    if eid:
        add_candidacy(eid, "Emma MacDonald", "Independent", votes_text="Unopposed", elected=True, position=1)
        add_candidacy(eid, "Andrea Manson", "Independent", votes_text="Unopposed", elected=True, position=2)
        add_candidacy(eid, "Tom Morton", "Labour", votes_text="Unopposed", elected=True, position=3)

    # Shetland West
    eid = add_election(wiki_2022, "2022-05-05", "general", "Shetland West",
                       electorate=1364, turnout_pct=56.5)
    if eid:
        add_candidacy(eid, "Liz Boxwell", "Independent", 298, elected=True, position=1)
        add_candidacy(eid, "John Leask", "Independent", 135, elected=True, position=2)
        add_candidacy(eid, "Mark Robinson", "Independent", 128, elected=False, position=3)
        add_candidacy(eid, "Debra Nicolson", "Green", 69, elected=False, position=4)
        add_candidacy(eid, "Zara Pennington", "SNP", 59, elected=False, position=5)
        add_candidacy(eid, "Andrew Holt", "Independent", 43, elected=False, position=6)
        add_candidacy(eid, "Ian Tinkler", "Independent", 21, elected=False, position=7)
        add_candidacy(eid, "Peter Fraser", "Independent", 5, elected=False, position=8)

    # Shetland Central
    eid = add_election(wiki_2022, "2022-05-05", "general", "Shetland Central",
                       electorate=3168, turnout_pct=44.9)
    if eid:
        add_candidacy(eid, "Moraig Lyall", "Independent", 414, elected=True, position=1)
        add_candidacy(eid, "Davie Sandison", "Independent", 322, elected=True, position=2)
        add_candidacy(eid, "Catherine Hughson", "Independent", 282, elected=True, position=3)
        add_candidacy(eid, "Ian Scott", "Independent", 271, elected=True, position=4)
        add_candidacy(eid, "Martin Randall", "Green", 99, elected=False, position=5)
        add_candidacy(eid, "Brian Nugent", "Sovereignty", 26, elected=False, position=6)

    # Lerwick North and Bressay (renamed ward)
    eid = add_election(wiki_2022, "2022-05-05", "general", "Lerwick North and Bressay",
                       electorate=2410, turnout_pct=39.0)
    if eid:
        add_candidacy(eid, "Stephen Leask", "Independent", 310, elected=True, position=1)
        add_candidacy(eid, "Gary Robinson", "Independent", 301, elected=True, position=2)
        add_candidacy(eid, "Arwed Wenger", "Independent", 136, elected=True, position=3)
        add_candidacy(eid, "Marie Williamson", "Independent", 115, elected=False, position=4)
        add_candidacy(eid, "Stephen Ferguson", "Independent", 66, elected=False, position=5)

    # Lerwick South
    eid = add_election(wiki_2022, "2022-05-05", "general", "Lerwick South",
                       electorate=3036, turnout_pct=44.8)
    if eid:
        add_candidacy(eid, "Dennis Leask", "Independent", 302, elected=True, position=1)
        add_candidacy(eid, "John Fraser", "Independent", 262, elected=True, position=2)
        add_candidacy(eid, "Cecil Smith", "Independent", 251, elected=True, position=3)
        add_candidacy(eid, "Neil Pearson", "Independent", 171, elected=True, position=4)
        add_candidacy(eid, "Amanda Hawick", "Independent", 153, elected=False, position=5)
        add_candidacy(eid, "Peter Coleman", "Independent", 106, elected=False, position=6)
        add_candidacy(eid, "Shayne McLeod", "Independent", 99, elected=False, position=7)

    # Shetland South
    eid = add_election(wiki_2022, "2022-05-05", "general", "Shetland South",
                       electorate=3348, turnout_pct=55.4)
    if eid:
        add_candidacy(eid, "Allison Duncan", "Independent", 805, elected=True, position=1)
        add_candidacy(eid, "Bryan Peterson", "Independent", 500, elected=True, position=2)
        add_candidacy(eid, "Alex Armitage", "Green", 274, elected=True, position=3)
        add_candidacy(eid, "Robbie McGregor", "SNP", 217, elected=True, position=4)
        add_candidacy(eid, "Stewart Douglas", "Independent", 46, elected=False, position=5)

    print("=== Adding 2022 By-Elections ===")

    # North Isles By-Election August 2022
    eid = add_election("North Isles By-Election August 2022", "2022-08-04", "by-election", "North Isles")
    if eid:
        add_candidacy(eid, "Robert Thomson", "Independent", 680, elected=True, position=1)
        add_candidacy(eid, "Sonia Robertson", "Independent", 106, elected=False, position=2)
        add_candidacy(eid, "Gary Cleaver", "Independent", 100, elected=False, position=3)
        add_candidacy(eid, "Marie Williamson", "Independent", 92, elected=False, position=4)
        add_candidacy(eid, "Stewart Douglas", "Independent", 13, elected=False, position=5)

    # Shetland West By-Election November 2022
    eid = add_election("Shetland West By-Election November 2022", "2022-11-17", "by-election", "Shetland West")
    if eid:
        add_candidacy(eid, "Mark Robinson", "Independent", 375, elected=True, position=1)
        add_candidacy(eid, "Debra Nicolson", "Green", 67, elected=False, position=2)
        add_candidacy(eid, "Zara Pennington", "SNP", 49, elected=False, position=3)

    print("=== Adding 2025 By-Election ===")

    # Shetland North By-Election January 2025
    eid = add_election("Shetland North By-Election January 2025", "2025-01-23", "by-election", "Shetland North")
    if eid:
        add_candidacy(eid, "Andrew Hall", "Independent", 887, elected=True, position=1)
        add_candidacy(eid, "Natasha Cornick", "Independent", 94, elected=False, position=2)

    db.commit()

    # Stats
    c.execute("SELECT COUNT(DISTINCT wiki_page_title) FROM elections WHERE council_id = ?", (sic_id,))
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM candidacies c JOIN elections e ON c.election_id = e.id WHERE e.council_id = ? AND e.election_date >= '2017-01-01'", (sic_id,))
    new_cands = c.fetchone()[0]
    print(f"\nTotal SIC elections (distinct pages): {total}")
    print(f"New candidacies added (2017+): {new_cands}")

    db.close()
    print("Done!")

if __name__ == '__main__':
    main()
