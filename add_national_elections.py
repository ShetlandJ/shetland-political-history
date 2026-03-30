#!/usr/bin/env python3
"""
Add missing UK General Elections (2017, 2019, 2024), fix existing date errors,
and add Scottish Parliament elections (1999-2021) + 2019 by-election.
"""

import sqlite3

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'


def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    # === Fix existing UK election date errors ===
    print("=== Fixing existing date errors ===")

    c.execute("UPDATE elections SET election_date = '2010-05-06' WHERE wiki_page_title = '2010 UK General Election, Orkney and Shetland Result'")
    print(f"  2010 UK election date: {c.rowcount} row(s) updated")

    c.execute("UPDATE elections SET election_date = '2015-05-07' WHERE wiki_page_title = '2015 UK General Election, Orkney and Shetland Result'")
    print(f"  2015 UK election date: {c.rowcount} row(s) updated")

    # === Add missing UK General Elections ===
    print("\n=== Adding missing UK General Elections ===")

    c.execute("SELECT id FROM councils WHERE slug = 'parliament-uk'")
    uk_id = c.fetchone()['id']

    def add_election(council_id, wiki_title, date, etype, electorate=None, turnout=None, turnout_pct=None, notes=None, constituency_id=None):
        c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (wiki_title,))
        if c.fetchone():
            print(f"  Skipping (exists): {wiki_title}")
            return None
        c.execute("""INSERT INTO elections (council_id, constituency_id, election_date, election_type,
                     electorate, turnout, turnout_pct, notes, wiki_page_title)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (council_id, constituency_id, date, etype, electorate, turnout, turnout_pct, notes, wiki_title))
        print(f"  Added: {wiki_title}")
        return c.lastrowid

    def add_candidacy(election_id, name, party=None, votes=None, votes_text=None, elected=False, position=None):
        if election_id is None:
            return
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, party, votes, votes_text, elected, position)
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
                  (election_id, name, party, votes, votes_text, 1 if elected else 0, position))

    # 2017 UK General Election
    eid = add_election(uk_id, "2017 UK General Election, Orkney and Shetland Result",
                       "2017-06-08", "general", electorate=34164, turnout=23277, turnout_pct=68.1)
    if eid:
        add_candidacy(eid, "Alistair Carmichael", "Liberal Democrats", 11312, elected=True, position=1)
        add_candidacy(eid, "Miriam Brett", "SNP", 6749, elected=False, position=2)
        add_candidacy(eid, "Robina Barton", "Labour", 2664, elected=False, position=3)
        add_candidacy(eid, "Jamie Halcro Johnston", "Conservative", 2024, elected=False, position=4)
        add_candidacy(eid, "Robert Smith", "UKIP", 283, elected=False, position=5)
        add_candidacy(eid, "Stuart Hill", "Independent", 245, elected=False, position=6)

    # 2019 UK General Election
    eid = add_election(uk_id, "2019 UK General Election, Orkney and Shetland Result",
                       "2019-12-12", "general", electorate=34211, turnout=23160, turnout_pct=67.7)
    if eid:
        add_candidacy(eid, "Alistair Carmichael", "Liberal Democrats", 10381, elected=True, position=1)
        add_candidacy(eid, "Robert Leslie", "SNP", 7874, elected=False, position=2)
        add_candidacy(eid, "Jenny Fairbairn", "Conservative", 2287, elected=False, position=3)
        add_candidacy(eid, "Coilla Drake", "Labour", 1550, elected=False, position=4)
        add_candidacy(eid, "Robert Smith", "Brexit Party", 900, elected=False, position=5)
        add_candidacy(eid, "David Barnard", "Independent", 168, elected=False, position=6)

    # 2024 UK General Election
    eid = add_election(uk_id, "2024 UK General Election, Orkney and Shetland Result",
                       "2024-07-04", "general", electorate=34236, turnout=20688, turnout_pct=60.4)
    if eid:
        add_candidacy(eid, "Alistair Carmichael", "Liberal Democrats", 11392, elected=True, position=1)
        add_candidacy(eid, "Robert Leslie", "SNP", 3585, elected=False, position=2)
        add_candidacy(eid, "Alex Armitage", "Scottish Green Party", 2046, elected=False, position=3)
        add_candidacy(eid, "Robert Smith", "Reform UK", 1586, elected=False, position=4)
        add_candidacy(eid, "Conor Savage", "Labour", 1493, elected=False, position=5)
        add_candidacy(eid, "Shane Painter", "Conservative", 586, elected=False, position=6)

    # === Create Scottish Parliament council and constituency ===
    print("\n=== Adding Scottish Parliament ===")

    c.execute("SELECT id FROM councils WHERE slug = 'scottish-parliament'")
    row = c.fetchone()
    if row:
        sp_id = row['id']
        print("  Scottish Parliament council already exists")
    else:
        c.execute("INSERT INTO councils (name, slug, level) VALUES ('Scottish Parliament', 'scottish-parliament', 'national')")
        sp_id = c.lastrowid
        print("  Created Scottish Parliament council")

    c.execute("SELECT id FROM constituencies WHERE slug = 'shetland' AND council_id = ?", (sp_id,))
    row = c.fetchone()
    if row:
        shet_con_id = row['id']
        print("  Shetland constituency already exists")
    else:
        c.execute("INSERT INTO constituencies (council_id, name, slug) VALUES (?, 'Shetland', 'shetland')", (sp_id,))
        shet_con_id = c.lastrowid
        print("  Created Shetland constituency")

    # 1999 Scottish Parliament Election
    eid = add_election(sp_id, "1999 Scottish Parliament Election, Shetland Result",
                       "1999-05-06", "general", turnout=9998, turnout_pct=58.3, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Tavish Scott", "Liberal Democrats", 5455, elected=True, position=1)
        add_candidacy(eid, "Jonathan Wills", "Labour", 2241, elected=False, position=2)
        add_candidacy(eid, "Willie Ross", "SNP", 1430, elected=False, position=3)
        add_candidacy(eid, "Gary Robinson", "Conservative", 872, elected=False, position=4)

    # 2003 Scottish Parliament Election
    eid = add_election(sp_id, "2003 Scottish Parliament Election, Shetland Result",
                       "2003-05-01", "general", turnout=8645, turnout_pct=51.8, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Tavish Scott", "Liberal Democrats", 3989, elected=True, position=1)
        add_candidacy(eid, "Willie Ross", "SNP", 1729, elected=False, position=2)
        add_candidacy(eid, "John Firth", "Conservative", 1281, elected=False, position=3)
        add_candidacy(eid, "Peter Hamilton", "Labour", 880, elected=False, position=4)
        add_candidacy(eid, "Peter Andrews", "Scottish Socialist", 766, elected=False, position=5)

    # 2007 Scottish Parliament Election
    eid = add_election(sp_id, "2007 Scottish Parliament Election, Shetland Result",
                       "2007-05-03", "general", turnout=9795, turnout_pct=57.3, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Tavish Scott", "Liberal Democrats", 6531, elected=True, position=1)
        add_candidacy(eid, "Val Simpson", "SNP", 1622, elected=False, position=2)
        add_candidacy(eid, "Mark Jones", "Conservative", 972, elected=False, position=3)
        add_candidacy(eid, "Scott Burnett", "Labour", 670, elected=False, position=4)

    # 2011 Scottish Parliament Election
    eid = add_election(sp_id, "2011 Scottish Parliament Election, Shetland Result",
                       "2011-05-05", "general", turnout=9428, turnout_pct=53.9, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Tavish Scott", "Liberal Democrats", 4462, elected=True, position=1)
        add_candidacy(eid, "Billy Fox", "Independent", 2845, elected=False, position=2)
        add_candidacy(eid, "Jean Urquhart", "SNP", 1134, elected=False, position=3)
        add_candidacy(eid, "Jamie Kerr", "Labour", 620, elected=False, position=4)
        add_candidacy(eid, "Sandy Cross", "Conservative", 330, elected=False, position=5)

    # 2016 Scottish Parliament Election
    eid = add_election(sp_id, "2016 Scottish Parliament Election, Shetland Result",
                       "2016-05-05", "general", turnout=11087, turnout_pct=62.3, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Tavish Scott", "Liberal Democrats", 7440, elected=True, position=1)
        add_candidacy(eid, "Danus Skene", "SNP", 2545, elected=False, position=2)
        add_candidacy(eid, "Robina Barton", "Labour", 651, elected=False, position=3)
        add_candidacy(eid, "Cameron Smith", "Conservative", 405, elected=False, position=4)

    # 2019 Shetland By-Election (Tavish Scott resigned)
    eid = add_election(sp_id, "2019 Scottish Parliament Shetland By-Election",
                       "2019-08-29", "by-election", turnout=11824, turnout_pct=66.4,
                       constituency_id=shet_con_id,
                       notes="Triggered by the resignation of Tavish Scott.")
    if eid:
        add_candidacy(eid, "Beatrice Wishart", "Liberal Democrats", 5659, elected=True, position=1)
        add_candidacy(eid, "Tom Wills", "SNP", 3822, elected=False, position=2)
        add_candidacy(eid, "Ryan Thomson", "Independent", 1286, elected=False, position=3)
        add_candidacy(eid, "Brydon Goodlad", "Conservative", 425, elected=False, position=4)
        add_candidacy(eid, "Debra Nicolson", "Scottish Green Party", 189, elected=False, position=5)
        add_candidacy(eid, "Johan Adamson", "Labour", 152, elected=False, position=6)
        add_candidacy(eid, "Michael Stout", "Independent", 134, elected=False, position=7)
        add_candidacy(eid, "Ian Scott", "Independent", 66, elected=False, position=8)
        add_candidacy(eid, "Stuart Martin", "UKIP", 60, elected=False, position=9)
        add_candidacy(eid, "Peter Tait", "Independent", 31, elected=False, position=10)

    # 2021 Scottish Parliament Election
    eid = add_election(sp_id, "2021 Scottish Parliament Election, Shetland Result",
                       "2021-05-06", "general", turnout=11968, turnout_pct=66.0, constituency_id=shet_con_id)
    if eid:
        add_candidacy(eid, "Beatrice Wishart", "Liberal Democrats", 5803, elected=True, position=1)
        add_candidacy(eid, "Tom Wills", "SNP", 4997, elected=False, position=2)
        add_candidacy(eid, "Nick Tulloch", "Conservative", 503, elected=False, position=3)
        add_candidacy(eid, "Martin Kerr", "Labour", 424, elected=False, position=4)
        add_candidacy(eid, "Peter Tait", "Independent", 116, elected=False, position=5)

    db.commit()

    # Stats
    print("\n=== Summary ===")
    for slug in ['parliament-uk', 'scottish-parliament']:
        c.execute("""SELECT co.name, COUNT(DISTINCT e.wiki_page_title) as elections, COUNT(ca.id) as candidacies
                     FROM councils co
                     JOIN elections e ON e.council_id = co.id
                     JOIN candidacies ca ON ca.election_id = e.id
                     WHERE co.slug = ?""", (slug,))
        row = c.fetchone()
        if row and row['name']:
            print(f"  {row['name']}: {row['elections']} elections, {row['candidacies']} candidacies")

    db.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
