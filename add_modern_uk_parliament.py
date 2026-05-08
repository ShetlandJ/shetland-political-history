#!/usr/bin/env python3
"""
Add modern UK General Elections (2017, 2019, 2024) for the Orkney & Shetland
constituency. Also fix the incorrectly-stored dates for the 2010 and 2015
elections so prev/next navigation orders correctly.

Source: en.wikipedia.org/wiki/Orkney_and_Shetland_(UK_Parliament_constituency)

Idempotent.
"""
import sqlite3
import sys

DB = "shetland.db"

# Date corrections for existing elections.
DATE_FIXES = [
    ("2010 UK General Election, Orkney and Shetland Result", "2010-05-06"),
    ("2015 UK General Election, Orkney and Shetland Result", "2015-05-07"),
]

# Person-id mappings for candidates already in the DB.
# candidate_name -> person_id
KNOWN_PEOPLE = {
    "Alistair Carmichael": 10,    # Alexander 'Alistair' Carmichael
    "Alex Armitage": 543,         # SIC Green councillor
}

# (wiki_title, date, turnout, turnout_pct, electorate, [(name, party, votes, elected)])
ELECTIONS = [
    (
        "2017 UK General Election, Orkney and Shetland Result",
        "2017-06-08", 23277, 68.1, None,
        [
            ("Alistair Carmichael", "Liberal Democrats", 11312, 1),
            ("Miriam Brett", "Scottish National Party", 6749, 0),
            ("Robina Barton", "Labour", 2664, 0),
            ("Jamie Halcro Johnston", "Conservative", 2024, 0),
            ("Robert Smith", "UKIP", 283, 0),
            ("Stuart Hill", "Independent", 245, 0),
        ],
    ),
    (
        "2019 UK General Election, Orkney and Shetland Result",
        "2019-12-12", 23160, 67.7, None,
        [
            ("Alistair Carmichael", "Liberal Democrats", 10381, 1),
            ("Robert Leslie", "Scottish National Party", 7874, 0),
            ("Jenny Fairbairn", "Conservative", 2287, 0),
            ("Coilla Drake", "Labour", 1550, 0),
            ("Robert Smith", "Brexit Party", 900, 0),
            ("David Barnard", "Independent", 168, 0),
        ],
    ),
    (
        "2024 UK General Election, Orkney and Shetland Result",
        "2024-07-04", 20688, 60.4, 34236,
        [
            ("Alistair Carmichael", "Liberal Democrats", 11392, 1),
            ("Robert Leslie", "Scottish National Party", 3585, 0),
            ("Alex Armitage", "Green", 2046, 0),
            ("Robert Smith", "Reform UK", 1586, 0),
            ("Conor Savage", "Labour", 1493, 0),
            ("Shane Painter", "Conservative", 586, 0),
        ],
    ),
]


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    uk_council_id = cur.execute(
        "SELECT id FROM councils WHERE name = 'Parliament of the United Kingdom'"
    ).fetchone()[0]
    # UK Parliament elections in this DB use NULL constituency_id.
    constituency_id = None

    print("== Fix wrong election dates ==")
    for title, correct_date in DATE_FIXES:
        cur.execute(
            "UPDATE elections SET election_date = ? "
            "WHERE wiki_page_title = ? AND election_date != ?",
            (correct_date, title, correct_date),
        )
        if cur.rowcount:
            print(f"  + {title}: -> {correct_date}")

    print("\n== Add modern UK GE elections + candidacies ==")
    for title, date, turnout, turnout_pct, electorate, candidates in ELECTIONS:
        existing = cur.execute(
            "SELECT id FROM elections WHERE wiki_page_title = ?", (title,)
        ).fetchone()
        if existing:
            eid = existing[0]
            print(f"  ~ {title} (id={eid}): exists, ensuring candidacies present")
        else:
            cur.execute(
                """
                INSERT INTO elections
                  (council_id, constituency_id, election_date, election_type,
                   electorate, turnout, turnout_pct, wiki_page_title)
                VALUES (?, ?, ?, 'general', ?, ?, ?, ?)
                """,
                (uk_council_id, constituency_id, date, electorate, turnout,
                 turnout_pct, title),
            )
            eid = cur.lastrowid
            print(f"  + {title} (id={eid})")

        for name, party, votes, elected in candidates:
            row = cur.execute(
                "SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?",
                (eid, name),
            ).fetchone()
            person_id = KNOWN_PEOPLE.get(name)
            if row:
                cur.execute(
                    "UPDATE candidacies SET party = ?, votes = ?, elected = ?, person_id = ? "
                    "WHERE id = ?",
                    (party, votes, elected, person_id, row[0]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO candidacies
                      (election_id, candidate_name, party, votes, elected, person_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (eid, name, party, votes, elected, person_id),
                )
                tag = " [linked]" if person_id else ""
                print(f"      + {name} ({party}): {votes}{tag}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    sys.exit(main() or 0)
