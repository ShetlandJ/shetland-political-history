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

2. November 1936 Lerwick Town Council co-option of Erling Clausen. Shetland Times preview of
   the 3 Nov 1936 election: "Only four candidates have been nominated for the five vacancies",
   so one seat was left empty at the general and Clausen's co-option on 10 November filled it.
   He did not replace Laurence Cogle: Cogle's seat had gone to T. A. Sinclair, co-opted in July
   1936 and re-elected at the general.

3. August 1938 Lerwick Town Council co-option of John A. Williamson, which the wiki doesn't
   record. Shetland Times, 27 Aug 1938: "A special meeting of Lerwick Town Council was held on
   Thursday of last week" (18 August), where "it was resolved to co-opt Mr John A. Williamson, the
   unsuccessful candidate at the last Council election receiving the highest number of votes", in
   the room of the late Councillor A. S. Manson (died 22 July 1938). He is listed as a retiring
   councillor at the November 1938 election, standing for Labour (Shetland Times, 29 Oct 1938).
   His profile's "Lerwick Town Councillor between 1941 and 1945" gains the 1938 spell.

4. William MacDougall's resignation from Lerwick Town Council. His profile says April 1912. He
   tendered it on 2 April 1912 but "had reconsidered his decision to resign" by that Friday
   (Shetland Times, 6 Apr 1912), attended the 1 October 1912 meeting (Shetland Times, 5 Oct 1912),
   and resigned on Tuesday 8 October 1912 (Shetland Times, 12 Oct 1912). Evidence with BNA links:
   research/bna/ltc-1912-1914.md.

5. The May 1957 Lerwick Town Council election. The wiki lists Grace Halcrow among the four
   returned unopposed. The Shetland Times, 19 Apr 1957, gives the four nominations: Robert B.
   Blance, Alexander Morrison, Andrew J. Nicolson and Robert Ollason. Its preview of the 1964
   election (17 Apr 1964) says Halcrow had been elected ten years before and served only one year
   before resigning to become county councillor. So the 1957 seat was Nicolson's (Labour, like his
   other candidacies), and her profile's "from 1957" becomes 1954-55 and from 1964.

6. The "Lerwick Town Council By-Election May 1961" was a co-option in June. The Rev. Kenneth
   Thomson resigned at the statutory meeting on Friday 5 May 1961, being ineligible (Shetland Times,
   12 May 1961). Robert Strachan was co-opted at "Tuesday's Town Council meeting" (Shetland Times,
   16 Jun 1961), i.e. 13 June 1961.

Evidence for 5 and 6 with BNA links: research/bna/ltc-1955-1965.md.

7. Lerwick Town Council polling days 1949-1964. From 1949 the elections were in May, on the
   Tuesday. The baseline dates twelve of them on the Monday before. The Shetland Times gives the
   Tuesday each year: "Tuesday first is polling day" (29 Apr 1949, 1 May 1953, 29 Apr 1955);
   "Tuesday, 2nd May" (21 Apr 1950); "Tuesday 6th May" (2 May 1952); "following Tuesday's
   municipal election" (7 May 1954); "poor weather on Tuesday" (9 May 1958); "5th May" (1 May
   1959); Tuesday 3 May 1960 (15 Apr 1960); "last Tuesday's poll" (12 May 1961); Tuesday 7 May
   1963 (19 Apr 1963); "Tuesday, 5th of May" (1 May 1964). Arthur Johnson's co-option in room of
   James Brownlie (September 1949) was at the monthly meeting "on Tuesday", 13 September 1949
   (Shetland Times, 16 Sep 1949), not Monday the 12th. Evidence: research/bna/ltc-election-dates-1949-1964.md.
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

GENERAL = 'Lerwick Town Council Election November 1884'
BY_ELECTION = 'Lerwick Town Council By-Election November 1884'
CLAUSEN_BY_ELECTION = 'Lerwick Town Council By-Election November 1936'
UNFILLED = '[unfilled seat]'
WILLIAMSON_BY_ELECTION = 'Lerwick Town Council By-Election August 1938'
WILLIAMSON_NOTE = (
    "Co-option at a special meeting of the Town Council on 18 August 1938 to fill the vacancy left by "
    "the death of Alexander S. Manson. John A. Williamson was chosen as the unsuccessful candidate at "
    "the November 1937 election with the highest number of votes. Not recorded on the wiki; from the "
    "Shetland Times, 27 August 1938."
)
WILLIAMSON_INTRO = ('Lerwick Town Councillor between 1941 and 1945',
                    'Lerwick Town Councillor in 1938 and between 1941 and 1945')
HALCROW_1957 = 'Lerwick Town Council Election May 1957'
HALCROW_INTRO = ('a Lerwick Town Councillor from 1957 till the late 1960s',
                 'a Lerwick Town Councillor in 1954-55 and from 1964 till the late 1960s')
STRACHAN_BY_ELECTION = 'Lerwick Town Council By-Election May 1961'
STRACHAN_NOTE = (
    "Co-option at the Town Council meeting of 13 June 1961, to fill the vacancy left when the Rev. "
    "Kenneth Thomson, being ineligible, resigned at the statutory meeting on 5 May 1961. Robert "
    "Strachan had headed the unsuccessful candidates at the May 1961 election. From the Shetland Times, "
    "12 May and 16 June 1961."
)
POLLING_DAYS = [  # (wiki title, baseline Monday, Tuesday from the Shetland Times)
    ('Lerwick Town Council Election May 1949', '1949-05-02', '1949-05-03'),
    ('Lerwick Town Council By-Election September 1949', '1949-09-12', '1949-09-13'),
    ('Lerwick Town Council Election May 1950', '1950-05-01', '1950-05-02'),
    ('Lerwick Town Council Election May 1952', '1952-05-05', '1952-05-06'),
    ('Lerwick Town Council Election May 1953', '1953-05-04', '1953-05-05'),
    ('Lerwick Town Council Election May 1954', '1954-05-03', '1954-05-04'),
    ('Lerwick Town Council Election May 1955', '1955-05-02', '1955-05-03'),
    ('Lerwick Town Council Election May 1958', '1958-05-05', '1958-05-06'),
    ('Lerwick Town Council Election May 1959', '1959-05-04', '1959-05-05'),
    ('Lerwick Town Council Election May 1960', '1960-05-02', '1960-05-03'),
    ('Lerwick Town Council Election May 1961', '1961-05-01', '1961-05-02'),
    ('Lerwick Town Council Election May 1963', '1963-05-06', '1963-05-07'),
    ('Lerwick Town Council Election May 1964', '1964-05-04', '1964-05-05'),
]
MACDOUGALL_INTRO =('until he resigned in April 1912', 'until he resigned in October 1912')

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

    print("=== 2. November 1936 LTC co-option ===")
    row = one(c, "SELECT id, replaced_person, replaced_person_id FROM elections WHERE wiki_page_title = ?",
              (CLAUSEN_BY_ELECTION,))
    if row['replaced_person'] == UNFILLED and row['replaced_person_id'] is None:
        print(f"  {CLAUSEN_BY_ELECTION}: already {UNFILLED}")
    elif row['replaced_person'] == 'Laurence Cogle':
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = NULL WHERE id = ?",
                  (UNFILLED, row['id']))
        print(f"  {CLAUSEN_BY_ELECTION} (id {row['id']}): replaced_person Laurence Cogle -> {UNFILLED}")
    else:
        raise SystemExit(f"'{CLAUSEN_BY_ELECTION}' has unexpected replaced_person {row['replaced_person']}")

    print("=== 3. August 1938 LTC co-option ===")
    ltc = one(c, "SELECT id FROM councils WHERE name = 'Lerwick Town Council'", ())
    manson = one(c, "SELECT id, name, died_date FROM people WHERE slug = 'alexander-manson'", ())
    if manson['died_date'] != '1938-07-22':
        raise SystemExit(f"people.alexander-manson died_date is {manson['died_date']}, expected 1938-07-22")
    williamson = one(c, "SELECT id FROM people WHERE slug = 'john-williamson-iii'", ())
    c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (WILLIAMSON_BY_ELECTION,))
    row = c.fetchone()
    if row:
        election_id = row['id']
        print(f"  by-election exists (id {election_id})")
    else:
        c.execute("""
            INSERT INTO elections (council_id, election_date, election_type, wiki_page_title,
                                   replaced_person, replaced_person_id, notes)
            VALUES (?, '1938-08-18', 'by-election', ?, ?, ?, ?)
        """, (ltc['id'], WILLIAMSON_BY_ELECTION, manson['name'], manson['id'], WILLIAMSON_NOTE))
        election_id = c.lastrowid
        print(f"  by-election created (id {election_id})")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = 'John A. Williamson'",
              (election_id,))
    if c.fetchone():
        print("  candidacy exists")
    else:
        c.execute("""
            INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes_text, elected, position)
            VALUES (?, ?, 'John A. Williamson', 'Labour', 'Co-opted', 1, 1)
        """, (election_id, williamson['id']))
        print("  candidacy created")

    old, new = WILLIAMSON_INTRO
    row = one(c, "SELECT intro FROM people WHERE id = ?", (williamson['id'],))
    if new in row['intro']:
        print("  intro: already mentions 1938")
    elif old in row['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (row['intro'].replace(old, new), williamson['id']))
        print("  intro: 1938 added")
    else:
        raise SystemExit(f"john-williamson-iii intro doesn't contain {old!r}")

    print("=== 4. William MacDougall's resignation ===")
    old, new = MACDOUGALL_INTRO
    row = one(c, "SELECT id, intro FROM people WHERE slug = 'william-macdougall'", ())
    if new in row['intro']:
        print("  intro: already October 1912")
    elif old in row['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (row['intro'].replace(old, new), row['id']))
        print("  intro: April 1912 -> October 1912")
    else:
        raise SystemExit(f"william-macdougall intro doesn't contain {old!r}")

    print("=== 5. May 1957 LTC election: Nicolson, not Halcrow ===")
    halcrow = one(c, "SELECT id, intro FROM people WHERE slug = 'grace-halcrow'", ())
    nicolson = one(c, "SELECT id FROM people WHERE slug = 'andrew-nicolson'", ())
    e1957 = one(c, "SELECT id FROM elections WHERE wiki_page_title = ?", (HALCROW_1957,))
    c.execute("SELECT id, person_id FROM candidacies WHERE election_id = ? AND person_id IN (?, ?)",
              (e1957['id'], halcrow['id'], nicolson['id']))
    found = c.fetchall()
    if len(found) != 1:
        raise SystemExit(f"{HALCROW_1957}: expected one Halcrow or Nicolson candidacy, found {len(found)}")
    if found[0]['person_id'] == nicolson['id']:
        print("  candidacy: already Nicolson")
    else:
        c.execute("""UPDATE candidacies SET person_id = ?, candidate_name = 'Andrew J. Nicolson', party = 'Labour'
                     WHERE id = ?""", (nicolson['id'], found[0]['id']))
        print(f"  candidacy {found[0]['id']}: Grace Halcrow -> Andrew J. Nicolson")
    old, new = HALCROW_INTRO
    if new in halcrow['intro']:
        print("  intro: already corrected")
    elif old in halcrow['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (halcrow['intro'].replace(old, new), halcrow['id']))
        print("  intro: from 1957 -> 1954-55 and from 1964")
    else:
        raise SystemExit(f"grace-halcrow intro doesn't contain {old!r}")

    print("=== 6. June 1961 LTC co-option of Robert Strachan ===")
    by_id = set_date(c, STRACHAN_BY_ELECTION, '1961-05-05', '1961-06-13')
    row = one(c, "SELECT notes FROM elections WHERE id = ?", (by_id,))
    if row['notes'] == STRACHAN_NOTE:
        print("  note: already set")
    elif not row['notes']:
        c.execute("UPDATE elections SET notes = ? WHERE id = ?", (STRACHAN_NOTE, by_id))
        print("  note: added")
    else:
        raise SystemExit(f"{STRACHAN_BY_ELECTION} already has notes, not overwriting: {row['notes']}")

    print("=== 7. LTC polling days 1949-1964 ===")
    for title, wrong, right in POLLING_DAYS:
        set_date(c, title, wrong, right)

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
