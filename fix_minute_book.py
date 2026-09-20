#!/usr/bin/env python3
"""
Corrections to shetland.db from the first Lerwick Town Council minute book (1818–1877).

See corrections-from-minute-book.md for the evidence. Runs after parse_wiki.py and is
idempotent: re-running does nothing once the corrections are in place.

1. The 1829 councillor "Andrew Duncan" is the Sheriff's son (Bayanne I10526), not the
   Sheriff Substitute (I10506). Creates the son, re-points the 1829 candidacy, drops the
   councillor category from the Sheriff, and rewords William Rae Duncan's intro.
2. Adds the 21 April 1830 by-election at which Magnus Burns, merchant (Bayanne I122332),
   replaced the late Alexander Cumming Irvine. Creates Magnus Burns.
"""

import json
import sqlite3

DB_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'

LTC_COUNCILLORS = 'Lerwick Town Councillors'


def get_person(c, slug):
    c.execute("SELECT * FROM people WHERE slug = ?", (slug,))
    row = c.fetchone()
    if row is None:
        raise SystemExit(f"people.{slug} not found — run parse_wiki.py first")
    return row


def ensure_person(c, **p):
    """Insert a person keyed on bayanne_id. Returns the row id."""
    c.execute("SELECT id, slug FROM people WHERE bayanne_id = ?", (p['bayanne_id'],))
    row = c.fetchone()
    if row:
        if row['slug'] != p['slug']:
            raise SystemExit(f"Bayanne {p['bayanne_id']} already on people.{row['slug']}")
        print(f"  exists: {p['name']} (id {row['id']})")
        return row['id']
    c.execute("SELECT id FROM people WHERE slug = ?", (p['slug'],))
    if c.fetchone():
        raise SystemExit(f"slug {p['slug']} already taken by a different person")
    cols = ', '.join(p.keys())
    marks = ', '.join('?' for _ in p)
    c.execute(f"INSERT INTO people ({cols}) VALUES ({marks})", list(p.values()))
    print(f"  created: {p['name']} (id {c.lastrowid})")
    return c.lastrowid


def set_categories(c, person_id, categories):
    c.execute("UPDATE people SET categories = ? WHERE id = ?",
              (json.dumps(categories), person_id))


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    c.execute("SELECT id FROM councils WHERE name = 'Lerwick Town Council'")
    ltc_id = c.fetchone()['id']

    # ------------------------------------------------------------------
    # 1. Andrew Duncan junior, writer — councillor 1829–1832
    # ------------------------------------------------------------------
    print("=== 1. Andrew Duncan (ii), councillor 1829–1832 ===")
    sheriff = get_person(c, 'andrew-duncan')
    if sheriff['bayanne_id'] != 'I10506':
        raise SystemExit("people.andrew-duncan is no longer the Sheriff (I10506) — check before running")

    junior_id = ensure_person(
        c,
        name='Andrew Duncan (ii)',
        slug='andrew-duncan-ii',
        born_date='1800-10-13',
        died_date='1852-09-01',
        birth_place='Lerwick',
        death_place='Lerwick',
        bayanne_id='I10526',
        born_in_shetland=1,
        died_in_shetland=1,
        categories=json.dumps([LTC_COUNCILLORS, 'Solicitors', '1800 Births', '1852 Deaths', 'Duncan']),
        intro=(
            "Andrew Duncan was a writer in Lerwick and a Lerwick Town Councillor between 1829 and 1832. "
            "He was the eldest son of [person:andrew-duncan:Andrew Duncan], Sheriff Substitute for Shetland, "
            "and a nephew of fellow councillor [person:gilbert-duncan:Gilbert Duncan]. "
            "He was elected to the council on 3 September 1829, styled \"Andrew Duncan Junior, Writer\" in the "
            "minute book to distinguish him from his father, who presided over the same meeting as Sheriff. "
            "He served until the election of 6 September 1832, at which he was not returned. "
            "As a lawyer he represented the plaintiff in Eunson v. Bruce of Symbister, a tenancy dispute that "
            "ran through the late 1840s and early 1850s."
        ),
    )

    # Re-point the 1829 candidacy from the Sheriff to the son.
    c.execute("""
        UPDATE candidacies SET person_id = ?
        WHERE person_id = ? AND candidate_name = 'Andrew Duncan'
          AND election_id IN (SELECT id FROM elections
                              WHERE council_id = ? AND election_date = '1829-09-03')
    """, (junior_id, sheriff['id'], ltc_id))
    print(f"  1829 candidacy re-pointed: {c.rowcount} row(s)")

    c.execute("SELECT COUNT(*) FROM candidacies WHERE person_id = ?", (sheriff['id'],))
    remaining = c.fetchone()[0]
    if remaining:
        print(f"  WARNING: Sheriff still has {remaining} candidacies — not touching his categories")
    else:
        cats = json.loads(sheriff['categories'] or '[]')
        if LTC_COUNCILLORS in cats:
            cats.remove(LTC_COUNCILLORS)
            set_categories(c, sheriff['id'], cats)
            print("  Sheriff: removed 'Lerwick Town Councillors' category")

    # Link the son from the Sheriff's intro (the text already names him).
    c.execute("""
        UPDATE people SET intro = REPLACE(intro,
            'also named Andrew Duncan, was a lawyer',
            'also named [person:andrew-duncan-ii:Andrew Duncan], was a lawyer and a Lerwick Town Councillor')
        WHERE id = ? AND intro LIKE '%also named Andrew Duncan, was a lawyer%'
    """, (sheriff['id'],))
    if c.rowcount:
        print("  Sheriff: linked son in intro")

    # William Rae Duncan (ii): grandfather Andrew was the Sheriff (Bayanne I10506 → William Rae
    # Duncan b.1805 → I10539), who was never a councillor. The councillor was his uncle.
    old = 'His grandfather [person:andrew-duncan:Andrew] was a Lerwick Town Councillor and Sheriff-Substitute.'
    new = ('His grandfather [person:andrew-duncan:Andrew] was Sheriff-Substitute for Shetland, and his uncle '
           '[person:andrew-duncan-ii:Andrew] a Lerwick Town Councillor.')
    c.execute("""
        UPDATE people SET intro = REPLACE(intro, ?, ?)
        WHERE slug = 'william-duncan-ii' AND intro LIKE '%' || ? || '%'
    """, (old, new, old))
    if c.rowcount:
        print("  William Duncan (ii): reworded grandfather sentence")

    # ------------------------------------------------------------------
    # 2. April 1830 by-election: Magnus Burns replaces the late Alexander Irvine
    # ------------------------------------------------------------------
    print("\n=== 2. April 1830 by-election ===")
    irvine = get_person(c, 'alexander-irvine')
    david_burns = get_person(c, 'david-burns')

    burns_id = ensure_person(
        c,
        name='Magnus Burns',
        slug='magnus-burns',
        born_date='1757',
        died_date='1846-01-09',
        birth_place='Cruicikirk, Unst',
        death_place='Lerwick',
        bayanne_id='I122332',
        born_in_shetland=1,
        died_in_shetland=1,
        categories=json.dumps([LTC_COUNCILLORS, '1757 Births', '1846 Deaths']),
        intro=(
            "Magnus Burns was a merchant in Lerwick and a Lerwick Town Councillor between 1830 and 1832. "
            "He was elected at a by-election on 21 April 1830 to fill the vacancy caused by the death of "
            "[person:alexander-irvine:Alexander Irvine], and was not returned at the election of "
            "6 September 1832. Pressed into the Royal Navy in 1793, he lost an arm at Lord Howe's victory "
            "of 1 June 1794 before settling in Lerwick as a merchant and property owner; Burns Lane is named "
            "after the houses he built there. He was the father of Lerwick Town Councillor "
            f"[person:{david_burns['slug']}:David Nisbet Burns]."
        ),
    )

    wiki_title = 'Lerwick Town Council By-Election April 1830'
    c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (wiki_title,))
    row = c.fetchone()
    if row:
        election_id = row['id']
        print(f"  by-election exists (id {election_id})")
    else:
        c.execute("""
            INSERT INTO elections (council_id, election_date, election_type, wiki_page_title,
                                   replaced_person, replaced_person_id, notes)
            VALUES (?, '1830-04-21', 'by-election', ?, ?, ?, ?)
        """, (ltc_id, wiki_title, irvine['name'], irvine['id'],
              "Held to fill the vacancy caused by the death of Alexander Cumming Irvine on 3 March 1830. "
              "Not recorded on the wiki; taken from the Lerwick Town Council minute book, pp. 53–54 "
              "(meeting called 6 April 1830, election 21 April 1830)."))
        election_id = c.lastrowid
        print(f"  by-election created (id {election_id})")

    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = 'Magnus Burns'",
              (election_id,))
    if c.fetchone():
        print("  candidacy exists")
    else:
        c.execute("""
            INSERT INTO candidacies (election_id, person_id, candidate_name, elected, position, role)
            VALUES (?, ?, 'Magnus Burns', 1, 1, 'councillor')
        """, (election_id, burns_id))
        print("  candidacy created")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
