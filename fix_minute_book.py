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
3. Un-hides the 3 May 1844 by-election (parse_wiki.py hides it as "not actually a
   by-election") and corrects it: the burgesses elected Joseph Leask Junior Bailie in place
   of the late Gilbert Duncan, not Charles Duncan (his son, never a Bailie by 1844).
4. Corrects two general election dates: September 1826 was held on the 7th (the 6th was
   an ordinary council meeting the evening before) and September 1844 on the 5th (the 2nd
   is the date of Bailie Leask's notice calling the meeting).
5. The Junior Bailie in Duncan's 3 September 1874 election was John Robertson Senior, not
   his nephew. The candidacy is named "John Robertson Snr" but was linked to John Robertson (ii).
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

    # ------------------------------------------------------------------
    # 3. May 1844 by-election: Joseph Leask elected Junior Bailie in place of Gilbert Duncan
    # ------------------------------------------------------------------
    print("\n=== 3. May 1844 by-election ===")
    gilbert = get_person(c, 'gilbert-duncan')
    if gilbert['died_date'] != '1844-02-19':
        raise SystemExit(f"people.gilbert-duncan died_date is {gilbert['died_date']}, expected 1844-02-19")
    leask = get_person(c, 'joseph-leask')

    wiki_title = 'Lerwick Town Council By-Election May 1844'
    c.execute("SELECT id FROM elections WHERE wiki_page_title = ? AND election_date = '1844-05-03'",
              (wiki_title,))
    row = c.fetchone()
    if row is None:
        raise SystemExit(f"elections '{wiki_title}' not found — run parse_wiki.py first")
    election_id = row['id']

    c.execute("""
        UPDATE elections
        SET hidden = 0, replaced_person = ?, replaced_person_id = ?, notes = ?
        WHERE id = ?
    """, (gilbert['name'], gilbert['id'],
          "Held to fill the office of Junior Bailie after the death of Bailie Gilbert Duncan on "
          "19 February 1844. Joseph Leask, already a councillor since 1835, was proposed by Andrew "
          "Duncan junior, seconded by Mr Heddell, Comptroller of Customs, and elected unanimously by "
          "the twenty-one burgesses present. The wiki recorded the vacancy as Charles Duncan's; the "
          "Lerwick Town Council minute book, pp. 101–102, is explicit that it was Gilbert's (notice "
          "20 April 1844 by Bailie Charles Ogilvy; election 3 May 1844 in the Sheriff Court Room, "
          "Fort Charlotte).",
          election_id))
    print(f"  election {election_id}: shown, replaced_person = Gilbert Duncan")

    c.execute("""
        UPDATE candidacies
        SET person_id = ?, elected = 1, role = 'Junior Bailie', votes_text = 'Elected as Junior Bailie'
        WHERE election_id = ? AND candidate_name = 'Joseph Leask'
    """, (leask['id'], election_id))
    if c.rowcount != 1:
        raise SystemExit(f"expected 1 Joseph Leask candidacy on election {election_id}, updated {c.rowcount}")
    print("  candidacy: Elected as Junior Bailie")

    # ------------------------------------------------------------------
    # 4. Election dates: Sept 1826 was the 7th, Sept 1844 the 5th
    # ------------------------------------------------------------------
    print("\n=== 4. Election dates ===")
    for wiki_title, wrong, right in [
        ('Lerwick Town Council Election September 1826', '1826-09-06', '1826-09-07'),
        ('Lerwick Town Council Election September 1844', '1844-09-02', '1844-09-05'),
    ]:
        c.execute("SELECT id, election_date FROM elections WHERE wiki_page_title = ? AND council_id = ?",
                  (wiki_title, ltc_id))
        rows = c.fetchall()
        if len(rows) != 1:
            raise SystemExit(f"expected 1 election for '{wiki_title}', found {len(rows)}")
        row = rows[0]
        if row['election_date'] == right:
            print(f"  {wiki_title}: already {right}")
        elif row['election_date'] == wrong:
            c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (right, row['id']))
            print(f"  {wiki_title} (id {row['id']}): {wrong} -> {right}")
        else:
            raise SystemExit(f"'{wiki_title}' has unexpected date {row['election_date']}")

    # ------------------------------------------------------------------
    # 5. Sept 1874: Duncan's Junior Bailie was John Robertson Senior
    # ------------------------------------------------------------------
    print("\n=== 5. Sept 1874 Junior Bailie ===")
    senior = get_person(c, 'john-robertson-i')
    nephew = get_person(c, 'john-robertson-ii')
    c.execute("""
        UPDATE candidacies SET person_id = ?
        WHERE person_id = ? AND candidate_name = 'John Robertson Snr' AND role = 'Junior Bailie'
          AND election_id IN (SELECT id FROM elections
                              WHERE council_id = ? AND election_date = '1874-09-03')
    """, (senior['id'], nephew['id'], ltc_id))
    if c.rowcount:
        print(f"  1874 Junior Bailie re-pointed to {senior['name']}: {c.rowcount} row(s)")
    else:
        c.execute("""
            SELECT COUNT(*) FROM candidacies
            WHERE person_id = ? AND candidate_name = 'John Robertson Snr'
              AND election_id IN (SELECT id FROM elections
                                  WHERE council_id = ? AND election_date = '1874-09-03')
        """, (senior['id'], ltc_id))
        if c.fetchone()[0] != 1:
            raise SystemExit("1874 'John Robertson Snr' candidacy not found on either Robertson")
        print(f"  1874 Junior Bailie: already {senior['name']}")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
