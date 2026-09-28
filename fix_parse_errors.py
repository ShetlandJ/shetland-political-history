#!/usr/bin/env python3
"""
Repair things parse_wiki.py got wrong when it read the wiki text. Checked against the wiki source
(shetland_history2, mwfn_) on 2026-09-25. Idempotent.

1. Category trailers. Some pages ended their text with a template close and category links
   ("}} Category: Members of the Parliament of Great Britain ..."), which were kept in the
   intro/biography. The categories are already in people.categories, so the trailer is dropped.
2. Footnote tags. The Shetland sheepdog passage on Thomas Loggie's page kept its <ref> as escaped
   text; the citation is kept, in brackets.
3. Intros that lost the person's name. On pages opening with a disambiguation line ("For other
   people with the same name, please see [[James Anderson]]"), stripping that line also took the
   bold name and the "(b. " that followed it, leaving the intro starting mid-date. The name is
   restored from the page's bold text and the birth/death parenthetical dropped, as on every
   other intro.
4. Charles Charleson, Northmavine South, May 1949: the wiki row is
   "[[Charles Charleson]] || Labout || Unopposed", but the party landed in votes_text.
5. County Council by-elections linked to the wrong namesake. The parser matched the replaced
   member's name to a person with the same name from another era (a 1914-born GP for the
   Nesting author who died in 1920). The build's fallback by name hid most of these. The wiki
   text of each page names who went, and where it gives the day the date is set from it too.
   - Burra, Feb 1898: "Reverend David Gray resigned", 3 February. David Gray (i), not (ii).
   - Dunrossness North, Aug 1898: "the death of councillor Robert Henderson". Robert
     Henderson (ii), not (iii). The date is left alone: the wiki says "Thursday 1 August", but
     1 August 1898 was a Monday.
   - Nesting, Nov 1920: the death of James Hunter, 18 November; "his brother Robert was
     appointed". The wiki links James Hunter (iv) and Robert Hunter (i), but James Hunter (iii)'s
     page has the death and the vacancy "filled by his brother Robert Hunter (ii)", and Robert
     Hunter (ii)'s page has the co-option.
   - Aithsting, Feb 1921: 17 February (Thursday). The link was already right.
   - Aithsting, May 1932: "the resignation of Captain James Anderson", 17 May. James Anderson
     (ii), the master mariner, not (iv).
   - Gulberwick, Mar 1945: "the resignation of Reverend George Smith", 20 March. George Smith
     (ii), not (iii).
   - Gulberwick, May 1951: 15 May. The link was already right.
6. Robert Hunter (ii)'s intro: "the death of his brother, [[James Hunter (iv)|James]]". The
   brother who died in 1920 is James Hunter (iii), the Nesting author, whose page names Robert as
   his successor. James Hunter (iv) is a GP born in 1914. The wiki page has the wrong link.
7. County Council Election December 1919, Delting North: the wiki row is
   "[[Joseph Peterson (i)|Joseph Peterson]] || 15 || [[Image:cross.gif]]" (James Hay won with 29),
   but the baseline has Peterson elected there as well as in Delting South. His page: "County
   Councillor for Delting South between 1919 and 1922".
8. County Council Election December 1922: "This election recombined the Aithsting & Sandsting
   parishes", with one result under "===Aithsting & Sandsting===" (Leslie 110, Clark 89). The parser
   made a row for each ward with the same result, giving John Leslie (ii) two seats. The Sandsting
   row is hidden and the Aithsting row shows the combined name.
"""

import os
import re
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

CATEGORY_TRAILER = re.compile(r'\s*(?:\}\}\s*)?Category:.*\Z', re.S)

# (people.id, garbled start of intro up to and including ") ", the bold name from the wiki page)
LOST_NAMES = [
    (205, '29 December 1829, Kergord, Weisdale, d. 26 February 1907, Aithsting) ', 'James Anderson '),
    (206, '27 May 1858, Kergord, Weisdale, d. 25 April 1934, Bixter) ', 'Captain James Anderson '),
    (207, '27 December 1877, Nesting, d. 26 February 1966, Nesting) ', 'James Anderson '),
    (208, '29 January 1910, Voe, Delting, d. 14 March 1994, Stapness, Walls) ', 'James Anderson '),
    (460, '3 November 1855, Hillswick, d. 19 December 1936, Pahiatua, New Zealand) ', 'Thomas Anderson '),
    (461, '21 January 1861, Bressay, d. 17 April 1938, Lerwick) ', 'Thomas James Anderson '),
    (462, '19 June 1866, Kergord, Weisdale, d. 17 December 1920, Lerwick) ', 'Thomas Angus Anderson '),
]

# (wiki title, replaced person_id wrong -> right, date wrong -> right); None where unchanged
WRONG_NAMESAKE = [
    ('Burra County Council By-Election February 1898', (112, 111), ('1898-02-01', '1898-02-03')),
    ('Dunrossness North County Council By-Election August 1898', (425, 424), None),
    ('Nesting County Council By-Election November 1920', (231, 230), ('1920-11-01', '1920-11-18')),
    ('Aithsting County Council By-Election February 1921', None, ('1921-02-01', '1921-02-17')),
    ('Aithsting County Council By-Election May 1932', (208, 206), ('1932-05-01', '1932-05-17')),
    ('Gulberwick County Council By-Election March 1945', (168, 167), ('1945-03-01', '1945-03-20')),
    ('Gulberwick County Council By-Election May 1951', None, ('1951-05-01', '1951-05-15')),
]
# Nesting 1920 winner: Robert Hunter (ii) (430), not the Lerwick bank agent Robert Hunter (i) (429)
HUNTER_1920 = ('Nesting County Council By-Election November 1920', 'Robert Hunter', 429, 430)

PETERSON_1919 = ('County_Council_Election_December_1919', 'Delting North', 'Joseph Peterson')
COMBINED_1922 = ('County Council Election December 1922', 'Aithsting', 'Sandsting', 'Aithsting & Sandsting')

HUNTER_BROTHER = (430, '[person:james-hunter-iv:James]', '[person:james-hunter-iii:James]')

REF_OLD = ('larger dogs.&lt;ref&gt;Beryl Thynne, The Shetland Sheepdog, The Illustrated Kennel News, '
           'London, 1916 (first monograph of the breed).&lt;/ref&gt; ')
REF_NEW = ('larger dogs (Beryl Thynne, The Shetland Sheepdog, The Illustrated Kennel News, '
           'London, 1916, the first monograph of the breed). ')


def main():
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()

    n = 0
    for col in ('intro', 'biography'):
        for pid, text in c.execute(f"SELECT id, {col} FROM people WHERE {col} LIKE '%Category:%'").fetchall():
            new = CATEGORY_TRAILER.sub('', text)
            if new != text:
                c.execute(f"UPDATE people SET {col} = ? WHERE id = ?", (new, pid))
                n += 1
    print(f"Category trailers removed: {n}")

    row = c.execute("SELECT id, intro FROM people WHERE instr(intro, ?) > 0", (REF_OLD,)).fetchall()
    if len(row) > 1:
        raise SystemExit("ref passage found on more than one page")
    for pid, text in row:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (text.replace(REF_OLD, REF_NEW), pid))
        print(f"people#{pid}: footnote tag replaced")
    left = c.execute("SELECT count(*) FROM people WHERE intro LIKE '%&lt;ref%' OR biography LIKE '%&lt;ref%'").fetchone()[0]
    if left:
        raise SystemExit(f"{left} pages still have &lt;ref&gt; tags")

    for pid, old, name in LOST_NAMES:
        text = c.execute("SELECT intro FROM people WHERE id = ?", (pid,)).fetchone()[0]
        if text.startswith(old):
            c.execute("UPDATE people SET intro = ? WHERE id = ?", (name + text[len(old):], pid))
            print(f"people#{pid}: intro name restored ({name.strip()})")
        elif not text.startswith(name):
            raise SystemExit(f"people#{pid}: unexpected intro start {text[:60]!r}")

    c.execute("SELECT c.id, c.party, c.votes_text FROM candidacies c JOIN elections e ON e.id = c.election_id "
              "WHERE e.wiki_page_title = 'County Council Election May 1949' AND c.candidate_name = 'Charles Charleson'")
    cid, party, votes_text = c.fetchone()
    if (party, votes_text) == (None, 'Labout'):
        c.execute("UPDATE candidacies SET party = 'Labour', votes_text = 'Unopposed' WHERE id = ?", (cid,))
        print(f"candidacy {cid}: Charleson 1949 party = Labour, Unopposed")
    elif (party, votes_text) != ('Labour', 'Unopposed'):
        raise SystemExit(f"candidacy {cid}: unexpected party/votes_text {party!r}/{votes_text!r}")

    for title, rp, dates in WRONG_NAMESAKE:
        eid, rp_id, date = c.execute("SELECT id, replaced_person_id, election_date FROM elections "
                                     "WHERE wiki_page_title = ?", (title,)).fetchone()
        if rp:
            if rp_id == rp[0]:
                c.execute("UPDATE elections SET replaced_person_id = ? WHERE id = ?", (rp[1], eid))
                print(f"election {eid}: replaced_person_id {rp[0]} -> {rp[1]}")
            elif rp_id != rp[1]:
                raise SystemExit(f"election {eid}: unexpected replaced_person_id {rp_id}")
        if dates:
            if date == dates[0]:
                c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (dates[1], eid))
                print(f"election {eid}: date {dates[0]} -> {dates[1]}")
            elif date != dates[1]:
                raise SystemExit(f"election {eid}: unexpected date {date}")

    title, name, wrong, right = HUNTER_1920
    cid, pid = c.execute("SELECT c.id, c.person_id FROM candidacies c JOIN elections e ON e.id = c.election_id "
                         "WHERE e.wiki_page_title = ? AND c.candidate_name = ?", (title, name)).fetchone()
    if pid == wrong:
        c.execute("UPDATE candidacies SET person_id = ? WHERE id = ?", (right, cid))
        print(f"candidacy {cid}: Robert Hunter 1920 -> person {right}")
    elif pid != right:
        raise SystemExit(f"candidacy {cid}: unexpected person_id {pid}")

    pid, old, new = HUNTER_BROTHER
    intro = c.execute("SELECT intro FROM people WHERE id = ?", (pid,)).fetchone()[0]
    if intro.count(old) == 1:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (intro.replace(old, new), pid))
        print(f"people#{pid}: brother link -> james-hunter-iii")
    elif new not in intro:
        raise SystemExit(f"people#{pid}: brother link not found")

    title, ward, name = PETERSON_1919
    cid, elected = c.execute("""SELECT ca.id, ca.elected FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                                JOIN constituencies k ON k.id = e.constituency_id
                                WHERE e.wiki_page_title = ? AND k.name = ? AND ca.candidate_name = ?""",
                             (title, ward, name)).fetchone()
    if elected == 1:
        c.execute("UPDATE candidacies SET elected = 0 WHERE id = ?", (cid,))
        print(f"candidacy {cid}: Peterson, Delting North 1919, not elected")

    title, keep, drop, combined = COMBINED_1922
    rows = {k: (eid, hidden, disp) for eid, k, hidden, disp in c.execute(
        """SELECT e.id, k.name, e.hidden, e.constituency_display_name FROM elections e
           JOIN constituencies k ON k.id = e.constituency_id
           WHERE e.wiki_page_title = ? AND k.name IN (?, ?)""", (title, keep, drop))}
    results = [c.execute("SELECT candidate_name, votes, elected FROM candidacies WHERE election_id = ? ORDER BY position",
                         (rows[k][0],)).fetchall() for k in (keep, drop)]
    if results[0] != results[1]:
        raise SystemExit(f"{title}: {keep} and {drop} results differ, not a duplicate")
    if not rows[drop][1]:
        c.execute("UPDATE elections SET hidden = 1 WHERE id = ?", (rows[drop][0],))
        print(f"election {rows[drop][0]}: duplicate {drop} 1922 row hidden")
    if rows[keep][2] != combined:
        if rows[keep][2]:
            raise SystemExit(f"election {rows[keep][0]}: unexpected display name {rows[keep][2]!r}")
        c.execute("UPDATE elections SET constituency_display_name = ? WHERE id = ?", (combined, rows[keep][0]))
        print(f"election {rows[keep][0]}: shown as {combined}")

    db.commit()
    db.close()


if __name__ == '__main__':
    main()
