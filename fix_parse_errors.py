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

    db.commit()
    db.close()


if __name__ == '__main__':
    main()
