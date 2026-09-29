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
   - Gulberwick, May 1951: the wiki says 15 May, but it was a poll on Tue 8 May (Shetland Times,
     11 May 1951; fix_newspapers.py #51). The link was already right.
6. Robert Hunter (ii)'s intro: "the death of his brother, [[James Hunter (iv)|James]]". The
   brother who died in 1920 is James Hunter (iii), the Nesting author, whose page names Robert as
   his successor. James Hunter (iv) is a GP born in 1914. The wiki page has the wrong link.
7. County Council Election December 1919, Delting North: the wiki row is
   "[[Joseph Peterson (i)|Joseph Peterson]] || 15 || [[Image:cross.gif]]" (James Hay won with 29),
   but the baseline has Peterson elected there as well as in Delting South. His page: "County
   Councillor for Delting South between 1919 and 1922".
8. County Council Election December 1922: the wiki says "This election recombined the Aithsting &
   Sandsting parishes", with one result under "===Aithsting & Sandsting===" (Leslie 110, Clark 89),
   and the parser made a row for each ward with that result, giving John Leslie (ii) two seats. The
   wards were not combined. The nominations list "Aithsting—Mr John Leslie ... and Mr Andrew D.
   Clark" and, separately, "Sandsting—Mr Robert A. Sutherland, Sand" (Shetland Times, 25 Nov 1922),
   and R. A. Sutherland sat at the new Council's first meeting on Thursday 21 Dec (ST 30 Dec 1922).
   The Aithsting row keeps the Leslie-Clark result; the Sandsting row becomes Sutherland,
   unopposed, and his intro's 1922 becomes 1925 (Bowie won Sandsting in Dec 1925).
   Evidence: research/bna/zcc-1920-1939.md.
9. Burra County Council By-Election April 1920: "took place on 15 April. In 1919, William Sinclair
   was elected to both Burra and to Whiteness and Weisdale, and as he chose to represent the latter
   George Anderson was appointed." The baseline has 1 April (month only) and Sinclair as the member
   replaced. He never took the Burra seat (listed in data/not_seated.csv), so the replaced member
   is the marker [double return], and the seat is vacant until Anderson.
10. Burra County Council By-Election February 1898: the wiki's table has a "Council decision"
   column, and its vote cells are "One petition of 59<br>Second petition of 35" (Henderson,
   appointed) and "Petition of 113" (Lennie, rejected). The parser kept the first number of each
   as votes. There was no poll (Shetland Times, 5 Feb 1898). Votes cleared; the text kept.
11. Delting North County Council By-Election May 1890: "took place on May 22"; the baseline has
   1 May (month only). Inkster was appointed at the Council's first meeting, reported in the
   Shetland Times of 24 May 1890.
12. ZCC by-elections 1914-1919 filled by the Council: the wiki tables have "Local petition" and
   "Council votes" columns, or "16 (council votes)", "29 (petition of rate payers)"; the parser
   kept the first number as votes. None was a poll. Votes cleared; the wiki's text kept. The
   figures agree with the Shetland Times reports (ST 26 Dec 1914, 28 Aug 1915, 21 Jul 1917,
   1 Mar 1919; Whiteness 1914's report is unreadable in the OCR).
13. Delting North, December 1919 (goes further than #7): Joseph Peterson withdrew, "leaving Mr Jas.
   Hay unopposed" (Shetland Times, 22 Nov 1919). The wiki's "Hay 29, Peterson 15" looks borrowed
   from 1913 (Hay 29, Smith 26, Henderson 15). Peterson's row is removed and Hay's votes become
   "Unopposed". Evidence: research/bna/zcc-1900-1919.md.
14. ZCC by-elections 1921-1937 filled by the Council, as in #12: the wiki gives "8 council votes
   (petition of 88)" and the like, and the parser kept the first number as votes. None was a poll.
   Votes cleared; the wiki's text kept, tidied to the #12 form. Checked against the Shetland Times
   where the report was read (research/bna/zcc-1920-1939.md).
15. By-election days the parser dropped, from the wiki's first sentence, where the weekday fits
   the Council's meeting day and the Shetland Times report wasn't found or couldn't be read:
   - Lerwick Central, May 1921: "took place on Thursday 19 May". J. T. J. Sinclair for J. J.
     Pottinger, resigned (the report is ST 28 May 1921, not read in this run).
   - Gulberwick, December 1921: "took place on 15 December" (a Thursday). Goodlad for Samuel
     Fordyce, whose death was announced at the 20 Oct 1921 meeting.
   - Dunrossness South, April 1937: "took place on 20 April" (a Tuesday), after W. L. McDougall's
     death. The ST 24 Apr 1937 report is garbled in the OCR.
   - Burra, July 1959: "took place on 21 July" (a Tuesday). Alexander Bennet for Robert Strachan;
     three searches of the ST for Jul-Aug 1959 found no report (data/searches.csv).
   Evidence: research/bna/zcc-1920-1939.md, research/bna/zcc-1940-1959.md.
16. ZCC by-elections 1940-1942 filled by the Council, as in #12 and #14: the wiki gives "18 council
   votes" and "11 council votes (petition of 55)", and the parser kept the first number as votes.
   Cunningsburgh 1940's petitions (86 and 72) are from the Shetland Times, 20 Apr 1940; Yell South
   1940 and Unst South 1942 as in the wiki (ST 14 Dec 1940, 26 Dec 1942 confirm the co-options).
   Evidence: research/bna/zcc-1940-1959.md.
17. Orkney and Shetland by-election, 1902: the navigation template links the redirect "1902 Orkney
   and Shetland by-election", so the parser found no results table and the baseline has the
   election hidden, with no candidates. The page it redirects to has them: "contested on 18-19
   November 1902", Wason (Independent Liberal) 2,412, Thomas McKinnon Wood (Liberal) 2,001,
   Theodore Angier (Liberal Unionist) 740; turnout 5,153. The Shetland Times confirms the polling
   days (22 Nov 1902) and Wason's and Wood's votes (29 Nov 1902); Angier's figure is illegible
   there. Evidence: research/bna/westminster-1873-1974.md.
18. Westminster polling days the parser dropped, from the wiki's first sentence: the 1873
   by-election "was contested on 6-7 January 1873" (the Shetland Times of 30 Dec 1872 has the
   poll in "the first week of January"); 1922 "15 November 1922"; 1923 "6 December 1923".
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
    ('Gulberwick County Council By-Election May 1951', None, ('1951-05-01', '1951-05-08')),  # poll day, fix_newspapers.py #51
]
# Nesting 1920 winner: Robert Hunter (ii) (430), not the Lerwick bank agent Robert Hunter (i) (429)
HUNTER_1920 = ('Nesting County Council By-Election November 1920', 'Robert Hunter', 429, 430)

PETERSON_1919 = ('County_Council_Election_December_1919', 'Delting North', 'Joseph Peterson')
BURRA_1920 = ('Burra County Council By-Election April 1920', ('1920-04-01', '1920-04-15'),
              (('William Sinclair', 522), ('[double return]', None)))
BURRA_1898_PETITIONS = ('Burra County Council By-Election February 1898', [
    ('Henry Henderson', 59, 'Petitions of 59 and 35; appointed'),
    ('Charles Lennie', 113, 'Petition of 113; rejected'),
])
COUNCIL_APPOINTMENTS_1914_1919 = [  # (wiki title, candidate, wrong votes, text)
    ('Whiteness And Weisdale County Council By-Election June 1914', 'John Henderson', 22, 'Petition of 22; 10 Council votes'),
    ('Whiteness And Weisdale County Council By-Election June 1914', 'William Sinclair', 67, 'Petition of 67; 7 Council votes'),
    ('Nesting County Council By-Election December 1914', 'John Pearson', 16, '16 Council votes'),
    ('Nesting County Council By-Election December 1914', 'H. E. Denis De Vitre', 3, '3 Council votes'),
    ('Aithsting County Council By-Election August 1915', 'Thomas Anderson', 12, '12 Council votes'),
    ('Aithsting County Council By-Election August 1915', 'Dr. James C. Bowie', 4, '4 Council votes'),
    ('Fetlar County Council By-Election July 1917', 'Sir Arthur J. Nicolson', 29, 'Petition of 29 ratepayers'),
    ('Cunningsburgh_County_Council_By-Election_February_1919', 'Laurence Anderson', 5, 'Petitions of 18; 5 Council votes'),
    ('Cunningsburgh_County_Council_By-Election_February_1919', 'James Laing', 3, 'Petition of 70; 3 Council votes'),
]
COUNCIL_APPOINTMENTS_1921_1937 = [  # (wiki title, candidate, wrong votes, text)
    ('Aithsting County Council By-Election February 1921', 'Andrew Clark', 8, 'Petition of 88; 8 Council votes'),
    ('Aithsting County Council By-Election February 1921', 'John Leslie', 4, 'Petition of 120; 4 Council votes'),
    ('Lerwick Central County Council By-Election May 1921', 'John T. J. Sinclair', 8, '8 Council votes'),
    ('Lerwick Central County Council By-Election May 1921', 'George Duffin', 3, '3 Council votes'),
    ('Northmavine_South_County_Council_By-Election_February_1924', 'James Hay', 10, 'Petition of 57; 10 Council votes'),
    ('Northmavine_South_County_Council_By-Election_February_1924', 'Arthur White', 8, 'Petition of 165; 8 Council votes'),
    ('Gulberwick County Council By-Election June 1924', 'Thomas J. Anderson', 8, 'Petition of 54; 8 Council votes'),
    ('Gulberwick County Council By-Election June 1924', 'Robert Nicolson', 2, 'Petition of 16; 2 Council votes'),
    ('Cunningsburgh County Council By-Election February 1927', 'William Sinclair', 13, 'Petition of 61; 13 Council votes'),
    ('Cunningsburgh County Council By-Election February 1927', 'James Hay', 3, 'Petition of 16; 3 Council votes'),
    ('Sandsting County Council By-Election September 1930', 'James Robert White', 14, 'Petition of 89; 14 Council votes'),
    ('Sandsting County Council By-Election September 1930', 'Lewis Garriock', 7, 'Petition of 164; 7 Council votes'),
    ('Yell North County Council By-Election August 1932', 'Charles Nicolson', 77, 'Petition of 77'),
    ('Bressay County Council By-Election September 1933', 'James A. Smith', 19, 'Petition of 54; 19 Council votes'),
    ('Bressay County Council By-Election September 1933', 'Norman Cameron', 9, 'Petition of 52; 9 Council votes'),
    ('Whalsay And Skerries County Council By-Election February 1937', 'James Hay', 16, 'Petition of 79; 16 Council votes'),
    ('Whalsay And Skerries County Council By-Election February 1937', 'Robert Ollason', 5, 'Petition of 142; 5 Council votes'),
    ('Fetlar County Council By-Election October 1937', 'John A. Campbell', 18, 'Petition of 71; 18 Council votes'),
    ('Fetlar County Council By-Election October 1937', 'Magnus Manson', 4, 'Petition of 42; 4 Council votes'),
]
COUNCIL_APPOINTMENTS_1940_1942 = [  # (wiki title, candidate, wrong votes, text)
    ('Cunningsburgh County Council By-Election April 1940', 'Laurence Laurenson', 18, 'Petition of 86; 18 Council votes'),
    ('Cunningsburgh County Council By-Election April 1940', 'Magnus Manson', 8, 'Petition of 72; 8 Council votes'),
    ('Yell South County Council By-Election December 1940', 'John Williamson', 14, '14 Council votes'),
    ('Yell South County Council By-Election December 1940', 'William Leask', 2, '2 Council votes'),
    ('Unst South County Council By-Election December 1942', 'Captain Henry Hunter', 11, 'Petition of 55; 11 Council votes'),
    ('Unst South County Council By-Election December 1942', 'John Sutherland', 6, 'Petition of 19; 6 Council votes'),
]
WIKI_BY_ELECTION_DAYS = [
    ('Lerwick Central County Council By-Election May 1921', '1921-05-01', '1921-05-19'),
    ('Gulberwick County Council By-Election December 1921', '1921-12-01', '1921-12-15'),
    ('Dunrossness South County Council By-Election April 1937', '1937-04-01', '1937-04-20'),
    ('Burra County Council By-Election July 1959', '1959-07-01', '1959-07-21'),
]
BY_ELECTION_1902 = ('1902 Orkney and Shetland by-election', ('1902-01-01', '1902-11-18'), 5153, [
    # (candidate_name, person slug, party, votes, elected)
    ('John Cathcart Wason', 'cathcart-wason', 'Independent Liberal', 2412, 1),
    ('Thomas McKinnon Wood', None, 'Liberal', 2001, 0),
    ('Theodore Angier', None, 'Liberal Unionist', 740, 0),
])
WIKI_WESTMINSTER_DAYS = [
    ('Orkney and Shetland by-election, 1873', '1873-01-01', '1873-01-06'),
    ('1922 UK General Election, Orkney and Shetland Result', '1922-01-01', '1922-11-15'),
    ('1923 UK General Election, Orkney and Shetland Result', '1923-01-01', '1923-12-06'),
]
DELTING_NORTH_1890 =('Delting North County Council By-Election May 1890', '1890-05-01', '1890-05-22')
SANDSTING_1922 = ('County Council Election December 1922', 'Aithsting', 'Sandsting', 'Aithsting & Sandsting',
                  ('robert-sutherland', 'Robert A. Sutherland'))
SUTHERLAND_INTRO = ('robert-sutherland', 'Sandsting between 1919 and 1922', 'Sandsting between 1919 and 1925')

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
    # 13: he withdrew, so Hay was unopposed and there were no votes
    c.execute("DELETE FROM candidacies WHERE id = ? AND elected = 0 AND votes = 15", (cid,))
    if c.rowcount:
        print(f"candidacy {cid}: Peterson, Delting North 1919, withdrew: removed")
    hay, votes, votes_text = c.execute("""SELECT ca.id, ca.votes, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                                JOIN constituencies k ON k.id = e.constituency_id
                                WHERE e.wiki_page_title = ? AND k.name = ? AND ca.candidate_name = 'James Hay'""",
                             (title, ward)).fetchone()
    if (votes, votes_text) == (29, None):
        c.execute("UPDATE candidacies SET votes = NULL, votes_text = 'Unopposed' WHERE id = ?", (hay,))
        print(f"candidacy {hay}: Hay, Delting North 1919: 29 votes -> Unopposed")
    elif (votes, votes_text) != (None, 'Unopposed'):
        raise SystemExit(f"candidacy {hay}: unexpected votes {votes!r}/{votes_text!r}")

    title, aith, sand, combined, (slug, name) = SANDSTING_1922
    rows = {k: (eid, hidden, disp) for eid, k, hidden, disp in c.execute(
        """SELECT e.id, k.name, e.hidden, e.constituency_display_name FROM elections e
           JOIN constituencies k ON k.id = e.constituency_id
           WHERE e.wiki_page_title = ? AND k.name IN (?, ?)""", (title, aith, sand))}
    if rows[aith][2] == combined:
        c.execute("UPDATE elections SET constituency_display_name = NULL WHERE id = ?", (rows[aith][0],))
        print(f"election {rows[aith][0]}: no longer shown as {combined}")
    elif rows[aith][2]:
        raise SystemExit(f"election {rows[aith][0]}: unexpected display name {rows[aith][2]!r}")
    if rows[sand][1]:
        raise SystemExit(f"election {rows[sand][0]}: {sand} 1922 row is hidden")
    sid = rows[sand][0]
    pid = c.execute("SELECT id FROM people WHERE slug = ?", (slug,)).fetchone()[0]
    results = [c.execute("SELECT candidate_name, votes, elected FROM candidacies WHERE election_id = ? ORDER BY position",
                         (rows[k][0],)).fetchall() for k in (aith, sand)]
    got = c.execute("SELECT candidate_name, person_id, votes_text, elected FROM candidacies WHERE election_id = ?",
                    (sid,)).fetchall()
    if got == [(name, pid, 'Unopposed', 1)]:
        print(f"election {sid}: already {name} unopposed")
    elif results[0] == results[1]:
        c.execute("DELETE FROM candidacies WHERE election_id = ?", (sid,))
        c.execute("""INSERT INTO candidacies (election_id, person_id, candidate_name, votes_text, elected, position)
                     VALUES (?, ?, ?, 'Unopposed', 1, 1)""", (sid, pid, name))
        print(f"election {sid}: copied Aithsting result replaced by {name}, unopposed")
    else:
        raise SystemExit(f"election {sid}: unexpected {sand} 1922 candidacies {got}")
    slug, wrong, right = SUTHERLAND_INTRO
    intro = c.execute("SELECT intro FROM people WHERE slug = ?", (slug,)).fetchone()[0]
    if right in intro:
        print("  Sutherland intro: already 1925")
    elif wrong in intro:
        c.execute("UPDATE people SET intro = ? WHERE slug = ?", (intro.replace(wrong, right), slug))
        print("  Sutherland intro: 1922 -> 1925")
    else:
        raise SystemExit(f"{slug} intro: expected text not found")

    title, (d_wrong, d_right), (rp_wrong, rp_right) = BURRA_1920
    eid, date, rp = c.execute("SELECT id, election_date, replaced_person FROM elections WHERE wiki_page_title = ?",
                              (title,)).fetchone()
    rp_id = c.execute("SELECT replaced_person_id FROM elections WHERE id = ?", (eid,)).fetchone()[0]
    if date == d_wrong:
        c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (d_right, eid))
        print(f"election {eid}: date {d_wrong} -> {d_right}")
    elif date != d_right:
        raise SystemExit(f"election {eid}: unexpected date {date}")
    if (rp, rp_id) == rp_wrong:
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = ? WHERE id = ?", (*rp_right, eid))
        print(f"election {eid}: replaced {rp_wrong[0]} -> {rp_right[0]}")
    elif (rp, rp_id) != rp_right:
        raise SystemExit(f"election {eid}: unexpected replaced member {rp!r}/{rp_id}")

    title, rows = BURRA_1898_PETITIONS
    for name, wrong_votes, text in rows:
        cid, votes, votes_text = c.execute(
            """SELECT ca.id, ca.votes, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
               WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name)).fetchone()
        if (votes, votes_text) == (wrong_votes, None):
            c.execute("UPDATE candidacies SET votes = NULL, votes_text = ? WHERE id = ?", (text, cid))
            print(f"candidacy {cid} ({name}, Burra 1898): {wrong_votes} votes -> {text!r}")
        elif (votes, votes_text) != (None, text):
            raise SystemExit(f"candidacy {cid}: unexpected votes {votes!r}/{votes_text!r}")

    title, d_wrong, d_right = DELTING_NORTH_1890
    eid, date = c.execute("SELECT id, election_date FROM elections WHERE wiki_page_title = ?", (title,)).fetchone()
    if date == d_wrong:
        c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (d_right, eid))
        print(f"election {eid}: date {d_wrong} -> {d_right}")
    elif date != d_right:
        raise SystemExit(f"election {eid}: unexpected date {date}")

    title, (d_wrong, d_right), turnout, rows = BY_ELECTION_1902
    eid, date, hidden, old_turnout = c.execute(
        "SELECT id, election_date, hidden, turnout FROM elections WHERE wiki_page_title = ?", (title,)).fetchone()
    if (date, hidden, old_turnout) == (d_wrong, 1, None):
        c.execute("UPDATE elections SET election_date = ?, hidden = 0, turnout = ? WHERE id = ?", (d_right, turnout, eid))
        print(f"election {eid} ({title}): shown, {d_right}, turnout {turnout}")
    elif (date, hidden, old_turnout) != (d_right, 0, turnout):
        raise SystemExit(f"election {eid}: unexpected {date}/{hidden}/{old_turnout}")
    if c.execute("SELECT COUNT(*) FROM candidacies WHERE election_id = ?", (eid,)).fetchone()[0] == 0:
        for position, (name, slug, party, votes, elected) in enumerate(rows, 1):
            pid = c.execute("SELECT id FROM people WHERE slug = ?", (slug,)).fetchone()[0] if slug else None
            c.execute("""INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes, elected, position)
                         VALUES (?, ?, ?, ?, ?, ?, ?)""", (eid, pid, name, party, votes, elected, position))
            print(f"election {eid}: {name} {votes} added")

    for title, wrong, right in WIKI_BY_ELECTION_DAYS + WIKI_WESTMINSTER_DAYS:
        eid, date = c.execute("SELECT id, election_date FROM elections WHERE wiki_page_title = ?", (title,)).fetchone()
        if date == wrong:
            c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (right, eid))
            print(f"election {eid}: date {wrong} -> {right}")
        elif date != right:
            raise SystemExit(f"election {eid}: unexpected date {date}")

    for title, name, wrong_votes, text in COUNCIL_APPOINTMENTS_1914_1919 + COUNCIL_APPOINTMENTS_1921_1937 + COUNCIL_APPOINTMENTS_1940_1942:
        cid, votes, votes_text = c.execute(
            """SELECT ca.id, ca.votes, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
               WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name)).fetchone()
        if (votes, votes_text) == (wrong_votes, None):
            c.execute("UPDATE candidacies SET votes = NULL, votes_text = ? WHERE id = ?", (text, cid))
            print(f"candidacy {cid} ({name}): {wrong_votes} votes -> {text!r}")
        elif (votes, votes_text) != (None, text):
            raise SystemExit(f"candidacy {cid}: unexpected votes {votes!r}/{votes_text!r}")

    db.commit()
    db.close()


if __name__ == '__main__':
    main()
