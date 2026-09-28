# BNA research queue

`/bna next` takes the first unticked item: it runs the searches, applies the ledger or correction
edits, rebuilds (`python3 build.py`, then `--check`), saves evidence to `research/bna/<topic>.md`,
logs anything we had wrong in `research/corrections-log.md`, and ticks the item with a one-line
result. Ranked by how many /data-review issues each search should clear.

Most LTC overlaps have the same shape: someone won a seat, the generator gave it a full 3-year
term, and they were re-elected a year or two later. So the first seat was a short-term fill, a
co-option they had to re-stand for, or a retirement in place of a Provost who stayed on. The
Clerk's retiring list at the October meeting (from 1949, April, before the May elections) says
which. See the timing rules in the skill.

## LTC

- [x] **1912–1914**: Goodlad and J. Smith (i) overlaps, 1913–19 oversize, 1912 council size. Cleared 7 issues (62 → 59); `ltc-1912-1914.md`.
- [x] **1958–1965 chain**: Cleared 8 issues (59 → 53, with 2 genuine vacancies now shown). Lots, Provosts staying on and short seats; the 1957 seat was Nicolson's, not Halcrow's. `ltc-1955-1965.md`.
- [x] **LTC election dates 1949–1964**: every Monday date was really the Tuesday; 12 generals and the Sep 1949 co-option moved (102 ledger dates). No issues cleared (53). Found that May 1954 was contested. `ltc-election-dates-1949-1964.md`.
- [x] **May 1954 result**: contested (1913 voted); votes and the four losers added, Senior Bailie John N. Inkster defeated. The 1951 "James Inkster" was John Inkster (ii). 0 issues (53); 7 ledger rows confirmed. `ltc-1954-result.md`.
- [x] **Resignation dates of James Tait (1958) and James Daniel (1962)**: Tait resigned at the meeting of Tue 17 Jun 1958 but the seat was held open to 24 Jun (confirmed, no gap). Daniel: `resigned`, but the date wasn't found, so still unconfirmed and ending at the 7 Aug co-option. 0 issues (53). `ltc-1958-1962-resignations.md`.
- [x] **Grace Halcrow on the County Council**: 1955–58, not 1955–61. McLeod opposed her in 1958 and she withdrew before the poll (ST 25 Apr, 9 May 1958). Intro fixed (`fix_newspapers.py` #9); the election data was already right. 0 issues (53). `grace-halcrow-county.md`.
- [x] **1947–1952**: Cleared 5 issues (53 → 48): the four overlaps and the 1951–52 short row. Lowest winners got short seats; Treasurer Ollason kept on to 1951 and Provost R. A. Anderson to 1953; Shearer resigned Jun 1947 and Dalziel was co-opted. `ltc-1945-1953.md`.
- [x] **Wartime co-options 1940–42**: Irvine (ii) and Linklater resigned Aug 1941 (Irvine's term had run to his death in 1946); Gear and Inkster co-opted Tue 7 Oct 1941, not the 9th. The 3 size-over rows cleared, replaced by 4 genuine vacancy rows (48 → 49). Other wartime co-options not checked. `ltc-wartime-1941.md`.
- [x] **Shearer's 1947 resignation and Dalziel's co-option**: Dalziel co-opted at the monthly meeting of Tue 1 Jul 1947 (confirmed). Shearer resigned "last month" but the day wasn't found (absent 3 Jun, nothing in 6–27 Jun), so his 1938 row stays unconfirmed. He had gone to the County Council for Whalsay and Skerries on 20 May. 0 issues (49). `ltc-1947-shearer-dalziel.md`.
- [x] **1896–1900**: Cleared 3 issues (49 → 46). Hunter, not Stove, had Robertson's short seat in 1895; Halcrow retired a year early in 1897 while Provost Leisk stayed on; Kay volunteered to retire in 1900, and Johnson's 1899 seat ran to 1901. 26 rows confirmed. `ltc-1895-1901.md`.
- [x] **1908–1909**: Cleared 2 issues (46 → 44). Provost Porteous stayed on to 1910; Ganson retired in his place in 1908 and J. Smith a year early in 1909. 12 rows confirmed. `ltc-1908-1910.md`.
- [x] **1969–1973**: Cleared 3 issues (44 → 42, with the Apr–May 1970 vacancy now shown). Provost E. Gray stayed on in 1969 and W. A. Smith in 1972; Halcrow retired 1970; E. Gray and Butler had 2-year seats from 1971; Adair co-opted 8 May 1970, not 8 Aug. 1971–73 unopposed. `ltc-1969-1973.md`.
- [x] **1922**: Cleared 1 issue (42 → 41). Duffin's 1920 seat ended 1922 (retired by rotation, ST 14 Oct 1922) and A. S. Manson's 1921 seat ran to 1923 (ST 6 Oct 1923). 11 rows confirmed; why Duffin went a year early isn't stated. `ltc-1920-1923.md`.
- [x] **Date of James Daniel's 1962 resignation**: not in the paper. Only ST 10 Aug 1962 mentions it; the July reports and `resignation`/`resigned` Jun–Aug turned up nothing. Row unchanged (unconfirmed, ends at the 7 Aug co-option); left for the minute book in open-questions. 0 issues (41). `ltc-1958-1962-resignations.md`.
- [x] **Uncontested-looking generals**: all 12 (1900, 1902, 1911, 1923, 1924, 1927, 1928, 1930, 1931, 1956, 1965, 1966) confirmed unopposed; the wiki is right. In 1966 there were five nominations, but Junior Bailie Adair withdrew (ST 22 Apr 1966). No edits, 0 issues (41). `ltc-uncontested-generals.md`.
- [x] **ZCC polling days 1949–1964**: all six on a Tuesday: 10 May 1949, **13 May 1952** (not the 5th), 10 May 1955 (not Thu 5th), 13 May 1958, 9 May 1961, 12 May 1964. `fix_newspapers.py` #12 (144 ward rows). 0 issues (41). `zcc-election-dates-1949-1964.md`.

- [x] **May 1968 LTC polling day**: Tuesday 7 May 1968 (ST 19 Apr 1968); the votes match (ST 10 May 1968). `fix_newspapers.py` #13; 8 ledger dates moved and the 4 winners confirmed. 0 issues (41). `ltc-1968-polling-day.md`.

- [x] **Date of Shearer's ZCC appointment, May 1947**: Tue 20 May 1947, appointed on petition at that meeting (ST 23 May 1947). The attendance list is just those present. Election 692 moved (`fix_newspapers.py` #14). 0 issues (41). `ltc-1947-shearer-dalziel.md`.

## ZCC

- [x] **ZCC by-elections (who was replaced)**: Cleared 7 issues (42 → 35). Mostly namesake mislinks, not research: `build.py` matched a dead member's by-election to a namesake in another ward by name (fixed), and five replaced members plus the 1920 Nesting winner were linked to people from other eras (`fix_parse_errors.py` #5). Yell South 1950 was Spence's seat, not Ross's (ST 23 Jun, 21 Jul 1950; `fix_newspapers.py` #15). 1898 Dunrossness North date in open-questions. `zcc-by-elections.md`.

## Next (added 2026-09-28, 35 issues left)

- [x] **LTC 1885–1889**: 8 → 5 (the 5 left are dated vacancies). Stove's 1883 seat ran to 1886
  (confirmed row was wrong), Jamieson's 1886 seat to 1888; Hunter resigned 4 Jan 1887; co-options
  19 Nov 1886 and 18 Mar 1887 (`fix_newspapers.py` #16); Mitchell resigned Oct 1889. 16 rows
  confirmed. With the two ZCC fixes below, 35 → 30. `ltc-1885-1889.md`.
- [x] **LTC general dates 1885–1891**: 1889 polled Tue 5 Nov, not Sat 9th (ST 2 Nov 1889;
  `fix_newspapers.py` #17, 8 ledger dates). 1888: nominations Thu 1 Nov, unopposed; 1 Nov vs
  6 Nov left in open-questions. 1890–91 fit the Thursday-nomination, Tuesday-poll pattern.
  0 issues (30). `ltc-general-dates-1885-1891.md`.
- [x] **Duncan's resignation (Jul 1886) and John Harrison's disqualification (Oct 1886)**: Duncan's
  letter read Mon 12 Jul 1886 (ST 17 Jul), refusal to reconsider reported Fri 15 Oct (ST 16 Oct).
  Harrison "will be disqualified" after 15 Oct (ST 16 Oct), so 1 Oct is too early; cause not found.
  Both end dates in open-questions; sources updated, no dates changed. 0 issues (30).
  `ltc-1886-duncan-harrison.md`.
- [x] **LTC 1934–1938 (5 size-short rows)**: 30 → 28. All genuine vacancies. Sandison died Sun
  30 Sep 1934; Johnston resigned Tue 9 Oct 1934 (his and Sandison's 1932 seats were two-year);
  Cogle resigned Tue 5 May 1936, not July; Duffin died 28 Jun 1936; co-options 7 Jul, 4 Aug and
  10 Nov 1936 confirmed; A. S. Manson died 22 Jul 1938. 9 rows confirmed. `ltc-1934-1938.md`.
- [x] **Adam Halcrow (i)'s death and Clausen's departure**: 28 → 26 (the Nov 1936, Oct 1937 and
  Jul 1938 rows cleared; new 29 Nov–10 Dec 1945 row is Laing's dated vacancy). Halcrow died late
  Tue 24 Dec 1940; Williamson co-opted Tue 7 Jan 1941; Clausen resigned (read Tue 4 Jan 1938, moved
  to Thurso); Dalziel co-opted Tue 1 Feb 1938, not Thu 3rd (`fix_newspapers.py` #18); W. G. Smith
  died Sat 28 Feb 1942; Laing retired from 29 Nov 1945. 6 rows confirmed. `ltc-1936-1945.md`.
- [x] **LTC Dec 1940 short row**: 26 → 21 (Dec 1940, both Aug–Oct 1941, Feb–Apr 1942 and
  Mar–May 1946 rows cleared: all genuine vacancies, now fully confirmed). 1938 result sourced
  (Shearer's row confirmed). W. Sinclair resigned 24 May 1940 (seat declared vacant early June);
  Mouat co-opted Tue 2 Jul 1940, not 6 Aug; T. A. Sinclair 31 Mar 1942, not 9 Apr; Morrison Tue
  6 Feb 1945; Nov 1945 general Tue 6 Nov, not Mon 5th (`fix_newspapers.py` #19). Gear, Inkster
  and P. Smith resigned before their successors' co-options. 11 rows confirmed; J. Smith (i)'s
  1941 end in open-questions. `ltc-1940-1945.md`.
- [x] **LTC 1915–1916 (3 size-short rows)**: 21 → 18. All genuine vacancies. Stout died Sat
  10 Apr 1915, Sinclair elected Tue 27 Apr (Shetland News 1 May; the ST for 24 Apr–12 Jun 1915
  isn't digitised); Grierson died 3 Jul, C. B. Stout 3 Aug 1915; Laurenson died 14 Jul, Henderson
  1 Aug 1916; A. Smith resigned, Manson 5 Sep; W. S. Smith resigned Tue 7 Nov 1916 (not 5 Dec),
  Robertson 5 Dec. 1919 notice confirms the seven Nov 1919 ends. 17 rows confirmed.
  `ltc-1915-1916.md`.
- [x] **LTC holdovers 1919–1921**: 18 → 17 (the Oct–Nov 1921 row cleared, a genuine vacancy).
  Goodlad sat on to Nov 1920 and retired by rotation (Ex-Provost by May 1921); the five holdover
  rows confirmed from the 1919–21 notices. J. M. Goodlad co-opted for Pottinger Tue 7 Jun 1921,
  not the 11th, on the Provost's casting vote (`fix_newspapers.py` #20); Bailie W. Sinclair's
  resignation read Tue 4 Oct 1921, not 1 Oct. 8 rows confirmed; Pottinger's retiral day in
  open-questions. `ltc-1919-1921.md`.
- [x] **LTC Oct–Nov 1912 short row**: 17 → 16 (a genuine vacancy, now fully confirmed). Starts
  sourced from the 1909, 1910 and 1911 results (ST 6 Nov 1909, 5 Nov 1910, 11 Nov 1911) and the
  1908 cohort's from ST 7 Nov 1908 and the Oct 1911 retiring list. 12 rows confirmed.
  `ltc-1908-1911.md`.

## Batches (added 2026-09-28, 16 issues left)

Each batch is one `/bna next` run; commit after each sub-item. Targets are the unconfirmed rows
sitting through each size-short window (listed per sub-item).

- [x] **Batch 1: LTC 1894–1905 (4 issues)**: 16 → 12, all four genuine vacancies, now fully
  confirmed (28 rows edited). The 1901 general was Tue 5 Nov, not Fri 1st (`fix_newspapers.py`
  #21). Provost Goudie stayed on 1906–07 ("had to serve a period of three years" as Provost) and
  J. T. J. Sinclair, lowest in 1904, retired in 1906 in his place. Irvine resigned Tue 7 Dec 1909;
  Loggie appointed Tue 4 Jan 1910, not the 1st (#22). One commit for the batch: the sub-items
  shared the 1901 date change. `ltc-1894-1907.md`.
  - [x] 21 Jul–5 Nov 1895: Garriock's 1894 seat (ST 10 Nov 1894).
  - [x] 28 Feb–2 May 1899: Stove's 1898 seat (unopposed, ST 29 Oct 1898; died Sun 13 Oct 1901).
  - [x] 13 Oct–1 Nov 1901: the 1899 and 1900 cohorts (results 1899, 1900; lists 1902, 1903).
  - [x] 2–28 Feb 1905: the 1902, 1903 and 1904 cohorts (results; lists 1905, 1906, 1907).
  - [x] John Irvine (iii)'s 1907 row: resigned 7 Dec 1909.
- [x] **Batch 2: LTC 1945–1956 (3 issues)**: 12 → 9, all three genuine vacancies, now fully
  confirmed (11 rows). Nov 1946 general was Tue 5 Nov, not Mon 4th (`fix_newspapers.py` #23);
  Brownlie resigned (letter read at Johnson's 13 Sep 1949 co-option). `ltc-1945-1956.md`.
  - [x] 29 Nov–10 Dec 1945 (cleared, 12 → 11; died Mon 4 Mar 1946, ST 8 Mar 1946): David Gray (ii) 1945-11-06→1946-03-04 (start: the 9 Nov 1945 result,
    ST p2 art. 047, whose OCR list is garbled; zoom if needed; end: his Mar 1946 departure).
  - [x] **Nov 1946 polling day** (Tue 5 Nov: ST 1 and 8 Nov 1946; `fix_newspapers.py` #23, 10 ledger dates; 0 issues): the DB has Mon 4 Nov 1946; 1945 was the Tuesday, so probably
    Tue 5 Nov. Check the Friday 1 and 8 Nov 1946 papers. Do this before the next sub-item.
  - [x] 3 Jun–1 Jul 1947 (cleared, 11 → 10; resignation read 13 Sep 1949, ST 16 Sep 1949): James Brownlie 1946→1949-09-13 (start: the Nov 1946 result; end: his
    1949 departure before the Sep 1949 co-option).
  - [x] 26 Apr–6 May 1955 (cleared, 10 → 9; ST 9 May 1952, 8 May 1953, 11 Mar 1955, 16 Mar 1956): Johnson, Peterson, Tait, Conochie (1952-05-06→1955-05-03); Burgess,
    Harry Gray, Eunson, R. Anderson (i) (1953-05-05→1956-05-01). April retiring lists 1955–56.
- [x] **Batch 3: LTC 1965–1970 (2 issues)**: 9 → 7, both genuine vacancies, now fully confirmed
  (11 rows). Provost Nicolson resigned from 27 Apr 1967 (ill-health); Halcrow co-opted at the
  statutory meeting Fri 5 May 1967, not the 12th; R. A. Anderson died Sun 25 Jun 1967, not the 26th
  (`fix_newspapers.py` #24); Cumming co-opted Tue 8 Aug. W. A. Smith's 1969 row is the first
  abolition-ending row confirmed. One commit for the batch. `ltc-1965-1970.md`.
  - [x] 26 Jun–8 Aug 1967: Harry Gray, James Paton (i) (1965-05-04→1968-05-07); Grace Halcrow
    1967-05-12→1968-05-07 (her co-option date, and the 1968 retiring list).
  - [x] 14 Apr–8 May 1970: William Smith (iv) 1969-05-06→1975-05-15 (start: the May 1969
    result; end is abolition).

Not batched: 1884–1889 (6 issues) waits on the open questions (Harrison, Mitchell, the 1888
date); the two John Robertsons' 1884 and 1887 rows sit through all of them. 1961 is James
Daniel's resignation, not in the paper (minute book).

## Citations (added 2026-09-28)

`data/citations.csv` and `data/citation_links.csv` now hold every source used so far (293
citations, 801 links). Gaps:

- [x] **Citations batch 1: LTC 1872–1962 (Shetland Times)** (done 2026-09-28, four sub-items; seven dates corrected, `fix_newspapers.py` #25–29). Collect a citation for every LTC
  election and confirmed ledger row from 1872 that has none. For each: find the article
  (result, nominations report, retiring list or co-option report), add it to
  `data/citations.csv`, link it in `data/citation_links.csv` (`election` and/or `term`
  `slug@start_date`, with `basis` read/inferred and a `note` saying what it supports), and add
  dead ends to `data/searches.csv`. Check the votes and dates against the DB while there;
  anything that differs goes in the corrections log as usual. Rebuild and commit after each
  sub-item. Find what's still missing with:
  `sqlite3 shetland.db "select e.id, e.election_date, e.wiki_page_title from elections e where council_id=1 and hidden=0 and election_date>='1872' and not exists (select 1 from citation_links l where l.election_id=e.id) order by 2"`
  (and the same over `council_terms` with `confirmed=1` for rows).
  - [x] **Undated previews, 1932, 1933, 1936** (done: ST 29 Oct 1932 p5, 4 Nov 1933 p4, 31 Oct 1936 p4; 3 citations, 22 links; all agree with the ledger. The 1929 Manson, Sandison, Ganson (i) rows and Morrison's Jun 1932 row now have their ends sourced; starts still open. `ltc-previews-1932-1936.md`) (8 rows): the Saturday before each general.
    Link to `william-sinclair@1929-11-05`, `william-bruce-ii@1930-11-04`,
    `james-laing@1930-11-04`, `john-sinclair@1930-11-04`, `robert-ollason@1930-11-04`,
    `adam-halcrow-i@1933-11-07`, `charles-manson@1933-11-07`, `robert-ollason@1933-11-07`, and
    to elections 91 (Nov 1932) and 92 (Nov 1933).
  - [x] **1874–1883** (done: 16 citations, 105 links, 38 ledger sources. All agree except the Nov 1881 general, Tue 1 Nov not the 8th (`fix_newspapers.py` #25, 10 ledger dates). The paper was a Monday one until 15 Mar 1875 (`build.py` check updated). Laurenson 1879 and Duncan's 1880 co-option day in open-questions. 0 issues (7). `ltc-1874-1883.md`) (elections 21–32; 37 confirmed rows from the April 2026 research with no
    source): the results and the October retiring lists. Both ends of each row, so a row's end
    is linked from the next retiring list. Check the Shetland Times exists for 1874 first.
  - [x] **1884–1935** (done: 15 citations, 73 links, 17 rows confirmed. 1893 general Tue 7 Nov not Thu 2nd, Johnson elected 4 Apr 1899 not 2 May, Reid Tait 6 May 1924 not 5th (`fix_newspapers.py` #26–28); Campbell resigned May 1932, not at Morrison's co-option; Duncan's 1884 co-option was before 18 Nov (open question). ST mid-1932 not digitised (Shetland News used). 0 issues (7). `ltc-1884-1935-citations.md`): elections 34 (co-option, 22 Nov 1884), 39 (1887), 44 (1892), 45 (1893),
    51 (by-election May 1899), 58 (by-election Feb 1905), 81 (by-election May 1924), 83 (1925),
    84 (1926), 87 (1929), 90 (by-election Jun 1932).
  - [x] **1946–1950** (done: Sinclair elected Tue 2 Apr 1946, not 22 May (`fix_newspapers.py` #29); Blance's Sep 1950 co-option confirmed, day not printed. 2 citations, 6 links. 0 issues (7). `ltc-1946-1950-citations.md`): elections 112 (T. A. Sinclair's co-option for David Gray, May 1946: the
    DB's Wed 22 May is unconfirmed, so check the day) and 118 (by-election Sep 1950).
- [ ] **Citations batch 2: ZCC elections 1890–1973** (130 without a citation): one result
  article per general covers every ward row. By decade.
- [ ] **Citations batch 3: Westminster 1872–1975** (30 without a citation): the Orkney and
  Shetland results in the Shetland Times.
- Not BNA: SIC 1976+ and Holyrood (38; official results pages, needs a `web` publication type),
  and pre-1872 (minute book).
- [ ] **244 confirmed rows with no recorded source**: the April 2026 rows ("per-row source not
  recorded"), all starting 1818–1883 (most before 1872, when the Shetland Times began).
  Mostly minute-book work: record the page for each as an `mb-pN` citation. The 1876–1883
  rows now have Shetland Times sources (`ltc-1874-1883.md`).
- **Basis not reviewed**: the 801 backfilled links have `basis` empty. Set `read` or `inferred`
  as each is next used; no separate run needed.

## Not BNA: wiki checks (found 2026-09-28)

- [x] **ZCC Delting North 1919** (done, `fix_parse_errors.py` #7): the wiki marks Joseph Peterson (i) as losing (15 votes, cross),
  but the baseline has him elected there as well as in Delting South. His profile says he sat for
  Delting South 1919–22. Set candidacy 1169 to elected=0 (`fix_parse_errors.py`). Clears 1
  overlap. The wiki's Delting South row gives Peterson a cross and J. T. J. Sinclair a tick,
  which looks the wrong way round (78 v 30); the DB already has it right.
- [x] **ZCC Aithsting & Sandsting 1922** (done, `fix_parse_errors.py` #8): the wiki has one combined ward ("This election
  recombined the Aithsting & Sandsting parishes"), but the parser made two rows (1242, 1243)
  with identical results, so John Leslie (ii) gets two seats. Hide 1243 and show 1242 as
  "Aithsting & Sandsting" (`fix_parse_errors.py`). Clears 1 overlap.
- [x] **ZCC William Sinclair, Dec 1919** (done: Burra win in `not_seated.csv`, by-election 15 Apr 1920 with `[double return]`, `fix_parse_errors.py` #9; ward councils now honour `not_seated.csv`): a genuine double return (Burra and Whiteness &
  Weisdale). He chose Whiteness, which caused the Burra by-election of Apr 1920. Needs a way to
  record a win that wasn't taken up on a ward council (see open-questions).

## Not for BNA yet

- **By-election days from the wiki text**: the parser kept only the month for by-elections, but many wiki pages give the day in their first sentence ("took place on 17 May"). Seven were set in `fix_parse_errors.py` #5; the rest of the ~100 dated the 1st could be done the same way, checking the weekday. No BNA needed.
- **James Hunter (iii)'s birth date**: DB 1872-02-06, but his wiki page says 6 January 1872. Check which is right.

- **LTC size-short rows (25)**: mostly the gap between a death or resignation and the co-option, so probably genuine vacancies. Re-check after the overlaps are cleared, since the counts will shift.
- **ZCC overlaps on the same day** (Peterson (i) and Sinclair 1919, Leslie (ii) 1922): someone starting two terms at the same general looks like a data problem (two wards, or a duplicate row). Check the wiki first.
