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
- [ ] **LTC Dec 1940 short row**: holds on Shearer's 1938 row (resignation day, Jun 1947, not found)
  and Mouat's Aug 1940 co-option (and the 1938 William Sinclair row it replaced). Check Sinclair's
  1940 departure and Mouat's co-option, and the ends of the 1940–45 co-optees at the Nov 1945 general.
- [ ] **LTC 1915–1916 (3 size-short rows)**: Apr 1915, Jul 1915, Jul 1916. Wartime departures and
  co-options.

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
