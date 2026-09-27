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
- [ ] **LTC election dates 1949–1964** (from 1958–65). The DB dates these generals on a **Monday**: 1949-05-02, 1950-05-01, 1952-05-05, 1953-05-04, 1954-05-03, 1955-05-02, 1958-05-05, 1959-05-04, 1960-05-02, 1961-05-01, 1963-05-06, 1964-05-04 (1951, 1956, 1957 and 1962 are already Tuesdays). Already seen: polling Tue 6 May 1958 ("poor weather on Tuesday", ST 9 May 1958), Tue 3 May 1960 (ST 15 Apr 1960), Tue 2 May 1961 (ST 12 May 1961, "last Tuesday's poll"), Tue 7 May 1963 (ST 19 Apr 1963). For each remaining year, search the Friday before and after polling for `municipal election` / `polling day`. Uncontested years (1954, 1956, 1957, 1965) had no poll: take the date the council treated as the election (the statutory meeting report usually says). Fix with `set_date` in `fix_newspapers.py`, and move the matching `start_date`/`end_date` in `data/ltc_terms.csv` (the ledger holds literal dates). Also check the Monday by-election 1949-09-12. No issues cleared, but every term boundary in the period is a day early.
- [ ] **Resignation dates of James Tait (1958) and James Daniel (1962)** (from 1958–65). Tait: poll-topper May 1958, "has had to intimate his resignation" by ST 6 Jun 1958 (p4 art. 050); Johnson co-opted 24 Jun. Search ST 23 May–13 Jun 1958 for `Tait resignation` / `town council`, and look for the meeting that accepted it (Tuesday 3 Jun?). Daniel: "recently submitted his resignation" (ST 10 Aug 1962, p6 art. 090); Shearer co-opted Tue 7 Aug. Search ST Jun–Aug 1962 for `Daniel resign`. Then set `end_date`/`end_reason=resigned` and `confirmed=1` on `james-tait` 1958-05-05 and `james-daniel` 1960-05-02. Expect one new genuine size-short row each.
- [ ] **Grace Halcrow on the County Council** (from 1958–65). The wiki says County Councillor for Cunningsburgh 1955–61; the ST preview of 17 Apr 1964 (p4 art. 054) says "one three-year term" and that she didn't re-stand, i.e. 1955–58. Check the ZCC data for a 1958 Cunningsburgh result first, then search ST Apr–May 1958 for `Cunningsburgh county council`. If 1955–58, correct her intro in `fix_newspapers.py` (guard on "between 1955 and 1961") and log it.
- [ ] **1947–1952**: overlaps for Robert Anderson (i) (1947), William Anderson (i) (1949), James Williamson (1950) and Arthur Johnson (1952). Retiring lists for Oct 1947, then Apr 1949, 1950 and 1952. Note the switch from November to May elections in 1949. 4 issues.
- [ ] **Wartime co-options 1940–42**: the size-over rows for 1941–42, 1942–46 and May–Jun 1946. There were no elections, so check the co-options of Mouat (Aug 1940), J. Williamson (iii) (Jan 1941), Johnson (Jul 1941), Gear and Inkster (Oct 1941) and T. Sinclair (Apr 1942). Keywords `co-opted`, `in room of`: whose seat did each fill, and are two departures missing from the ledger? 3 issues.
- [ ] **1896–1900**: overlaps for Robert Hunter (i) (1896), Francis Halcrow (1897) and George Kay (1900). Retiring lists for Oct 1896, 1897 and 1900. 3 issues.
- [ ] **1908–1909**: overlaps for Robert Ganson (i) (1908) and John Smith (i) (1909). Retiring lists for Oct 1908 and 1909. 2 issues.
- [ ] **1969–1973**: overlaps for William Smith (iv) (1969), William Peterson (1972) and Eric Gray (1973). Retiring lists for Apr 1969, 1972 and 1973. 3 issues.
- [ ] **1922**: the George Duffin overlap. Retiring list for Oct 1922. 1 issue.

## Not for BNA yet

- **LTC size-short rows (25)**: mostly the gap between a death or resignation and the co-option, so probably genuine vacancies. Re-check after the overlaps are cleared, since the counts will shift.
- **ZCC overlaps on the same day** (Peterson (i) and Sinclair 1919, Leslie (ii) 1922): someone starting two terms at the same general looks like a data problem (two wards, or a duplicate row). Check the wiki first.
- **ZCC by-elections** (1898 Henderson, 1920 Hunter, 1921 Anderson (ii), 1950 Ross and the Spence overlap / Yell South over-full, 1951 J. Williamson (iv)): each needs its own "who was replaced" search. Do them after the LTC items.
