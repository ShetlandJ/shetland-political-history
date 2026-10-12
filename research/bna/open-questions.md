# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-10-12: Balfour Spence's 1823 seat after the Shetlopedia versions.** Shetlopedia's 2009/10
  list had one Spence (Balfour) among the ten members seen in 1823–24; in 2012 William Spence
  ("W. Spence", p29) was added and Balfour kept, making eleven. So the 1823 Balfour row looks like the
  first identification of the minute book's Spence, not a separate record. Options: (a) remove his
  1823 row; (b) keep it at confirmed=0; (c) keep it and look further. Row: `balfour-spence`
  1823-09-04. Evidence: `balfour-spence-custody.md`.
- **2026-10-12: Which Ogilvy was the 1823–26 councillor?** The Bailie signs as Charles Ogilvy (p30,
  p31, p33; Senior Bailie in the 30 Aug 1826 notice, p34). The councillor "Ogilvy" (p31, 22 Oct 1824)
  has no first name in the book. Shetlopedia 2009–12 had Charles as councillor and James as Senior
  Bailie; the 2016 wiki swapped them. The ledger has James. Options: (a) James, as the ledger has it;
  (b) Charles Ogilvy junior; (c) unknown, set confirmed=0. Row: `james-ogilvy` 1823-09-04. Evidence:
  `balfour-spence-custody.md`.
- **2026-10-12: ZCC resignation dates.** A ward term ends only at the by-election that fills the
  seat, so a member who resigned is shown sitting until his successor came in. Two found in the
  roll call: Andrew W. Peterson (Yell North) resigned "with effect from 19th June" 1962 (ST 22 Jun
  1962 p6 a100), but sits in `council_terms`
  until the by-election of 4 Dec 1962; Barbara Anderson (Burra) resigned from 30 Jun 1972 (ST 26
  May 1972 p13 a179), sitting until 8 Aug 1972. The 1890–1939 by-election research has many more
  such dates. A new `data/ward_departures.csv` (person, date, reason, citation) applied by
  `build.py` like a death would show the vacancies on /council-terms, but needs a build change.
  Options: (a) add resignation dates as a data file; (b) keep ward terms ending at the by-election. Evidence: `zcc-roll-call.md`.

- **2026-09-29: Day of the Unst South by-election, 1936.** The Order (ST 11 Jan 1936 p4 a66): nominations by 21 Jan, withdrawals by 28 Jan, "in the event of polls being necessary" voting on Tue 11 Feb 1936. Manson withdrew and Clark was returned unopposed (ST 1 Feb 1936). The 1960s by-elections were dated to the fixed day even when unopposed. Options: (a) 11 Feb, the day fixed for the election; (b) 28 Jan, the close of withdrawals; (c) leave 1 Feb. Election 632. Evidence: `answers-2026-10-12.md`.

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)

- **2026-09-29: Was Balfour Spence elected in Sept 1823?** Answered 2026-10-12: (c) his 1823 row set to confirmed=0, with the evidence in its source (bankrupt Aug 1821, sat to the end of the 1820 council but absent from every meeting after the 1823 election (corrected 2026-10-12: he attended 12 Aug 1822 and 2 Sep 1823), ten other members seen 1823-24; not a councillor in Sons and Daughters of Shetland or the Lerwick book). He still shows on the site, marked provisional. Almanacs checked (no Lerwick list). Custody traced 2026-10-12: the claim starts with the wiki page created 17 May 2016 and no earlier source names him (`balfour-spence-custody.md`).

- **2026-10-01: Charles Brown's birth date.** Answered 2026-10-12: (b) checked Bayanne I153461: b. 19 Dec 1888, North Dale, Fetlar, as in the DB. No change.

- **2026-09-29: Gulberwick, October 1936: when was Rev. George Smith appointed?** Answered 2026-10-12: (b) co-opted on a petition at the statutory meeting of Tue 20 Oct 1936 (Shetland News 22 Oct 1936 p5 a088). Election 633 now 20 Oct (`fix_newspapers.py` #83). Evidence: `answers-2026-10-12.md`.

- **2026-09-28: Aithsting, Dec 1892: a contest the DB shows as unopposed.** Answered 2026-10-12: no contest. Grierson "was not aware of any intention" to nominate him, and the seat reverted to McCullie (ST 3 Dec 1892 p2 a038). The DB's unopposed return stands. Evidence: `answers-2026-10-12.md`.

- **2026-10-03: two councillors' death days differ from Bayanne.** Answered 2026-10-12: (b) follow Bayanne: 11 Oct 1852 and 5 Apr 1828; `fix_newspapers.py` #71 no longer changes them, ledger ends moved.

- **2026-10-03: Henry Mouat, Provost of Lerwick?** Answered 2026-10-12: (a) the line is wrong: Henry was Convener of the County. James Mouat (iii)'s intro corrected (#82).

- **2026-10-03: Burra Isle by-election 1959: which date?** Answered 2026-10-12: (b) 30 Jun 1959 (#82; the wiki-day entry in `fix_parse_errors.py` dropped).

- **2026-09-29: Unst South (County Council), 1942: when did Andrew Irvine (i)'s seat end?** Answered 2026-10-12: (c) keep 22 Dec. No change.

- **2026-09-29: The Police Act special meeting, May 1940: Thursday 23 or Friday 24?** Answered 2026-10-12: (a) Thu 23 May; `ltc-1940-1945.md` corrected.

- **2026-09-29: The Shetland Times moved from Saturday to Friday on 21 May 1943.** Answered 2026-10-12: (a) Friday from 21 May 1943 in `build.py` and the `/bna` skill.

- **2026-09-29: Arthur Johnson or Johnston?** Answered 2026-10-12: (a) keep Johnson. No change.

- **2026-09-29: Add Arthur Gifford of Busta to the Sept 1844 election?** Answered 2026-10-12: (b) leave it. No change.

- **2026-09-29: Orkney and Shetland 1892: Younger 1616 or 1617?** Answered 2026-10-12: (a) keep 1617. No change.

- **2026-09-29: When did the 1928 ZCC councillors' terms end: Dec 1929 or May 1930?** Answered 2026-10-12: (b) keep the Dec 1929 poll date. No change.

- **2026-09-28: Day of the Jan 1911 ZCC by-elections (Dunrossness South, Sandwick, Unst North).** Answered 2026-10-12: (a) keep 12 Jan. No change.

- **2026-09-28: Day of the Feb 1902 ZCC by-elections (Unst South, Fetlar, Yell South).** Answered 2026-10-12: (a) leave as is. No change.

- **2026-09-28: Should unconfirmed rows with checked end dates be confirmed?** Answered 2026-10-12: (a) yes, both ends must be sourced. Rule kept.

- **2026-09-28: Day of Alexander Mitchell's resignation, October 1889.** Answered 2026-10-12: (a) 18 Oct accepted; row confirmed.

- **2026-09-28: Election day of the Nov 1888 LTC general.** Answered 2026-10-12: (a) Thu 1 Nov 1888 (#82); the ledger's eight dates moved.

- **2026-09-28: End of William Duncan (i)'s 1885 seat, 1886.** Answered 2026-10-12: (c) 15 Oct 1886, the council's acceptance; ledger edited.

- **2026-09-28: John Harrison (i)'s disqualification, 1886.** Answered 2026-10-12: (a) ends at the 2 Nov 1886 general; ledger edited.

- **2026-09-28: Day of J. J. Pottinger's retiral from the Town Council, 1921.** Answered 2026-10-12: (a) keep 7 Jun. No change.

- **2026-09-28: Arthur Laurenson in 1879: declined office, or never a candidate?** Answered 2026-10-12: (a) 1879 candidacy and not_seated row removed (#82).

- **2026-09-28: Day of William Duncan (i)'s co-option, Nov 1880.** Answered 2026-10-12: (a) keep 25 Nov. No change.

- **2026-10-01: William Angus's death place.** Answered 2026-10-12: (a) Edinburgh (`fix_parse_errors.py` #30).

- **2026-10-01: James Pottinger (i)'s birth.** Answered 2026-10-12: (b) abt 1776 (`fix_parse_errors.py` #30).

- **2026-10-01: The Sept 1850 general: Thursday 5th or Friday 6th?** Answered 2026-10-12: (a) keep the 6th. No change.

- **2026-10-01: 1856: which John Robertson?** Answered 2026-10-12: Checked the minute book (James asked): the 1856 burgess roll has "Mr John Robertson (Senior) Merchant" (p143) and the ballot elected "John Robertson (Senior)" (p145); John Robertson junior first sits in 1859. Kept (i).

- **2026-10-03: James Greig 1773–1855 and Lewis Garriock 1825–1902, pink boxes on the chart.** Pink on James's chart marks relatives and links, not councillors (James, 2026-10-12). Both stay relatives in `data/relatives.csv`; no edit.

- **2026-10-03: Thomas Fea and Barbara Fea.** Barbara (I37427) is not a daughter of Thomas Fea b. 1767 (I4241, six children by Jane Green, none Barbara); she was the daughter of an older Thomas Fea (I363803) and Margaret Bolt. The link was removed on 2026-10-11 (commit d3e8d0d); James confirmed 2026-10-12. The older Thomas isn't added.

- **2026-09-29: Dunrossness North, November 1971: day and votes.** Day confirmed: "fixed for Tuesday, 9th November" (ST 29 Oct 1971 p1, page image). The result isn't in the 12 or 19 Nov issues, so Leask 55, Fisher 15 stays the wiki's. (2026-10-03, `open-questions-images.md`)

- **2026-09-29: Where did Hugh T. Sutherland sit from 1970?** He didn't stand in 1970: not in the full nominations list (ST 24 Apr 1970 p8, page image); Delting South went to Robb. Intro now "Delting South between 1967 and 1970" (`fix_newspapers.py` #80). (2026-10-03, `open-questions-images.md`)

- **2026-09-29: Walls, May 1930: day, and was Halcrow unopposed?** Tue 20 May 1930, the new Council's first meeting. Two petitions, Halcrow 17 and William Hales 15; Halcrow "elected by a large majority" on a Council vote (ST 24 May 1930 p5, page image). Date moved, Hales added (`fix_newspapers.py` #77). (2026-10-03, `open-questions-images.md`)

- **2026-09-28: Day of James Budge's appointment for Dunrossness North, June 1907.** Thu 20 Jun 1907: the monthly meeting "on Thursday" (ST 22 Jun 1907 p8, page image). `fix_newspapers.py` #78. (2026-10-03, `open-questions-images.md`)

- **2026-09-28: Day of William Duncan (i)'s co-option for Bailie Hay, Nov 1884.** Tue 11 Nov 1884: the annual meeting of Mon 10 Nov "adjourned till Tuesday evening for the purpose of filling up the vacancy", and the adjourned meeting elected him (ST 15 Nov 1884 p3, page image). Election 34 back to 11 Nov (`fix_newspapers.py` #79) and the ledger row confirmed. (2026-10-03, `open-questions-images.md`)

- **2026-09-28: Day of Bailie John Campbell's resignation, May 1932.** Tue 3 May 1932, as the ledger has it: the monthly meeting "was held ... on Tuesday evening" (SN 5 May 1932 p5, page image). The row stays unconfirmed until its 1931 start is sourced. (2026-10-03, `open-questions-images.md`)

- **2026-10-03: Sandsting 1910's third candidate: "R. T. C. Scott" or Bayanne's Robert Scott?** The
  same man; the link is right. The candidate was "R. T. C. Scott, Lerwick" (ST 19 Nov 1910 p4 art. 066).
  His silver-wedding notice (ST 23 Jul 1910 p4 art. 046) names R. T. C. Scott of Melby, only son of
  the late R. T. C. Scott of Melby, married Agnes Georgeson at Melby House 1885, "Present address—1
  Gardie Court, Lerwick". Bayanne I45835, Robert Thomas Charles Scott (1864–1935, d. Whanganui), is
  at 1 Gardie Court in the 1911 census. James's 2021 wiki edit stands; no change (`zcc-leftovers.md`).

- **2026-09-29: Whalsay and Skerries, May 1947: Shearer's petition 207 or 307?** About 300: the
  Shetland Times page image reads "?07" with a broken first digit shaped like a 3, and the Shetland
  News (22 May 1947 p6) has "309". The wiki's "Petition of 307" stays; no edit (2026-10-03,
  `zcc-leftovers.md`).

- **2026-10-01: James Ogilvy: uncle or cousin of Charles (ii) and John Ogilvy?** Cousin (James, from
  Bayanne). James (I18310) was a son of Thomas Ogilvy (I18309, 1762-1819), and Thomas and Charles (i)
  (I7816) were brothers, both sons of Thomas Ogilvy and Charlotte Willamina Neven (F2643). The two
  intros now say "cousin" (`fix_newspapers.py` #73).

- **2026-10-01: Did James Ogilvy die at New Orleans in 1848, or in 1862?** Grant (1893, p210)
  says 1848, but FamilySearch has no Louisiana death for an Ogilvy or Ogilvie in 1846-50. A James
  Ogilvie was a householder in the Upper Suburbs of New Orleans in 1830, is in the 1861 New Orleans
  directory, and died there on 20 May 1862, "aged 71, born Scotland" (ours would have been 67).
  Options: keep 1848 (Grant); change to 1862 (the cemetery record), which would need more to tie
  it to him, such as the 1830 tally or a probate record; or say "1848 (Grant)" in the biography
  and leave the date alone. Affects `people.james-ogilvy` died_date. Evidence:
  `biographies-ltc-1818-1850.md` (James Ogilvy).
  **Answered 2026-10-01 (James):** probably a different man. Keep 1848 from Grant; no change.
