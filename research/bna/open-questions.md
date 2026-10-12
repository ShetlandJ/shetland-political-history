# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-10-12: ZCC resignation dates.** A ward term ends only at the by-election that fills the
  seat, so a member who resigned is shown sitting until his successor came in. Two found in the
  roll call: Andrew W. Peterson (Yell North) resigned "with effect from 19th June" 1962 (ST 22 Jun
  1962 p6 a100), but sits in `council_terms`
  until the by-election of 4 Dec 1962; Barbara Anderson (Burra) resigned from 30 Jun 1972 (ST 26
  May 1972 p13 a179), sitting until 8 Aug 1972. The 1890–1939 by-election research has many more
  such dates. A new `data/ward_departures.csv` (person, date, reason, citation) applied by
  `build.py` like a death would show the vacancies on /council-terms, but needs a build change.
  Options: (a) add resignation dates as a data file; (b) keep ward terms ending at the by-election. Evidence: `zcc-roll-call.md`.

- **2026-10-01: Charles Brown's birth date.** The DB (from the wiki) has 19 Dec 1888, which makes
  him 78 when he retired in 1967. The 1967 nominations report calls him "now over eighty years of
  age" (ST 21 Apr 1967 p5 art. 076). His death notice gives no age. Options: (a) keep 1888, the paper guessed; (b) check Bayanne I153461. Evidence: `biographies-1950s-1980s.md`. Row:
  `people.charles-brown` born_date.
  **Your answer (b), 2026-10-12: waiting.** Bayanne held me at its bot check ("Please wait while your request is being verified"), which I can't get past. Paste I153461's birth line here, or I'll try again next run.

- **2026-09-29: Was Balfour Spence elected in Sept 1823?** Checked the transcribed minute book
  (`~/projects/town_council_minutes`, 2026-10-12). He sat 1820–23 (p22–25). From the first meeting
  after the 4 Sep 1823 election (29 Oct, p29) the only Spence present is "Councillor W. Spence"
  (William, Bailie from 1826), and in three years of sederunts (p29–34) Balfour appears only on the
  burgess rolls of voters (p28, 1823; p34, 1826). The book never records the 1823 result. The
  ledger's 1823 council is eleven with him, ten without. Options: (a) remove his 1823 term and
  candidacy, the wiki's mistake for William; (b) keep him, elected but never attending; (c) set
  his row confirmed=0 and leave it open. Row `balfour-spence` 1823-09-04, election 3.

- **2026-09-29: Day of the Unst South by-election, 1936.** Two nominations; withdrawals closed Tue
  28 Jan 1936; Manson withdrew and Clark "is therefore returned" (ST 25 Jan, 1 Feb 1936). No poll
  day printed. The DB has 1 Feb (the paper's date, a Saturday). Options: (a) 28 Jan, close of withdrawals; (b) leave 1 Feb; (c) look for the Order fixing the day.
  Same question as Jan 1911. Election 632. Evidence: `zcc-1920-1939.md`.
  **Your answer (c), 2026-10-12: queued.** The BNA started refusing article text partway through (403 on every OCR request), so the Order's day isn't read yet. ST 11 Jan 1936 p4 a66 is the lead: "New elections have been ordered for Unst South".

- **2026-09-29: Gulberwick, October 1936: when was Rev. George Smith appointed?** Johnston resigned
  by letter (ST 26 Sep 1936). Two searches found no appointment (`data/searches.csv`). The DB has
  1 Oct (the wiki says only "October"). Options: (a) leave 1 Oct; (b) search the Oct and Nov Council reports. Election 633. Evidence: `zcc-1920-1939.md`.
  **Your answer (b), 2026-10-12: queued** for when the BNA lets article text through again.

- **2026-09-28: Aithsting, Dec 1892: a contest the DB shows as unopposed.** Grierson and McCullie
  were both nominated; McCullie "had practically a walk over" (ST 26 Nov, 10 Dec 1892). No
  figures were printed, and the wiki page says "Unopposed". Options: (a) add Grierson as a candidate with no votes; (b) leave it.
  Election 184. Evidence: `zcc-1890-1899.md`.
  **Your answer, 2026-10-12:** needs more newspaper evidence; if there was no poll, leave the election as unopposed. **Queued** for when the BNA lets article text through again.

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)

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
