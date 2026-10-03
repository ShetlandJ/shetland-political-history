# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-10-03: Burra Isle by-election 1959: which date?** The DB has the wiki's Tue 21 Jul 1959.
  The paper: Strachan left the seat on joining the Town Council; the Council ordered a by-election
  on Tue 19 May, "probable date ... about 30th June" (ST 22 May 1959); A. D. Bennet was the only
  nominee when nominations closed on Tue 9 Jun and "will be" the new member (ST 12 Jun 1959). No
  report of the declaration found. Options: keep 21 Jul; 30 Jun (the probable election day, the
  usual date for an unopposed return); 9 Jun (close of nominations). Election 802. Evidence:
  `zcc-leftovers.md`.

- **2026-10-01: Charles Brown's birth date.** The DB (from the wiki) has 19 Dec 1888, which makes
  him 78 when he retired in 1967. The 1967 nominations report calls him "now over eighty years of
  age" (ST 21 Apr 1967 p5 art. 076). His death notice gives no age. Options: keep 1888 (the paper
  guessed), or check Bayanne I153461. Evidence: `biographies-1950s-1980s.md`. Row:
  `people.charles-brown` born_date.

- **2026-09-29: Unst South (County Council), 1942: when did Andrew Irvine (i)'s seat end?** His
  resignation letter ("owing to the scarcity of labour") was read at the meeting of Tue 18 Aug
  1942 (ST 22 Aug 1942 p3 art. 064) and he was asked to reconsider. On Tue 27 Oct 1942 he declined
  and asked that it take effect "as from the date of his last letter, viz., 8th August" (SN 29 Oct
  1942 p2 art. 031). `council_terms` ends his seat at Hunter's co-option on 22 Dec 1942 (derived
  from by-election 665). Options: end on 8 Aug 1942 (his own date), 27 Oct 1942 (accepted), or
  keep 22 Dec. ZCC terms are derived, so a change needs a correction, not a ledger edit. The same
  report gives an earlier petition for John Sutherland, Bixter, as "signed by 323 ratepayers in
  South Unst" (OCR). `zcc-1940-1959.md` has petitions of 55 (Hunter) and 19 (Sutherland) from ST
  26 Dec 1942. Settled 2026-10-03 from the page image: "signed by 23", an October petition separate from
  December's (`open-questions-images.md`). Evidence: WW2 sweep notes (fork B); election 665.

- **2026-09-29: The Police Act special meeting, May 1940: Thursday 23 or Friday 24?**
  `ltc-1940-1945.md` §2 reads "last evening" in ST Sat 25 May 1940 (p4 art. 077) as Fri 24 May.
  But ST 1 Jun 1940 p5 art. 099 says the Town Council "met on Thursday evening", and Scarth refers
  to "the following day's meeting of the County Council", which was fixed for "to-day (Friday)"
  (ST 25 May p5 art. 101). So the 25 May paper was written on the Friday, and the meeting was
  **Thu 23 May**. No ledger row depends on it (W. Sinclair's end is 4 Jun). Option: correct the
  day in `ltc-1940-1945.md`. Evidence: `ww2-councils.md` (sweep), `ltc-1940-1945.md`.

- **2026-09-29: The Shetland Times moved from Saturday to Friday on 21 May 1943.** The last
  Saturday issue was 15 May 1943 and the first Friday issue 21 May 1943; every week to Aug 1945 is
  digitised. CLAUDE.md and the `/bna` skill say "Saturday ... to at least Feb 1943; Friday by Oct
  1944". `build.py` (line 575) also only accepts Saturdays for 1943, so citing a Friday issue from
  21 May 1943 on would fail the build. Option: update all three to "Friday from 21 May 1943". No
  data affected yet.

- **2026-09-29: Arthur Johnson or Johnston?** The Shetland Times prints both for the councillor
  co-opted on 1 Jul 1941: "Arthur Johnston" in the co-option report (5 Jul 1941) and most 1942
  lists, "Arthur Johnson" in Aug, Oct, Nov 1941 and Mar 1942; "A. E. Johnston" once (Oct 1942).
  The ledger slug is `arthur-johnson`. Options: keep, or check the 1945 nomination papers or his
  obituary for the spelling. Row: `arthur-johnson` 1941-07-01.

- **2026-09-29: Was Balfour Spence elected in Sept 1823?** The minute book never recorded the
  1823 result (p27–28, then blank). Ten of the wiki's eleven sat at meetings from Oct 1823 (p29–31),
  but Balfour Spence isn't seen at any meeting from Oct 1823 to Sep 1826. His row stays
  `confirmed=1` on the wiki's word. Options: keep, set `confirmed=0`, or check the wiki's source
  (the Shetland Times doesn't exist yet). Row `balfour-spence` 1823-09-04. Evidence:
  `ltc-1818-1871-minute-book.md`.

- **2026-09-29: Add Arthur Gifford of Busta to the Sept 1844 election?** Elected Senior Bailie on
  5 Sep 1844 (mb p105) but "could not accept the office" (p107, 27 Sep). The DB has no candidacy
  for him. Options: add him as elected Senior Bailie with a `not_seated.csv` row (like Hay 1884,
  needs a person record), or leave it. Election 11. Evidence: `ltc-1818-1871-minute-book.md`.

- **2026-09-29: Orkney and Shetland 1892: Younger 1616 or 1617?** The Shetland Times of 30 Jul
  1892 has "Lyell, 2624; Younger, 1616" twice (majority 1008). Its 1902 table has 2624 and 1617
  (1007); the Shetland News (10 Aug 1895) and ST 1906 have 2623 and 1617 (1006). The DB keeps the
  wiki's 2624 and 1617. Options: keep, or take the night's 1616. Election 1206. Evidence:
  `westminster-1873-1974.md`.

- **2026-09-29: Day of the Unst South by-election, 1936.** Two nominations; withdrawals closed Tue
  28 Jan 1936; Manson withdrew and Clark "is therefore returned" (ST 25 Jan, 1 Feb 1936). No poll
  day printed. The DB has 1 Feb (the paper's date, a Saturday). Options: 28 Jan (the close of
  withdrawals, when the return was settled), leave 1 Feb, or look for the Order fixing the day.
  Same question as Jan 1911. Election 632. Evidence: `zcc-1920-1939.md`.

- **2026-09-29: Gulberwick, October 1936: when was Rev. George Smith appointed?** Johnston resigned
  by letter (ST 26 Sep 1936). Two searches found no appointment (`data/searches.csv`). The DB has
  1 Oct (the wiki says only "October"). Options: leave, or search the October and November
  Council reports by the meeting day (Tuesdays). Election 633. Evidence: `zcc-1920-1939.md`.

- **2026-09-29: When did the 1928 ZCC councillors' terms end: Dec 1929 or May 1930?** The Dec
  1929 general elected the re-constituted Council under the Local Government (Scotland) Act 1929,
  which "only takes over full duty after 15th May next"; "The present County Council will
  continue to function till May" (ST 23 Nov 1929). The DB ends all 27 terms from Dec 1928 at the
  3 Dec 1929 poll, and starts the new ones then. The 12 Lerwick Town Councillors who also sat on
  the new County Council for the burgh aren't in the DB at all. Options: leave it (the poll date
  as the changeover), or end the old terms and start the new ones on 15/16 May 1930 (a build.py
  rule for this one general). Evidence: `zcc-1920-1939.md`.

- **2026-09-28: Day of the Jan 1911 ZCC by-elections (Dunrossness South, Sandwick, Unst North).**
  Nominations closed Tue 10 Jan 1911, one each, "no contests" (ST 14 Jan 1911). The wiki and DB
  have Thu 12 Jan, which the paper neither confirms nor contradicts (probably the day fixed for
  the election). Options: keep 12 Jan, or date them 10 Jan (the close of nominations, when the
  returns were settled). Elections 396–398. Evidence: `zcc-1900-1919.md`.

- **2026-09-28: Day of the Feb 1902 ZCC by-elections (Unst South, Fetlar, Yell South).** An Order
  read on Thu 2 Jan fixed a day "instant" (January; the date is lost in the OCR); the three were
  the only nominations, "no poll" (ST 18 Jan), and were reported elected at the meeting of Thu
  6 Feb 1902 (ST 8 Feb). The DB has Fetlar 1 Feb and the other two 8 Feb (the wiki says 8 Feb for
  all three, the paper's date). Options: leave, set all three to 6 Feb (the meeting that
  recorded it), or check the Order's day in the minute book / Edinburgh Gazette. Elections
  302–304. Evidence: `zcc-1900-1919.md`.

- **2026-09-28: Aithsting, Dec 1892: a contest the DB shows as unopposed.** Grierson and McCullie
  were both nominated; McCullie "had practically a walk over" (ST 26 Nov, 10 Dec 1892). No
  figures were printed, and the wiki page says "Unopposed". Options: add Grierson as a candidate with no votes, or leave it.
  Election 184. Evidence: `zcc-1890-1899.md`.

- **2026-09-28: Should unconfirmed rows with checked end dates be confirmed?** Gear and Inkster
  (co-opted 7 Oct 1941) have confirmed start dates but unchecked end dates. The six 1945
  retirements are confirmed by ST 5 Oct 1945, but their start dates weren't checked. So far a row
  only gets `confirmed=1` when both ends are sourced. Keep that rule?
  Evidence: `ltc-wartime-1941.md`, `ltc-1945-1953.md`.

- **2026-09-28: Day of Alexander Mitchell's resignation, October 1889.** ST Sat 19 Oct 1889 (p2,
  art. 051) reports it accepted "At a meeting of the Town Council, held last ..."; the word after
  "last" was never printed (checked on the page image, 2026-10-03), so the paper can't settle it. The item just above says the Commissioners met "last night" (Fri 18 Oct).
  The ledger now ends his 1888 seat on 1889-10-18, unconfirmed. Options: accept 18 Oct and
  confirm, or check the minute book or the 26 Oct report for the meeting date.
  Evidence: `ltc-1885-1889.md`. Row: `alexander-mitchell-i` 1888-11-08.

- **2026-09-28: Election day of the Nov 1888 LTC general.** The ledger and election 40 have
  Thu 8 Nov 1888, which fits nothing. Nominations closed Thu 1 Nov with four for four vacancies,
  so no poll, and the paper says the election "fell upon Thursday" (ST 3 Nov 1888). But in 1885
  the Clerk dated an unopposed return to the polling Tuesday (3 Nov), which in 1888 would be Tue
  6 Nov. No 1888 election notice found. Options: 1 Nov (the paper's wording), 6 Nov (the 1885
  convention and the other years' dates), or leave 8 Nov. Affects election 40 and 8 ledger dates
  (the 1885/1886/1887 rows ending then and the four 1888 rows starting then).
  Evidence: `ltc-general-dates-1885-1891.md`.

- **2026-09-28: End of William Duncan (i)'s 1885 seat, 1886.** His letter was read at the
  meeting of Mon 12 Jul 1886, saying he would cease "three weeks hence"; the council sent a
  deputation, and on Fri 15 Oct it was reported he "definitely refused" (ST 17 Jul, 16 Oct 1886).
  He was absent on 12 Jul, 30 Jul, 11 and 15 Oct (other meetings not checked). Options: 12 Jul (as now), about 2 Aug (his
  three weeks), or 15 Oct (the council's acceptance; for Hunter in 1887 we used the acceptance
  date). Row: `william-duncan-i` 1885-11-03. Evidence: `ltc-1886-duncan-harrison.md`.

- **2026-09-28: John Harrison (i)'s disqualification, 1886.** After the 15 Oct 1886 meeting the
  paper says "one member will be disqualified" (ST 16 Oct 1886), so the ledger's 1 Oct end is too
  early. The cause and day aren't in the paper. Options: end at the 2 Nov 1886 general (the seven
  vacancies were to be filled then), keep 1 Oct, or check the minute book. Row: `john-harrison-i`
  1884-11-04. Evidence: `ltc-1886-duncan-harrison.md`.

- **2026-09-28: Day of J. J. Pottinger's retiral from the Town Council, 1921.** Last seen
  present at a meeting reported 23 Apr 1921; absent 3 May and 7 Jun. His County Council seat was
  already vacant by the meeting reported 28 May. On Tue 7 Jun the council filled "the vacancy
  caused by the retiral of Mr J. J. Pottinger" (ST 11 Jun 1921), but no letter or date was
  found. The row now ends at the 7 Jun co-option, unconfirmed. Options: keep 7 Jun, or check the
  minute book for the meeting that accepted it. Row: `james-pottinger-iii` 1919-11-04.
  Evidence: `ltc-1919-1921.md`.

- **2026-09-28: Arthur Laurenson in 1879: declined office, or never a candidate?** ST 1 Nov 1879
  says he "would not allow himself to be re-nominated", so he wasn't nominated at all. The DB
  has him as an 1879 candidate (not elected), and `not_seated.csv` and CLAUDE.md call it
  "declined office". Options: hide or delete his 1879 candidacy (`fix_parse_errors.py`) and drop
  the `not_seated.csv` row; or keep both and only reword the reason. Election 27.
  Evidence: `ltc-1874-1883.md`.

- **2026-09-28: Day of William Duncan (i)'s co-option, Nov 1880.** ST 11 Dec 1880 says he was
  elected "at last meeting" for Goudie, resigned; the meeting isn't reported (searched 6 Nov–11
  Dec). The DB's Thu 25 Nov 1880 is unchecked. Options: keep, or check the minute book.
  Election 29; row `william-duncan-i` 1880-11-25. Evidence: `ltc-1874-1883.md`.

- **2026-10-01: William Angus's death place.** The wiki says "d. 1848, Edinburgh", but the DB's
  `death_place` is empty, so the parser seems to have dropped it. No notice found in 1847–49
  ("angus lerwick"). Options: set Edinburgh from the wiki text (`fix_parse_errors.py`), or leave it.
  Evidence: `biographies-ltc-1818-1850.md`.

- **2026-10-01: James Pottinger (i)'s birth.** The wiki says "b. abt 1776" and the DB says
  `bef 1790`. Nothing found in the papers. Options: keep, or take the wiki's "abt 1776". Evidence:
  `biographies-ltc-1818-1850.md`.

- **2026-10-01: The Sept 1850 general: Thursday 5th or Friday 6th?** The minute book dates it
  "the sixth day of September" 1850 (p123), a Friday; the ledger and election 13 follow it. The
  John o' Groat Journal (Fri 13 Sep 1850 p3 art. 013) has "On Thursday the 5th instant", and
  every other general 1826–1871 was on a Thursday. The Northern Ensign's report (12 Sep 1850)
  loses the day in the OCR. Options: keep the 6th (the book is the record), or check the book's
  page image for the date and the 2 Sep notice. Evidence: `ltc-pre-1872-press.md` §4. Rows:
  election 13 and its 11 ledger rows (1850-09-06).

- **2026-10-01: 1856: which John Robertson?** The minute book's 1856 councillors include "Mr
  John Robertson (Senior)" (p145), so the ledger has `john-robertson-i@1856-09-04`. The John
  o' Groat Journal (12 Sep 1856 p3 art. 023) lists "John Robertson, jun., fishcurer" instead.
  Robertson (ii) first sits in 1859 in the ledger, when both were elected. Options: keep (i),
  the book is explicit, or check which of them was a fishcurer. Evidence:
  `ltc-pre-1872-press.md` §4. Row: `john-robertson-i` 1856-09-04.

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)

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
