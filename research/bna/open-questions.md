# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-09-29: Unst South (County Council), 1942: when did Andrew Irvine (i)'s seat end?** His
  resignation letter ("owing to the scarcity of labour") was read at the meeting of Tue 18 Aug
  1942 (ST 22 Aug 1942 p3 art. 064) and he was asked to reconsider. On Tue 27 Oct 1942 he declined
  and asked that it take effect "as from the date of his last letter, viz., 8th August" (SN 29 Oct
  1942 p2 art. 031). `council_terms` ends his seat at Hunter's co-option on 22 Dec 1942 (derived
  from by-election 665). Options: end on 8 Aug 1942 (his own date), 27 Oct 1942 (accepted), or
  keep 22 Dec. ZCC terms are derived, so a change needs a correction, not a ledger edit. The same
  report gives an earlier petition for John Sutherland, Bixter, as "signed by 323 ratepayers in
  South Unst" (OCR). `zcc-1940-1959.md` has petitions of 55 (Hunter) and 19 (Sutherland) from ST
  26 Dec 1942. Is 323 a misreading, or an October petition separate from the December ones? Needs
  the page image. Evidence: WW2 sweep notes (fork B); election 665.

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

- **2026-09-29: Whalsay and Skerries, May 1947: Shearer's petition 207 or 307?** The wiki has
  "Petition of 307"; the Shetland Times OCR (23 May 1947 p7 art. 145) has "signed by 207
  persons". The page image wouldn't render in the viewer, so the digit isn't checked. The DB now
  shows "Petition of 307" (no votes, `fix_parse_errors.py` #19). Options: keep 307, take 207, or
  zoom on the page. Election 692. Evidence: `zcc-1940-1959.md`.

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

- **2026-09-29: Dunrossness North, November 1971: day and votes.** "Fixed for Tuesday, [..]th
  November" (ST 29 Oct 1971; the day is lost in the OCR). Set to Tue 9 Nov (the wiki's Wed 10 Nov
  is probably the count). The result report wasn't found in three searches, so Leask 55, Fisher 15
  is still the wiki's. Options: accept, or zoom on the 12 Nov 1971 front page. Election 913.
  Evidence: `zcc-1960-1974.md`.

- **2026-09-29: Where did Hugh T. Sutherland sit from 1970?** The wiki gives him Northmavine South
  in May 1970, but that was Balfour's seat until March 1972 (now fixed, `fix_newspapers.py` #55),
  and Delting South went to Rev. W. C. Robb. So he probably didn't stand in 1970 (nine members
  retired); his intro now ends "then for Delting South." Options: leave, or check the 1970
  nominations list (ST 24 Apr 1970 p8 art. 068, OCR unreadable; needs a zoom). Evidence:
  `zcc-1960-1974.md`.

- **2026-09-29: Walls, May 1930: day, and was Halcrow unopposed?** Rev. T. Andrew resigned (ST 3
  May 1930). The re-constituted Council's first meeting filled the seat (ST 24 May 1930 p5 art.
  088), but the OCR is garbled; it mentions a petition "in favour of Mr William Hales, Spurries,
  Walls" (17 signatures?). The DB has Halcrow alone, dated 1 May (the wiki's "Thursday 20 May"
  was a Tuesday). Options: set Tue 20 May 1930 (the Council met on Tuesdays from then) and leave
  Halcrow unopposed; or zoom on the page (images don't render) or check the Shetland News.
  Election 577. Evidence: `zcc-1920-1939.md`.

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

- **2026-09-28: Day of James Budge's appointment for Dunrossness North, June 1907.** Reported in
  ST 22 Jun 1907 (p8 art. 131), but the report's opening, with the day, is garbled. The DB has
  1 Jun (month only); the wiki's "Thursday 16th June" was a Sunday. The Council met on
  Thursdays, so probably Thu 20 Jun (or 13 Jun). Options: set 20 Jun (inferred), or leave 1 Jun.
  Election 337. Evidence: `zcc-1900-1919.md`.

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
  art. 051) reports it accepted "At a meeting of the Town Council, held last ..."; the scan loses
  the word after "last". The item just above says the Commissioners met "last night" (Fri 18 Oct).
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

- **2026-09-28: Day of William Duncan (i)'s co-option for Bailie Hay, Nov 1884.** Election 34
  and his ledger row have 22 Nov, the paper's date. ST 15 Nov 1884 reports his election at a
  meeting after the annual meeting of Mon 10 Nov (C. G. Duncan, who died that night, died
  "since last meeting"), and he sat at the adjourned meeting of Tue 18 Nov (ST 22 Nov). The
  meeting's own day isn't in the OCR. Options: Fri 14 Nov (the council met on Friday evenings
  in the 1880s), check the minute book, or zoom on the ST 15 Nov p3 heading. It also shortens the
  4–22 Nov 1884 size-short row. Row `william-duncan-i` 1884-11-22. Evidence:
  `ltc-1884-1935-citations.md`.

- **2026-09-28: Day of Bailie John Campbell's resignation, May 1932.** His letter was read at the
  monthly meeting reported in the Shetland News of Thu 5 May 1932; the day is lost in the OCR
  and the Shetland Times isn't digitised for May–June 1932. The ledger now ends his 1931 row on
  Tue 3 May 1932 (the first Tuesday), unconfirmed. Options: accept 3 May and confirm, or zoom
  on the SN page. Evidence: `ltc-1884-1935-citations.md`.

### Party labels (added 2026-09-30; evidence `party-labels-ltc-*.md`)

A label row in `data/candidacy_labels.csv` that contradicts the DB fails the build unless it sets
`override=1`, so each of these needs your call before anything changes.

- **2026-09-30: William Sinclair, LTC Nov 1905: "Labour" or "Working Men's Association"?** The DB
  (from the wiki) has Labour. ST 11 Nov 1905 p4 a071 says the Working Men's Association "were
  running two candidates", and he and Ratter thanked the electors "as Working-Class
  Representatives" (p1 a007). Ratter now has "Working Men's Association". Options: (a) override
  Sinclair to "Working Men's Association", so the pair match; (b) keep Labour. Election 59.
  Evidence: `party-labels-ltc-1890-1913.md`.
- **2026-09-30: The four winners of LTC Nov 1907: keep "Ratepayers Association (unofficial)"?**
  Irvine, A. Smith, W. S. Smith and J. Smith signed a joint thanks notice (ST 9 Nov 1907 p1 a016)
  with no group name, and the Lerwick Ratepayers' Association wasn't formed until Oct 1908.
  Nothing in the paper supports the label. Options: (a) clear it (an override row with an empty
  label isn't allowed, so this would be a `fix_parse_errors.py` entry); (b) keep it if the wiki
  had another source. Election 61.
- **2026-09-30: Ratter and Sinclair, LTC Nov 1908: "Ratepayers Association" or "Socialist"?** The
  new Ratepayers' Association heard every candidate at its 2 Nov meeting (ST 7 Nov 1908 p5 a161):
  a hustings, not a slate. The count report says a voter "signed for the Socialists" (p4 a148) and
  letters call the Association a Socialist caucus. Options: (a) keep "Ratepayers Association";
  (b) override to "Socialist" (the paper's word, not their own); (c) clear both. Election 62.
- **2026-09-30: Robert John Groat, LTC Nov 1910: alias "Socialist/Democrat" to "Social-Democrat"?**
  ST 5 Nov 1910 p4 a077: "the nominee of the local Social-Democrat Party". Options: (a) add a
  `party_aliases.csv` row, treating the wiki's form as a spelling variant; (b) override the one
  row; (c) keep. Election 65.
- **2026-09-30: James Laing, LTC Nov 1906: Socialist?** "The Socialist section was well
  represented" with a circular (ST 10 Nov 1906 p4 a073), but no candidate is named. Laing was
  nominated by Pottinger and M. L. Manson, and the paper called him the Socialists' nominee in
  1909. Options: (a) label him Socialist, basis `inferred`; (b) leave blank. Election 60.
- **2026-09-30: James Robertson, LTC Nov 1903: Social-Democrat?** He spoke as a Social-Democrat in
  1901, but the 1903 reports (ST 7 Nov 1903 p4 a058) give no label. Options: (a) Social-Democrat,
  `inferred`; (b) leave blank. Election 56.
- **2026-09-30: Grierson and Ramsay, LTC Nov 1912: "Sunday Closing"?** A ratepayers' committee of
  clergymen "brought forward" them and H. J. Robertson, pledged against Sunday opening (ST 2 Nov
  1912 p4 a061); it is the Sunday Closing Committee of 1913, but isn't named in 1912. Stout and
  J. Smith also favoured Sunday closing but weren't its candidates. Options: (a) "Sunday
  Closing", `inferred`; (b) leave blank. Election 67.
- **2026-09-30: The 1921 LTC slates: label them Social Democrat and Moderate?** ST 5 Nov 1921 p4
  a083: "the Moderates and Social Democrats" issued circulars, but neither slate is listed. On the
  nominations (Laing and M. L. Manson nominated W. Pottinger; Bruce and Murray were 1920's Labour
  candidates) the Social Democrats were probably Bruce, Murray, A. S. Manson and W. Pottinger, and
  the Moderates J. T. J. Sinclair, Ollason, Goodlad, J. Smith, Ramsay and John Manson. Options:
  (a) label them `inferred`, "Social Democrat" and "Moderate"; (b) leave blank unless the
  circulars turn up (Shetland News for 3–10 Nov 1921 not yet checked). Election 78. Evidence:
  `party-labels-ltc-1914-1924.md`.
- **2026-09-30: The four retiring LTC councillors of Nov 1932: keep "Socialist"?** Bruce, A. S.
  Manson, Morrison and Sandison are "Socialist" in the DB. The paper calls them only "the four
  retiring Councillors" against the Ratepayers' nominees and Johnston, and their joint address
  (ST 29 Oct 1932 p4 a026) gives no party. They stood as Labour in 1925–29. Options: (a) keep;
  (b) clear (a `fix_parse_errors.py` entry, since an empty label row isn't allowed); (c) look in
  the Shetland News of 3 Nov 1932. Election 91. Evidence: `party-labels-ltc-1925-1938.md`.
- **2026-09-30: "Constitutional/Moderate" (LTC 1926) v "Constitutional" (1925, 1929).** The same
  side: in 1926 the paper says "the 'Constitutional' or 'Moderate' party". Options: (a) keep the
  DB's combined label (the chart shows it as its own band); (b) override the 1926 four to
  "Constitutional". Election 84.
- **2026-09-30: Unconfirmed LTC labels 1935–38.** Not in the paper for that year, and not
  contradicted: Ganson, Duffin, Nicol (Ratepayers Association) and T. A. Sinclair (Labour) in Nov
  1935; Halcrow (Ratepayers) and T. A. Sinclair (Labour) in the unopposed Nov 1936; the co-options
  of T. A. Sinclair (Jul 1936) and Williamson (Aug 1938), both Labour. Options: (a) keep (my
  default: all are consistent with the years either side); (b) clear the ones on co-options.
- **2026-09-30: Edward Reid, LTC Nov 1946: "Workers Party" or "Workers'"?** The result table tags
  him "(Workers)" and his notice says he "stood as a WORKERS' CANDIDATE" (ST 8 and 15 Nov 1946).
  No party of that name is mentioned. Options: (a) override to "Workers'"; (b) keep "Workers
  Party". Election 113. Evidence: `party-labels-ltc-1945-1975.md`.
- **2026-09-30: Inkster and Ollason, LTC May 1951: "Moderate" or "Independent"?** Their joint
  address has no party name; the paper calls them Independents (ST 13 Apr 1951 p4 a047) and then
  "Moderates" in quotes (4 May p4 a039). Options: (a) keep "Moderate"; (b) override to
  "Independent"; (c) clear. Election 119.
- **2026-09-30: The four non-Labour candidates of LTC May 1952: "Moderate", not "Independent"?**
  W. Anderson, Conochie, Peterson and A. H. Robertson were "nominated as Moderate candidates"
  in their own joint address (ST 2 May 1952 p5 a077); the paper says "four Moderate and four
  Socialist aspirants". The DB has all four as Independent. Options: (a) override to
  "Moderate" (my recommendation: it's their own word); (b) keep. Election 120.
- **2026-09-30: John R. Smith, LTC May 1966: Labour, not Independent?** "Messrs Adair and
  Morrison are Labour men, as is the fifth candidate to enter the fray, Mr John R. Smith" (ST 15
  Apr 1966 p5 a079). The DB has John Smith (ii) as Independent in 1966 and Labour in 1969
  (confirmed, "The Labour Group"). Options: (a) override 1966 to Labour; (b) keep. Election 139.
- **2026-09-30: Unconfirmed LTC "Independent" labels, 1953–73.** The paper gives no label for
  these candidates in that year (only a council count, or "contesting the election
  independently"): H. Gray, Burgess, J. D. Williamson (1953); Conochie, H. Gray, Bennet (1959);
  E. Gray, Shearer, A. Peterson (1963); G. Blance, Taylor, P. Robertson, Halcrow, Cumming,
  Georgeson (1967); and every candidate in the unopposed years 1956, 1957, 1965, 1966, 1971–73.
  Options: (a) keep (my default: each was on the non-Labour side, and most were labelled
  Independent in the years either side); (b) clear the ones never labelled in any year.

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)
