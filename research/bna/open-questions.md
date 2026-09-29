# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-09-29: Yell North, May 1955: Youngman 118 or 119?** The ST 13 May 1955 (p4 art. 061)
  OCR has "AMANDA YOUNGMAN 118", Stewart 62; the wiki and DB have 119 and a turnout of 181 (119 +
  62). "Over 47 per cent of 383" fits either. Options: leave 119, or zoom on the page (images
  don't render in the viewer). Election 771. Evidence: `zcc-1940-1959.md`.

- **2026-09-29: Is Gulberwick 1951's Prophet Smith the `prophet-smith` person page?** He stood as a
  Socialist against Nicolson (ST 11 May 1951, 98 to 49). The new candidacy is unlinked. Options:
  link to person 398 (`fix_newspapers.py` #51), or leave. Election 722. Evidence: `zcc-1940-1959.md`.

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

- **2026-09-28: End of Provost James Smith (i)'s seat, June 1941.** His letter resigning as
  Councillor and Provost "three weeks from this date" was read and accepted at the meeting of Tue
  3 Jun 1941; the letter's date isn't printed, but Shearer had learnt of it the night before
  (ST 7 Jun 1941). Johnson was co-opted Tue 1 Jul (ST 5 Jul 1941). The confirmed row ends 1 Jul.
  Options: 3 Jun (acceptance, the Hunter 1887 convention), about 23 Jun (three weeks from a
  2 Jun letter; the Linklater 1941 convention), or keep 1 Jul. No issue depends on it.
  Row: `james-smith-i` 1934-11-06. Evidence: `ltc-1940-1945.md`.

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

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)
