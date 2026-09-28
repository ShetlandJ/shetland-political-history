# What we had wrong

A running list of things the research turned out to contradict: what the wiki, our notes or the
generated ledger said, what the sources show, and whether the site text has been fixed. Newest
first. Evidence links are in the `research/bna/` file named in each entry.

**Where it was wrong:**
- **wiki**: the text on shetlandhistory.com or a person page
- **notes**: research notes (CLAUDE.md, earlier findings)
- **draft**: the ledger's `confirmed=0` rows, drafted by the generator

## 2026-09-28: ZCC 1900–1919 (`research/bna/zcc-1900-1919.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Delting South Apr 1903 dated 1 Apr (wiki "Saturday 18 April"); Northmavine North Sep 1903 dated Sat 19 Sep | Appointed at Council meetings on Thu 16 Apr and Thu 17 Sep 1903 (ST 18 Apr, 19 Sep 1903) | Fixed: `fix_newspapers.py` #32 |
| 4 | wiki | Sandwick Dec 1907: Smith 91, Thomson 70 (electorate 276, turnout 161) | Smith unopposed; the figures are 1904's, copied (ST 23 and 30 Nov 1907) | Fixed: `fix_newspapers.py` #34. Wiki table still has the copy |
| 3 | wiki | Walls Feb 1905 "February 21"; Yell North "Saturday 19th March" 1906; Dunrossness South "Thursday 16th August" 1907 | Tue 14 Feb 1905 (the fixed day); Thu 15 Feb 1906; Thu 15 Aug 1907 (ST 21 Jan 1905, 17 Feb 1906, 24 Aug 1907) | Fixed: `fix_newspapers.py` #32 |
| 2 | wiki | Delting North Dec 1904: Pole 31 beat **Arthur White** 13 | Pole beat **James Inkster**, the sitting member (ST 26 Nov, 10 Dec 1904); White was returned unopposed for Northmavine South | Fixed: `fix_newspapers.py` #33. Wiki table still says White |

## 2026-09-28: ZCC 1890–1899 (`research/bna/zcc-1890-1899.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Eleven 1890s by-election dates (mostly the 1st of the month or the paper's Saturday) | The fixed election day or the Council meeting that appointed: e.g. Goudie 16 Dec 1890 (not Jan 1891), Tingwall 4 Feb 1897 (not Mar), Northmavine North 3 Nov 1897 (not Dec), Delting South 24 Jan 1899 (not Feb) | Fixed: `fix_newspapers.py` #30. Wiki page titles still give the old months |
| 2 | wiki, notes | Dunrossness North: Whyte appointed "Thursday 1 August" 1898 | Put off on 4 Aug "till the September meeting"; appointed then (ST 6 Aug, 3 Sep 1898), so Thursday 1 September | Fixed: #30; open question closed |
| 3 | wiki | Burra, Feb 1898: Lennie 113 votes, Henderson 59, Henderson elected | No poll: petition signatures; the Council chose Henderson (ST 5 Feb 1898). The wiki table says so; the parser read the petitions as votes | Fixed: `fix_parse_errors.py` #10 |
| 4 | wiki | Walls North (Sandness): no by-election between Feb 1890 (no nomination) and Dec 1892 | Still vacant in May 1890, when the Council deferred it (ST 24 May 1890). The wiki's Sandness page has "1890 - Vacant" | No change: the DB was right |
| 5 | wiki | Aithsting Dec 1892: McCullie unopposed | Grierson was nominated too; McCullie "had practically a walk over" (ST 10 Dec 1892) | Open question |
| 7 | parse | Delting North May 1890 dated 1 May | The wiki text says "took place on May 22" (first Council meeting, ST 24 May 1890) | Fixed: `fix_parse_errors.py` #11 |
| 6 | wiki | Whiteness and Weisdale 1890 electorate 126; Nesting 83 | Whiteness: 116 (86 men, 30 women), ST 8 Feb 1890; the wiki's own breakdown adds up to 116. Nesting: the OCR's 93 is a misread; the wiki's 77 + 6 = 83 | Whiteness fixed: `fix_newspapers.py` #31. Nesting unchanged |

## 2026-09-28: LTC co-options 1946 and 1950 (`research/bna/ltc-1946-1950-citations.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | T. A. Sinclair co-opted for David Gray in **May 1946** (DB Wed 22 May) | Elected at the council meeting of **Tue 2 Apr 1946** (ST 5 Apr 1946) | Fixed: `fix_newspapers.py` #29, ledger. The wiki page title still says May |

## 2026-09-28: LTC citations 1884–1935 (`research/bna/ltc-1884-1935-citations.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Nov 1893 general on **Thu 2 Nov** | **Tue 7 Nov 1893** ("on Tuesday next, the day of election", ST 4 Nov 1893); the 2nd was nomination day | Fixed: `fix_newspapers.py` #26, ledger |
| 2 | wiki | Sinclair Johnson elected for Garriock on **2 May 1899** | Elected **Tue 4 Apr 1899** (ST 8 Apr 1899); 2 May was when he took his seat | Fixed: #27, ledger |
| 3 | wiki | Reid Tait co-opted for Goodlad on **Mon 5 May 1924** | **Tue 6 May 1924**, the monthly meeting (ST 10 May 1924) | Fixed: #28, ledger |
| 4 | notes, wiki | Duncan co-opted for Hay on **22 Nov 1884** (CLAUDE.md "Confirmed DB corrections") | 22 Nov is the paper's date. He was elected at a meeting between Tue 11 and Fri 14 Nov (ST 15 Nov) and sat on 18 Nov (ST 22 Nov) | Not changed: day in open-questions |
| 5 | draft | Bailie John Campbell sat until Morrison's co-option, **7 Jun 1932** | His resignation letter was read at the **May 1932** monthly meeting (Shetland News 5 May 1932) | Ledger end moved to 3 May 1932 (unconfirmed) |

## 2026-09-28: LTC 1874–1883 (`research/bna/ltc-1874-1883.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Nov 1881 general on **8 Nov 1881** | **Tuesday 1 Nov 1881** (Town Clerk's notice, ST 22 Oct 1881; "Tuesday first", ST 29 Oct 1881) | Fixed: `fix_newspapers.py` #25, ledger (10 dates) |
| 2 | notes, wiki | Arthur Laurenson **declined office** in 1879 (CLAUDE.md groups him with Goudie and Hay), and the wiki lists him as a 1879 candidate | He "would not allow himself to be re-nominated" (ST 1 Nov 1879), so he was never a candidate; his vacancy went to Goudie at the 1880 general | Not changed: the DB candidacy (not elected) and `not_seated.csv` row still stand. James's call |
| 3 | notes | James Tulloch's 1881 row cited the 1884 retiring list | He wasn't in it: his 1881 seat was the one-year fifth vacancy, re-nominated in 1882 (ST 29 Oct 1881, 4 Nov 1882) | Ledger source fixed |

## 2026-09-28: LTC 1965–1970 (`research/bna/ltc-1965-1970.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Robert A. Anderson (i) died **26 June 1967** | Died at home **Sunday 25 June 1967** (obituary, ST 30 Jun; death notice "on 25th June", ST 7 Jul 1967) | Fixed: `fix_newspapers.py` #24 (`died_date`), ledger |
| 2 | wiki | Grace Halcrow's co-option for Nicolson on **12 May 1967** | Co-opted at the statutory meeting **Friday 5 May 1967** (ST 12 May 1967; the 12th is the paper's date) | Fixed: `fix_newspapers.py` #24, ledger |
| 3 | draft | Provost Nicolson sat until Halcrow's co-option (`replaced`) | He **resigned through ill-health from 27 April 1967** (ST 14 Apr 1967), before the May general | Ledger fixed |

## 2026-09-28: LTC 1945–1956 (`research/bna/ltc-1945-1956.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Nov 1946 general on **Mon 4 Nov 1946** | Polled on **Tuesday 5 Nov** ("Tuesday's choice", ST 1 Nov; "the municipal election on Tuesday", ST 8 Nov 1946) | Fixed: `fix_newspapers.py` #23, ledger (10 dates) |
| 2 | draft | Brownlie's seat ended at Johnson's co-option, reason `replaced` | He **resigned**: ill and on six months' leave earlier in 1949, then transferred south; the letter was read at the same meeting that co-opted Johnson (ST 16 Sep 1949) | Ledger fixed |

## 2026-09-28: LTC 1894–1910 (`research/bna/ltc-1894-1907.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Nov 1901 general on **Fri 1 Nov 1901** | Polled on **Tue 5 Nov 1901** ("Municipal Election on TUESDAY NEXT", ST 2 Nov 1901) | Fixed: `fix_newspapers.py` #21, ledger (7 dates) |
| 2 | wiki | Loggie's "By-Election January 1910" on 1 Jan 1910 | Appointed at a special meeting on **Tue 4 Jan 1910** (ST 8 Jan 1910) | Fixed: `fix_newspapers.py` #22, ledger |
| 3 | draft | Goudie's 1903 seat ended 1906 and J. T. J. Sinclair's 1904 seat ran to 1907 | Goudie, as Provost, "had to serve a period of three years", so Sinclair, the lowest winner of 1904, retired in **1906** in his place; Goudie retired in **1907** (ST 6 Oct 1906, 5 Oct 1907) | Ledger fixed. The wiki profiles were already right |
| 4 | draft | John Irvine (iii) sat until Loggie's appointment | His resignation letter was read at the meeting of **Tue 7 Dec 1909** (ST 11 Dec 1909) | Ledger fixed. His profile (Dec 1909) was right |

## 2026-09-28: LTC 1919–1921 (`research/bna/ltc-1919-1921.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | J. M. Goodlad co-opted for Pottinger on **11 Jun 1921** | Co-opted at the monthly meeting of **Tue 7 Jun 1921**, on the Provost's casting vote (5–5 against William Bruce) (ST 11 Jun 1921). 11 Jun was the paper's date | Fixed: `fix_newspapers.py` #20, ledger |
| 2 | draft | Bailie W. Sinclair "retired" on 1 Oct 1921 | His resignation as Magistrate and Councillor was read at the monthly meeting of **Tue 4 Oct 1921**; he said he had too many other public duties (ST 8 Oct 1921) | Ledger fixed |
| 3 | notes | Open question whether Provost Goodlad left in 1919–20 ("at the end of the war") | He sat on to Nov 1920 and retired by rotation (ST 16 Oct 1920); he was "Ex-Provost" by May 1921, with Ganson as Provost | Ledger confirmed |

## 2026-09-28: LTC 1940–1945 (`research/bna/ltc-1940-1945.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Mouat co-opted for W. Sinclair ("By-Election **August** 1940") on 6 Aug 1940 | Co-opted at the meeting of **Tue 2 Jul 1940** (ST 6 and 13 Jul 1940). Sinclair had resigned in writing on 24 May and the seat was declared vacant in early June (ST 25 May, 8 Jun 1940) | Fixed: `fix_newspapers.py` #19, ledger |
| 2 | wiki | T. A. Sinclair co-opted ("By-Election April 1942") on 9 Apr 1942 | At a special meeting on **Tue 31 Mar 1942** (ST 4 Apr 1942) | Fixed (same) |
| 3 | wiki | Morrison co-opted for Prophet Smith on **Mon 5 Feb 1945** | At the monthly meeting of **Tue 6 Feb 1945** (ST 9 Feb 1945) | Fixed (same) |
| 4 | wiki | Nov 1945 general on **Mon 5 Nov 1945** | Polled on **Tuesday 6 Nov 1945** (ST 9 Nov 1945) | Fixed (same), 12 ledger dates |
| 5 | draft | Gear, Inkster and Prophet Smith sat until their successors were co-opted | Gear resigned (read **2 Feb 1943**, postmaster at Lyness), Inkster **3 Oct 1944** (leaving for the south), P. Smith **9 Jan 1945** (letter of 2 Jan, Greenock) (ST 6 Feb 1943, 6 Oct 1944, 12 Jan 1945) | Ledger fixed |
| 6 | notes | Provost J. A. Smith (i) left at the 1 Jul 1941 co-option (confirmed row, from his profile) | His letter giving **three weeks' notice** was accepted at the meeting of **Tue 3 Jun 1941** (ST 7 Jun 1941), so he probably left about 23 Jun | Not changed: open question |

## 2026-09-28: LTC 1936–1945 (`research/bna/ltc-1936-1945.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Dalziel's co-option for Clausen ("By-Election February 1938") on **Thu 3 Feb 1938** | At the monthly meeting "on Tuesday evening", **Tue 1 Feb 1938** (ST 5 Feb 1938) | Fixed: `fix_newspapers.py` #18, ledger |
| 2 | draft | Clausen sat until Dalziel replaced him on 3 Feb 1938 | His resignation letter was read at the meeting of **Tue 4 Jan 1938**; he had moved to Thurso (ST 1 and 8 Jan 1938) | Ledger fixed |
| 3 | draft | James Laing sat until Johnston replaced him on 10 Dec 1945 | He **retired from 29 Nov 1945** (ST 16 Nov 1945) | Ledger fixed |

## 2026-09-28: LTC 1934–1938 (`research/bna/ltc-1934-1938.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | draft | **Cogle** sat until Sinclair's co-option on 7 Jul 1936 | He **resigned at the meeting of Tue 5 May 1936** over the water question and refused to reconsider (ST 9 May 1936); the seat was empty two months | Ledger fixed. Wiki intro ("until 1936") is fine |
| 2 | notes | **Johnston** resigned at some point between Nov 1933 and Nov 1934 | Resignation (ill-health) read at the meeting of **Tue 9 Oct 1934** (ST 13 Oct 1934); the Clerk also listed him as due to retire in Nov 1934, so his 1932 seat was two-year, like Sandison's | Ledger fixed |

## 2026-09-28: LTC 1886, Duncan and Harrison (`research/bna/ltc-1886-duncan-harrison.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | notes, draft | John Harrison (i) **disqualified 1 Oct 1886** ("Newspaper 23 Oct 1886" in CLAUDE.md) | Still a member after the 15 Oct meeting: "one member will be disqualified" (ST 16 Oct 1886). Cause not given | CLAUDE.md fixed; ledger date in open-questions |
| 2 | notes | Duncan **resigned 12 Jul 1886** | His letter was read that day, to take effect three weeks later; the council tried to keep him and only on **15 Oct** was his refusal reported (ST 17 Jul, 16 Oct 1886). His profile already says so | Ledger date in open-questions |

## 2026-09-28: LTC general dates 1885–1891 (`research/bna/ltc-general-dates-1885-1891.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Nov 1889 general on **Sat 9 Nov 1889** | Polled on **Tue 5 Nov 1889** ("municipal election on Tuesday next", ST 2 Nov 1889); the 9th is the day the result was printed | Fixed: `fix_newspapers.py` #17, ledger (8 dates) |
| 2 | wiki | Nov 1888 general on **Thu 8 Nov 1888** | Nominations closed **Thu 1 Nov**, unopposed; the paper calls that day the election (ST 3 Nov 1888). The 8th fits nothing | Not changed: 1 Nov or Tue 6 Nov is in open-questions |

## 2026-09-28: LTC 1885–1889 (`research/bna/ltc-1885-1889.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | notes (confirmed row) | **Stove's 1883 seat ended Nov 1885** (a confirmed April 2026 row) | Still sitting after the Nov 1885 election, at the statutory meeting (ST 21 Nov 1885); retiring in 1886 (ST 23 Oct 1886). All five 1883 winners had full terms | Ledger fixed |
| 2 | draft | **Jamieson's 1886 seat ended Nov 1887** | He retired in 1888 and didn't stand, having left town (ST 6 Oct, 3 Nov 1888). CLAUDE.md already said so; the ledger didn't | Ledger fixed |
| 3 | notes, draft | **James Hunter (ii)** left "early 1887", reason unknown | Resigned on moving to the Union Bank at Portsoy; accepted **Tue 4 Jan 1887** (ST 11 Dec 1886, 8 Jan 1887) | Ledger and CLAUDE.md fixed |
| 4 | wiki | Porteous by-election **24 Mar 1887**, replacing James Hunter (iv) (a GP born 1914) | Elected at the meeting of **Fri 18 Mar 1887** (ST 19 Mar 1887), in the room of James Hunter (ii) | Fixed (`fix_newspapers.py` #16) |
| 5 | wiki | Robertson and Anderson by-election **24 Nov 1886** | Co-opted **Fri 19 Nov 1886** (ST 20 Nov 1886) | Fixed (same) |
| 6 | notes, draft | Mitchell **retired** "before Oct 1889" (1 Sep in the ledger) | **Resigned**; accepted at a Town Council meeting reported ST 19 Oct 1889, probably Fri 18 Oct | Ledger fixed (day in open-questions) |
| 7 | wiki | ZCC Dec 1919: Joseph Peterson (i) elected for Delting North as well as Delting South | The wiki's own table has him losing Delting North (15 v 29); his page says Delting South only | Fixed (`fix_parse_errors.py` #7) |
| 8 | wiki | ZCC Dec 1922: John Leslie (ii) elected for Aithsting and for Sandsting | One combined Aithsting & Sandsting ward that year (wiki text) | Fixed (`fix_parse_errors.py` #8) |
| 9 | draft | Arthur Hay sat 4–22 Nov 1884 (ledger row) | He topped the poll and declined office, so the seat was vacant until Duncan's co-option on 22 Nov (James, 2026-09-28; `not_seated.csv`) | Ledger row removed; shows as a dated vacancy |
| 10 | wiki | ZCC Burra by-election **1 Apr 1920** replacing William Sinclair, who sat for Burra from Dec 1919 | Sinclair was returned for Burra and Whiteness & Weisdale and sat for Whiteness; Anderson appointed to Burra **15 Apr 1920** (wiki text) | Fixed (`fix_parse_errors.py` #9, `not_seated.csv`) |

## 2026-09-28: ZCC by-elections, who was replaced (`research/bna/zcc-by-elections.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Yell South by-election Aug 1950 replaced **George Ross** | Ross resigned **Tingwall** (accepted 20 Jun 1950); **George Spence** resigned Yell South to contest Tingwall (ST 23 Jun, 21 Jul, 18 Aug 1950) | Fixed: `fix_newspapers.py` #15. The wiki prose was right; the replaced-member field wasn't |
| 2 | wiki | Nesting Nov 1920 page links **James Hunter (iv)** (a GP born 1914) and **Robert Hunter (i)** (the Lerwick bank agent) | The councillor who died was James Hunter (iii), and his brother Robert Hunter (ii) was appointed. Both person pages say so | Fixed in the DB (`fix_parse_errors.py` #5), and Robert Hunter (ii)'s intro link to his brother now goes to James Hunter (iii) (#6). The wiki election page and Robert's wiki page still have the wrong links |
| 3 | draft | Thomas Anderson (ii) left Bressay in **Feb 1921**; John Williamson (iv) left Yell South in **May 1951** | Neither did. `build.py` matched a dead member's by-election to a namesake by name. Bressay ran to Dec 1922 and Yell South to May 1952, as their intros say | Fixed (`build.py`) |
| 4 | draft | Burra 1898, Dunrossness North 1898, Aithsting 1932 and Gulberwick 1945 replaced members linked to namesakes from other eras | The wiki text names Rev. David Gray (i), Robert Henderson (ii), Capt. James Anderson (ii) and Rev. George Smith (ii) | Fixed (`fix_parse_errors.py` #5) |

## 2026-09-28: Shearer's resignation from the Town Council, June 1947 (`research/bna/ltc-1947-shearer-dalziel.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | notes | ST 6 Jun 1947's report of the 3 Jun meeting has "no resignation mentioned", so Shearer's seat was held to 1 Jul | The same issue reports "Lt.-Col. Magnus Shearer has resigned from the council" (he had been appointed county councillor for Whalsay), with the vacancy to be filled in July | Fixed: ledger ends 1947-06-03 (found by James) |

## 2026-09-28: Shearer's County Council appointment, May 1947 (`research/bna/ltc-1947-shearer-dalziel.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Whalsay and Skerries by-election, **May 1947** (dated the 1st) | An appointment on petition (207 signatures) at the County Council meeting of **Tue 20 May 1947** (ST 23 May 1947) | Fixed: `fix_newspapers.py` #14 |

## 2026-09-28: LTC May 1968 polling day (`research/bna/ltc-1968-polling-day.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | May 1968 Town Council election on **Thursday 2 May 1968** | "Polling takes place in Lerwick on **7th May**" (ST 19 Apr 1968), a Tuesday. Votes as the wiki has them (ST 10 May 1968) | Fixed: `fix_newspapers.py` #13, ledger (8 dates) |

## 2026-09-28: ZCC polling days 1949–1964 (`research/bna/zcc-election-dates-1949-1964.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | County Council generals on **Mon 9 May 1949, Mon 5 May 1952, Thu 5 May 1955, Mon 12 May 1958, Mon 8 May 1961, Mon 11 May 1964** | All on a **Tuesday**: 10 May 1949, **13 May 1952** (a week later, not the day after), 10 May 1955, 13 May 1958, 9 May 1961, 12 May 1964 | Fixed: `fix_newspapers.py` #12 (144 ward rows); ZCC terms follow |

## 2026-09-28: LTC 1969–1973 (`research/bna/ltc-1969-1973.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | "By-Election **May** 1970" (Adair for J. R. Smith) dated **8 Aug 1970** | Smith resigned **Tue 14 Apr 1970**; Adair co-opted at the statutory meeting of **Fri 8 May 1970** (ST 17 Apr, 15 May 1970) | Fixed: `fix_newspapers.py` #11, ledger |
| 2 | wiki | Grace Halcrow a town councillor "from 1964 till the **late 1960s**" | She retired by rotation in **May 1970** and did not re-stand (ST 13 Mar, 17 Apr 1970) | Fixed: intro (`fix_newspapers.py` #5) |
| 3 | draft | Every 1966–71 winner sat 3 years | Provost E. Gray stayed on 1969–71 (W. A. Smith retired in his place in 1969); Halcrow's 1968 seat ended 1970; Provost W. A. Smith stayed on in 1972 (Tait and Peterson retired a year early); E. Gray and Butler had 2-year seats from 1971 | Ledger fixed (34 rows) |

## 2026-09-28: LTC 1908–1910 (`research/bna/ltc-1908-1910.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | draft | Provost Arthur Porteous's seat ended Nov 1908, and Ganson (1906) and J. Smith (1907) each sat a full 3 years | Porteous **stayed on as Provost until Nov 1910**. Ganson retired in his place in 1908 and J. Smith a year early in 1909 (ST 17 Oct 1908, 16 Oct 1909, 8 Oct 1910). The wiki profile's "1887 to 1910" was already right | Ledger fixed |

## 2026-09-28: LTC 1895–1901 (`research/bna/ltc-1895-1901.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | draft | Alfred Stove took the short seat (Charles Robertson's) in Nov 1895; Hunter sat 1895–98 | **Hunter** had the short seat and retired in 1896; Stove sat to 1898 (ST 24 Oct 1896, 29 Oct 1898) | Ledger fixed |
| 2 | draft | Leisk retired in 1897 with his 1894 cohort; Halcrow sat 1895–98 | **Halcrow** retired a year early in 1897; Provost Leisk stayed on to 1898 (ST 16 Oct 1897, 29 Oct 1898) | Ledger fixed |
| 3 | draft | Kay sat 1898–1901; Sinclair Johnson's 1899 seat ended in 1900 | Only three were due out in 1900, so **Kay volunteered to retire**; Johnson retired in 1901 (ST 13 Oct 1900, 5 Oct 1901) | Ledger fixed |

## 2026-09-27: LTC wartime resignations 1941 (`research/bna/ltc-wartime-1941.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | draft | Thomas Irvine (ii) sat until his death on 6 Jun 1946 | He **resigned**, accepted 5 Aug 1941, having not attended since Dec 1939 (ST 9 Aug 1941) | Ledger fixed |
| 2 | wiki | Joseph Linklater replaced at the Oct 1941 by-election | He **resigned** by letter of 26 Jul 1941, effective three weeks later (ST 9 Aug 1941) | Ledger fixed |
| 3 | wiki | Gear and Inkster co-opted on **Thu 9 Oct 1941** | "At Tuesday's meeting": **Tue 7 Oct 1941** (ST 11 Oct 1941) | Fixed: `fix_newspapers.py` #10 |

## 2026-09-27: LTC 1945–1953 (`research/bna/ltc-1945-1953.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Not recorded: **Magnus Shearer (i) resigned** from the Town Council in June 1947, and **Peter Dalziel was co-opted** in his place | ST 4 Jul 1947; Dalziel then stood down in Nov 1947 (ST 10 Oct 1947) | Ledger fixed (both rows unconfirmed: exact dates not found). No by-election page on the wiki |
| 2 | wiki | Peter Dalziel's 1938 seat ran to 1947 | He retired in **Nov 1946** (ST 4 Oct 1946); the intro's "1938 to 1946" was right | Ledger fixed |
| 3 | draft | Thomas Irvine (ii) sat until his death in June 1946 | **Not on the council by Oct 1945**: the six staying on that month don't include him (ST 5 Oct 1945) | Open: wartime item |
| 4 | draft | Every 1945–52 winner sat 3 years | The lowest winners got short seats (R. Anderson and Morrison 1945; W. Anderson and Johnson 1946; Williamson 1947; Johnson 1950). **Treasurer Ollason** was kept on to 1951 and **Provost R. A. Anderson** to 1953 | Ledger fixed (27 rows) |

## 2026-09-27: Grace Halcrow on the County Council (`research/bna/grace-halcrow-county.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Halcrow County Councillor for Cunningsburgh "between 1955 and 1961" | **1955–58**. Opposed by Joan McLeod in 1958, she withdrew before the poll and McLeod was returned unopposed (ST 25 Apr, 9 May 1958) | Fixed: intro (`fix_newspapers.py` #9). Election data was already right |

## 2026-09-27: LTC May 1954 result (`research/bna/ltc-1954-result.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | **May 1954**: Morrison, Halcrow, Blance, Ollason returned unopposed | A poll of 1913 (effective electorate 3918). Morrison 1031, Halcrow 1013, Blance 1010, Ollason 929; **Senior Bailie John N. Inkster lost his seat** (813), then Strachan 719, J. B. A. Sutherland 704, Gair 692 (ST 7 May 1954) | Fixed: `fix_newspapers.py` #8 |
| 2 | wiki | **"James Inkster"** elected in May 1951 (no person page) | It was **John N. Inkster**, Cairnfield, junior Bailie and retiring member (ST 13 Apr 1951), i.e. John Inkster (ii), sitting since 1947 | Fixed: candidacy relinked (`fix_newspapers.py` #8), ledger |

## 2026-09-27: LTC polling days 1949–1964 (`research/bna/ltc-election-dates-1949-1964.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | Twelve LTC generals 1949–1964 dated on the **Monday** | Every one was on the **Tuesday** (ST each year, see evidence) | Fixed: `fix_newspapers.py` #7, ledger (102 dates) |
| 2 | wiki | Arthur Johnson co-opted for Brownlie on **Mon 12 Sep 1949** | Reported with the monthly meeting "on Tuesday", **13 Sep 1949** (ST 16 Sep 1949) | Fixed (same). The co-option report doesn't name the day itself |
| 3 | wiki | **May 1954** election uncontested: the four winners, no votes | **Contested**: "eight candidates" (ST 23 Apr 1954); Blance was "third … 21 votes behind the top of the poll" (ST 14 May 1954) | Fixed 2026-09-27: see "LTC May 1954 result" |

## 2026-09-27: LTC 1955–1965 (`research/bna/ltc-1955-1965.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki | **Grace Halcrow** returned unopposed in May 1957, and a councillor "from 1957" | The 1957 four were Blance, Morrison, **Andrew Nicolson** and Ollason (ST 19 Apr 1957). Halcrow sat 1954–55, resigned for the county council, and came back in 1964 (ST 17 Apr 1964) | Fixed: candidacy and intro (`fix_newspapers.py` #5), ledger |
| 2 | wiki | "By-Election **May** 1961": Strachan for Thomson on 5 May | Thomson resigned on 5 May; Strachan was **co-opted on 13 June 1961** (ST 12 May, 16 Jun 1961) | Fixed (`fix_newspapers.py` #6) |
| 3 | wiki | Not recorded: **John Eunson resigned** in January 1958 | Resignation accepted 14 Jan 1958; seat left empty until May (ST 17 and 24 Jan 1958) | Ledger fixed |
| 4 | wiki | Magnus Sandison sat until the Sept 1959 by-election | He **resigned 11 Aug 1959** (ST 14 Aug 1959) | Ledger fixed |
| 5 | draft | Every 1955–64 winner sat 3 years | Uncontested years broke the rotation; the council **drew lots** (1958: Burgess, Johnson; 1959: Nicolson) and gave short seats to the lowest winners. Provosts Conochie (1956–59) and Blance (1959–62) stayed on | Ledger fixed (20 rows) |
| 6 | wiki | Halcrow a County Councillor "between 1955 and 1961" | ST 17 Apr 1964: "one three-year term", then did not re-stand, which reads as 1955–58 | Fixed 2026-09-27: see "Grace Halcrow on the County Council" |
| 7 | wiki | LTC general elections 1949–1964 dated on **Mondays** (e.g. 1960-05-02, 1963-05-06) | Polling was on Tuesdays: "Tuesday, 3rd May" 1960 (ST 15 Apr 1960), "Tuesday, 7th May" 1963 (ST 19 Apr 1963), "Tuesday" 1958 | Fixed 2026-09-27: see "LTC polling days 1949–1964" |

## 2026-09-27: LTC 1912–1914 (`research/bna/ltc-1912-1914.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki, notes | William MacDougall resigned in **April 1912** | He resigned on 2 Apr 1912 but withdrew it within days. He resigned for good on **Tue 8 Oct 1912** (ST 6 Apr, 5 Oct, 12 Oct 1912) | Fixed: ledger, notes, and the person page intro (`fix_newspapers.py` #4) |
| 2 | notes | The council sat at **11 from 1912 until 1919** | **12 throughout.** The retiring lists and attendance for 1912, 1913 and 1914 add up to 12 | Fixed |
| 3 | notes | At the Nov 1912 election **all 5 got full terms**, with no short-term re-standing | John Smith (i)'s 1912 seat was a **two-year** one (retired and re-elected in 1914), and **Goodlad retired a year early** in 1913. The papers don't say why | Ledger fixed |
| 4 | draft | Provost Arthur Laing's seat ended Nov 1912, and Loggie sat until Nov 1913 | Laing **stayed on as Provost until Nov 1913**. Loggie retired in his place in Nov 1912. The same "Provost stays on" rule as 1932 and 1937 | Ledger fixed. The wiki profiles were already right |
