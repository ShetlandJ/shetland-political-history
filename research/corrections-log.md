# What we had wrong

A running list of things the research turned out to contradict: what the wiki, our notes or the
generated ledger said, what the sources show, and whether the site text has been fixed. Newest
first. Evidence links are in the `research/bna/` file named in each entry.

**Where it was wrong:**
- **wiki**: the text on shetlandhistory.com or a person page
- **notes**: research notes (CLAUDE.md, earlier findings)
- **draft**: the ledger's `confirmed=0` rows, drafted by the generator

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
