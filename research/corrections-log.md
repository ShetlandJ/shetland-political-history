# What we had wrong

A running list of things the research turned out to contradict: what the wiki, our notes or the
generated ledger said, what the sources show, and whether the site text has been fixed. Newest
first. Evidence links are in the `research/bna/` file named in each entry.

**Where it was wrong:**
- **wiki**: the text on shetlandhistory.com or a person page
- **notes**: research notes (CLAUDE.md, earlier findings)
- **draft**: the ledger's `confirmed=0` rows, drafted by the generator

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
| 6 | wiki | Halcrow a County Councillor "between 1955 and 1961" | ST 17 Apr 1964: "one three-year term", then did not re-stand, which reads as 1955–58 | **Open**: not changed; check the 1958 ZCC election |
| 7 | wiki | LTC general elections 1949–1964 dated on **Mondays** (e.g. 1960-05-02, 1963-05-06) | Polling was on Tuesdays: "Tuesday, 3rd May" 1960 (ST 15 Apr 1960), "Tuesday, 7th May" 1963 (ST 19 Apr 1963), "Tuesday" 1958 | Fixed 2026-09-27: see "LTC polling days 1949–1964" |

## 2026-09-27: LTC 1912–1914 (`research/bna/ltc-1912-1914.md`)

| # | Where | We had | Sources show | Status |
|---|---|---|---|---|
| 1 | wiki, notes | William MacDougall resigned in **April 1912** | He resigned on 2 Apr 1912 but withdrew it within days. He resigned for good on **Tue 8 Oct 1912** (ST 6 Apr, 5 Oct, 12 Oct 1912) | Fixed: ledger, notes, and the person page intro (`fix_newspapers.py` #4) |
| 2 | notes | The council sat at **11 from 1912 until 1919** | **12 throughout.** The retiring lists and attendance for 1912, 1913 and 1914 add up to 12 | Fixed |
| 3 | notes | At the Nov 1912 election **all 5 got full terms**, with no short-term re-standing | John Smith (i)'s 1912 seat was a **two-year** one (retired and re-elected in 1914), and **Goodlad retired a year early** in 1913. The papers don't say why | Ledger fixed |
| 4 | draft | Provost Arthur Laing's seat ended Nov 1912, and Loggie sat until Nov 1913 | Laing **stayed on as Provost until Nov 1913**. Loggie retired in his place in Nov 1912. The same "Provost stays on" rule as 1932 and 1937 | Ledger fixed. The wiki profiles were already right |
