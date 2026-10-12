# ZCC roll call: checking every ward's member against the papers

Researched 2026-10-12 in the British Newspaper Archive (Shetland Times). Every link opens the
BNA viewer on the article; click **Articles** for the OCR text.

Question: are any Zetland County Council members or by-elections missing from `council_terms`?
The size checks on /data-review can't see a missing pair (a member who left and the one who
replaced him, both absent from the wiki): the ward still shows one member. So the members the
papers name are compared with `council_terms`.

Method:
1. **Eve of each general.** The nominations reports mark the sitting member in each ward (1925,
   1928, 1932 fully; other years in part). Each one was compared with who `council_terms` has
   holding that ward the day before the poll.
2. **Manifest sweep** of every issue 1890–1975 for headings about vacancies, new members,
   petitions, co-options and resignations, checked against the known by-elections. (In progress;
   results below when done.)

## 1. Eve-of-general comparison

- **1892, 1898, 1910, 1925, 1928, 1932, 1935**: every sitting member the paper names agrees with
  `council_terms`.
  - ST Sat 14 Nov 1925, p4 (art. 092): all 27 wards with "(sitting member)".
    https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19251114&page=0004&article=092
  - ST Sat 17 Nov 1928, p5 (art. 135): "* Denotes the sitting member".
    https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19281117&page=0005&article=135
  - ST Sat 19 Nov 1932, p4 (art. 028): "retiring members being marked with an asterisk".
    https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19321119&page=0004&article=028
  - ST Sat 19 Nov 1910, p4 (arts. 063, 067): the only changes were Yell North, Lerwick South and
    Cunningsburgh.

### 1907: the two Sandisons are in each other's wards

The DB has Thomas J. Sandison winning **Unst South** and John Peter Sandison **Yell North** at the
Dec 1907 general, each moving ward and moving back in 1910. The papers have each staying put:

- **ST Sat 14 Dec 1907, p8 (art. 125)**, "The New County Council", the complete list of members:
  "Unst South J. P. Sandison" and "Yell North Mr T. J. Sandison".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19071214&page=0008&article=125
- **ST Sat 19 Nov 1910, p4 (art. 063)**: Henry Mouat nominated for Yell North "in place of Mr
  Thomas J. Sandison"; art. 067: "The retiral of Mr T. J. Sandison from North Yell".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19101119&page=0004&article=063
- **ST Sat 23 Nov 1907, p4 (art. 091)**, nominations: only Pole and T. J. Anderson of the
  former members drop out, and sixteen divisions are unchanged. Its table does print "Unst South
  ... Mr T. Sandison" and "Yell North ... Mr J. P. Sandison" (the line positions on the page
  agree), which is probably where the wiki got it; the list of members three weeks later and the
  1910 report both have it the other way round.

**Fixed** (`fix_newspapers.py` #81, James's go-ahead 2026-10-12): swapped the winners of the two 1907 elections
(election 360 Unst South: Thomas Sandison → John Peter Sandison; election 364 Yell North: John
Peter Sandison → Thomas Sandison), and corrected both intros: John Peter was Unst South
1902–1929 throughout (his intro also leaves out 1902–07); Thomas was Unst South 1898–1901 and
Yell North 1906–1910.

### 1892 and 1895: Northmavine South was Thomas Anderson (i), not (iii)

- **ST Sat 26 Nov 1892, p3 (art. 045)**: "Northmavine, south Thomas Anderson, Hillswick", the
  same man as 1890 ("Thomas Anderson, Hillswick, (unopposed)", ST 8 Feb 1890) and 1898.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18921126&page=0003&article=045
- The wiki links the bare "[[Thomas Anderson]]" in 1892 and 1895 and the parser chose (iii), the
  Aithsting sheep farmer (b. 1866), whose intro has only Aithsting 1915–20. (i)'s intro already
  has Northmavine South 1890–1901.

**Fixed**: `fix_parse_errors.py` #29 relinks both candidacies to `thomas-anderson-i`.

Checked and left alone: John Jamieson (Whalsay 1945–47, Northmavine South 1972–75) is one man;
his intro gives a coherent career.

## Found along the way: Lerwick Town Council

### John Tulloch (i) resigned in August 1894

- **ST Sat 11 Aug 1894, p2 (arts. 043, 045)**: Burgh Commissioners' monthly meeting "on Tuesday
  evening" (7 Aug 1894). "RESIGNATION OF A COUNCILLOR": "Mr Tulloch's letter intimating his
  resignation as a Councillor, owing to his having gone abroad" was accepted, and with the annual
  election near it was "unnecessary to fill the vacancy"; Laing took his place on the Finance
  Committee.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18940811&page=0002&article=045
- He was the only Tulloch on the council (ledger: John Tulloch (i), the solicitor, elected Nov 1891).

**Ledger edit made**: his 1891 row now ends 1894-08-07, resigned (was 1894-11-06, term expired; an
unconfirmed draft row). This opens a genuine Aug–Nov 1894 vacancy on /data-review (term_issues
5 → 6), which clears once the rows sitting through it are confirmed.

## Method note: ex officio members, 1890s

The 1890–92 sederunts include members with no ward: J. C. Grierson "ex officio" (and, by April
1892, R. D. Ganson and J. Mouat). Under s. 109 of the Local Government (Scotland) Act 1889 the
Council had four ex officio members "in addition to" the elected ones
(`zcc-first-council-1890-1892.md`). They aren't modelled as terms, so the roll-call matcher flags
them; they are not missing councillors.

## 2. Sederunt roll call, 1890–1975

Every Shetland Times issue 1890–1975 was swept for County Council report headings (contents
lists only). For each report, the "Present" list was read from the OCR and every surname checked
against the members `council_terms` has sitting in the 40 days before the issue (ZCC members,
and from 1930 Lerwick town councillors, who sat ex officio). Issues not digitised: the known
gaps (24 Apr–12 Jun 1915, 9 Feb 1918), plus 21 Aug 1909, 3 Aug 1918, 8 May 1926 (General Strike),
4 Jan 1930, 30 Nov 1945, 26 Dec 1952 and 2 Jan 1953.

Names that didn't match, and what they were:
- **1890–92 Grierson, Ganson, Mouat, Johnstone, Bayne**: ex officio members (above).
- **Arthur Smith, Nov–Dec 1916**: an added member of the Mainland District Committee, which sat
  straight after the Council ("The meeting, with the addition of Mr Arthur Smith, then sat as the
  Mainland District Committee", ST 26 Jun 1915 p5 a168). Not a county councillor.
- **J. R. White, Mar–Apr 1930**: the old Council sat until May 1930; already an open question (29 Sep), now with this evidence added.
- **Jan–Apr 1930, W. Sinclair, Cogle, Duffin, Ollason, Laing, Campbell**: Lerwick town
  councillors at the new Council's preliminary meetings (ST 26 Apr 1930 p4 a60, Provost J. T. J.
  Sinclair and the Bailies among them).
- Everything else that didn't match was OCR damage ("Hsmjltnn" for Hamilton) or a split name
  ("Neven Spence").

Coverage: 333 County Council reports with a usable "Present" list (77 in the 1890s, 40 in the
1900s, 82 in the 1910s, 74 in the 1920s, 54 in the 1930s, 6 in the 1940s), about six a year to
1939. After 1946 the reports stop printing sederunts (3 of 280), so 1946–75 was checked
differently, below.

## 3. Headings check, 1946–1975

Every heading mentioning a vacancy, co-option, by-election, resignation, nomination or new
member was read against the known by-elections. Everything that wasn't a ZCC by-election already
in the DB was a church vacancy, a job advert, a Town Council or Harbour Trust matter, or a
district council by-election (Gulberwick & Quarff District Council, Nov 1970: a body we don't
cover). Two resignations are dated earlier than the DB shows (open questions):
- **Andrew W. Peterson, Yell North**: resigned "with effect from 19th June" 1962, letter read at
  the Council on Tuesday (ST 22 Jun 1962 p6 a100). The DB has him sitting to the 4 Dec 1962 by-election.
- **Barbara Anderson, Burra**: to resign "from 30th June" 1972 (ST 26 May 1972 p13 a179). The DB
  has her sitting to the 8 Aug 1972 by-election.

## Result

No missing County Councillors and no missing ZCC by-elections found, 1890–1975. Corrections made:
the 1907 Sandison swap (`fix_newspapers.py` #81), the 1892/1895 Thomas Anderson link
(`fix_parse_errors.py` #29) and, for the Town Council, Tulloch's 1894 resignation. Open: the
1929–30 handover date and ZCC resignation dates.
