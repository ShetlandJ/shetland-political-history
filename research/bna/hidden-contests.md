# Hidden contests: an audit of the "unopposed" seats

Researched 2026-11-01 in the British Newspaper Archive (Shetland Times). Every link opens the BNA
viewer on the article; click **Articles** for the OCR text.

Question: which seats the DB records as won without a contest (one candidacy: "Unopposed",
"Appointed", "Unanimously appointed" and the like) were really polls or contested Council votes?
Earlier finds: ZCC Dunrossness North 1922 (#37), Yell South 1938 (#45), six ZCC by-elections
1960–74, SIC Dunrossness Jul 1977 (#92).

## Worklist

From `shetland.db` (hidden rows excluded): every LTC, ZCC or SIC election row with exactly one
candidacy: **706 rows**. **481** already had a `read` election citation and were dropped. The
remaining **225** by council and decade:

| Council | Generals | By-elections and co-options |
|---|---|---|
| LTC | none (LTC generals have several candidacies; see `ltc-uncontested-generals.md`) | 1830s 1, 1840s 1, 1880s 1, 1910s 3, 1930s 4, 1940s 9, 1950s 2, 1960s 4, 1970s 1 (26) |
| ZCC | 1900s 35, 1910s 55, 1920s 18, 1930s 14, 1950s 1 (123) | 1900s 1, 1930s 2, 1940s 3, 1950s 1 (7) |
| SIC | 1970s 18, 1980s 19, 1990s 23, 2000s 8 (68) | 1970s 1 (1) |

(Some of these already had an `inferred` link from a nominations list; the "unopposed" in those
was by elimination, and the contest lists were re-checked against the DB.)

Checked: **215**. Not checkable in the BNA: the 8 SIC seats of May 2003 (the Shetland Times in
the BNA ends with 2000), Magnus Burns 1830 (a minute-book co-option, before the paper) and Joseph
Leask May 1844 (an election to the office of Junior Bailie, not a seat).

## Result

- **One hidden contest: Lerwick Sound SIC by-election, October 1976.** The wiki and DB had Ian
  Byrne unopposed on Tue 26 Oct 1976. It was a three-way poll on **Tue 5 Oct 1976**: Byrne 183,
  James Gair 142, Gordon Harmer 66; 391 of 728 electors (53.7%). `fix_newspapers.py` #96.
- **A date slip found on the way: Sandness ZCC 1902.** Holbourn was appointed, unanimously, at
  the statutory meeting of **Thu 17 Jul 1902**, not the 24th. #96.
- **Every other seat checked was unopposed** (or a Council appointment with no rival):
  - ZCC generals 1901, 1907, 1910, 1913, 1919, 1922, 1938, 1958: the papers already on file name
    the contests or give their number, and it matches the DB every time (`zcc-1900-1919.md`,
    `zcc-1920-1939.md`, `zcc-1940-1959.md`). Linked as `inferred` (unopposed by elimination).
  - SIC generals 1974–1999: below. 1978, 1990 and 1999 name every unopposed member (`read`);
    1974, 1986 and 1994 give the number of contests, which matches (`inferred`, except members
    named in the article); 1982 prints every nomination by ward (`inferred`, `read` for the four
    the article calls unopposed).
  - LTC co-options 1887–1970 and ZCC appointments 1902–1952: below. Eighteen say "unanimous", "no
    other nomination" or "no amendment" (`read`); thirteen report the co-option with no rival
    named and no vote (`inferred`).
- **Not in the DB at all: the Sandwick SIC contest of May 1982** (Malcolm S. Bray v Peter R.
  Watts). The DB has 24 rows for the 25 seats; Sandwick is missing. The wiki has the table (Bray
  elected, votes "coming soon", turnout 349, 55%) under a broken heading the parser dropped.
  Figures not legible in the paper: open question.

Basis note: the audit's drop rule asks for a `read` link. Seats unopposed by elimination (the
paper lists the contests or gives their number, and this ward isn't one) are linked `inferred`,
as the earlier ZCC runs did, so they will show again unless the next audit also accepts
`inferred` links whose note begins "unopposed" or "co-option"/"appointment".

## Evidence

### 1. Lerwick Sound by-election, October 1976 (election 963)
- **Shetland Times, Fri 1 Oct 1976, p3 (art. 209)**: Ian Byrne's address, "Independent": a
  candidate "in the election for councillor for the Sound area to be held on 5th October, 1976"
  (the OCR has "sth"; read on the page image). → #96.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19761001&page=0003&article=209
- **Shetland Times, Fri 8 Oct 1976, p1 (art. 006)**: "BYRNE WINS THE SOUND WARD": "In a 53.7 per
  cent poll he won 183 votes, ahead of driver James Gair ... (142), and Gordon Harmer, driver
  ... (66). A total of 391 people voted, out of an electorate of 728." Poll closed 9 p.m.; count
  at the Central School. The three add up to 391. → #96.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19761008&page=0001&article=006
- Edit (#96): date 1976-10-26 → 1976-10-05; Byrne 183 (was Unopposed); Gair 142 and Harmer 66
  added, unlinked (no person rows: they never sat); electorate 728, turnout 391 (53.7%). The
  wiki page title still says "October 1976", which stays right.

### 2. Sandness by-election, July 1902 (election 305)
- **Shetland Times, Sat 19 Jul 1902, p8 (art. 148)**: "ZETLAND COUNTY COUNCIL. A statutory meeting
  ... on Thursday at noon" (17 Jul 1902).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19020719&page=0008&article=148
- **Same, art. 150**: "MEMBER FOR SANDNESS": Holborn of Foula proposed in room of the late
  Bankart, seconded by Ganson, "the appointment was unanimously approved of". → #96 (date
  1902-07-24 → 1902-07-17). The earlier run searched 19 Jul–2 Aug with keywords and the 26 Jul
  issue only (`data/searches.csv`).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19020719&page=0008&article=150

### 3. SIC generals 1974–1999
- **1974: Shetland Times, Fri 19 Apr 1974, p1 (art. 006)**: "LOCAL ELECTION APATHY": "only eleven
  out of the twenty-two divisions ... may be contested"; "Returned unopposed are Provost William
  Smith, Dean of Guild Alex Morrison, and Councillor William Peterson"; Thomason "will be taking
  a seat" for Nesting. DB: 11 single, 11 contested.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19740419&page=0001&article=006
- **1978: Shetland Times, Fri 14 Apr 1978, p1 (art. 010)**: "FOURTEEN CONTESTS LIKELY" in 25
  divisions. Unopposed: Tulloch, McLeod, Byrne, Morrison, Johnston, Peterson, Irvine (retiring
  members) and Bentley, Dr J. Albert Hunter, P. B. A. Hunter, Henry A. Stewart (new). All eleven
  of the DB's.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19780414&page=0001&article=010
- **1982: Shetland Times, Fri 16 Apr 1982, p1 (art. 005)**: "Only nine of the 25 seats on the
  council will be contested"; Leslie Eunson takes a Lerwick seat unopposed (the article says
  "his Lerwick Central seat", of Butler's; the list has Eunson for Breiwick), Graham unopposed
  in Sound. **p2 (art. 011)**, the list continued: one nomination in each of the DB's sixteen;
  photo captions "Edward Thomason — unopposed", "Fraser Peterson — no contest" (and "W. A.
  Cumming — unopposed", a slip: Burra was polled, Cumming 316, Morrow-Irvine 104). The nine
  contests: Clickimin, North, Unst, Northmavine, Whiteness, Sandsting, Burra, Dunrossness South
  and **Sandwick (Malcolm S. Bray, Peter R. Watts*)**, which the DB lacks.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19820416&page=0001&article=005
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19820416&page=0002&article=011
- **1982 Sandwick result: Shetland Times, Fri 14 May 1982, p2 (art. 026)**: "Results": Sandwick,
  Malcolm S. Bray, Peter R. Watts*, poll 53.5 per cent. The figures run into the binding: not in
  the OCR, not on the page image. Open question.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19820514&page=0002&article=026
- **1986: Shetland Times, Fri 18 Apr 1986, p1 (art. 001)**: "There will be contests for 16 of the
  25 seats"; "nine councillors have been returned unopposed". DB: 9 single, 16 contested.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19860418&page=0001&article=001
- **1990: Shetland Times, Fri 13 Apr 1990, p3 (art. 026)**: "11 divisions being contested".
  Unopposed: Groat, Thomason, Stuart Gray, Andrea Manson, Jim Irvine, Alex Arthur, Henry Stewart,
  Florence Grains, Joan McLeod, Jim Smith, Willie Tait; new members Mary Colligan, Greta
  McElvogue, Jim Gear. All fourteen of the DB's.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19900413&page=0003&article=026
- **1994: Shetland Times, Fri 15 Apr 1994, p1 (art. 007)**: "only 10 of the 26 seats being
  contested"; Guy, Easten, R. L. Johnson and Stove named unopposed. DB: 16 single, 10 contested.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19940415&page=0001&article=007
- **1999: Shetland Times, Fri 23 Apr 1999, p35 (arts. 380, 381) and p37 (arts. 408, 412)**:
  "NOTICE OF UNCONTESTED ELECTION": "not more than one candidate remains validly nominated ...
  there will be NO POLL": Yell (10), Delting West (12), Nesting Whiteness Girlsta and Gott (14),
  Whalsay and Skerries (16), Sandsting Aithsting and Weisdale (17), Lerwick Sound (1), Clickimin
  (2), South Central (5), Upper Sound Gulberwick and Quarff (8). All nine of the DB's.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19990423&page=0035&article=380
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19990423&page=0037&article=408

### 4. LTC co-options (from the meeting reports already on file)
Each was a Council co-option at a meeting, not a public poll; none had a rival or a division.
- Porteous, Mar 1887: "unanimously elected" (ST 19 Mar 1887 p2 a054).
- Loggie, Jan 1910: "unanimously agreed to appoint" (ST 8 Jan 1910 p4 a071).
- C. B. Stout, Aug 1915: "There was no other proposal" (ST 7 Aug 1915 p4 a096, new citation).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19150807&page=0004&article=096
- M. L. Manson, Sep 1916: "co-opted" (local news, ST 9 Sep 1916 p4 a075). No vote reported.
- T. Sinclair, Jul 1936: "no other nomination ... unanimously co-opted" (ST 11 Jul 1936 p4 a106).
- T. A. Irvine, Aug 1936: "There was no amendment" (ST 8 Aug 1936 p4 a074).
- Clausen, Nov 1936: "unanimously elected" (ST 14 Nov 1936 p4 a127).
- Dalziel, Feb 1938: "This motion was unanimous" (ST 5 Feb 1938 p4 a077).
- Mouat, Jul 1940: "The motion was accepted unanimously" (ST 13 Jul 1940 p2 a028).
- J. A. Williamson, Jan 1941: Ganson's motion to defer found no seconder (ST 11 Jan 1941 p5 a054).
- A. Johnston, Jul 1941; T. A. Sinclair, Mar 1942; R. A. Anderson, Feb 1943; Morrison, Feb 1945:
  co-opted on a motion, no other name (ST 5 Jul 1941 p2 a031; 4 Apr 1942 p2 a041; 13 Feb 1943 p2
  a080; 9 Feb 1945 p2 a044).
- Prophet Smith, Nov 1944: "There were no other nominations" (ST 10 Nov 1944 p2 a042).
- A. Johnston, Dec 1945: "no counter-motion forthcoming" (ST 14 Dec 1945 p3 a076).
- A. Johnson, Sep 1949: "There was no other nomination" (ST 16 Sep 1949 p5 a055).
- A. Johnson, Jun 1958: "the only man nominated" (ST 27 Jun 1958 p4 a055).
- Harry Gray, Sep 1959: co-opted (ST 11 Sep 1959 p6 a073). No vote reported.
- Strachan, Jun 1961: nominated by ex-Provost Conochie; the debate was over Thomson's letter, and
  no other name was put (ST 16 Jun 1961 p3 a057; OCR mixed with adverts).
- M. M. Shearer, Aug 1962: "the unanimous choice" (ST 10 Aug 1962 p6 a090).
- Grace Halcrow, May 1967: "the council unanimously decided to co-opt" (ST 12 May 1967 p3 a046).
- Cumming, Aug 1967: co-opted (news in brief, ST 11 Aug 1967 p4 a067). No vote reported.
- Adair, May 1970: the council agreed to appoint the top unsuccessful candidate (ST 15 May 1970
  p11 a097).

### 5. ZCC appointments
- Unst North, Dec 1937: "the motion was unanimous" (ST 18 Dec 1937 p5 a092).
- Sandsting, Feb 1938: "unanimously adopted" (ST 26 Feb 1938 p5 a107).
- Dunrossness North, Dec 1946: Bruce on a petition of 75 (ST 20 Dec 1946 p7 a105).
- Nesting and Yell North, May 1948: Pearson (petition of 43) and Ratter (324) co-opted (ST 21 May
  1948 p4 a078).
- Dunrossness North, Feb 1952: petition of 109 for Henderson accepted (ST 22 Feb 1952 p7 a110).
- Sandness, Jul 1902: section 2.
