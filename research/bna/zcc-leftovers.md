# ZCC leftovers: figures and rows the citation sweeps couldn't settle (2026-10-03)

Queue item "ZCC leftovers". The 2026-09-28/29 sweeps (`zcc-1890-1899.md` to `zcc-1960-1974.md`)
left vote figures unchecked where the OCR lost them, six election rows uncited, and the 1947
Whalsay petition figure open. The page images now render: each figure below was read from a crop
of the page through the BNA image service, located by the OCR line coordinates.

**Result.** Every unchecked figure agrees with the DB except one gap: Grierson's 31 in the 1913
Aithsting tie, now added (`fix_newspapers.py` #76). The six rows are cited. The Whalsay petition
was about 300, not 207. Burra 1959's date is an open question.

## Vote figures (page images)

- **Dec 1892, Sandwick**: ST Sat 10 Dec 1892, p2 (art. 038). "The total vote was 98, of which 4
  papers were rejected, the remainder giving 73 for Mr Duncan, and 21" for Tulloch. Agrees.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18921210&page=0002&article=038
- **Dec 1898**: ST Sat 10 Dec 1898, p4 (arts. 045–047). Sandsting Mitchell 72, Garriock 2, Harcus 1;
  Burra Isle Lennie 61, Henderson 9; Nesting Small 31, Hunter 2. Agrees.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18981210&page=0004&article=045
- **Dec 1901**: ST Sat 7 Dec 1901, p4 (art. 071). Lerwick North Tulloch 129, Robertson 38; Tingwall
  Duncan 133, Leslie 71; Northmavine North Robertson 70, Hughson 35; Northmavine South Anderson 71,
  Wallace 13; Nesting Small 35, Johnson 33. Agrees.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19011207&page=0004&article=071
- **Dec 1904**: ST Sat 10 Dec 1904, p4 (art. 104). Northmavine North Haldane 46, Loggie 37; Delting
  North Pole 31, Inkster 13; Whiteness and Weisdale P. Anderson 48, H. J. Robertson 25; Sandwick
  W. Smith 91, J. Thomson 70. Agrees. Sandwick's 91–70 is 1904's own result (the wiki's 1907 table
  was the copy, `fix_newspapers.py` #34).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19041210&page=0004&article=104
- **Dec 1910**: ST Sat 10 Dec 1910, p4 (art. 046). Lerwick Landward (Gulberwick) Fordyce 43, Shand 42;
  Lerwick North Tulloch 82, Arcus 61; Sandsting H. J. Robertson 56, Dr J. C. Bowie 39, **R. T. C.
  Scott** 1; Nesting J. A. Loggie 69, W. L. MacDougall 30, J. Small 4. Agrees.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19101210&page=0004&article=046
  The Sandsting third candidate is "Mr R. T. C. Scott" in the nominations too (ST 19 Nov 1910). The
  DB's candidate name links Bayanne I45835 as "Robert Scott": not checked that it's the same man
  (open question).
- **Dec 1913**: ST Sat 6 Dec 1913, p4 (art. 094). Bressay T. J. Anderson 48, James Laing 26; Burra and
  Quarff A. J. Jamieson 36, W. Sinclair 26; **Aithsting "Mr J. C. Grierson, 31; Dr J. C. Bowie, 31"**;
  Walls Thomason 53, Rev. A. W. Groundwater 30; Delting North Hay 29, Arthur Smith 26, J. P.
  Henderson 15. All agree, but the DB had no figure for Grierson → `fix_newspapers.py` #76.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19131206&page=0004&article=094
- **Dec 1925**: ST Sat 12 Dec 1925, p5 (art. 085). Sandsting Bowie 145, Hobbin 67, **Andrew J.
  Anderson 18** (the OCR's 13 was a misread; the DB's 18 is right); Aithsting John Leslie 96,
  Christopher F. Irvine 55. Agrees.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19251212&page=0005&article=085

## Uncited rows

- **Dec 1910, Unst North, Sandwick, Dunrossness South (376, 387, 389)**: ST Sat 19 Nov 1910, p4
  (art. 067). "The nominations for the various constituencies are" runs from Unst South to
  Dunrossness North and leaves these three out: no nomination, hence the Jan 1911 `[unfilled seat]`
  by-elections (inferred).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19101119&page=0004&article=067
- **Burra Apr 1920 (461)**: ST Sat 24 Apr 1920, p5 (art. 102, under "Zetland County Council", the
  monthly meeting "on Thursday last", 22 Apr). "The Returning Officer reported that Mr George T.
  Anderson had been elected as the representative of Burra and Quarff". So an election, held before
  22 Apr; the DB's Thu 15 Apr fits and is kept.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19200424&page=0005&article=102
- **Dunrossness South Apr 1937 (635)**: the Shetland Times column runs into the binding. **Shetland
  News, Thu 22 Apr 1937, p5 (art. 077)**: "NEW MEMBER FOR DUNROSSNESS SOUTH". A petition for "Mr G. W.
  B. Leslie, Quendale ... in place of the late Mr McDougall"; unanimously appointed, at the meeting
  "on Tuesday" (20 Apr; art. 062). The DB's 20 Apr is right.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F19370422&page=0005&article=077
- **Burra Jul 1959 (802)**: ST Fri 22 May 1959, p2 (art. 019): Strachan "is now a town councillor"
  and no longer represents Burra Isle; at Tuesday's meeting (19 May) a by-election was ordered, "the
  probable date ... about 30th June". ST Fri 12 Jun 1959, p4 (art. 041): "Mr A. D. Bennet, 21
  Hillhead, Lerwick, will be Burra Isle's new representative"; his was the only nomination when
  nominations closed on Tuesday (9 Jun). ST 23 Oct 1959, p4 (art. 039): his first meeting. The
  DB's 21 Jul (the wiki's) isn't supported: open question (9 Jun, about 30 Jun, or 21 Jul).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19590612&page=0004&article=041

## Whalsay May 1947 petition

- ST Fri 23 May 1947, p7 (art. 145): "was signed by ?07 persons". The first digit is broken on the
  page; its shape is a 3 more than a 2.
- **Shetland News, Thu 22 May 1947, p6 (art. 095)**: the Clerk to Whalsay District Council
  "forwarded a petition signed by 309 people".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F19470522&page=0006&article=095

So about 300 signatures, not 207. The wiki's "Petition of 307" fits the Shetland Times. Votes were
already cleared (`fix_parse_errors.py` #19); no change.
