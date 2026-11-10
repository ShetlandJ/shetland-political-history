# Book-session checks, November 2026

Five mismatches between the site DB and the book's ch26–28 beat guides, raised by the book
session on 2026-11-10. Correction: `fix_newspapers.py` #107.

## 1. Town Council election, May 1951: turnout percentage

- **ST Fri 4 May 1951, p4 (art. 039)**: "only 740 out of 4013 on the roll, a percentage of only
  18.4" (OCR "184"); "by far the lowest poll ever recorded in Lerwick".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19510504&page=0004&article=039
- 740/4,013 = 18.44%. The DB had 18.8; electorate and turnout were already 4013 and 740.
- **ST Fri 5 May 1961, p5 (art. 066)** looks back and gives "18.2 in 1951". The 1951 report and
  the arithmetic agree on 18.4, so that's what goes in.
- Edit: election 119 turnout_pct 18.8 → 18.4.

## 2. Whalsay and Skerries County Council by-election, May 1947: petition 207 or 307

- **ST Fri 23 May 1947, p7 (art. 145)**: the OCR reads "signed by 207 persons".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19470523&page=0007&article=145
- Page image read 2026-11-10 (IIIF crop of 1790,1562,80,42 and the line at 1520,1480,700,130).
  The first digit is damaged in the middle, but it has a rounded lower bowl with a ball terminal:
  a 3. The 2s in the same article ("£525", "£25", "1929") have a flat foot. So **307**, as the
  wiki has it; the OCR's 207 is the error.
- The Shetland News of Thu 22 and 29 May 1947 (`BL/0003210`) has no report of the petition.
- Edit: none. Election 692's votes_text "Petition of 307" stands. The citation summary and the
  citation link note that quoted 207 are corrected.

## 3. Town Council election, May 1968: electorate and turnout

- **ST Fri 10 May 1968, p4 (art. 061)**, "A 'NO CHANGE' ELECTION": "the turnout was only 1280
  (including 38 postal votes), compared with 1593 (including 55 postal) a year ago; that is 32.2
  per cent, compared with 39.3."
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19680510&page=0004&article=061
  (The result table is art. 062, already cited; it has no poll figures.)
- **ST Fri 9 May 1969, p1 (art. 002)**: "a percentage poll of 41.65, which compares very
  favourably with last year's 32.2 per cent".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19690509&page=0001&article=002
- The DB's 1968 electorate (4048) and turnout (1593) are 1967's: 1593/4048 = 39.35%, the 1967
  figure. The 1968 electorate isn't printed (1280/0.322 ≈ 3,975, not stored).
- Edits: election 143 electorate 4048 → NULL, turnout 1593 → 1280, turnout_pct → 32.2;
  election 140 (May 1967) turnout_pct → 39.3.

## 4. Town Council election, May 1961: turnout

- **ST Fri 5 May 1961, p5 (art. 066)**: "The final percentage was 48.5"; the hourly figures end
  "8 p.m., 1863, including 28 postal votes". Highest for seven years.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19610505&page=0005&article=066
- The DB already had turnout 1863. No electorate is given (1863/0.485 ≈ 3,841, not stored).
- Edit: election 132 turnout_pct → 48.5.
- The same article lists post-war percentages that don't all match the DB: 1947 "47" (DB 44.0),
  1951 "18.2" (18.4, above), 1955 "38.5" (DB 35.8), 1959 "33.2" (DB 33.0). Not changed: each
  needs that year's own report.

## 5. Rev. W. C. Robb: one person, one name

- One person record already: `william-robb` (id 517), intro "Reverend William Clement Robb". The
  wiki's `Clem_Robb` page is a redirect to `William_Robb`.
- Nominated as **William C. Robb** every time:
  - ST Fri 24 Apr 1970, p8 (art. 068): Delting South, William C. Robb (already cited).
  - ST Fri 20 Apr 1973, p8 (art. 080), "THEY ALL SEEK ELECTION": "DELTING SOUTH: William C.
    Robb, minister, Delting Manse, Brae".
    https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19730420&page=0008&article=080
  - ST Fri 19 Apr 1974, p1 (art. 014), "FEW CONTESTS": Delting North and South, "William C.
    Robb, Manse, Brae", against Fraser Peterson.
    https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19740419&page=0001&article=014
- "Clem" was his everyday name: **ST Fri 7 Jun 1968, p1 (art. 010)**, church notices, "the
  induction of the new minister, the Rev. Clem Robb, will take place at Brae Church, on Monday,
  17th June".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19680607&page=0001&article=010
- Edit: candidacies for 1970 ("William Robb"), 1973 ("Clem Robb") and 1974 ("William Robb") all
  become "William C. Robb".
