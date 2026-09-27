# LTC polling days 1949–1964

Researched 2026-09-27 in the British Newspaper Archive (Shetland Times, a Friday paper by then).
Every link opens the BNA viewer on the article; click **Articles** in the toolbar for the OCR text.

Question: the DB dated twelve May generals 1949–64 on a Monday. Were they on the Tuesday?
Answer: yes, every one. Fixed by `fix_newspapers.py` #7, with the matching `start_date`/`end_date`
moves in `data/ltc_terms.csv` (102 dates). No term_issues cleared (53 before and after); only the
dates in the existing issues moved.

## Evidence

| Election | DB had | Polling day | Citation | Quote |
|---|---|---|---|---|
| May 1949 | Mon 2 May | **Tue 3 May** | ST Fri 29 Apr 1949, p4 (art. 050) | "Tuesday first is polling day in the Town Council election" |
| May 1950 | Mon 1 May | **Tue 2 May** | ST Fri 21 Apr 1950, p4 (art. 047) | "will therefore take place on Tuesday, 2nd May" |
| May 1952 | Mon 5 May | **Tue 6 May** | ST Fri 2 May 1952, p3 (art. 043) | "Municipal Election Tuesday 6th May, 1952" (advert) |
| May 1953 | Mon 4 May | **Tue 5 May** | ST Fri 1 May 1953, p4 (art. 044) | "The Town Council election takes place on Tuesday first" |
| May 1954 | Mon 3 May | **Tue 4 May** | ST Fri 7 May 1954, p4 (art. 048) | "following Tuesday's municipal election" |
| May 1955 | Mon 2 May | **Tue 3 May** | ST Fri 29 Apr 1955, p4 (art. 062) | "Tuesday first is the Town Council polling day" |
| May 1958 | Mon 5 May | **Tue 6 May** | ST Fri 9 May 1958, p4 (art. 054) | "With poor weather on Tuesday" (result report) |
| May 1959 | Mon 4 May | **Tue 5 May** | ST Fri 1 May 1959, p8 (art. 120) | "the Town Council election on the 5th May, 1959" (OCR "sth") |
| May 1960 | Mon 2 May | **Tue 3 May** | ST Fri 15 Apr 1960, p4 (art. 060) | Tuesday 3 May (seen in the 1955–65 research) |
| May 1961 | Mon 1 May | **Tue 2 May** | ST Fri 12 May 1961, p7 (art. 099) | "last Tuesday's poll" (seen in the 1955–65 research) |
| May 1963 | Mon 6 May | **Tue 7 May** | ST Fri 19 Apr 1963, p5 (art. 079) | Tuesday 7 May (seen in the 1955–65 research) |
| May 1964 | Mon 4 May | **Tue 5 May** | ST Fri 1 May 1964, p4 (art. 087) | "on Election Day, Tuesday, 5th of May" (OCR "sth") |

Links:
- 1949: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19490429&page=0004&article=050
- 1950: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500421&page=0004&article=047
- 1952: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19520502&page=0003&article=043
- 1953: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19530501&page=0004&article=044
- 1954: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19540507&page=0004&article=048
- 1955: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550429&page=0004&article=062
- 1958: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19580509&page=0004&article=054
- 1959: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19590501&page=0008&article=120
- 1960: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19600415&page=0004&article=060
- 1961: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19610512&page=0007&article=099
- 1963: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19630419&page=0005&article=079
- 1964: https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19640501&page=0004&article=087

1951, 1956, 1957, 1962 and 1965 were already Tuesdays in the DB and weren't checked.

### September 1949 co-option: Tue 13 Sep, not Mon 12 Sep
- **ST Fri 16 Sep 1949, p5 (art. 055)**: "Politics Laid Aside for Co-Option". On an Independent's
  motion, Arthur Johnson, who headed the unsuccessful candidates at the last election, was invited
  to fill the vacancy left by James H. Brownlie's resignation (transferred south after illness).
  The report doesn't name the meeting day.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19490916&page=0005&article=055
- **ST Fri 16 Sep 1949, p3 (art. 030)**: same issue, the council's "monthly meeting on Tuesday"
  (13 Sep). No other meeting is reported that week, so the co-option is taken as the same meeting.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19490916&page=0003&article=030
- Edit: by-election date 1949-09-13; Brownlie's term ends and Johnson's starts 1949-09-13.

### New finding: May 1954 was contested
The wiki records the four winners (Morrison, Halcrow, Blance, Ollason) with no votes, i.e. as
unopposed. It was a poll:
- **ST Fri 23 Apr 1954, p4 (art. 056)**: "eight candidates for the municipal election".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19540423&page=0004&article=056
- **ST Fri 14 May 1954, p7 (art. 118)**: statutory meeting "last Friday night" (7 May). New junior
  Bailie Robert B. Blance "was third (only 21 votes behind the top-of-the poll candidate) in last
  week's municipal election". Miss Grace M. T. Halcrow welcomed as the new member, and the three
  retiring members who were successful.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19540514&page=0007&article=118
- The result itself is in ST 7 May 1954, p4 (art. 048), but the OCR is unreadable there. The
  vote figures and the four unsuccessful candidates need a zoomed read. Queued.
