# Zetland County Council polling days 1949–1964

Queue item: the baseline dated the ZCC generals of 1949, 1952, 1958, 1961 and 1964 on a Monday,
and 1955 on a Thursday. Checked in the Shetland Times on 2026-09-28 with `county polling` (and
`county tuesday` for 1961), from the Friday before polling to the Friday after.

**Result: all six were on a Tuesday.** `fix_newspapers.py` #12 moves every ward row of each
general. ZCC terms are derived from the results, so their start and end dates follow. Issues:
41 → 41.

| General | Baseline | Correct | Source |
|---|---|---|---|
| May 1949 | Mon 9 May | **Tue 10 May 1949** | ST 29 Apr 1949 p7 |
| May 1952 | Mon 5 May | **Tue 13 May 1952** | ST 9 May 1952 p4; ST 2 May 1952 p5 |
| May 1955 | Thu 5 May | **Tue 10 May 1955** | ST 6 May 1955 p5 |
| May 1958 | Mon 12 May | **Tue 13 May 1958** | ST 16 May 1958 p5 (and ST 9 May 1958, from an earlier run) |
| May 1961 | Mon 8 May | **Tue 9 May 1961** | ST 28 Apr 1961 p4; ST 12 May 1961 p5 |
| May 1964 | Mon 11 May | **Tue 12 May 1964** | ST 8 May 1964 p4 |

The county polled a week after the town in 1949, 1952, 1955 and 1961. The LTC dates are 3 May
1949, 6 May 1952, 3 May 1955 and 2 May 1961 (`ltc-election-dates-1949-1964.md`).

## Evidence

- **ST Fri 29 Apr 1949, p7 (art. 100)**: "Only seven County Council elections". There will be
  only seven polls "in the County Council election on 10th May".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19490429&page=0007&article=100
- **ST Fri 2 May 1952, p5 (art. 074)**: District Council contests "in ten days time".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19520502&page=0005&article=074
- **ST Fri 9 May 1952, p4 (art. 066)**: "Polling day". The town has had its election, and "the
  county will have its turn on Tuesday first": 13 May. The baseline's 5 May is neither the town's
  day (6 May) nor the county's.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19520509&page=0004&article=066
- **ST Fri 6 May 1955, p5 (art. 073)**: "County election on Tuesday". Electors in seven parishes
  go to the poll "on Tuesday first, 10th". The baseline's Thursday 5 May has no support.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550506&page=0005&article=073
- **ST Fri 16 May 1958, p5 (art. 061)**: "Three out of five were very close". Polling took place
  in five County Council elections "on Tuesday": 13 May.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19580516&page=0005&article=061
- **ST Fri 28 Apr 1961, p4 (art. 028)**: News in brief. Five County Council elections "a week from
  Tuesday", i.e. Tuesday 9 May.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19610428&page=0004&article=028
- **ST Fri 12 May 1961, p5 (art. 065)**: "Shocks for retiring councillors". The count for all five
  was in the County Buildings "on Wednesday afternoon" (10 May), the day after polling.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19610512&page=0005&article=065
- **ST Fri 8 May 1964, p4 (art. 050)**: News in brief. The County and District Council elections
  "take place in Shetland on Tuesday first", with counts on the Wednesday.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19640508&page=0004&article=050

Not checked: the ZCC by-elections in these years. Most are dated to the 1st of the month (month
precision only), which is a separate question.
