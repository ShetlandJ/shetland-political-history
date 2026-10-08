# Family connections between the early councillors

James's hand-drawn chart of how the first Lerwick councillors were related, transcribed on
2026-10-03 and checked person by person and link by link on Bayanne (James's login, read only).
The data is `data/relatives.csv` and `data/family_links.csv`; the page is /connections.

## Summary

- 116 people: 46 councillors already in `people`, and 70 relatives. The relatives are 67
  non-councillor boxes on the chart, 2 pink boxes with no DB match, and 1 person added in the
  middle of a chain (Andrew Umphray, b. 1815, I18404).
- 130 links: 126 drawn on the chart and 4 added. 129 are confirmed on Bayanne. The one left
  unchecked is Thomas Fea to Barbara Fea, which Bayanne contradicts (below).
- Every link and every person is one connected family. On the chart the clusters look separate,
  but they are joined.
- Where the chart's line type was wrong, the CSV records what Bayanne shows and the row's note
  says what the chart drew.

## Relationships the chart gets wrong

- **James Hoseason and Ann Hoseason:** the chart has them as siblings. Bayanne has them as
  husband and wife (I3358, I3499). They were also related: James's grandfather Arthur Hoseason
  (I1197) was Ann's great-grandfather.
- **James Hoseason and Jane Hoseason:** the chart has them as siblings. Jane (I3413) is Ann's
  sister, so James's sister-in-law. Recorded as Ann–Jane siblings, plus an added link from their
  father Robert Hoseason to Jane.
- **William Hay to Mary Hay and Andrew Hay:** the chart has these as parent to child. All three
  were siblings, children of James Hay (I6233) and Anne Umphray.
- **George Hay:** his arrow on the chart comes off the Ann Umphray → William Hay line. William was
  his father and Ann his grandmother (I6284).
- **Lewis Umphray to Louisa Umphray:** the chart has this as parent to child. Lewis was her
  grandfather, through Andrew Umphray, b. 1815: Lewis (I18402) → Andrew (I18404) → Louisa.
- **Thomas Fea to Barbara Fea:** contradicted, and left unchecked. Barbara (I37427, b. about 1743)
  was the daughter of an older Thomas Fea (I363803, born before 1716). The chart's Thomas Fea
  (I4241) was born 14 Dec 1767, so the chart seems to merge the two men. /data-review flags it
  ("unlikely parent age").
- **Sinclair Goudie:** the chart has him as Robert Goudie's sibling, with the dates 1771–1855.
  Bayanne agrees they were brothers, both sons of Robert Goudie senior (I45515, 28 May 1771 –
  1 Dec 1855) and Margaret Wishart. Sinclair's own dates are 1812 – 5 Jun 1900 (I45514); the chart
  has their father's. He married Barbara Hunter (I45513, daughter of James Hunter (i)) and died
  at Camberwell, Victoria.
- **William Goudie:** a third brother (I211691, 29 Aug 1809 – 2 Dec 1886), husband of Marable
  Hughson. The chart gives him Robert's dates, 1798–1869.
- **Sinclair Bruce** (I16965) was a woman: the wife of Thomas Charles Edmondston (I10640) and
  sister of Erasmus Bruce. The chart's green line is right.
- **Andrew Hay and Joan Mary Nicolson** were married (I6254). The chart doesn't draw this, so it
  was added (`on_chart=0`).

## Pink boxes with no DB match

Both were checked against `people` and `candidacies`.

- **James Greig, 1773–1855** (I45553, son of Catherine Innes, husband of Ann Deans). Every LTC
  candidacy for "James Greig", April 1818 to September 1832, is linked to `james-greig`
  (1785–1852). Some of them may be the older man (open question).
- **Lewis Garriock, 1825–1902** (I18411, Lewis Francis Umphray Garriock, died in Dundee). He has
  no candidacy anywhere.

## Councillor dates: the DB against Bayanne

- `archibald-greig`: the DB has died 12 Oct 1852, Bayanne 11 Oct 1852 (I18009).
- `gilbert-paterson`: the DB has died 8 Apr 1828, Bayanne 5 Apr 1828 (I18013).
- `joseph-leask`, `robert-deans` and `john-morrison-i` have a full birth date in the DB. Bayanne
  gives only the year, so the day comes from somewhere else.
- The other 41 councillors agree exactly.

## Chart dates and names that were wrong

The CSVs use Bayanne's dates. Each relative's row says what the chart had.

- Robert Hunter "1845" is `robert-hunter-i`, born 10 Jul 1856.
- James Mouat "1879?–1937" is `james-mouat-iii`, born 17 Mar 1867.
- Lewis Umphray "1764–1791": he lived 1771–1823. The chart has his sister Ann's dates.
- Thomas Fea "1787": born 1767.
- James Hoseason died in 1867, not 1875.
- Ann Deans died in 1864, not 1855.
- Anderina Heddell died in 1867 (the chart has "1841+").
- Catherine Innes died on 20 Jan 1808, not 1806. Bayanne gives no birth date (I45551).
- Margaret Mouat was born 2 Aug 1779, not 1780 (I18050).
- John Mouat was born 8 Feb 1752, not 1751 (I18051).
- James Smith (the chart has "1830–?") died in 1869 (I242471).
- Bayanne has only approximate dates for five people the chart gives exact years for. In the
  CSV, a "before" or "after" date is left blank and an "about" date gives the year, each with a
  note:
  - Margaret Mowat, born before 1702 (I1194)
  - Arthur Hoseason, born about 1724, died after 1783 (I1197)
  - John Hoseason, born after 1745 (I3332)
  - Peter Mouat, died after 1841 (I44993)
  - Captain John Mouat, died before 1845 (I15244)
- Names that differ on Bayanne:
  - Robert and Margaret "Mowat" are Mouat (I1195, I1194), children of Arthur Mowat and Ursula
    Neven.
  - "Margaret Hunter" is Martha Margaret Nicolson Hunter (I18100).
  - "Robert Spence" is Robert Neven Spence (I4282).
  - "Selina Darrell" is `selina-garriock` (I68324).
  - "Lewis Umphray" is Lewis Francis Cumming Umphray (I18402).
  - "Lewis Garriock 1825" is Lewis Francis Umphray Garriock (I18411).

## Found while building the page

- **James Mouat (iii):** his page calls him a great-nephew of James Mouat (i). On Bayanne he is a
  great-great-grandson of Jerome Mouat, James (i)'s brother. That makes him a great-great-great-
  nephew, and a first cousin three times removed of James (ii). James (ii)'s page says "cousin
  three times removed", which agrees.
- **Henry Mouat:** James Mouat (iii)'s page says his brother Henry became Provost of Lerwick. The
  DB has Henry on the County Council only (Lerwick North, 1910–1938), with no Town Council seats.
