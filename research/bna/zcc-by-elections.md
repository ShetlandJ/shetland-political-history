# ZCC by-elections: who was replaced

Researched 2026-09-28. Covers the five by-election issues and the Yell South over-full ward on
/data-review. Four of them weren't research questions. The parser had linked the replaced
member (or the winner) to a namesake from another era, and `build.py`'s fallback by name had
covered for it. Only the 1950 Yell South seat needed the newspaper. The Shetland Times was a
Friday paper in 1950. Click **Articles** in the viewer toolbar for the OCR text.

## Build fix

`build.py` `find_member`: when a by-election named a linked member who wasn't sitting (usually
because they had died before it), it fell back to matching by name, and closed a namesake's
term in another ward:
- Aithsting Feb 1921 (for Thomas Anderson (iii), died 17 Dec 1920) ended **Thomas Anderson
  (ii)**'s Bressay term 22 months early.
- Gulberwick May 1951 (for John Williamson (iii), died 7 Feb 1951) ended **John Williamson
  (iv)**'s Yell South term a year early.

The name fallback now applies only to unlinked terms when the replaced member is linked. Fixing
it exposed three more wrong links (Burra 1898, Aithsting 1932, Gulberwick 1945) that the
fallback had been matching correctly by accident. All five are fixed below. The change touched no
LTC or SIC terms.

## Wrong namesakes (wiki source, `fix_parse_errors.py` #5)

Checked against each page's wiki text (shetland_history2). The wiki gives the day as well as the
month, so the dates are set from it.

| Election | Wiki text | Was linked to | Now | Date |
|---|---|---|---|---|
| Burra Feb 1898 | "Reverend David Gray resigned", 3 February | David Gray (ii), d. 1946, LTC | **David Gray (i)**, the UF minister | 1898-02-03 |
| Dunrossness North Aug 1898 | "the death of councillor Robert Henderson" | Robert Henderson (iii), SIC | **Robert Henderson (ii)**, d. 19 Jun 1898 | left at 1898-08-01 (see open questions) |
| Nesting Nov 1920 | death of James Hunter, 18 November; "his brother Robert was appointed" | James Hunter (iv), the GP b. 1914; winner Robert Hunter (i), the Lerwick bank agent | **James Hunter (iii)**, d. 21 Sep 1920; winner **Robert Hunter (ii)** | 1920-11-18 |
| Aithsting Feb 1921 | death of Thomas A. Anderson, "Thursday 17 February" | (already right: Thomas Anderson (iii)) | | 1921-02-17 |
| Aithsting May 1932 | "resignation of Captain James Anderson", 17 May | James Anderson (iv), b. 1910 | **James Anderson (ii)**, master mariner | 1932-05-17 |
| Gulberwick Mar 1945 | "resignation of Reverend George Smith", 20 March | George Smith (iii), SIC | **George Smith (ii)**, UF minister | 1945-03-20 |
| Gulberwick May 1951 | death of John A. Williamson, 15 May | (already right: John Williamson (iii)) | | 1951-05-15 |

The Nesting page on the wiki itself links the wrong Hunters. James Hunter (iii)'s page says the
vacancy was "filled by his brother Robert Hunter (ii)", and Robert Hunter (ii)'s page has the
co-option. So the person pages are right and the election page is wrong.

## Yell South, August 1950 (`fix_newspapers.py` #15)

The DB had George Ross as the member replaced. Ross sat for Tingwall, so this was the Tingwall
vacancy copied across.

- **ST Fri 23 Jun 1950, p5 (art. 084)**: two petitions for "the vacancy existing in the Tingwall
  Division, due to the resignation of Mr George Ross", for G. M. Nelson and G. K. Spence, "who is
  already a member of the County Council representing South Yell". A similar by-election would
  follow in Yell South "if Mr Spence carries out his intention". Ross's resignation was
  accepted "at the Council meeting on Tuesday" (20 Jun).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500623&page=0005&article=084
- **ST Fri 21 Jul 1950, p4 (art. 051)**: "Mr Spence recently resigned his Yell seat to contest
  Tingwall".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500721&page=0004&article=051
- **ST Fri 21 Jul 1950, p5 (art. 058)**: South Yell nominations: Charles A. Guthrie and John
  Williamson, Bankend House, Ulsta.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500721&page=0005&article=058
- **ST Fri 28 Jul 1950, p1 (art. 004)**: election address to the electors of Yell South: the poll
  is "TUESDAY, 1st August", at East Yell Public School.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500728&page=0001&article=004
- **ST Fri 4 Aug 1950, p4 (art. 060)**: "BY-ELECTIONS": Spence 233, Nelson 106 (Tingwall); in
  Yell South "the former representative has been returned" (Williamson, who sat 1940–49).
  Matches the DB votes.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500804&page=0004&article=060
- **ST Fri 18 Aug 1950, p4 (art. 055)**: "Mr Spence having vacated membership of Yell South to
  contest Tingwall".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500818&page=0004&article=055

**Edit**: election 720 replaced George Ross → George Spence (169). Spence's Yell South term now
ends 1 Aug 1950 and Williamson (iv)'s runs to the May 1952 general. The 1950-08-01 dates were
already right.

**Not represented**: Ross's seat really ended at his resignation on 20 Jun 1950, and Spence's
Yell South seat when he resigned it, some time between 23 Jun and 21 Jul. The derived ZCC terms
end at the by-election, because `build.py` has no way to record a ZCC resignation date.

## Result

term_issues 42 → 35. All five by-election issues, the Spence overlap and the Yell South over-full
ward cleared. Nothing new.
