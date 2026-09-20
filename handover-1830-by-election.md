# Handover: the April 1830 Lerwick Town Council by-election

Written 2026-09-20 from the minute-book side. Everything below is sourced to the first Lerwick
Town Council minute book (1818–1877); page numbers are the book's own, and the transcripts are
at `~/projects/town_council_minutes/transcripts/NNN.md`.

## Where it stands

`fix_minute_book.py` (step 3 of the rebuild sequence in CLAUDE.md) already creates the record,
idempotently:

| Table | Row | Key facts |
|---|---|---|
| `elections` | id 1274, `wiki_page_title` "Lerwick Town Council By-Election April 1830" | council 1, 1830-04-21, `by-election`, `replaced_person` Alexander Irvine (linked), `notes` cite pp. 53–54, `hidden` 0 |
| `candidacies` | id 2583 | Magnus Burns, `elected` 1, `position` 1, `role` councillor |
| `people` | id 564, `magnus-burns` | 1757 Cruicikirk, Unst – 1846-01-09 Lerwick, merchant, Bayanne I122332, father of `david-burns` |

There is no wiki page for it — this is the first Lerwick election that exists only in the
SQLite layer. The `notes` field is the page's narrative. Nothing on the site has been checked
yet: run the build and open `/election/1274` to confirm it renders, sits between the 1829 and
1832 generals in the prev/next nav, and shows on Magnus Burns's and Alexander Irvine's pages.

## What happened

1. **3 March 1830.** Alexander Cumming Irvine, merchant, councillor since 1826, dies. (DB has the date.)
2. **6 April 1830** (p53). Council resolves "in terms of the Bye Law to that effect" to call a
   Burgesses' meeting "for the purpose of filling up the vacancy which has occurred in the
   Council by Mr. Irvines decease", summoned for Wednesday 21 April.
3. **7 April 1830.** Public notice issued by William Spence, Surgeon, "eldest Bailie for the time".
4. **21 April 1830, noon, Sheriff Court Room** (p54). Twelve burgesses attend: Hay, Charles
   Ogilvy, Dr Wm. Spence, Pottinger, Wm. Copland, Laurence Duncan, James Tait, Andrew Smith,
   Duncan Irvine, Gavin Gaudy, Robert Leask, Andrew Nicolson. The notice, the Charter's
   bye-law clause and the vacancy Regulation are read. Spence presides. "Mr. Magnus Burn
   Merchant in Lerwick was found duly elected, and declared to be one of the Councillors for
   this Burgh accordingly; and the clerk was directed to intimate said Election to him."
   Signed "Wm. Spence Presdt. of this Meetg. of Burgesses".
5. **1830–32.** Burn(s) sits through to the 6 September 1832 triennial election (Present lists
   p55–63) and is not returned at it (p64).

No vote count is recorded and no other candidate is named. Treat it as unopposed unless the
page says otherwise — it doesn't.

## The legal mechanism (if the page needs a sentence on it)

The Charter of Erection has no vacancy provision, only the first election and the triennial
rule, but it empowers the Bailies and Councillors to make bye-laws (p10). When Bailie
Linklater died in 1818 the Council had no procedure and wrote to Hay Donaldson W.S. in
Edinburgh for advice (p15). Spence gave notice of a motion on vacancies on 5 Dec 1826 (p36),
and on 6 Oct 1829 the Council passed the bye-law (p52): a vacancy in the Magistracy or Council,
once notified to a Council meeting, "shall be filled up at a meeting of the Burgesses called
for the purpose, as in the case of an ordinary election". So this is a burgess by-election on
public notice, not a co-option — the same procedure as the 1844 by-election below.

## The surname

The clerk writes "Burn" on p54 and p60–63 and "Burns" on p56–58. The body of the transcripts
keeps his spelling; the DB name is Burns, which Bayanne confirms. Early transcription passes
read "Bunn" — that is a misread, fully worked through in
`~/projects/town_council_minutes/context/handover-bunn-burns.md`.

## Sibling record that needs the same treatment: May 1844

Found while checking this one. `elections` id 10, "Lerwick Town Council By-Election May 1844":

- `replaced_person` is **Charles Duncan** (id 86, died 1884). The book is explicit that it was
  **Gilbert Duncan**: p101, "electing a Bailie for the Burgh in place of Gilbert Duncan,
  Esquire, deceased"; p102, "Mr Leask was accordingly declared duly elected Junior Bailie of
  the Burgh in place of the late Mr Gilbert Duncan". Gilbert Duncan died 1844-02-19 (DB agrees).
  Charles is his son and was never a Bailie by 1844.
- `hidden` is **1**, so the page does not build. Presumably hidden because the replaced person
  looked wrong. With the replacement corrected it can be shown.
- `notes` is empty. Same shape as 1830: notice 20 April by Charles Ogilvy, Bailie; meeting
  3 May 1844, noon, Sheriff Court Room at Fort Charlotte; 21 burgesses listed (p101); Joseph
  Leask, merchant, proposed by Andrew Duncan Jr, seconded by Heddell, unanimous (p102).
- The candidacy (id 100) says "Appointed as Junior Baillie" / role "Junior Baillie" — the book
  says elected by the burgesses, and the spelling elsewhere in the DB is "Bailie".

Add it to `fix_minute_book.py` as item 3 rather than editing the DB by hand.

## Not a by-election

The 1818 Linklater vacancy (p15) is the only other pre-1844 vacancy the book has raised so
far, and no election to fill it has been found in the transcribed pages. Don't create one
without a page reference.
