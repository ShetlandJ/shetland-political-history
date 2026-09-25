# Corrections from the Lerwick Town Council minute book

Findings from cross-referencing `shetland.db` against the first Lerwick Town Council minute
book (1818–1877), transcribed at `~/projects/town_council_minutes/transcripts/NNN.md`
(page numbers below are the book's own, and the file names). The minute book is a primary
source: where it and the DB disagree, the DB is wrong unless the transcript is flagged doubtful.

Raised 2026-09-20. Items 1 and 2 applied 2026-09-20 by `fix_minute_book.py` (idempotent, runs
after `add_modern_sic.py` in the rebuild sequence). Bayanne facts used:

- Andrew Duncan jr, I10526: b. 13 Oct 1800 Lerwick, d. 1 Sep 1852 Lerwick, writer; son of I10506
  and Grace Scott. Now `people.andrew-duncan-ii`.
- William Rae Duncan (1858–1945, I10539) is the son of William Rae Duncan (1805–1876), who is the
  Sheriff's fifth child. So the grandfather is the Sheriff; the councillor is the uncle.
- Magnus Burns, I122332: b. 1757 Cruicikirk, Unst, d. 9 Jan 1846 Lerwick; merchant in Lerwick,
  father of David Nisbet Burns (I122333). Now `people.magnus-burns`. Relationship is asserted by
  Bayanne, so item 2's caution no longer applies.

`parse_wiki.py` regenerates `shetland.db` from the wiki dump, so fixes need to live either in
the wiki-derived source or in a post-parse fix script (the `fix_bayanne_data.py` pattern),
not as one-off edits to the SQLite file.

---

## 1. The 1829 councillor "Andrew Duncan" is the Sheriff's son, not the Sheriff

**What the DB says.** `candidacies` for the 1829-09-03 election links councillor
"Andrew Duncan" to `people.andrew-duncan` — Andrew Duncan, Sheriff Substitute (1776–1847,
Bayanne I10506). That is the only candidacy on that person, and it is what puts him in the
"Lerwick Town Councillors" category. The Sheriff's `biography` is empty; only `intro` is populated.

**What the book says.** The councillor elected in 1829 is explicitly the younger man, and
the Sheriff is on the same page in a different role:

| Page | Text |
|---|---|
| p47 | 3 Sep 1829 election. "The Meeting then made choice of Andrew Duncan Esquire Sheriff Substitute of Zetland to be their preses" — then, among the nine councillors elected by ballot, "Andrew Duncan Junior, Writer". |
| p47 | "thanks of the meeting be returned to Mr Sheriff Duncan for his conduct during the day" — the preses, not a councillor-elect. |
| p49 | Acceptance of office signed "Wm. Spence, Charles Ogilvy, Andw. Duncan Junior, Wm. Sievwright, Jas. Pottinger, Gilbt. …" |
| p55 | Fined for non-attendance: "Mr A. Duncan Jr., Mr Sievwright, Mr Clark and Mr Robertson". |
| p63 | 15 May 1832, fined: "Councillors J. Ogilvy, J Greig and A Duncan". |
| p64 | 26 Sep 1832 election — the Sheriff is preses again ("Andrew Duncan (Sheriff Substitute) as preses"), while Gilbert Duncan is elected councillor. |
| p105 | 1844: "Mr Andrew Duncan Junr" moves a motion — still active, still styled Junior. |

Earlier appearances of the Sheriff are all non-council: petitioner for the charter (p6, 1818),
"Mr Duncan Sheriff" in the 1826 burgess roll (p34). Nowhere in the transcribed pages does the
Sheriff sit, sign, or get fined as a councillor. The 1818 first council (p7–8) does not include him.

**Who the son is.** Bayanne I10526 — Andrew Duncan, writer, child of Andrew Duncan snr:
https://www.bayanne.info/Shetland/getperson.php?personID=I10526&tree=ID1
(James confirmed this 2026-09-20.) Gilbert Duncan, the Sheriff's brother, was also a writer
and a councillor, so "Councillor Duncan" in the 1820s is Gilbert; from 1829–32 it is Andrew Junior.

**Why it happened.** `parse_wiki.py` resolves candidate names to people through
`person_name_map` (display name → person_id). "Andrew Duncan" matched the only wiki page of
that name. Any councillor who shares a name with a wiki page is exposed to the same error;
worth a scan for other Junior/Senior pairs (the book uses "Junior", "Junr", "Jr.", "Senr").

**To do.**
1. Create a `people` row for Andrew Duncan (junior), writer, `bayanne_id` I10526, dates and
   places from Bayanne. Category "Lerwick Town Councillors". Intro should say he is the
   Sheriff's son and was a councillor 1829–1832 (elected 3 Sep 1829, served to the 6 Sep 1832
   election; not re-elected then).
2. Re-point the 1829 candidacy's `person_id` to the new row.
3. Remove "Lerwick Town Councillors" from the Sheriff's categories and from the succession
   data on his page, unless some other source has him sitting. The book gives none.
4. `people.william-duncan-ii` (William Rae Duncan, 1858–1945, Bayanne I10539) says "His
   grandfather Andrew was a Lerwick Town Councillor and Sheriff-Substitute". Bayanne will say
   which Andrew is the grandfather. Either way the sentence conflates two men and needs
   rewording: the Sheriff was not a councillor, and the councillor was not the Sheriff.
5. `tools/generate_ltc_terms.py` — `council_terms` is empty for Lerwick Town Council, so
   nothing to regenerate yet, but whatever populates it should use the corrected link.

---

## 2. The DB has no 1830 by-election: Irvine died, Magnus Burn(s) replaced him

**What the DB says.** Alexander Cumming Irvine, elected councillor 1829, died 1830-03-03
(DB agrees). `elections` has no Lerwick Town Council by-election before 1844-05-03, so the
1829–32 council in the DB carries a dead man for two and a half years and has no record of
his replacement.

**What the book says.**

| Page | Text |
|---|---|
| p53 | 6 Apr 1830: a Burgesses' meeting is called "for the purpose of filling up the vacancy which has occurred in the Council by Mr. Irvines decease", summoned for Wednesday 21 April. |
| p54 | 21 Apr 1830: "Mr. Magnus Burn[s] Merchant in Lerwick was found duly elected, and declared to be one of the Councillors for this Burgh". |
| p55–63 | "Burns" / "Burn" in the Present lists through to May 1832. |
| p64 | Not re-elected at the Sep 1832 election. |

**The mechanism.** The Charter itself has no vacancy provision — only the first election and
the triennial rule — but it gives the Bailies and Councillors power to make bye-laws (p10).
When Bailie Linklater died in 1818 the Council had to write to Hay Donaldson W.S. in
Edinburgh for advice on how to fill the seat (p15). Spence intimated a motion on supplying
vacancies on 5 Dec 1826 (p36), and on 6 Oct 1829 the Council passed the bye-law (p52): a
vacancy in the Magistracy or Council, once notified to a Council meeting, "shall be filled up
at a meeting of the Burgesses called for the purpose, as in the case of an ordinary election".
The 21 Apr 1830 meeting recites exactly that — the Charter's bye-law clause and "a Regulation
made by them regarding the filling up of vacancies" (p54). So it is a burgess by-election, not
a co-option, and it is the same mechanism the DB already records for the 1844-05-03
by-election after Gilbert Duncan's death (p101).

The surname: the clerk writes both "Burns" (p56–58) and "Burn" (p60–63). "Burns" is the
attested Shetland surname. See `~/projects/town_council_minutes/context/handover-bunn-burns.md`
for the full reading history — early transcription passes had "Bunn", which is a misread.

**Who he is.** Not in `people`. The only Burns in the DB is David Nisbet Burns (1809–1848),
draper, councillor 1844–48. Magnus is plausibly David's father but nothing in the book or the
DB says so — do not assert a relationship without Bayanne.

**To do.**
1. Add a by-election row to `elections`: council 1, 1830-04-21, `election_type` by-election,
   `replaced_person` Alexander Irvine (`replaced_person_id` his row), notes citing p53–54.
2. Add a candidacy for Magnus Burns, merchant, elected, role councillor.
3. Add a `people` row for Magnus Burns if Bayanne has him (merchant in Lerwick, alive 1830–32).
4. If `data-review.md` is the running list of by-election checks, this belongs there as ✅ —
   it is confirmed from the primary source.

---

## 3. Two general election dates are off by a day or three

Applied 2026-09-20 by `fix_minute_book.py` step 4.

| Election | DB had | Book says | Pages |
|---|---|---|---|
| Sept 1826 (id 4) | 1826-09-06 | 7 September 1826. The 6th was an ordinary council meeting the evening before, at the senior Bailie's house (p33). The election minute is headed "the 7th September 1826" and held "within the Sheriff Court Room ... upon the Seventh day of September" (p33–34), on a notice dated 30 August. | 33–35 |
| Sept 1844 (id 11) | 1844-09-02 | 5 September 1844. The 2nd is the date of Bailie Leask's notice calling the meeting. Election held "within the Sheriff Court Room in Fort-Charlotte, the fifth day of September" (p104), "in terms of publick Notice dated the 2nd day of September current" (p104–105). | 104–106 |

Both corrected dates are the first Thursday of September, consistent with the other
pre-1876 elections below. Side note, not yet in the DB: on 5 Sept 1844 the burgesses elected
Arthur Gifford of Busta Senior Bailie (p105), but he declined by letter (p107), and a separate
burgess meeting was called for 27 September 1844 to elect a Senior Bailie in his place (notice
24 September, p106). Check who the DB has as Senior Bailie/Provost for 1844–47.

---

## 3a. Sept 1874: the Junior Bailie was John Robertson Senior, not his nephew

Applied 2026-09-25 by `fix_minute_book.py` step 5.

**What the DB said.** In the first 1874 council group (election id 21, Duncan's election),
the Junior Bailie candidacy is named "John Robertson Snr" but was linked to
`people.john-robertson-ii`, the nephew (1826–1905).

**What the book says.** p258, meeting of 9 September 1874, copies Duncan's declaration:
"John Robertson, Senior, Merchant, residing in New Town, Lerwick, Junior Bailie", and the
Council records "the election of the said Messrs Arthur James Hay and John Robertson Senior,
to the office of Bailies as aforesaid is invalid". The nephew appears separately as
"John Robertson Jr" in 1874 sederunts (p252–254). Re-pointed to `people.john-robertson-i`.

**Both men's records checked** against the book for every election up to 1876 (p123–124,
134–136, 143–145, 153–154, 165–167, 188–190, 225–227, 237–239). Positions, offices and the
senior/junior links all match. Senior was Junior Bailie 1865, 1868 and 1871. The one
exception was 1874. The book ends in July 1877, so 1878 onward is unchecked from this source.

---

## 4. Confirmed, no action

Points where the book and the DB agree, recorded so nobody re-checks them:

- Election dates 1829-09-03, 1832-09-06 and 1835-09-03 match the book exactly (first
  Thursday of September, per the Charter).
- The full 1829 and 1832 elected slates in `candidacies` match p47 and p64 name for name,
  including offices (Spence Sr Bailie / C. Ogilvy Jr Bailie in 1829; C. Ogilvy Sr / Greig Jr
  in 1832). The one exception is item 1.
- William Spence "Esquire Surgeon, senior Bailie" (p63) — DB has him as staff-surgeon,
  Provost 1827–32. Consistent.
- John Ogilvy, banker, was a councillor 1829–38 alongside his brother Charles as Bailie. The
  book distinguishes them as "Councillor J. Ogilvy" and "Bailie Ogilvy" (p63; p55 has the
  same pattern with the initial not yet verified).
