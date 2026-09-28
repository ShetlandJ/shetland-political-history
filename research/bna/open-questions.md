# Open questions for James

Points the `/bna` runs couldn't settle from the newspapers, or where the edit is James's call.
Each run adds its unresolved points here. Answer inline (or in chat), and the next run turns the
answer into ledger or correction edits and deletes the entry.

Format: date raised, the question, the options, the evidence file, the row it affects.

## Open

- **2026-09-28: Should unconfirmed rows with checked end dates be confirmed?** Gear and Inkster
  (co-opted 7 Oct 1941) have confirmed start dates but unchecked end dates. The six 1945
  retirements are confirmed by ST 5 Oct 1945, but their start dates weren't checked. So far a row
  only gets `confirmed=1` when both ends are sourced. Keep that rule?
  Evidence: `ltc-wartime-1941.md`, `ltc-1945-1953.md`.

- **2026-09-28: Date of the Dunrossness North by-election, 1898.** The wiki says Rev. Charles
  Whyte was appointed on "Thursday 1 August" 1898, two months after Robert Henderson's death
  (19 Jun). But 1 August 1898 was a Monday, and 1 September 1898 was a Thursday, which fits "two
  months" better. Options: 1 Aug (as now, month precision), or 1 Sep. The Shetland Times had
  nothing for 1898 in two searches (`whyte`, `county council dunrossness`, Jun–Sep), so it
  may not be digitised for that year. Election 246; Henderson's seat and Whyte's term start.
  Evidence: `zcc-by-elections.md`.

- **2026-09-28: How should William Sinclair's Burra win (Dec 1919) be recorded?** He was
  returned for both Burra and Whiteness & Weisdale and chose Whiteness (wiki), so the Burra seat
  was never taken up and was filled at the Apr 1920 by-election. Today he gets both seats from
  2 Dec 1919, which is the overlap on /data-review. Options: (a) let `build.py` apply
  `data/not_seated.csv` to ward councils as well, list candidacy 1164 there, and set the Apr 1920
  by-election's replaced person to a marker such as `[double return]` (otherwise it would close
  his Whiteness seat); (b) leave the overlap as a known, genuine case. (a) changes `build.py`.
  Row: ZCC term 944, election 435.

- **2026-09-28: Day of Alexander Mitchell's resignation, October 1889.** ST Sat 19 Oct 1889 (p2,
  art. 051) reports it accepted "At a meeting of the Town Council, held last ..."; the scan loses
  the word after "last". The item just above says the Commissioners met "last night" (Fri 18 Oct).
  The ledger now ends his 1888 seat on 1889-10-18, unconfirmed. Options: accept 18 Oct and
  confirm, or check the minute book or the 26 Oct report for the meeting date.
  Evidence: `ltc-1885-1889.md`. Row: `alexander-mitchell-i` 1888-11-08.

## Answered

(Move entries here with the answer and the edit made, or delete them once applied.)
