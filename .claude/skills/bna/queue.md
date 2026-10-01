# `/bna next`: work the queue

`research/bna/queue.md` is a ranked list of research items. At the start of a run, apply any
answers James has written in `research/bna/open-questions.md` and delete those entries. Then:

1. Take the first unticked item. Read its current term_issues and ledger rows first
   (`data/ltc_terms.csv`, looked up by `person_slug`). An item may be a **batch** of sub-items
   (one per issue): work them all in one run, in order, and do steps 3–8 (edits, rebuild,
   evidence, tick, commit) after **each** sub-item, so a run that stalls loses nothing. Share
   searches across sub-items where one retiring list or result covers several. Tick the batch
   when its last sub-item is done.
2. Run the searches (SKILL.md sections 1–4).
3. **Apply** the edits without asking. James authorised this for queue items. Ledger rows get
   `confirmed=1` and a comma-free `source` naming the issues. Site text (intros, notes) goes in a
   correction script such as `fix_newspapers.py`: guard on the old value, cite the source in the
   docstring.
4. `python3 build.py`, then `python3 build.py --check`. Compare the term_issues count before and
   after, and look at any new issues it creates. A size-short run is hidden once **every** row
   sitting through it is `confirmed=1`, so a genuine vacancy only clears when both ends of each
   sitting row are sourced. For a size-short item, list the unconfirmed rows sitting in that
   window first: those are the real targets, not the vacancy itself.
5. Record the findings (SKILL.md, "Record what you find"): evidence file and the three `data/`
   files.
6. Anything that contradicts the wiki, CLAUDE.md or a confirmed ledger row goes in
   `research/corrections-log.md` and is flagged in the reply.
7. Tick the queue item with a one-line result (issues cleared, evidence file). Add any new items
   the research turns up. Update CLAUDE.md where its notes are now wrong.
8. Commit when done: the ledger, `shetland.db`, the evidence file, the three citation files, the
   queue and any CLAUDE.md or correction-script changes together, with a message naming what was
   confirmed and the source (e.g. "Confirm LTC terms 1920-23 from the Shetland Times retiring
   lists"). Don't push.
