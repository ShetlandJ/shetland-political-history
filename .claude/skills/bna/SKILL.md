---
name: bna
description: Work the research queue ("/bna next") or look something up in the British Newspaper Archive (mainly the Shetland Times) through James's logged-in Chrome, and report back cited findings for the council ledger or correction scripts. Trigger on "/bna <question>", "check the BNA for", "search the Shetland Times for", or a request to confirm a council date, vacancy, co-option or retiring list from newspapers.
---

# /bna: cited newspaper lookups

James is logged in to britishnewspaperarchive.com in Chrome. You drive that session with the
Claude in Chrome tools to answer a bounded research question, and report findings with exact
citations. You **propose** ledger/correction edits; you don't make them unless James says so.

The order of work is set by cost. Always try the cheaper step first:

1. **Search-result snippets**: text only, cheap. Often enough on their own.
2. **Article OCR** from the viewer's Articles panel: text, medium.
3. **Zoomed screenshot** of the article: images, expensive. Last resort.

## Setup

- Load every browser tool you need in one ToolSearch call:
  `select:mcp__claude-in-chrome__tabs_context_mcp,mcp__claude-in-chrome__navigate,mcp__claude-in-chrome__computer,mcp__claude-in-chrome__find,mcp__claude-in-chrome__javascript_tool,mcp__claude-in-chrome__browser_batch,mcp__claude-in-chrome__tabs_close_mcp`
- `tabs_context_mcp` with `createIfEmpty: true`, and use the tab it gives you. Close it at the end.
- Use `browser_batch` whenever you can predict two or more steps (navigate + wait + extract).
- If a page shows a login or subscribe wall, stop and tell James. Never enter credentials.

## Two ways James uses this

- **Run a search**: "/bna when was Williamson co-opted in 1941?" Do the lookup and report.
- **Propose a search**: "what do you propose searching to find X?" James often wants the plan,
  not the lookup, because he searches himself. Answer with the search plan only (issues and
  date ranges in priority order, keywords, what to look for in each, and why), using the timing
  rules below and what the ledger already tells you. Don't open the browser unless he asks.

## `/bna next`: work the queue

`research/bna/queue.md` is a ranked list of research items. On `/bna next`:

1. Take the first unticked item. Read its current term_issues and ledger rows first
   (`data/ltc_terms.csv`, looked up by `person_slug`). An item may be a **batch** of sub-items
   (one per issue): work them all in one run, in order, and do steps 3–8 (edits, rebuild,
   evidence, tick, commit) after **each** sub-item, so a run that stalls loses nothing. Share
   searches across sub-items where one retiring list or result covers several. Tick the batch
   when its last sub-item is done.
2. Run the searches (sections 1–4 below).
3. **Apply** the edits without asking. James authorised this for queue items. Ledger rows get
   `confirmed=1` and a comma-free `source` naming the issues. Site text (intros, notes) goes in a
   correction script such as `fix_newspapers.py`: guard on the old value, cite the source in the
   docstring.
4. `python3 build.py`, then `python3 build.py --check`. Compare the term_issues count before and
   after, and look at any new issues it creates. A size-short run is hidden once **every** row
   sitting through it is `confirmed=1`, so a genuine vacancy only clears when both ends of each
   sitting row are sourced. For a size-short item, list the unconfirmed rows sitting in that
   window first: those are the real targets, not the vacancy itself.
5. Save the evidence to `research/bna/<topic>.md`: one entry per article, with the citation, the
   viewer link `https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2FYYYYMMDD&page=NNNN&article=NNN`,
   a short quote, and the ledger edit it supports. Get article ids from the result links:
   `[...document.querySelectorAll('a[href*="viewer"]')].map(a=>a.getAttribute('href'))`, with
   `?&=` replaced so the output isn't blocked. `ltc-1912-1914.md` is the model.
   **Record the citations in `data/`** too, so later runs don't have to search again:
   - `data/citations.csv`: one row per article, id `st-YYYYMMDD-pN-aNNN` (`sn-` for the Shetland
     News; `st-YYYYMMDD` for a whole issue; `mb-pN` for a minute-book page). `summary` is the short
     quote or paraphrase; `evidence_file` the research file. `build.py` builds the citation text
     and BNA link from the parts, and fails if the id doesn't match them or the date isn't a
     publication day. Before searching, check here and in `data/searches.csv`: the article may
     already be on file.
   - `data/citation_links.csv`: one row per fact the article supports. `target_type` is `term`
     (`person_slug@start_date` of the ledger row), `election` (the election id) or `person`
     (`slug:field`, e.g. `robert-anderson-i:died_date`). `basis` is `read` when the paper states
     the fact, `inferred` when it's worked out from it (a meeting day taken from "Tuesday
     night", a co-option dated by the only meeting that week), with a `note` saying how.
   - `data/searches.csv`: every search that found nothing useful, with keywords, date range,
     the question and what came back.
6. Anything that contradicts the wiki, CLAUDE.md or a confirmed ledger row goes in
   `research/corrections-log.md` and is flagged in the reply.
7. Tick the queue item with a one-line result (issues cleared, evidence file). Add any new items
   the research turns up. Update CLAUDE.md where its notes are now wrong.
8. Commit when done: the ledger, `shetland.db`, the evidence file, the three citation files, the queue and any CLAUDE.md or correction-script changes together, with a message naming what was confirmed and the source (e.g. "Confirm LTC terms 1920-23 from the Shetland Times retiring lists"). Don't push.

If the evidence is ambiguous on the point that decides an edit, don't guess. Leave that row
alone, record it in the evidence file, and add it to `research/bna/open-questions.md` (date
raised, the question, the options, the evidence file, the ledger row). James often works
remotely and answers there in a batch, so every unresolved point goes in that file, not only in
the reply. At the start of `/bna next`, apply any answers James has written in it and delete
those entries.

## 1. Plan the searches

Turn the question into one or more narrow searches: keywords plus a tight date range. Narrow
dates beat clever keywords, because the OCR is patchy (names split: "Sin clair", "Halerow").

Search URL (all parameters matter):

```
https://www.britishnewspaperarchive.com/search-newspapers/results?keywords=<kw>&newspaper=shetland%20times&startdate=YYYY-MM-DD&enddate=YYYY-MM-DD&exactdate=true&o=date&d=asc
```

`o=date&d=asc` gives oldest first. The Shetland Times has gaps (nothing 24 Apr–12 Jun 1915). If a
week returns 0 results, try `newspaper=shetland%20news` (viewer code `0003210` instead of `0000666`). For a single issue, set startdate = enddate = the publication
day (**Monday to 15 Mar 1875**, Saturday from 20 Mar 1875 to at least Feb 1943; **Friday by Oct 1944**).

Keywords are ANDed, and a long phrase often returns nothing ("town council election tuesday"
found 0 in a week that had the answer). Use one or two words and filter the snippets in JS
instead. Only 12 results show per page, so a common word over a two-week range (`tuesday`: 60
hits) hides most of them: narrow the dates or add a second word (`town election` worked well
for 1953–64).

### Lerwick Town Council timing (learned from 1932–1941, and 1949–64)

- The Shetland Times came out on **Saturdays**. Council meetings were on **Tuesdays**, and the
  report appears in the following Saturday's paper ("at their meeting on Tuesday").
- The **monthly meeting** was usually the first Tuesday of the month. Special meetings happened
  too, often on other weekdays ("held on Thursday of last week").
- The **October monthly meeting** is where the Clerk intimated the retiring councillors and the
  number of vacancies ("RETIRING COUNCILLORS"). The paper's election preview (the Saturday before
  the November general) repeats this with the candidates and their proposers; `*` marks retiring
  councillors in some years and former members in others, so read the key.
- The **statutory meeting** after the general elected the Provost and Bailies.
- A co-option usually went to the unsuccessful candidate with most votes at the last general, at
  the first meeting after the vacancy arose.
- **Wartime (1939–45)** there were no generals; every vacancy was filled by co-option. The wiki's
  co-option dates are often days or weeks out, and even the month in the title can be wrong
  ("By-Election August 1940" was 2 July). Departures were usually a resignation letter read at a
  meeting ("The Clerk read a letter from ..."), not the successor's co-option. Search the
  surname + `resignation`, or `co-opted`, over the two months before the co-option.
- **From 1949** the generals were in **May, on the first Tuesday** (Mon dates in the wiki are
  wrong; checked 1949–64). The Friday paper before says "Tuesday first is polling day", candidates'
  adverts give the date, and the next Friday has the result. Uncontested isn't safe to assume
  from the wiki: it shows 1954 with no votes, but there was a poll.
- A Provost due to retire stayed on, and someone else retired in his place (the Clerk,
  October 1937: "as the Provost was not retiring at this time, Mrs Nicol, after two years, had
  to retire").

Useful keywords: `town council`, `retiring councillors`, `co-opted`, `special meeting`,
`vacancy`, `statutory meeting`, `municipal election`, plus a surname.

## 2. Read the snippets

Navigate, wait about **6–8 seconds** (results render late, and at 3–4 seconds the page is often
still empty; `get_page_text` fails on this page), then read the results. Best: this helper pairs
each result with its issue, page and article id, so you get the citation and the snippet together.
Page navigation clears `window`, so define it in every batch that navigates:

```js
window._rs = () => { const seen = new Set(); const out = [];
  for (const a of document.querySelectorAll('a[href*="viewer"]')) {
    const m = a.getAttribute('href').match(/(\d{8}).page.(\d+).article.(\d+)/);
    if (!m || seen.has(m[0])) continue; seen.add(m[0]);
    let el = a; while (el && !/Added on/.test(el.innerText)) el = el.parentElement;
    const txt = el ? el.innerText.replace(/Lerwick, Shetland, Scotland[\s\S]*/, '').replace(/\s+/g, ' ').slice(0, 200) : '';
    out.push(m[1] + ' p' + m[2] + ' a' + m[3] + ': ' + txt); }
  return out.join('\n').replace(/[?&=]/g, ' '); };
document.body.innerText.match(/\d+ of \d+ results/) + '\n' +
  _rs().split('\n').filter(s => /tuesday|poll|elect/i.test(s)).join('\n').slice(0, 1400)
```

A `null` count means the page hadn't rendered yet (or there were no results): wait and re-run.

The plain version:

```js
const t = document.body.innerText;
const i = t.indexOf('Search results');
t.slice(i, i + 1500).replace(/[?&=]/g, ' ');
```

Tool output is **truncated at about 1,000–1,500 characters**, so read long pages in chunks
(`t.slice(i + 1500, i + 3000)` and so on). The `.replace(/[?&=]/g, ' ')` stops the tool from
blocking output that looks like query strings.

Each result gives: headline, the first ~300 characters of OCR, date, page. If a snippet answers
the question (it did for the Jan 1941 Williamson co-option), stop here and cite it.

## 3. Read the article OCR

Links can't be read via JavaScript (hrefs with query strings are blocked), so:

1. Fastest: with the issue, page and article id from `_rs()`, navigate straight to
   `https://www.britishnewspaperarchive.com/viewer/bl/0000666/YYYYMMDD/NNN/PPPP` (article, then
   4-digit page); it opens the viewer on that page. Otherwise `find` "search result link <headline
   words>" and click it.
2. Wait about 8 seconds. The viewer opens at
   `image-viewer?issue=BL/0000666/YYYYMMDD&page=N&article=NNN`. `0000666` is the Shetland Times.
3. Open the **Articles** panel. Clicking by coordinates (about x=860, y=27) often misses when the
   page hasn't finished loading. Clicking it in JS is reliable after an 8-second wait, and can go
   in the same batch as the read:
   `[...document.querySelectorAll('button')].find(b=>/^Articles on this page/.test(b.getAttribute('aria-label')||'')).click(); await new Promise(r=>setTimeout(r,3000));`
   The article text starts after "Select text below to edit it.".
4. Open the article. The panel doesn't open the one in the URL; you pick it from the list.
   Clicking a `find` ref usually does nothing (find often returns the `LI`, not its `BUTTON`).
   This helper clicks the button by its title and returns the text, and handles "Back to all
   articles" so you can open several in one call (define it after each navigation):
   ```js
   window._open = async (re) => { const back=[...document.querySelectorAll('button')].find(b=>/Back to all articles/.test(b.innerText)); if(back){back.click(); await new Promise(r=>setTimeout(r,1500));}
     const b=[...document.querySelectorAll('button')].find(e=>re.test((e.innerText||'').trim())); if(!b) return 'nf'; b.click(); await new Promise(r=>setTimeout(r,3000));
     const t=document.body.innerText; return t.slice(t.indexOf('Select text below')+30).replace(/[?&=]/g,' '); };
   const s = await _open(/^COUNCILLOR DEPARTS/); s.slice(0,1400)
   ```
   Titles come from the list (`t.slice(t.indexOf('Titles are generated'), ...)`). A council report
   is often split into sub-articles (a sub-heading like "NEW COUNCILLOR CO-OPTED"); the meeting's
   day is in the first one ("Lerwick Town Council. ..."), so open that too.
5. Read long text in chunks with `s.slice(1400, 2800)`, or `s.search(/surname/)` to jump to the
   point.

The page image itself is not in the text; only the Articles panel is.

## 4. Screenshot (last resort)

Only when the OCR is too garbled to trust on the point that matters (a name, a number, a date),
or when layout matters (a table). Use `computer` `zoom` on just the article region, at
`scale: 0.5–0.7`. Never screenshot full pages to read text.

## Reporting

For each question, report:

- **Answer**, in a sentence.
- **Citation**: Shetland Times, Saturday D Month YYYY, page N (article NNN), plus the meeting
  date if it's a meeting report ("meeting of Tuesday 7 Jan 1941").
- **Evidence**: a short quote (under 15 words) or a paraphrase. Don't reproduce whole articles.
- **What it changes**: the ledger row (`data/ltc_terms.csv`) or correction (`fix_newspapers.py`)
  you'd edit, and how, with the source string you'd write. Look people up by `person_slug` /
  `person_id`, never by name.
- **Anything ambiguous**, flagged for James rather than resolved by you.

For a sweep (for example the retiring notices for every year 1885–1975), write findings to a
scratch file as you go (one block per year: citation, retiring list, vacancies, notes) and give
James a summary at the end, not a running commentary.

## Limits

- Targeted lookups at a normal reading pace, in James's session, for his research. No bulk
  downloading, no scraping loops over hundreds of pages.
- Keep quotes short in anything you write back to the repo or the chat.
- After 2–3 searches that turn up nothing, report what you tried (keywords, date ranges) and
  suggest where to look next, rather than widening endlessly.
