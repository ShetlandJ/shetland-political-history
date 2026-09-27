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
   (`data/ltc_terms.csv`, looked up by `person_slug`).
2. Run the searches (sections 1–4 below).
3. **Apply** the edits without asking. James authorised this for queue items. Ledger rows get
   `confirmed=1` and a comma-free `source` naming the issues. Site text (intros, notes) goes in a
   correction script such as `fix_newspapers.py`: guard on the old value, cite the source in the
   docstring.
4. `python3 build.py`, then `python3 build.py --check`. Compare the term_issues count before and
   after, and look at any new issues it creates.
5. Save the evidence to `research/bna/<topic>.md`: one entry per article, with the citation, the
   viewer link `https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2FYYYYMMDD&page=NNNN&article=NNN`,
   a short quote, and the ledger edit it supports. Get article ids from the result links:
   `[...document.querySelectorAll('a[href*="viewer"]')].map(a=>a.getAttribute('href'))`, with
   `?&=` replaced so the output isn't blocked. `ltc-1912-1914.md` is the model.
6. Anything that contradicts the wiki, CLAUDE.md or a confirmed ledger row goes in
   `research/corrections-log.md` and is flagged in the reply.
7. Tick the queue item with a one-line result (issues cleared, evidence file). Add any new items
   the research turns up. Update CLAUDE.md where its notes are now wrong.
8. Don't commit unless James asks.

If the evidence is ambiguous on the point that decides an edit, don't guess. Leave that row
alone, record it in the evidence file, and ask James.

## 1. Plan the searches

Turn the question into one or more narrow searches: keywords plus a tight date range. Narrow
dates beat clever keywords, because the OCR is patchy (names split: "Sin clair", "Halerow").

Search URL (all parameters matter):

```
https://www.britishnewspaperarchive.com/search-newspapers/results?keywords=<kw>&newspaper=shetland%20times&startdate=YYYY-MM-DD&enddate=YYYY-MM-DD&exactdate=true&o=date&d=asc
```

`o=date&d=asc` gives oldest first. For a single issue, set startdate = enddate = the Saturday.

### Lerwick Town Council timing (learned from 1932–1941)

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
- A Provost due to retire stayed on, and someone else retired in his place (the Clerk,
  October 1937: "as the Provost was not retiring at this time, Mrs Nicol, after two years, had
  to retire").

Useful keywords: `town council`, `retiring councillors`, `co-opted`, `special meeting`,
`vacancy`, `statutory meeting`, `municipal election`, plus a surname.

## 2. Read the snippets

Navigate, wait about 3 seconds (results render late; `get_page_text` fails on this page), then
read the results text:

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

1. `find` "search result link <headline words>" to get a ref, then click it.
2. Wait about 5 seconds. The viewer opens at
   `image-viewer?issue=BL/0000666/YYYYMMDD&page=N&article=NNN`. `0000666` is the Shetland Times,
   so you can also navigate straight to a known issue and page.
3. Click the **Articles** button in the toolbar (top right, about x=860, y=27 in the 1133-wide
   frame). The selected article's OCR loads into the page text.
4. Read it: `const t=document.body.innerText; const i=t.indexOf('<first words of article>'); t.slice(i, i+1500).replace(/[?&=]/g,' ')`,
   continuing in chunks as needed.

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
