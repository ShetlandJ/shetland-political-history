# Reading through the viewer (when you need to see the page)

The OCR and manifest endpoints (SKILL.md section 3) have mostly replaced this. Use the viewer
when you need the Articles panel's own text or are about to zoom on the image. The page image
sometimes stays black after navigating (seen Jun 1982); don't count on a screenshot.

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
