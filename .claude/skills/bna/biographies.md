# Biographies and person facts

Read this when the question is about a person (a biography, birth or death date, occupation)
rather than a council date. Learned writing five 1970s County Councillor biographies
(2026-10-01, `research/bna/biographies-zcc-1970s.md`).

## Before searching

- **Skip living people.** No new biographies of the living.
- **Check coverage.** The Shetland Times in the BNA runs to the end of 2000 (manifests 404 from
  2001). For anyone who died later there's no obituary, so only pick them if their career years
  have enough on their own.
- **Bayanne** (`bayanne.info`) is behind a Cloudflare bot check in the browser. Don't try to get
  past it; note it as not consulted.
- Read the wiki page (MySQL, `mwfn_`) and any existing research notes (`grep -rn surname
  research/bna/`). Earlier runs often have the person's election and resignation reports on file
  already, with citations in `data/citations.csv`.

## Where to look

- **Death notices**: the funeral notice comes first, then the death notice one to three issues
  after the death (p4 in the 1980s–90s, p14 in 1975). Allow three weeks: Irvine's ran 16 days
  after. A notice gives the place of death, age, spouse (often by maiden name), parents and home.
  Some leave out the year ("on 26th April"): take it from the issue and mark the citation link
  `inferred`.
- **Search surname + place**, not the deaths columns. The columns are split across several
  articles, each labelled by whoever comes first, so scanning manifest labels misses them;
  `irvine gulberwick` found Irvine's notice at once.
- **A quoted full name** (`"john m. laurenson"`) narrows a common surname to the man himself.
- **Obituaries** of prominent members are on p1 ("Councillor dies at 45"). Long ones continue
  elsewhere and the continuation can be hard to find; note it if you can't.
- **Nominations lists** give occupation and address (1973: "GPO engineer, Westing, Ladysmith
  Road, Scalloway").
- **Resignation and retirement reports** list the committees a member sat on.
- **Letters page** ("Our Readers' Views"): candidates wrote election letters, split over
  consecutive articles on the page. Caldwell's gave his Nordport role, found nowhere else.
- **Adverts** date a business and name the proprietor (Aith Knitwear Factory, 1968).

## Checks

- Compare the notice's age with our birth date (Balfour: "aged 73", but born Jan 1901 makes 74).
  Don't change either on that alone: open question.
- Compare death date and place with the DB, and fix a wrong date in a correction script.
- Confirm identity before using a notice (middle initial, place, age against birth year).

## Writing the biography

- Goes in the `BIOGRAPHIES` dict in `fix_newspapers.py` (#69): only fills an empty biography,
  and stops if a different one is already there.
- Paragraphs are separated by `\n\n`; `[person:slug:Name]` links another person (check the slug).
- Name spouses and parents from the notice; don't name children. Keep quotes under 15 words.
- Don't repeat the intro word for word; the intro is shown above the biography.
- Each fact gets a `citation_links` row with `target_type` `person` and `slug:biography` (or the
  field it supports, e.g. `slug:died_date`).
