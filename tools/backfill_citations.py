#!/usr/bin/env python3
"""
One-off backfill of data/citations.csv and data/citation_links.csv (run 2026-09-28). Kept as
provenance, like generate_ltc_terms.py: don't re-run it over hand-edited files.

1. Every BNA viewer link in research/bna/*.md becomes a citation (one row per article), with the
   evidence entry's text as its summary.
2. Every newspaper date in a ledger `source` (data/ltc_terms.csv) is linked to that row's term.
   A date matches the article citations from step 1 on that issue (and page, if the ledger gives
   one). If none match, an issue-level citation is created (no article number). If several match,
   the ones whose evidence text names the person are kept; if that doesn't narrow it, the link
   goes to the issue as a whole and is listed in the report.
   Dates inside brackets are event dates ("co-opted Fri 5 May 1967"), not issues, unless they
   follow "Shetland Times", "ST" or "Shetland News". Every issue date must fall on the paper's
   publication day (Saturday to 1943, Friday from 1944), or it is reported and skipped.
3. "LTC minute book pN" in a ledger source becomes a minute-book citation.

All links get basis = '' (not yet reviewed): the backfill can't tell a fact read on the page from
one worked out from it.
"""

import csv
import datetime
import glob
import os
import re
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')

PAPERS = {'0000666': 'shetland-times', '0003210': 'shetland-news'}
PREFIX = {'shetland-times': 'st', 'shetland-news': 'sn', 'ltc-minute-book': 'mb'}
MONTHS = {m: i for i, m in enumerate(
    ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], start=1)}
URL = re.compile(r'https://www\.britishnewspaperarchive\.com/image-viewer\?issue=BL%2F(\d{7})%2F(\d{8})'
                 r'&page=(\d+)&article=(\d+)')
MON = 'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'
DATE = re.compile(r'\b(\d{1,2}) (' + MON + r')[a-z]*\.? (\d{4})\b')


# Resolved by hand from the evidence entries: (ledger target, issue date) -> the articles meant.
PICKS = {
    ('sinclair-johnson@1899-11-07', '1900-10-13'): ['st-19001013-p4-a058', 'st-19001013-p8-a132'],
    ('alexander-ratter@1908-11-03', '1908-11-07'): ['st-19081107-p4-a149'],
    ('robert-ganson-i@1908-11-03', '1908-11-07'): ['st-19081107-p4-a149'],
    ('william-sinclair@1908-11-03', '1908-11-07'): ['st-19081107-p4-a149'],
    ('alexander-manson@1921-11-01', '1921-10-08'): ['st-19211008-p5-a115'],
    ('david-murray@1921-11-01', '1921-10-08'): ['st-19211008-p5-a115'],
}
ELECTION_LINKS = [  # (citation, election_id): picked by hand from the evidence entries dated near each election
    ('sn-19150501-p4-a038', 70), ('st-18851024-p2-a040', 35), ('st-18851031-p2-a024', 35),
    ('st-18851121-p3-a049', 35), ('st-18861016-p2-a035', 36), ('st-18861016-p2-a035', 37),
    ('st-18861030-p2-a033', 36), ('st-18861030-p2-a033', 37), ('st-18861120-p2-a032', 37),
    ('st-18870319-p2-a054', 38), ('st-18881006-p3-a068', 40), ('st-18881103-p2-a044', 40),
    ('st-18891026-p2-a014', 41), ('st-18891102-p2-a078', 41), ('st-18891102-p2-a082', 41),
    ('st-18891102-p3-a097', 41), ('st-18891109-p2-a052', 41), ('st-18901025-p2-a066', 42),
    ('st-18911031-p2-a040', 43), ('st-18941110-p2-a040', 46), ('st-18971030-p4-a077', 49),
    ('st-18981008-p5-a080', 50), ('st-18981029-p4-a055', 50), ('st-18981105-p4-a052', 50),
    ('st-18991028-p4-a101', 52), ('st-18991111-p5-a117', 52), ('st-19001103-p4-a088', 53),
    ('st-19011102-p1-a009', 54), ('st-19011109-p4-a083', 54), ('st-19021108-p4-a078', 55),
    ('st-19021115-p5-a111', 55), ('st-19031107-p4-a058', 56), ('st-19041105-p4-a112', 57),
    ('st-19061006-p5-a125', 60), ('st-19081017-p1-a014', 62), ('st-19081107-p1-a020', 62),
    ('st-19081107-p4-a149', 62), ('st-19091016-p1-a007', 63), ('st-19091106-p1-a006', 63),
    ('st-19091106-p4-a070', 63), ('st-19100108-p4-a071', 64), ('st-19101015-p4-a078', 65),
    ('st-19101105-p4-a077', 65), ('st-19111014-p1-a004', 66), ('st-19111028-p4-a075', 66),
    ('st-19111104-p3-a113', 66), ('st-19111111-p4-a093', 66), ('st-19121012-p4-a101', 67),
    ('st-19121019-p4-a093', 67), ('st-19121102-p4-a074', 67), ('st-19131101-p4-a054', 68),
    ('st-19131101-p4-a057', 68), ('st-19131108-p4-a064', 68), ('st-19141024-p4-a063', 69),
    ('st-19141031-p4-a059', 69), ('st-19150807-p4-a093', 71), ('st-19160805-p5-a107', 72),
    ('st-19160909-p4-a075', 73), ('st-19191018-p5-a082', 75), ('st-19191108-p4-a077', 75),
    ('st-19191108-p4-a089', 75), ('st-19201016-p1-a009', 76), ('st-19210611-p5-a101', 77),
    ('st-19211015-p4-a121', 78), ('st-19221014-p4-a083', 79), ('st-19221104-p5-a283', 79),
    ('st-19231013-p4-a094', 80), ('st-19231103-p4-a062', 80), ('st-19241025-p4-a060', 82),
    ('st-19241101-p4-a091', 82), ('st-19271022-p4-a061', 85), ('st-19271029-p4-a073', 85),
    ('st-19281103-p5-a090', 86), ('st-19301101-p4-a070', 88), ('st-19311031-p4-a097', 89),
    ('st-19341013-p4-a088', 93), ('st-19341103-p4-a045', 93), ('st-19360711-p4-a101', 95),
    ('st-19360711-p4-a106', 95), ('st-19360808-p4-a068', 96), ('st-19360808-p4-a074', 96),
    ('st-19361114-p4-a119', 98), ('st-19361114-p4-a127', 97), ('st-19361114-p4-a127', 98),
    ('st-19371106-p4-a072', 99), ('st-19380205-p4-a077', 100), ('st-19381105-p4-a093', 101),
    ('st-19400608-p5-a094', 102), ('st-19400706-p4-a059', 102), ('st-19400713-p2-a028', 102),
    ('st-19410111-p5-a048', 103), ('st-19410111-p5-a054', 103), ('st-19410705-p2-a031', 104),
    ('st-19411011-p2-a030', 105), ('st-19411011-p2-a034', 105), ('st-19420404-p2-a041', 106),
    ('st-19430213-p2-a080', 107), ('st-19441110-p2-a042', 108), ('st-19450209-p2-a044', 109),
    ('st-19451102-p2-a034', 110), ('st-19451109-p2-a047', 110), ('st-19451214-p3-a076', 111),
    ('st-19461004-p5-a071', 113), ('st-19461101-p4-a044', 113), ('st-19461108-p4-a059', 113),
    ('st-19461108-p4-a061', 113), ('st-19470523-p7-a145', 692), ('st-19471010-p4-a083', 114),
    ('st-19490429-p4-a050', 115), ('st-19490429-p7-a100', 695), ('st-19490916-p3-a030', 116),
    ('st-19490916-p5-a055', 116), ('st-19500721-p4-a051', 719), ('st-19500721-p4-a051', 720),
    ('st-19500721-p5-a058', 720), ('st-19500728-p1-a004', 720), ('st-19500804-p4-a060', 719),
    ('st-19500804-p4-a060', 720), ('st-19500818-p4-a055', 720), ('st-19510413-p4-a047', 119),
    ('st-19510413-p4-a048', 119), ('st-19520418-p4-a055', 120), ('st-19520502-p3-a043', 120),
    ('st-19520502-p5-a074', 724), ('st-19520509-p4-a066', 724), ('st-19530501-p4-a044', 121),
    ('st-19530508-p5-a151', 121), ('st-19540423-p4-a056', 122), ('st-19540507-p4-a048', 122),
    ('st-19540514-p7-a118', 122), ('st-19550429-p4-a062', 123), ('st-19550506-p5-a073', 749),
    ('st-19560413-p4-a064', 125), ('st-19570419-p4-a052', 126), ('st-19580425-p4-a062', 776),
    ('st-19580509-p4-a051', 776), ('st-19580509-p4-a054', 127), ('st-19580516-p5-a061', 776),
    ('st-19580606-p4-a050', 128), ('st-19580627-p4-a055', 128), ('st-19590501-p8-a120', 129),
    ('st-19590911-p6-a073', 130), ('st-19600415-p4-a060', 131), ('st-19610428-p4-a028', 805),
    ('st-19610512-p5-a065', 805), ('st-19610512-p7-a099', 132), ('st-19610512-p7-a099', 133),
    ('st-19610616-p3-a057', 133), ('st-19620810-p6-a090', 135), ('st-19630419-p5-a079', 136),
    ('st-19640417-p4-a054', 137), ('st-19640501-p4-a087', 137), ('st-19640508-p4-a050', 832),
    ('st-19640508-p4-a055', 137), ('st-19650416-p5-a056', 138), ('st-19660415-p5-a079', 139),
    ('st-19660422-p4-a060', 139), ('st-19670414-p4-a059', 140), ('st-19670414-p4-a061', 140),
    ('st-19670414-p4-a061', 141), ('st-19670512-p3-a046', 141), ('st-19670811-p4-a067', 142),
    ('st-19670811-p5-a094', 142), ('st-19680419-p4-a059', 143), ('st-19680510-p4-a062', 143),
    ('st-19690509-p1-a149', 144), ('st-19700417-p1-a002', 145), ('st-19700417-p1-a003', 146),
    ('st-19700515-p11-a097', 146), ('st-19720414-p1-a001', 148), ('st-19730413-p1-a002', 149),
    ('st-19210528-p5-a151', 464),
]
# Facts on person pages set by fix_newspapers.py: (citation, person slug:field)
PERSON_LINKS = [
    ('st-19670630-p6-a127', 'robert-anderson-i:died_date'), ('st-19670707-p4-a074', 'robert-anderson-i:died_date'),
    ('st-19120406-p4-a076', 'william-macdougall:intro'), ('st-19120406-p4-a060', 'william-macdougall:intro'),
    ('st-19121012-p4-a101', 'william-macdougall:intro'),
    ('st-19570419-p4-a052', 'grace-halcrow:intro'), ('st-19580425-p4-a062', 'grace-halcrow:intro'),
    ('st-19580509-p4-a051', 'grace-halcrow:intro'),
    ('st-19380827', 'john-williamson-iii:intro'), ('st-19381029', 'john-williamson-iii:intro'),
]
# Ledger sources that cite an election preview without its issue date.
# (phrase in the source, citation, basis, note)
PREVIEWS = [
    ('preview of the 6 Nov 1934 election', 'st-19341103-p4-a045', '', ''),
    ('preview of the 1 Nov 1938 election', 'st-19381029', 'inferred',
     "the preview's issue date is taken from John A. Williamson's row (ST 29 Oct 1938)"),
    ('Shetland Times preview: Dalziel', 'st-19381029', 'inferred',
     "the preview's issue date is taken from John A. Williamson's row (ST 29 Oct 1938)"),
]


def cid(pub, date, page=None, article=None):
    s = f"{PREFIX[pub]}-{date.replace('-', '')}"
    if page:
        s += f"-p{int(page)}"
    if article:
        s += f"-a{article}"
    return s


def clean(text):
    text = re.sub(r'\*\*|`', '', text)
    text = re.sub(r'https://\S+', '', text)
    return re.sub(r'\s+', ' ', text).strip(' -:')


def publication_day_ok(pub, date):
    d = datetime.date.fromisoformat(date)
    if pub != 'shetland-times':
        return True
    return d.weekday() == (5 if d.year <= 1943 else 4) or (d.year == 1944 and d.weekday() in (4, 5))


def evidence_citations():
    cites = {}
    for path in sorted(glob.glob(os.path.join(ROOT, 'research/bna/*.md'))):
        name = os.path.basename(path)
        lines = open(path).read().split('\n')
        for i, line in enumerate(lines):
            for m in URL.finditer(line):
                code, ymd, page, art = m.groups()
                pub = PAPERS[code]
                date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
                # The entry: the bullet this URL belongs to (walk back to its "- " line).
                j = i
                while j > 0 and not lines[j].lstrip().startswith(('- ', '|')) and lines[j].strip():
                    j -= 1
                entry = ' '.join(l.strip() for l in lines[j:i + 1])
                summary = clean(entry)
                if len(summary) < 40 or re.match(r'^\d{4}$', summary):
                    # A bare link list ("- 1954: <url>"): use the table row citing the same article.
                    art_ref = f"(art. {art})"
                    rows = [l for l in lines if art_ref in l and l.strip().startswith(('|', '- **'))]
                    if rows:
                        summary = clean(rows[0].replace('|', ' '))
                # Drop the citation itself ("Shetland Times, Sat 6 Apr 1912, p4 (art. 076):"): the id has it.
                summary = re.sub(r'^.{0,80}?\(art\. \d+\)[,:]?\s*', '', summary)
                key = cid(pub, date, page, art)
                if key in cites:
                    if name not in cites[key]['evidence_file'].split():
                        cites[key]['evidence_file'] += ' ' + name
                    continue
                cites[key] = dict(id=key, publication=pub, issue_date=date, page=str(int(page)),
                                  article=art, summary=summary[:400], evidence_file=name)
    return cites


def issue_dates(segment):
    """Newspaper issue dates in one ledger source segment, with the page if given."""
    # "8 and 29 Oct 1898" -> "8 Oct 1898 and 29 Oct 1898"
    segment = re.sub(r'\b(\d{1,2}) and (\d{1,2}) (' + MON + r')[a-z]*\.? (\d{4})\b',
                     r'\1 \3 \4 and \2 \3 \4', segment)
    out = []
    depth = 0
    for m in re.finditer(r'[()]|' + DATE.pattern + r'(?: (?:p|page )(\d+))?', segment):
        tok = m.group(0)
        if tok == '(':
            depth += 1
            continue
        if tok == ')':
            depth = max(0, depth - 1)
            continue
        if depth:
            # Inside brackets, a date is an issue only in a run of dates that follows the paper's name:
            # "(Shetland Times 8 Oct 1898 and 29 Oct 1898 p4: ...)".
            paper = list(re.finditer(r'Shetland Times|Shetland News|\bST\b', segment[:m.start()]))
            if not paper:
                continue
            between = segment[paper[-1].end():m.start()]
            if not re.fullmatch(r'(?:[ ,]|and|(?:Sat|Fri|Mon|Tue|Wed|Thu|Sun)\w*|' + DATE.pattern + r'|p\d+)*', between):
                continue
        day, mon, year, page = m.group(1), m.group(2), m.group(3), m.group(4)
        date = f"{year}-{MONTHS[mon.lower()]:02d}-{int(day):02d}"
        out.append((date, page))
    return out


def main():
    cites = evidence_citations()
    people = {}
    links, report = [], []
    by_issue = defaultdict(list)
    for c in cites.values():
        by_issue[(c['publication'], c['issue_date'])].append(c)

    rows = list(csv.DictReader(open(os.path.join(DATA, 'ltc_terms.csv'), newline='')))
    for n, r in enumerate(rows, start=2):
        src = r['source']
        if not src:
            continue
        target = f"{r['person_slug'] or r['person_name']}@{r['start_date']}"
        surname = (r['person_name'].split('(')[0].strip().split() or [''])[-1]
        seen = set()
        for seg in re.split(r';', src):
            for mb in re.finditer(r'minute book p(\d+)', seg):
                key = f"mb-p{mb.group(1)}"
                cites.setdefault(key, dict(id=key, publication='ltc-minute-book', issue_date='',
                                           page=mb.group(1), article='', summary='', evidence_file=''))
                if key not in seen:
                    seen.add(key)
                    links.append(dict(citation_id=key, target_type='term', target=target, basis='', note=''))
            if 'minute book' in seg and not re.search(r'Shetland|ST\b', seg):
                continue
            pub = 'shetland-news' if 'Shetland News' in seg else 'shetland-times'
            for date, page in issue_dates(seg):
                if not publication_day_ok(pub, date):
                    report.append(f"line {n} {target}: {date} is not a {pub} publication day: {seg.strip()[:90]}")
                    continue
                if (target, date) in PICKS:
                    for key in PICKS[(target, date)]:
                        if key not in seen:
                            seen.add(key)
                            links.append(dict(citation_id=key, target_type='term', target=target, basis='', note=''))
                    continue
                cands = [c for c in by_issue[(pub, date)] if not page or c['page'] == str(int(page))]
                if len(cands) > 1:
                    named = [c for c in cands if surname and surname.lower() in c['summary'].lower()]
                    if len(named) >= 1:
                        cands = named
                if len(cands) == 1 or (cands and len(cands) <= 3 and all(
                        surname.lower() in c['summary'].lower() for c in cands)):
                    keys = [c['id'] for c in cands]
                else:
                    if len(cands) > 1:
                        report.append(f"line {n} {target}: {date} matches {len(cands)} articles, linked to the issue: "
                                      + ', '.join(c['id'] for c in cands))
                    key = cid(pub, date, page)
                    cites.setdefault(key, dict(id=key, publication=pub, issue_date=date, page=page or '',
                                               article='', summary='', evidence_file=''))
                    keys = [key]
                for key in keys:
                    if key not in seen:
                        seen.add(key)
                        links.append(dict(citation_id=key, target_type='term', target=target, basis='', note=''))
        for phrase, key, basis, note in PREVIEWS:
            if phrase in src and key not in seen:
                seen.add(key)
                cites.setdefault(key, dict(id=key, publication='shetland-times', issue_date='1938-10-29', page='',
                                           article='', summary='', evidence_file=''))
                links.append(dict(citation_id=key, target_type='term', target=target, basis=basis, note=note))
        if 'preview' in src and not any(p in src for p, *_ in PREVIEWS):
            report.append(f"line {n} {target}: cites a preview with no issue date: {src[:100]}")
        if not seen and 'per-row source not recorded' not in src:
            report.append(f"line {n} {target}: no citation found in: {src[:100]}")

    for key, eid in ELECTION_LINKS:
        assert key in cites, key
        links.append(dict(citation_id=key, target_type='election', target=str(eid), basis='', note=''))

    for key, target in PERSON_LINKS:
        assert key in cites, key
        links.append(dict(citation_id=key, target_type='person', target=target, basis='', note=''))

    fields = ['id', 'publication', 'issue_date', 'page', 'article', 'summary', 'evidence_file']
    out = sorted(cites.values(), key=lambda c: (c['publication'], c['issue_date'], int(c['page'] or 0), c['article'], c['id']))
    with open(os.path.join(DATA, 'citations.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator='\n')
        w.writeheader()
        w.writerows(out)
    with open(os.path.join(DATA, 'citation_links.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['citation_id', 'target_type', 'target', 'basis', 'note'], lineterminator='\n')
        w.writeheader()
        w.writerows(links)
    print(f"citations: {len(out)} ({sum(1 for c in out if c['article'])} articles), links: {len(links)}")
    print('\n'.join(report))


if __name__ == '__main__':
    main()
