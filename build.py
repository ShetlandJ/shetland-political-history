#!/usr/bin/env python3
"""
Build shetland.db from the committed text sources. This is the only way the DB should change.

    python3 build.py           # rebuild shetland.db
    python3 build.py --check   # rebuild to a temp file and fail if shetland.db differs (CI)

Steps:
  1. data/baseline.sql        Frozen extraction: the wiki parse plus every script-based fix up to
                              2026-09-25 (parse_wiki.py, add_*.py, populate_*.py, ...). Those
                              scripts are provenance now; the wiki is frozen and is not re-parsed.
  2. Correction scripts       fix_minute_book.py, fix_newspapers.py. Idempotent, source-cited.
                              New corrections go here (or in a new script added to CORRECTIONS).
  3. data/party_aliases.csv   Spelling and markup variants of party labels.
  4. council_terms            LTC from the hand-kept ledger data/ltc_terms.csv. ZCC and SIC are
                              ward-based, so their terms are derived from election results.
  5. term_issues              Checks over council_terms: the research to-do list, shown on
                              /data-review.
"""

import csv
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import defaultdict

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, 'shetland.db')
DATA = os.path.join(ROOT, 'data')

CORRECTIONS = ['fix_minute_book.py', 'fix_newspapers.py', 'fix_sic_by_elections.py']

# Councils that stopped existing at local government reorganisation (SIC took over).
ABOLISHED = {'lerwick-town-council': '1975-05-15', 'zetland-county-council': '1975-05-15'}
# LTC council size after the 1876 reform.
LTC_REFORM_DATE = '1876-11-07'
LTC_SIZE = 12

TERMS_DDL = """
DROP TABLE IF EXISTS council_terms;
CREATE TABLE council_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    council_id INTEGER NOT NULL REFERENCES councils(id),
    person_id INTEGER REFERENCES people(id),
    person_name TEXT NOT NULL,
    candidacy_id INTEGER REFERENCES candidacies(id),  -- the win that gave the seat; party comes from here
    constituency_id INTEGER REFERENCES constituencies(id),
    start_date TEXT NOT NULL,
    end_date TEXT,        -- exclusive; NULL = still serving
    start_reason TEXT,    -- 'elected', 'by-election', 'holdover'
    end_reason TEXT,      -- 'term_expired', 'died', 'resigned', 'replaced', 'council_abolished', ...
    confirmed INTEGER NOT NULL DEFAULT 0,  -- 1 = confirmed from primary sources
    source TEXT
);
CREATE INDEX idx_terms_council_dates ON council_terms(council_id, start_date, end_date);
CREATE INDEX idx_terms_person ON council_terms(person_id);
DROP TABLE IF EXISTS term_issues;
CREATE TABLE term_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    council_id INTEGER NOT NULL REFERENCES councils(id),
    kind TEXT NOT NULL,
    date_from TEXT,
    date_to TEXT,
    person_id INTEGER REFERENCES people(id),
    person_name TEXT,
    election_id INTEGER REFERENCES elections(id),
    detail TEXT NOT NULL
);
"""

FULL_DATE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def read_csv(name):
    with open(os.path.join(DATA, name), newline='') as f:
        return list(csv.DictReader(f))


def strip_suffix(name):
    return re.sub(r'\s*\([^)]*\)\s*$', '', name or '').strip()


def display_name(candidate_name):
    """'[url Display Name]' -> 'Display Name'."""
    m = re.match(r'^\[\S+\s+(.+)\]$', candidate_name.strip())
    return m.group(1) if m else candidate_name.strip()


# ---------------------------------------------------------------------------
# Steps 1-3
# ---------------------------------------------------------------------------

def load_baseline(db_path):
    db = sqlite3.connect(db_path)
    with open(os.path.join(DATA, 'baseline.sql')) as f:
        db.executescript(f.read())
    db.commit()
    db.close()


def run_corrections(db_path):
    env = dict(os.environ, SHETLAND_DB=db_path)
    for script in CORRECTIONS:
        r = subprocess.run([sys.executable, os.path.join(ROOT, script)], env=env,
                           capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"{script} failed:\n{r.stdout}\n{r.stderr}")


def apply_party_aliases(db):
    n = 0
    for a in read_csv('party_aliases.csv'):
        cur = db.execute("UPDATE candidacies SET party = ? WHERE party = ?", (a['to'] or None, a['from']))
        n += cur.rowcount
    return n


# ---------------------------------------------------------------------------
# Step 4: council_terms
# ---------------------------------------------------------------------------

class Terms:
    def __init__(self, db):
        self.db = db
        self.rows = []
        self.issues = []

    def issue(self, council_id, kind, detail, date_from=None, date_to=None,
              person_id=None, person_name=None, election_id=None):
        self.issues.append((council_id, kind, date_from, date_to, person_id, person_name,
                            election_id, detail))


def council_ids(db):
    return {slug: cid for cid, slug in db.execute("SELECT id, slug FROM councils")}


def load_ltc_ledger(db, T):
    ltc = council_ids(db)['lerwick-town-council']
    people = {slug: (pid, name) for pid, slug, name in db.execute("SELECT id, slug, name FROM people")}
    elections = {eid: title for eid, title in db.execute(
        "SELECT id, wiki_page_title FROM elections WHERE council_id = ?", (ltc,))}

    for i, r in enumerate(read_csv('ltc_terms.csv'), start=2):
        where = f"data/ltc_terms.csv line {i}"
        pid, pname = people.get(r['person_slug'], (None, None))
        if r['person_slug'] and pid is None:
            sys.exit(f"{where}: unknown person_slug {r['person_slug']}")
        if not FULL_DATE.match(r['start_date']) or (r['end_date'] and not FULL_DATE.match(r['end_date'])):
            sys.exit(f"{where}: dates must be YYYY-MM-DD")

        candidacy_id = None
        if r['election_id']:
            eid = int(r['election_id'])
            if elections.get(eid) != r['election']:
                sys.exit(f"{where}: election_id {eid} is '{elections.get(eid)}', ledger says '{r['election']}'")
            if pid is not None:
                row = db.execute("SELECT id FROM candidacies WHERE election_id = ? AND person_id = ? AND elected = 1",
                                 (eid, pid)).fetchone()
            else:
                row = db.execute("""SELECT id FROM candidacies WHERE election_id = ? AND person_id IS NULL
                                    AND candidate_name = ? AND elected = 1""", (eid, r['person_name'])).fetchone()
            if row is None:
                sys.exit(f"{where}: no elected candidacy for {r['person_slug']} in election {eid}")
            candidacy_id = row[0]
        elif pid is not None:
            # Holdover: the seat came from their last win before this term.
            row = db.execute("""
                SELECT c.id FROM candidacies c JOIN elections e ON e.id = c.election_id
                WHERE c.person_id = ? AND c.elected = 1 AND e.council_id = ? AND e.hidden = 0
                  AND e.election_date < ?
                ORDER BY e.election_date DESC LIMIT 1
            """, (pid, ltc, r['start_date'])).fetchone()
            candidacy_id = row[0] if row else None

        T.rows.append(dict(council_id=ltc, person_id=pid, person_name=pname or r['person_name'],
                           candidacy_id=candidacy_id, constituency_id=None,
                           start_date=r['start_date'], end_date=r['end_date'] or None,
                           start_reason=r['start_reason'], end_reason=r['end_reason'] or None,
                           confirmed=int(r['confirmed'] or 0), source=r['source'] or None))


def derive_ward_terms(db, T, slug):
    """Ward-based councils: each general election replaces every seat; a by-election replaces the
    named member, or the only member of a single-member ward. Deaths end terms on the day."""
    cid = council_ids(db)[slug]
    deaths = {pid: d for pid, d in db.execute("SELECT id, died_date FROM people WHERE died_date IS NOT NULL")}

    rows = db.execute("""
        SELECT e.id, e.wiki_page_title, e.election_date, e.election_type, e.constituency_id,
               e.replaced_person, e.replaced_person_id, e.notes
        FROM elections e WHERE e.council_id = ? AND e.hidden = 0
        ORDER BY e.election_date, e.id
    """, (cid,)).fetchall()

    # Generals span many rows (one per ward) under one wiki title.
    events = []
    for r in rows:
        if events and r[3] == 'general' and events[-1]['type'] == 'general' and events[-1]['title'] == r[1]:
            events[-1]['wards'].append(r)
        else:
            events.append({'title': r[1], 'date': r[2], 'type': r[3], 'wards': [r]})

    open_terms = []  # dicts in T.rows with end_date None, still sitting
    seats = {}       # constituency_id -> seats at the last general

    def winners(election_id):
        return db.execute("""
            SELECT c.id, c.person_id, COALESCE(p.name, c.candidate_name)
            FROM candidacies c LEFT JOIN people p ON p.id = c.person_id
            WHERE c.election_id = ? AND c.elected = 1 ORDER BY c.position, c.id
        """, (election_id,)).fetchall()

    def close(t, date, reason):
        t['end_date'] = date
        t['end_reason'] = reason
        open_terms.remove(t)

    def start(cand, constituency_id, date, reason):
        cand_id, pid, name = cand
        t = dict(council_id=cid, person_id=pid, person_name=display_name(name), candidacy_id=cand_id,
                 constituency_id=constituency_id, start_date=date, end_date=None,
                 start_reason=reason, end_reason=None, confirmed=0, source=None)
        T.rows.append(t)
        open_terms.append(t)

    def find_member(pid, name):
        for t in open_terms:
            if pid is not None and t['person_id'] == pid:
                return t
        for t in open_terms:
            if name and (t['person_name'] == name or strip_suffix(t['person_name']) == strip_suffix(name)):
                return t
        return None

    for ev in events:
        date = ev['date']
        for t in list(open_terms):
            d = deaths.get(t['person_id'])
            if d and FULL_DATE.match(d) and t['start_date'] <= d < date:
                close(t, d, 'died')

        if ev['type'] == 'general':
            for t in list(open_terms):
                close(t, date, 'term_expired')
            seats = {}
            for w in ev['wards']:
                eid, ward = w[0], w[4]
                won = winners(eid)
                seats[ward] = max(1, len(won))
                for cand in won:
                    start(cand, ward, date, 'elected')
            continue

        # By-election
        eid, ward, rp_name, rp_id, notes = ev['wards'][0][0], ev['wards'][0][4], ev['wards'][0][5], ev['wards'][0][6], ev['wards'][0][7]
        named = []
        if rp_id or rp_name:
            named.append((rp_id, rp_name))
        also = re.search(r'Also replaced:\s*(.+)', notes or '')
        if also:
            named.append((None, also.group(1).strip()))

        for pid, name in named:
            if not pid and re.match(r'^\[.*\]$', name or ''):
                continue  # '[unfilled seat]', '[voided election re-run]': no member to replace
            t = find_member(pid, name)
            if t is not None:
                if ward is not None and t['constituency_id'] is not None and t['constituency_id'] != ward:
                    T.issue(cid, 'by-election', f"{ev['title']}: replaced member {t['person_name']} sat for a different ward",
                            date, election_id=eid, person_id=t['person_id'], person_name=t['person_name'])
                if ward is None:
                    ward = t['constituency_id']
                close(t, date, 'replaced')
            else:
                # Already gone (usually died before the by-election) or never recorded.
                person = pid and db.execute("SELECT died_date FROM people WHERE id = ?", (pid,)).fetchone()
                if not (person and person[0] and person[0] <= date):
                    T.issue(cid, 'by-election', f"{ev['title']}: replaced member '{name}' is not sitting at this date",
                            date, election_id=eid, person_name=name)

        if not named and not rp_name:
            sitting = [t for t in open_terms if ward is not None and t['constituency_id'] == ward]
            if ward is None:
                T.issue(cid, 'by-election', f"{ev['title']}: no ward and no replaced member recorded, so the seat can't be placed",
                        date, election_id=eid)
            elif ward not in seats:
                name = db.execute("SELECT name FROM constituencies WHERE id = ?", (ward,)).fetchone()[0]
                T.issue(cid, 'by-election', f"{ev['title']}: ward '{name}' wasn't one of the wards at the last general, "
                        f"and no replaced member is recorded", date, election_id=eid)
            elif len(sitting) >= seats.get(ward, 1):
                if seats.get(ward, 1) == 1 and len(sitting) == 1:
                    close(sitting[0], date, 'replaced')
                else:
                    T.issue(cid, 'by-election', f"{ev['title']}: multi-member ward and no replaced member recorded",
                            date, election_id=eid)

        for cand in winners(eid):
            start(cand, ward, date, 'by-election')

    end = ABOLISHED.get(slug)
    for t in list(open_terms):
        d = deaths.get(t['person_id'])
        if d and FULL_DATE.match(d) and t['start_date'] <= d and (end is None or d < end):
            close(t, d, 'died')
    if end:
        for t in list(open_terms):
            close(t, end, 'council_abolished')


def insert_terms(db, T):
    T.rows.sort(key=lambda t: (t['council_id'], t['start_date'], t['person_name']))
    db.executemany("""
        INSERT INTO council_terms (council_id, person_id, person_name, candidacy_id, constituency_id,
            start_date, end_date, start_reason, end_reason, confirmed, source)
        VALUES (:council_id, :person_id, :person_name, :candidacy_id, :constituency_id,
            :start_date, :end_date, :start_reason, :end_reason, :confirmed, :source)
    """, T.rows)


# ---------------------------------------------------------------------------
# Step 5: checks
# ---------------------------------------------------------------------------

def check_terms(db, T):
    ids = council_ids(db)
    slug_of = {v: k for k, v in ids.items()}
    deaths = {pid: d for pid, d in db.execute("SELECT id, died_date FROM people WHERE died_date IS NOT NULL")}
    by_council = defaultdict(list)
    for t in T.rows:
        by_council[t['council_id']].append(t)

    for cid, terms in by_council.items():
        # Term shape, deaths, overlaps
        per_person = defaultdict(list)
        for t in terms:
            if t['end_date'] and t['end_date'] < t['start_date']:
                T.issue(cid, 'bad-dates', f"ends {t['end_date']} before it starts {t['start_date']}",
                        t['start_date'], t['end_date'], t['person_id'], t['person_name'])
            d = deaths.get(t['person_id'])
            if d:
                # Compare at the precision of the death date (year-only deaths compare by year).
                end = t['end_date'] or '9999-12-31'
                if end[:len(d)] > d and not (t['end_reason'] == 'died' and t['end_date'] == d):
                    T.issue(cid, 'after-death', f"term runs to {t['end_date'] or 'present'} but died {d}",
                            t['start_date'], t['end_date'], t['person_id'], t['person_name'])
            per_person[t['person_id'] or t['person_name']].append(t)
        for key, ts in per_person.items():
            ts.sort(key=lambda t: t['start_date'])
            for a, b in zip(ts, ts[1:]):
                if a['end_date'] is None or a['end_date'] > b['start_date']:
                    T.issue(cid, 'overlap', f"already sitting (since {a['start_date']}, {a['start_reason']}) "
                            f"when a new term starts {b['start_date']} ({b['start_reason']})",
                            b['start_date'], a['end_date'], b['person_id'], b['person_name'])

        # Elected candidacies with no term
        seated = {t['candidacy_id'] for t in terms if t['candidacy_id']}
        exempt = defaultdict(set)
        for r in read_csv('not_seated.csv'):
            exempt[int(r['election_id'])].add(r['person_slug'])
        for cand_id, eid, title, date, pname, slug in db.execute("""
            SELECT c.id, e.id, e.wiki_page_title, e.election_date, COALESCE(p.name, c.candidate_name), p.slug
            FROM candidacies c JOIN elections e ON e.id = c.election_id LEFT JOIN people p ON p.id = c.person_id
            WHERE e.council_id = ? AND e.hidden = 0 AND c.elected = 1
        """, (cid,)):
            if cand_id in seated or '' in exempt[eid] or slug in exempt[eid]:
                continue
            if any(t['person_name'] == pname and t['start_date'] <= date and (t['end_date'] or '9999') > date
                   for t in terms):
                continue  # re-elected while sitting (holdover rows, office elections)
            T.issue(cid, 'no-term', f"elected at {title} but has no term", date,
                    person_name=display_name(pname), election_id=eid)

        if slug_of[cid] == 'lerwick-town-council':
            check_ltc_size(db, T, cid, terms)
        else:
            check_ward_occupancy(db, T, cid, terms)


def membership_segments(terms):
    """[(from, to, [terms sitting])] between every start/end boundary."""
    points = sorted({t['start_date'] for t in terms} | {t['end_date'] for t in terms if t['end_date']})
    segs = []
    for a, b in zip(points, points[1:] + [None]):
        sitting = [t for t in terms if t['start_date'] <= a and (t['end_date'] is None or t['end_date'] > a)]
        if sitting:
            segs.append((a, b, sitting))
    return segs


def check_ltc_size(db, T, cid, terms):
    overrides = [r for r in read_csv('council_size.csv') if r['council'] == 'lerwick-town-council']
    generals = db.execute("""
        SELECT e.election_date, COUNT(*) FROM elections e JOIN candidacies c ON c.election_id = e.id
        WHERE e.council_id = ? AND e.hidden = 0 AND e.election_type = 'general' AND c.elected = 1
          AND e.id NOT IN (SELECT CAST(election_id AS INTEGER) FROM temp.not_seated WHERE person_slug = '')
        GROUP BY e.wiki_page_title ORDER BY e.election_date
    """, (cid,)).fetchall()

    def expected(date):
        for o in overrides:
            if o['from'] <= date < o['to']:
                return int(o['size'])
        if date >= LTC_REFORM_DATE:
            return LTC_SIZE
        last = [n for d, n in generals if d <= date]
        return last[-1] if last else None

    runs = []
    for a, b, sitting in membership_segments(terms):
        exp = expected(a)
        if exp is None or len(sitting) == exp or all(t['confirmed'] for t in sitting):
            continue
        if runs and runs[-1]['to'] == a and runs[-1]['n'] == len(sitting) and runs[-1]['exp'] == exp:
            runs[-1]['to'] = b
        else:
            runs.append({'from': a, 'to': b, 'n': len(sitting), 'exp': exp})
    for r in runs:
        if r['n'] > r['exp']:
            T.issue(cid, 'size-over', f"{r['n']} members sitting, council size {r['exp']}", r['from'], r['to'])
        else:
            T.issue(cid, 'size-short', f"{r['n']} members sitting, council size {r['exp']} "
                    f"(a genuine vacancy, or a missing co-option)", r['from'], r['to'])


def check_ward_occupancy(db, T, cid, terms):
    wards = {k: n for k, n in db.execute("SELECT id, name FROM constituencies")}
    # Seats per ward = winners there at the most recent general, plus any seat left empty then.
    seat_rows = db.execute("""
        SELECT e.constituency_id, e.election_date, COUNT(c.id)
        FROM elections e LEFT JOIN candidacies c ON c.election_id = e.id AND c.elected = 1
        WHERE e.council_id = ? AND e.election_type = 'general' AND e.hidden = 0
        GROUP BY e.id ORDER BY e.election_date
    """, (cid,)).fetchall()

    # A seat left empty at a general for want of nominations is filled later at a by-election
    # recorded as replacing '[unfilled seat]'.
    unfilled = db.execute("""
        SELECT constituency_id, election_date FROM elections
        WHERE council_id = ? AND election_type = 'by-election' AND hidden = 0
          AND replaced_person = '[unfilled seat]'
    """, (cid,)).fetchall()

    def seats(ward, date):
        won, since = 0, None
        for w, d, k in seat_rows:
            if w == ward and d <= date:
                won, since = k, d
        extra = sum(1 for w, d in unfilled if w == ward and since is not None and since < d <= date)
        return max(1, won + extra)

    runs = {}  # ward -> open run
    for a, b, sitting in membership_segments(terms):
        per_ward = defaultdict(list)
        for t in sitting:
            if t['constituency_id'] is not None:
                per_ward[t['constituency_id']].append(t)
        over = {w: ts for w, ts in per_ward.items() if len(ts) > seats(w, a)}
        for w in [w for w in runs if w not in over]:
            r = runs.pop(w)
            T.issue(cid, 'ward-overfull', r['detail'], r['from'], r['to'])
        for w, ts in over.items():
            names = ', '.join(sorted(t['person_name'] for t in ts))
            detail = f"{wards.get(w)}: {len(ts)} sitting for {seats(w, a)} seat(s): {names}"
            r = runs.get(w)
            if r and r['detail'] == detail:
                r['to'] = b
            else:
                if r:
                    T.issue(cid, 'ward-overfull', r['detail'], r['from'], r['to'])
                runs[w] = {'from': a, 'to': b, 'detail': detail}
    for r in runs.values():
        T.issue(cid, 'ward-overfull', r['detail'], r['from'], r['to'])


# ---------------------------------------------------------------------------

def build(db_path):
    if os.path.exists(db_path):
        os.remove(db_path)
    load_baseline(db_path)
    run_corrections(db_path)

    db = sqlite3.connect(db_path)
    aliased = apply_party_aliases(db)
    db.executescript(TERMS_DDL)
    db.execute("CREATE TEMP TABLE not_seated (election_id, person_slug)")
    db.executemany("INSERT INTO temp.not_seated VALUES (?, ?)",
                   [(r['election_id'], r['person_slug']) for r in read_csv('not_seated.csv')])

    T = Terms(db)
    load_ltc_ledger(db, T)
    derive_ward_terms(db, T, 'zetland-county-council')
    derive_ward_terms(db, T, 'shetland-islands-council')
    insert_terms(db, T)
    check_terms(db, T)
    T.issues.sort(key=lambda i: (i[0], i[2] or '', i[1], i[5] or '', i[7]))
    db.executemany("""INSERT INTO term_issues (council_id, kind, date_from, date_to, person_id, person_name,
                      election_id, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", T.issues)
    db.commit()
    db.execute("VACUUM")
    db.close()
    return aliased, T


def dump(path):
    db = sqlite3.connect(path)
    lines = list(db.iterdump())
    db.close()
    return lines


def main():
    check = '--check' in sys.argv
    target = DB_PATH
    if check:
        tmpdir = tempfile.mkdtemp()
        target = os.path.join(tmpdir, 'shetland.db')

    aliased, T = build(target)
    counts = defaultdict(int)
    for t in T.rows:
        counts[t['council_id']] += 1
    kinds = defaultdict(int)
    for i in T.issues:
        kinds[i[1]] += 1
    print(f"party labels normalised: {aliased}")
    print(f"council_terms: {dict(counts)} (by council_id)")
    print(f"term_issues: {len(T.issues)} {dict(kinds)}")

    if check:
        same = dump(target) == dump(DB_PATH)
        shutil.rmtree(tmpdir)
        if not same:
            sys.exit("shetland.db is out of date with data/ and the correction scripts. "
                     "Run `python3 build.py` and commit the result. (Edits made directly to "
                     "shetland.db are lost on rebuild: put them in a correction script.)")
        print("shetland.db matches its sources")


if __name__ == '__main__':
    main()
