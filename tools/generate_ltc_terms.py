"""
Draft Lerwick Town Council service terms from election data.

data/ltc_terms.csv is the source of truth for LTC membership, and build.py loads it.
This script only writes a *draft* of that file: it walks the LTC elections with the
cohort model and the corrections below, and prints CSV. Use it to draft years that
haven't been researched, then edit the ledger by hand and mark rows confirmed.

    python3 tools/generate_ltc_terms.py > draft.csv
"""

import csv
import os
import re
import sqlite3
import sys

DB_PATH = os.environ.get('SHETLAND_DB', 'shetland.db')

# Confirmed mid-term departures not recorded as by-elections
MANUAL_DEPARTURES = [
    ("Thomas Cameron", "1883-09-01", "resigned"),
    ("William Duncan (i)", "1886-07-12", "resigned"),
    ("John Harrison (i)", "1886-10-01", "disqualified"),
    # James Hunter (ii) left early 1887, replaced by Porteous Mar 1887 — exact date unknown
    ("James Hunter (ii)", "1887-03-01", "unknown"),
    ("Alexander Mitchell (i)", "1889-09-01", "retired"),  # Newspaper 26 Oct 1889
    ("William MacDougall", "1912-04-01", "resigned"),
    ("William Sinclair", "1921-10-01", "retired"),  # Newspaper Nov 1921
]

# People who were nominated/elected but declined to take office
DECLINED_OFFICE = {
    # (election_wiki_page_title, person_name) -> True
    ("Lerwick Town Council Election November 1879", "Arthur Laurenson"),
    ("Lerwick Town Council Election November 1880", "James Goudie"),
    ("Lerwick Town Council Election November 1884", "Arthur Hay"),
}

# Elections where all elected members got full terms (no short-term redistribution).
# Confirmed: next general had only 4 vacancies = no re-standing.
SKIP_REDISTRIBUTE = {
    'Lerwick Town Council Election November 1883',  # 1884 general had 4 vacancies
    'Lerwick Town Council Election November 1912',  # 1913 general had 4 vacancies
    'Lerwick Town Council Election November 1932',  # 1933 general had 4 vacancies
}

# Cohort corrections: redistribution put the wrong person in the short-term cohort.
# Format: wiki_page_title -> (wrong_person, right_person) to swap between cohorts.
COHORT_CORRECTIONS = {
    # 1886: Jamieson was short-term fill, not Stove (confirmed 1888 newspaper)
    'Lerwick Town Council Election November 1886': ('Laurence Stove', 'Andrew Jamieson'),
    # 1887: Anderson was short-term fill, not Charles Robertson (confirmed 1888 newspaper)
    'Lerwick Town Council Election November 1887': ('Charles Robertson', 'John Anderson'),
}


# Elections that did not seat anyone. Sept 1874: the council split and voted in two
# rooms; the minute book (p258, 9 Sept 1874) records the first group's election of
# Bailies as invalid, and the second group took office.
INVALID_ELECTION_IDS = {21}


def remove_person(lst, name):
    """Remove a person by name, handling disambiguation suffixes."""
    clean = re.sub(r'\s*\([^)]*\)\s*$', '', name)
    return [m for m in lst if m['name'] != name and re.sub(r'\s*\([^)]*\)\s*$', '', m['name']) != clean]


def generate_terms():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # Get LTC council id
    ltc = cur.execute("SELECT id FROM councils WHERE slug = 'lerwick-town-council'").fetchone()
    ltc_id = ltc['id']

    # Get all LTC elections
    elections = cur.execute("""
        SELECT e.id, e.wiki_page_title, e.election_date, e.election_type,
               e.replaced_person, e.replaced_person_id,
               rp.name as replaced_person_name, e.notes
        FROM elections e
        LEFT JOIN people rp ON e.replaced_person_id = rp.id
        WHERE e.council_id = ? AND e.hidden = 0
        GROUP BY e.wiki_page_title ORDER BY e.election_date
    """, (ltc_id,)).fetchall()

    # Death dates
    death_dates = {}
    for r in cur.execute("SELECT id, died_date FROM people WHERE died_date IS NOT NULL"):
        death_dates[r['id']] = r['died_date']

    # Person lookups
    person_ids = {}
    person_slugs = {}
    for r in cur.execute("SELECT id, name, slug FROM people"):
        person_ids[r['name']] = r['id']
        person_slugs[r['id']] = r['slug']

    def get_elected(wiki_page_title):
        rows = cur.execute("""
            SELECT c.candidate_name, c.person_id, p.name as person_name, c.elected,
                   p.slug, e.id as election_id, c.role
            FROM candidacies c JOIN elections e ON c.election_id = e.id
            LEFT JOIN people p ON c.person_id = p.id
            WHERE e.wiki_page_title = ? AND c.elected = 1 AND e.hidden = 0
            ORDER BY e.id, c.position
        """, (wiki_page_title,)).fetchall()
        return [r for r in rows if r['election_id'] not in INVALID_ELECTION_IDS]

    # Manual departure lookup
    departure_map = {}
    for name, date, reason in MANUAL_DEPARTURES:
        departure_map[name] = (date, reason)

    # Track all terms: list of dicts
    all_terms = []

    cohorts = []  # { members: [{ name, person_id, start_date, start_reason }], exp: int }
    by_members = []  # [{ name, person_id, start_date, start_reason }]
    general_count = 0

    def end_term(member, end_date, end_reason):
        """Record a completed term."""
        all_terms.append({
            'person_slug': person_slugs.get(member.get('person_id'), ''),
            'person_name': member['name'],
            'election_id': member.get('election_id', ''),
            'election': member.get('election', ''),
            'start_date': member.get('start_date', ''),
            'end_date': end_date,
            'start_reason': member.get('start_reason', 'elected'),
            'end_reason': end_reason,
        })

    def find_and_remove(name, end_date, end_reason):
        """Remove a person from cohorts/by_members and record their term end."""
        clean = re.sub(r'\s*\([^)]*\)\s*$', '', name)
        for c in cohorts:
            for m in list(c['members']):
                m_clean = re.sub(r'\s*\([^)]*\)\s*$', '', m['name'])
                if m['name'] == name or m_clean == clean:
                    end_term(m, end_date, end_reason)
                    c['members'].remove(m)
                    return True
        for m in list(by_members):
            m_clean = re.sub(r'\s*\([^)]*\)\s*$', '', m['name'])
            if m['name'] == name or m_clean == clean:
                end_term(m, end_date, end_reason)
                by_members.remove(m)
                return True
        return False

    def mk_member(row, edate):
        return {
            'name': row['person_name'] or row['candidate_name'],
            'person_id': row['person_id'],
            'start_date': edate,
            'start_reason': 'elected',
            'election_id': row['election_id'],
            'election': e['wiki_page_title'],
        }

    def is_sitting(name):
        return any(m['name'] == name for c in cohorts for m in c['members']) or \
            any(m['name'] == name for m in by_members)

    def mk_holdover(name, edate, reason='holdover'):
        pid = person_ids.get(name)
        return {
            'name': name,
            'person_id': pid,
            'start_date': edate,
            'start_reason': reason,
        }

    for e in elections:
        edate = e['election_date'] or ''
        elected = get_elected(e['wiki_page_title'])

        # Remove dead people before this election
        for c in list(cohorts):
            for m in list(c['members']):
                dd = death_dates.get(m['person_id'])
                if dd and dd < edate:
                    end_term(m, dd, 'died')
                    c['members'].remove(m)
        for m in list(by_members):
            dd = death_dates.get(m['person_id'])
            if dd and dd < edate:
                end_term(m, dd, 'died')
                by_members.remove(m)

        # Remove manual departures before this election
        for c in list(cohorts):
            for m in list(c['members']):
                dep = departure_map.get(m['name'])
                if dep and m['start_date'] <= dep[0] < edate:
                    end_term(m, dep[0], dep[1])
                    c['members'].remove(m)
        for m in list(by_members):
            dep = departure_map.get(m['name'])
            if dep and m['start_date'] <= dep[0] < edate:
                end_term(m, dep[0], dep[1])
                by_members.remove(m)

        if e['election_type'] == 'by-election':
            # Remove replaced person
            replaced = e['replaced_person_name'] or e['replaced_person']
            if replaced:
                find_and_remove(replaced, edate, 'replaced')

            notes = e['notes'] or ''
            also = re.search(r'Also replaced:\s*(.+)', notes)
            if also:
                find_and_remove(also.group(1).strip(), edate, 'replaced')

            # Add new by-election members. A sitting councillor elected to an office at a
            # by-election (Joseph Leask, Junior Bailie, May 1844) takes no new seat.
            for c in elected:
                name = c['person_name'] or c['candidate_name']
                if is_sitting(name) and c['role'] not in (None, '', 'councillor'):
                    continue
                by_members.append({
                    'name': name,
                    'person_id': c['person_id'],
                    'start_date': edate,
                    'start_reason': 'by-election',
                    'election_id': c['election_id'],
                    'election': e['wiki_page_title'],
                })
        else:
            general_count += 1

            if e['wiki_page_title'] == 'Lerwick Town Council Election November 1876':
                # Special: 1876 reform — 3 cohorts
                for c in cohorts:
                    for m in c['members']:
                        end_term(m, edate, 'term_expired')
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                cohorts = []
                by_members = []

                members = [mk_member(c, edate) for c in elected]
                cohorts.append({'members': members[0:4], 'exp': general_count + 3})
                cohorts.append({'members': members[4:8], 'exp': general_count + 2})
                cohorts.append({'members': members[8:12], 'exp': general_count + 1})

            elif e['wiki_page_title'] == 'Lerwick Town Council Election November 1919':
                # Post-WWI reset. Newspaper evidence: 9 Oct, 18 Oct, 30 Oct 1919.
                # 7 vacancies: 2 by rotation + 5 ad interim (wartime co-options).
                for c in cohorts:
                    for m in c['members']:
                        end_term(m, edate, 'term_expired')
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                cohorts = []
                by_members = []

                new_elected = [mk_member(c, edate) for c in elected]
                # 1-year cohort (retire 1920): holdovers from 1914
                cohorts.append({'members': [
                    mk_holdover('Robert Ganson (i)', edate),
                    mk_holdover('Alexander Ratter', edate),
                    mk_holdover('William Sinclair', edate),
                    mk_holdover('Peter Goodlad', edate),
                ], 'exp': general_count + 1})
                # 3-year cohort (retire 1922): top 4 elected by votes
                cohorts.append({'members': new_elected[0:4], 'exp': general_count + 3})
                # 2-year cohort (retire 1921): Smith + bottom 3 elected
                # Newspaper Nov 1921: Smith, Ramsay, Sinclair, Ollason went out of office
                cohorts.append({'members': [
                    mk_holdover('John Smith (i)', edate),
                    *new_elected[4:]
                ], 'exp': general_count + 2})

            elif len(elected) >= 10:
                # Full council replacement (pre-1876)
                for c in cohorts:
                    for m in c['members']:
                        end_term(m, edate, 'term_expired')
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                cohorts = []
                by_members = []

                members = [mk_member(c, edate) for c in elected]
                cohorts.append({'members': members, 'exp': general_count + 3})

            else:
                # Annual rotation
                # 1. Expire cohorts
                for c in list(cohorts):
                    if c['exp'] <= general_count:
                        for m in c['members']:
                            end_term(m, edate, 'term_expired')
                        cohorts.remove(c)

                # 2. End by-election member terms (must re-stand)
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                by_members = []

                # 3. Add new cohort
                new_members = [mk_member(c, edate) for c in elected]
                cohorts.append({'members': new_members, 'exp': general_count + 3})

                # 4. Redistribute overflow to fill gaps (skip for confirmed full-term elections)
                if e['wiki_page_title'] not in SKIP_REDISTRIBUTE:
                    moved = True
                    while moved:
                        moved = False
                        ov = next((c for c in cohorts if len(c['members']) > 4), None)
                        if ov:
                            gap = next((c for c in cohorts if len(c['members']) < 4), None)
                            if gap:
                                gap['members'].append(ov['members'].pop())
                                moved = True

                # 5. Cohort corrections (swap wrong/right short-term fill)
                correction = COHORT_CORRECTIONS.get(e['wiki_page_title'])
                if correction:
                    wrong, right = correction
                    wc = next((c for c in cohorts if any(m['name'] == wrong for m in c['members'])), None)
                    rc = next((c for c in cohorts if any(m['name'] == right for m in c['members'])), None)
                    if wc and rc and wc is not rc:
                        wm = next(m for m in wc['members'] if m['name'] == wrong)
                        rm = next(m for m in rc['members'] if m['name'] == right)
                        wc['members'].remove(wm)
                        rc['members'].remove(rm)
                        wc['members'].append(rm)
                        rc['members'].append(wm)

    # End any remaining terms at council dissolution (1975)
    for c in cohorts:
        for m in c['members']:
            end_term(m, '1975-05-15', 'council_abolished')
    for m in by_members:
        end_term(m, '1975-05-15', 'council_abolished')

    db.close()

    all_terms.sort(key=lambda t: (t['start_date'], t['person_name']))
    w = csv.writer(sys.stdout, lineterminator='\n')
    w.writerow(LEDGER_COLUMNS)
    for t in all_terms:
        w.writerow([t['person_slug'], t['person_name'], t['start_date'], t['end_date'],
                    t['start_reason'], t['end_reason'], t['election_id'], t['election'], 0, ''])


LEDGER_COLUMNS = ['person_slug', 'person_name', 'start_date', 'end_date', 'start_reason',
                  'end_reason', 'election_id', 'election', 'confirmed', 'source']


if __name__ == '__main__':
    generate_terms()
