"""
Generate council_terms for Lerwick Town Council from election data.

This script traces through LTC elections and generates service terms for each
councillor. It uses the cohort model as a starting point but applies confirmed
corrections from newspaper research.

Run after parse_wiki.py and add_modern_sic.py:
    python3 tools/generate_ltc_terms.py
"""

import sqlite3
import re

DB_PATH = "shetland.db"

# Confirmed mid-term departures not recorded as by-elections
MANUAL_DEPARTURES = [
    ("Thomas Cameron", "1883-09-01", "resigned"),
    ("William Duncan (i)", "1886-07-12", "resigned"),
    ("John Harrison (i)", "1886-10-01", "disqualified"),
    # James Hunter (ii) left early 1887, replaced by Porteous Mar 1887 — exact date unknown
    ("James Hunter (ii)", "1887-03-01", "unknown"),
    ("William MacDougall", "1912-04-01", "resigned"),
]

# People who were nominated/elected but declined to take office
DECLINED_OFFICE = {
    # (election_wiki_page_title, person_name) -> True
    ("Lerwick Town Council Election November 1879", "Arthur Laurenson"),
    ("Lerwick Town Council Election November 1880", "James Goudie"),
    ("Lerwick Town Council Election November 1884", "Arthur Hay"),
}


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
    for r in cur.execute("SELECT name, died_date FROM people WHERE died_date IS NOT NULL"):
        death_dates[r['name']] = r['died_date']

    # Person ID lookup
    person_ids = {}
    for r in cur.execute("SELECT id, name FROM people"):
        person_ids[r['name']] = r['id']

    def get_elected(wiki_page_title):
        return cur.execute("""
            SELECT c.candidate_name, c.person_id, p.name as person_name, c.elected
            FROM candidacies c JOIN elections e ON c.election_id = e.id
            LEFT JOIN people p ON c.person_id = p.id
            WHERE e.wiki_page_title = ? AND c.elected = 1 AND e.hidden = 0
            ORDER BY c.position
        """, (wiki_page_title,)).fetchall()

    # Manual departure lookup
    departure_map = {}
    for name, date, reason in MANUAL_DEPARTURES:
        departure_map[name] = (date, reason)

    # Track all terms: list of dicts
    all_terms = []

    # Track currently serving members
    # Each: { name, person_id, start_date, start_reason, cohort_exp }
    serving = []

    cohorts = []  # { members: [{ name, person_id }], exp: int }
    by_members = []  # [{ name, person_id }]
    general_count = 0

    def end_term(member, end_date, end_reason):
        """Record a completed term."""
        all_terms.append({
            'person_id': member.get('person_id'),
            'person_name': member['name'],
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

    # Map general_count to election dates for term end tracking
    general_dates = {}
    gc = 0
    for e in elections:
        if e['election_type'] != 'by-election':
            gc += 1
            general_dates[gc] = e['election_date']

    gc = 0
    for e in elections:
        edate = e['election_date'] or ''
        elected = get_elected(e['wiki_page_title'])

        # Remove dead people before this election
        for c in list(cohorts):
            for m in list(c['members']):
                dd = death_dates.get(m['name'])
                if dd and dd < edate:
                    end_term(m, dd, 'died')
                    c['members'].remove(m)
        for m in list(by_members):
            dd = death_dates.get(m['name'])
            if dd and dd < edate:
                end_term(m, dd, 'died')
                by_members.remove(m)

        # Remove manual departures before this election
        for c in list(cohorts):
            for m in list(c['members']):
                dep = departure_map.get(m['name'])
                if dep and dep[0] < edate:
                    end_term(m, dep[0], dep[1])
                    c['members'].remove(m)
        for m in list(by_members):
            dep = departure_map.get(m['name'])
            if dep and dep[0] < edate:
                end_term(m, dep[0], dep[1])
                by_members.remove(m)

        if e['election_type'] == 'by-election':
            # Remove replaced person
            replaced = e['replaced_person_name'] or e['replaced_person']
            if replaced:
                # Don't double-end if already removed by manual departure
                find_and_remove(replaced, edate, 'replaced')

            notes = e['notes'] or ''
            also = re.search(r'Also replaced:\s*(.+)', notes)
            if also:
                find_and_remove(also.group(1).strip(), edate, 'replaced')

            # Add new by-election members
            for c in elected:
                name = c['person_name'] or c['candidate_name']
                by_members.append({
                    'name': name,
                    'person_id': c['person_id'],
                    'start_date': edate,
                    'start_reason': 'by-election',
                })
        else:
            general_count += 1

            if e['wiki_page_title'] == 'Lerwick Town Council Election November 1876':
                # Special: 1876 reform — 3 cohorts
                # End all existing terms
                for c in cohorts:
                    for m in c['members']:
                        end_term(m, edate, 'term_expired')
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                cohorts = []
                by_members = []

                members = [{'name': c['person_name'] or c['candidate_name'],
                           'person_id': c['person_id'],
                           'start_date': edate, 'start_reason': 'elected'}
                          for c in elected]
                cohorts.append({'members': members[0:4], 'exp': general_count + 3})
                cohorts.append({'members': members[4:8], 'exp': general_count + 2})
                cohorts.append({'members': members[8:12], 'exp': general_count + 1})

            elif len(elected) >= 10:
                # Full council replacement (pre-1876)
                for c in cohorts:
                    for m in c['members']:
                        end_term(m, edate, 'term_expired')
                for m in by_members:
                    end_term(m, edate, 'term_expired')
                cohorts = []
                by_members = []

                members = [{'name': c['person_name'] or c['candidate_name'],
                           'person_id': c['person_id'],
                           'start_date': edate, 'start_reason': 'elected'}
                          for c in elected]
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
                new_members = [{'name': c['person_name'] or c['candidate_name'],
                               'person_id': c['person_id'],
                               'start_date': edate, 'start_reason': 'elected'}
                              for c in elected]
                cohorts.append({'members': new_members, 'exp': general_count + 3})

                # 4. Redistribute overflow to fill gaps
                moved = True
                while moved:
                    moved = False
                    ov = next((c for c in cohorts if len(c['members']) > 4), None)
                    if ov:
                        gap = next((c for c in cohorts if len(c['members']) < 4), None)
                        if gap:
                            gap['members'].append(ov['members'].pop())
                            moved = True

    # End any remaining terms at council dissolution (1975)
    for c in cohorts:
        for m in c['members']:
            end_term(m, '1975-05-15', 'council_abolished')
    for m in by_members:
        end_term(m, '1975-05-15', 'council_abolished')

    # Write to database
    cur.execute("DELETE FROM council_terms WHERE council_id = ?", (ltc_id,))

    for t in all_terms:
        cur.execute("""
            INSERT INTO council_terms (person_id, person_name, council_id, start_date, end_date, start_reason, end_reason, confirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """, (t['person_id'], t['person_name'], ltc_id, t['start_date'], t['end_date'],
              t['start_reason'], t['end_reason']))

    db.commit()

    # Stats
    total = cur.execute("SELECT COUNT(*) as c FROM council_terms WHERE council_id = ?", (ltc_id,)).fetchone()['c']
    print(f"Generated {total} council terms for LTC")

    # Check a few snapshots
    for check_date in ['1879-12-01', '1883-12-01', '1886-12-01', '1890-12-01', '1920-12-01', '1950-06-01']:
        count = cur.execute("""
            SELECT COUNT(*) as c FROM council_terms
            WHERE council_id = ? AND start_date <= ? AND end_date > ?
        """, (ltc_id, check_date, check_date)).fetchone()['c']
        print(f"  {check_date}: {count} members")

    db.close()


if __name__ == '__main__':
    generate_terms()
