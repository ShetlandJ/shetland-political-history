#!/usr/bin/env python3
"""Cross-reference all people with Bayanne IDs against live Bayanne data."""

import sqlite3, urllib.request, re, ssl, time, json
from datetime import datetime

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

DB_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'

def parse_bayanne_title(title):
    """Parse 'Firstname LASTNAME b. 9 Dec 1872 Place d. 24 Dec 1940 Place' into structured data."""
    result = {'name': '', 'born_date': None, 'born_place': None, 'died_date': None, 'died_place': None}

    # Extract name (everything before "b.")
    name_match = re.match(r'^(.+?)\s+b\.', title)
    if name_match:
        result['name'] = name_match.group(1).strip()
    else:
        result['name'] = title.strip()
        return result

    # Extract birth: "b. [day] [month] year place"
    birth_match = re.search(r'b\.\s+(.+?)(?:\s+d\.|$)', title)
    if birth_match:
        birth_str = birth_match.group(1).strip()
        # Try full date: "9 Dec 1872 Place"
        dm = re.match(r'(\d{1,2}\s+\w+\s+\d{4})\s*(.*)', birth_str)
        if dm:
            try:
                dt = datetime.strptime(dm.group(1), "%d %b %Y")
                result['born_date'] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
            result['born_place'] = dm.group(2).strip().rstrip(',') if dm.group(2) else None
        else:
            # Year only: "1872 Place" or just "1872"
            ym = re.match(r'(\d{4})\s*(.*)', birth_str)
            if ym:
                result['born_date'] = ym.group(1)
                result['born_place'] = ym.group(2).strip().rstrip(',') if ym.group(2) else None
            else:
                # "Abt 1872" or "Bef 1872"
                approx = re.match(r'(?:Abt|Bef|Aft)\s+(\d{4})', birth_str)
                if approx:
                    result['born_date'] = approx.group(1)

    # Extract death: "d. [day] [month] year place"
    death_match = re.search(r'd\.\s+(.+?)$', title)
    if death_match:
        death_str = death_match.group(1).strip()
        dm = re.match(r'(\d{1,2}\s+\w+\s+\d{4})\s*(.*)', death_str)
        if dm:
            try:
                dt = datetime.strptime(dm.group(1), "%d %b %Y")
                result['died_date'] = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
            result['died_place'] = dm.group(2).strip().rstrip(',') if dm.group(2) else None
        else:
            ym = re.match(r'(\d{4})\s*(.*)', death_str)
            if ym:
                result['died_date'] = ym.group(1)
                result['died_place'] = ym.group(2).strip().rstrip(',') if ym.group(2) else None
            else:
                approx = re.match(r'(?:Abt|Bef|Aft)\s+(\d{4})', death_str)
                if approx:
                    result['died_date'] = approx.group(1)

    return result


def dates_match(our_date, bayanne_date):
    """Check if two dates match (handling year-only vs full date)."""
    if not our_date and not bayanne_date:
        return True
    if not our_date or not bayanne_date:
        return False
    # Compare year at minimum
    our_year = our_date[:4]
    bay_year = bayanne_date[:4]
    if our_year != bay_year:
        return False
    # If both are full dates, compare exactly
    if len(our_date) >= 10 and len(bayanne_date) >= 10:
        return our_date == bayanne_date
    return True  # Year matches, one is year-only


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    people = db.execute("""
        SELECT id, slug, name, born_date, died_date, birth_place, death_place, bayanne_id
        FROM people WHERE bayanne_id IS NOT NULL ORDER BY name
    """).fetchall()

    print(f"Cross-referencing {len(people)} people against Bayanne...")

    mismatches = []
    errors = []
    missing_in_ours = []  # Bayanne has data we don't

    for i, p in enumerate(people):
        if i % 50 == 0 and i > 0:
            print(f"  ...{i}/{len(people)}")

        url = f"https://www.bayanne.info/Shetland/getperson.php?personID={p['bayanne_id']}&tree=ID1"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            resp = urllib.request.urlopen(req, timeout=10, context=ctx)
            html = resp.read().decode('utf-8', errors='replace')
            title_match = re.search(r'<title>([^<]+)</title>', html)
            if not title_match:
                errors.append((p['name'], p['bayanne_id'], 'No title found'))
                continue

            title = title_match.group(1).split(':')[0].strip()
            bayanne = parse_bayanne_title(title)

            issues = []

            # Check birth date
            if not dates_match(p['born_date'], bayanne['born_date']):
                issues.append(f"born: ours={p['born_date']} bayanne={bayanne['born_date']}")

            # Check death date
            if not dates_match(p['died_date'], bayanne['died_date']):
                issues.append(f"died: ours={p['died_date']} bayanne={bayanne['died_date']}")

            # Check if Bayanne has data we're missing
            if not p['born_date'] and bayanne['born_date']:
                missing_in_ours.append((p['name'], p['slug'], f"born_date: bayanne has {bayanne['born_date']}"))
            if not p['died_date'] and bayanne['died_date']:
                missing_in_ours.append((p['name'], p['slug'], f"died_date: bayanne has {bayanne['died_date']}"))
            if not p['birth_place'] and bayanne['born_place']:
                missing_in_ours.append((p['name'], p['slug'], f"birth_place: bayanne has {bayanne['born_place']}"))
            if not p['death_place'] and bayanne['died_place']:
                missing_in_ours.append((p['name'], p['slug'], f"death_place: bayanne has {bayanne['died_place']}"))

            # Check birth_place is actually a place (not "d. after 1950" type errors)
            if p['birth_place'] and p['birth_place'].startswith('d.'):
                issues.append(f"birth_place contains death info: '{p['birth_place']}'")

            if issues:
                mismatches.append({
                    'name': p['name'],
                    'slug': p['slug'],
                    'id': p['id'],
                    'bayanne_id': p['bayanne_id'],
                    'issues': issues,
                    'ours': {'born': p['born_date'], 'died': p['died_date'], 'bp': p['birth_place'], 'dp': p['death_place']},
                    'bayanne': bayanne,
                })

        except Exception as e:
            errors.append((p['name'], p['bayanne_id'], str(e)))

        time.sleep(0.05)  # Be polite

    # Output results
    print(f"\n{'='*80}")
    print(f"RESULTS: {len(people)} checked, {len(mismatches)} mismatches, {len(errors)} errors, {len(missing_in_ours)} missing data points")

    if mismatches:
        print(f"\n{'='*80}")
        print("DATE MISMATCHES:")
        for m in mismatches:
            print(f"\n  {m['name']} ({m['bayanne_id']})")
            for issue in m['issues']:
                print(f"    {issue}")
            print(f"    bayanne title: {m['bayanne']['name']} b.{m['bayanne']['born_date']} d.{m['bayanne']['died_date']}")

    if missing_in_ours:
        print(f"\n{'='*80}")
        print(f"BAYANNE HAS DATA WE DON'T ({len(missing_in_ours)} items):")
        for name, slug, info in missing_in_ours[:30]:
            print(f"  {name}: {info}")
        if len(missing_in_ours) > 30:
            print(f"  ... and {len(missing_in_ours) - 30} more")

    if errors:
        print(f"\n{'='*80}")
        print(f"ERRORS ({len(errors)}):")
        for name, bid, err in errors[:10]:
            print(f"  {name} ({bid}): {err}")

    # Save full results to JSON
    output = {
        'mismatches': mismatches,
        'missing': [{'name': n, 'slug': s, 'info': i} for n, s, i in missing_in_ours],
        'errors': [{'name': n, 'bayanne_id': b, 'error': e} for n, b, e in errors],
    }
    with open('/private/tmp/claude-501/bayanne_crossref.json', 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nFull results saved to /private/tmp/claude-501/bayanne_crossref.json")


if __name__ == '__main__':
    main()
