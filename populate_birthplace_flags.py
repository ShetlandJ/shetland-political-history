#!/usr/bin/env python3
"""Populate born_in_shetland and died_in_shetland flags in the people table.

Step 1: Mark known Shetland/non-Shetland places from birth_place/death_place.
Step 2: For remaining unknowns, fetch Bayanne pages and check for "SHI, SCT".
"""

import sqlite3
import re
import ssl
import time
import urllib.request

DB_PATH = 'shetland.db'

# Known NON-Shetland places (easier to enumerate since most places are Shetland)
NOT_SHETLAND = {
    'aberdeen', 'aboyne', 'adelaide', 'arbirlot', 'auldearn', 'ayrshire',
    'barbados', 'bellshill', 'bolton', 'bombay', 'bothwell', 'bourne',
    'bournemouth', 'braintree', 'burma', 'caithness', 'colmonell',
    'darlington', 'douglas', 'doune', 'dumfries', 'dundee', 'dunfermline',
    'east linton', 'east lothian', 'edinburgh', 'elderslie', 'ellon',
    'england', 'forfar', 'fraserburgh', 'glasgow', 'golspie', 'greenock',
    'halifax', 'hamilton', 'hawick', 'huddersfield', 'huyton', 'inverarity',
    'ireland', 'islay', 'jamaica', 'keiss', 'kenmore', 'kilmacolm',
    'kilwinning', 'kirkcaldy', 'kirkintilloch', 'kirkwall', 'lasswade',
    'leith', 'lochee', 'lockerbie', 'london', 'madeira', 'manchester',
    'melbourne', 'montrose', 'mortlake', 'muthil', 'new york, usa',
    'new zealand', 'old machar', 'orkney', 'oxford', 'paisley', 'peebles',
    'perth', 'peterhead', 'ryde', 'scoonie', 'shandon', 'shapinsay',
    'south ronaldsay', 'south shields', 'st machars', 'st ola', 'stromness',
    'stratford-on-avon', 'sydenham', 'tain', 'torquay', 'wallacetown',
    'walney island', 'wick',
    'ophir',  # Orkney
}

# Patterns that indicate non-Shetland (checked via substring match)
NOT_SHETLAND_PATTERNS = [
    'roxburghshire', 'dumfriesshire', 'hampshire', 'england', 'isle of man',
    'ayrshire', 'east lothian', 'west lothian', 'lancashire', 'fife',
    'nova scotia', 'canada', 'new zealand', 'australia', 'ontario',
    'manitoba', 'tynemouth', 'sussex', 'ross-shire', 'kensington',
    'isle of skye', 'clackmannanshire', 'blairgowrie', 'galloway',
    'edinburgh', 'peterhead', 'new orleans', 'aberdeen', 'glasgow', 'london',
]

# Known ambiguous places that ARE in Shetland (street addresses etc.)
SHETLAND_PATTERNS = [
    'commercial street', 'john street', 'chromate lane', 'magnus street',
    'christines gade', 'south albany street', 'olaf street', 'mounthooly',
    'middlebie house near lerwick', 'hayfield cottage', 'isleburgh',
    'gilbert bain', 'scalloway hotel', 'earl of zetland',
    'burgh road', 'king harald', 'hrossey',
]


def is_shetland_place(place):
    """Return True/False/None for known Shetland/not-Shetland/unknown."""
    if not place:
        return None

    clean = place.strip().rstrip('-').strip()
    lower = clean.lower()

    # Skip garbage data
    if lower in ('-', '') or lower.startswith('d.'):
        return None

    # Check non-Shetland patterns first
    for pat in NOT_SHETLAND_PATTERNS:
        if pat in lower:
            return False

    # Check known non-Shetland
    if lower in NOT_SHETLAND:
        return False

    # Check Shetland street patterns
    for pat in SHETLAND_PATTERNS:
        if pat in lower:
            return True

    # Most remaining places are Shetland parish/settlement names
    # If it's a simple name (no comma indicating "City, County" pattern)
    # and not in the non-Shetland set, it's almost certainly Shetland
    # But we'll let Bayanne confirm ambiguous ones

    # These are all Shetland parishes/settlements
    shetland_places = {
        'aith', 'aithsetter', 'aithsting', 'baltasound', 'baridster',
        'benston', 'bigton', 'billister', 'bixter', 'boddam', 'borough',
        'brae', 'bressay', 'brough', 'bruntskerry', 'burra', 'burradale',
        'burravoe', 'camb', 'clate', 'clousta', 'collafirth', 'colvadale',
        'cullivoe', 'culsetter', 'cunningsburgh', 'deepdale', 'delting',
        'duncansclate', 'dunrossness', 'eshaness', 'fetlar', 'firth',
        'fladdabister', 'foula', 'garth', 'geosetter', 'gletness',
        'graven', 'gremista', 'gulberwick', 'haggersta', 'hamar',
        'hamnavoe', 'haroldswick', 'heylor', 'hildasay', 'hillhead',
        'hillock', 'hillswick', 'hogan', 'hoswick', 'houl', 'houlland',
        'innhouse', 'kergord', 'kirkabister', 'kirkhouse', 'laxfirth',
        'leons', 'lerwick', 'levenwick', 'lunnasting', 'lunnister',
        'mailand', 'meal', 'melby', 'mid yell', 'midgarth', 'mossbank',
        'muckle roe', 'murron', 'nesting', 'norby', 'north dale',
        'north roe', 'north sandwick', 'north setter', 'north yell',
        'northmavine', 'norwick', 'noss sound', 'out skerries', 'papil',
        'punstow', 'quarff', 'quendale', 'quoys', 'raefirth', 'reafirth',
        'reawick', 'rumpa', 'sand', 'sandfield', 'sandness', 'sandsting',
        'sandwick', 'scalloway', 'scholland', 'scorradale', 'scousburgh',
        'seafield', 'setter', 'skeld', 'skellister', 'sound',
        'south califf', 'south voe', 'sullom', 'sumburgh', 'sursetter',
        'swinister', 'symbister', 'tingwall', 'toft', 'tresta', 'trondra',
        'turill', 'twatt', 'ulsta', 'unst', 'upper sound', 'uyea',
        'uyeasound', 'vaila', 'vidlin', 'voe', 'wadbister', 'walls',
        'wart', 'weathersta', 'weisdale', 'west sandwick', 'west yell',
        'whalsay', 'whiteness', 'wormadale', 'yell', 'old manse',
        'hillsgarth',
    }

    # Check with comma-stripped version (e.g. "Anness, Cunningsburgh" → check "cunningsburgh")
    parts = [p.strip().lower() for p in lower.split(',')]
    for part in parts:
        if part in shetland_places:
            return True

    if lower in shetland_places:
        return True

    # Also match "Böd of Gremista" etc.
    if 'gremista' in lower:
        return True

    return None  # Unknown — let Bayanne decide


def fetch_bayanne(bayanne_id):
    """Fetch birthPlace and deathPlace from Bayanne JSON-LD."""
    url = f'https://www.bayanne.info/Shetland/getperson.php?personID={bayanne_id}&tree=ID1'
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        print(f"    Error fetching {bayanne_id}: {e}")
        return None, None

    birth = None
    death = None
    bp = re.search(r'"birthPlace":"([^"]*)"', html)
    dp = re.search(r'"deathPlace":"([^"]*)"', html)
    if bp:
        birth = bp.group(1)
    if dp:
        death = dp.group(1)
    return birth, death


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    people = c.execute(
        'SELECT id, name, slug, birth_place, death_place, bayanne_id, born_in_shetland, died_in_shetland FROM people'
    ).fetchall()

    # Step 1: Mark from known places
    print("=== Step 1: Marking known places ===")
    born_set = 0
    died_set = 0
    born_unknown = []
    died_unknown = []

    for pid, name, slug, bp, dp, bayanne_id, existing_born, existing_died in people:
        born_flag = is_shetland_place(bp)
        died_flag = is_shetland_place(dp)

        if born_flag is not None:
            c.execute('UPDATE people SET born_in_shetland = ? WHERE id = ?', (1 if born_flag else 0, pid))
            born_set += 1
        elif bp:
            born_unknown.append((pid, name, slug, bp, bayanne_id))

        if died_flag is not None:
            c.execute('UPDATE people SET died_in_shetland = ? WHERE id = ?', (1 if died_flag else 0, pid))
            died_set += 1
        elif dp:
            died_unknown.append((pid, name, slug, dp, bayanne_id))

    conn.commit()
    print(f"  Born: {born_set} set from known places, {len(born_unknown)} unknown")
    print(f"  Died: {died_set} set from known places, {len(died_unknown)} unknown")

    if born_unknown:
        print("\n  Unknown birth places:")
        for pid, name, slug, bp, bid in born_unknown:
            print(f"    {name}: {bp} (bayanne: {bid})")

    if died_unknown:
        print("\n  Unknown death places:")
        for pid, name, slug, dp, bid in died_unknown:
            print(f"    {name}: {dp} (bayanne: {bid})")

    # Step 2: Fetch from Bayanne for people with NULL flags and a bayanne_id
    # This covers both people with unknown places AND people with no place at all
    remaining = c.execute('''
        SELECT id, name, slug, bayanne_id, born_in_shetland, died_in_shetland, birth_place, death_place
        FROM people
        WHERE bayanne_id IS NOT NULL
        AND (born_in_shetland IS NULL OR died_in_shetland IS NULL)
    ''').fetchall()

    print(f"\n=== Step 2: Fetching {len(remaining)} people from Bayanne ===")

    bayanne_born = 0
    bayanne_died = 0
    for i, (pid, name, slug, bayanne_id, existing_born, existing_died, bp, dp) in enumerate(remaining):
        if i > 0 and i % 10 == 0:
            print(f"  Progress: {i}/{len(remaining)}")

        bay_birth, bay_death = fetch_bayanne(bayanne_id)
        time.sleep(0.3)  # Be polite

        if existing_born is None and bay_birth:
            is_shi = 1 if 'SHI, SCT' in bay_birth else 0
            c.execute('UPDATE people SET born_in_shetland = ? WHERE id = ?', (is_shi, pid))
            bayanne_born += 1
            if not bp:  # Log only when we didn't have a birth_place
                print(f"    {name}: born {bay_birth} → {'SHI' if is_shi else 'not SHI'}")

        if existing_died is None and bay_death:
            is_shi = 1 if 'SHI, SCT' in bay_death else 0
            c.execute('UPDATE people SET died_in_shetland = ? WHERE id = ?', (is_shi, pid))
            bayanne_died += 1
            if not dp:
                print(f"    {name}: died {bay_death} → {'SHI' if is_shi else 'not SHI'}")

        # Commit every 20 to avoid losing progress
        if i % 20 == 0:
            conn.commit()

    conn.commit()
    print(f"\n  Bayanne: set born_in_shetland for {bayanne_born}, died_in_shetland for {bayanne_died}")

    # Summary
    stats = c.execute('''
        SELECT
            SUM(CASE WHEN born_in_shetland = 1 THEN 1 ELSE 0 END) as born_shi,
            SUM(CASE WHEN born_in_shetland = 0 THEN 1 ELSE 0 END) as born_not,
            SUM(CASE WHEN born_in_shetland IS NULL THEN 1 ELSE 0 END) as born_unk,
            SUM(CASE WHEN died_in_shetland = 1 THEN 1 ELSE 0 END) as died_shi,
            SUM(CASE WHEN died_in_shetland = 0 THEN 1 ELSE 0 END) as died_not,
            SUM(CASE WHEN died_in_shetland IS NULL THEN 1 ELSE 0 END) as died_unk
        FROM people
    ''').fetchone()

    print(f"\n=== Summary ===")
    print(f"  Born in Shetland: {stats[0]}, Born elsewhere: {stats[1]}, Unknown: {stats[2]}")
    print(f"  Died in Shetland: {stats[3]}, Died elsewhere: {stats[4]}, Unknown: {stats[5]}")

    conn.close()


if __name__ == '__main__':
    main()
