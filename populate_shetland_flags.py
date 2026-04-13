#!/usr/bin/env python3
"""
Populate born_in_shetland/died_in_shetland columns from existing birth_place/death_place data.
Uses a known set of Shetland place names to classify.
"""
import sqlite3
import sys

# Known Shetland place names (parishes, settlements, islands, landmarks)
SHETLAND_PLACES = {
    'aith', 'aithsetter', 'aithsting', 'baltasound', 'baridster', 'bayanne house',
    'benston', 'bigton', 'billister', 'bixter', 'boddam', 'borough', 'brae',
    'braehead', 'braeview', 'breiwick hospital', 'bressay', 'brevik hospital',
    'brough', 'bruntskerry', 'burra', 'burradale', 'burravoe',
    'böd of gremista', 'camb', 'clate', 'clousta', 'clumly', 'collafirth',
    'colvadale', 'crueton', 'cullivoe', 'culsetter', 'cunningsburgh',
    'deepdale', 'delting', 'duncansclate', 'dunrossness',
    'earl of zetland', 'effirth', 'eshaness',
    'fair isle', 'fetlar', 'firth', 'fladdabister', 'fleck', 'foula',
    'garth', 'geosetter', 'gilbert bain hospital', 'gletness', 'graven',
    'gremista', 'gulberwick',
    'haggersta', 'hamar', 'hamnavoe', 'haroldswick', 'hayfield cottage',
    'heylor', 'hildasay', 'hillhead', 'hillock', 'hillsgarth', 'hillswick',
    'hogan', 'hoswick', 'houl', 'houlland',
    'innhouse', 'isleburgh',
    'kelda', 'kergord', 'kirkabister', 'kirkhouse',
    'laxfirth', 'laxobigging', 'leog house', 'leons', 'lerwick', 'levenwick',
    'lunnasting', 'lunnister',
    'mailand', 'meal', 'medical hall', 'melby', 'melby house', 'mid yell',
    'midgarth', 'mossbank', 'muckle roe', 'murron',
    'nesting', 'noness', 'norby', 'north dale', 'north roe', 'north sandwick',
    'north setter', 'north yell', 'northmavine', 'norwick', 'noss sound',
    'ollaberry', 'ophir', 'out skerries', 'overtonlea',
    'papa stour', 'papil', 'punstow',
    'quarff', 'quendale', 'quendale house', 'quoys',
    'raefirth', 'raewick', 'reawick', 'reafirth', 'royal hairdressing saloon',
    'rumpa',
    'sand', 'sandfield', 'sandness', 'sandsting', 'sandwick', 'scalloway',
    'scalloway hotel', 'scholland', 'schoolhouse', 'scorradale', 'scousburgh',
    'seafield', 'seaview house', 'sellafirth', 'setter', 'shetland', 'skeld',
    'skellister', 'sound', 'south califf', 'south voe', 'stapness', 'sullom',
    'sumburgh', 'sursetter', 'swinister', 'symbister',
    'tingwall', 'toft', 'tresta', 'trondra', 'turill', 'twatt',
    'ulsta', 'union bank office', 'unst', 'upper sound', 'uyea', 'uyeasound',
    'vaila', 'vidlin', 'voe',
    'wadbister', 'walls', 'wart', 'weathersta', 'weisdale',
    'west sandwick', 'west yell', 'whalsay', 'whiteness', 'wormadale',
    'yell',
}

# Compound Shetland addresses (the first part before comma is Shetland)
# e.g. "Baltasound, Unst" or "Gilbert Bain Hospital, Lerwick"
# We check each comma-separated part against SHETLAND_PLACES

def is_shetland(place_str):
    """Classify a place as Shetland (1), not Shetland (0), or unknown (None)."""
    if not place_str or place_str.strip() in ('-', ''):
        return None

    # Check for explicit Shetland mention
    lower = place_str.lower().strip()
    if 'shetland' in lower:
        return 1

    # Check for explicit non-Shetland qualifiers
    non_shetland_qualifiers = [
        'lancashire', 'london', 'england', 'sussex', 'surrey', 'fife',
        'lothian', 'canada', 'australia', 'new zealand', 'usa', 'manitoba',
        'nova scotia', 'ontario', 'hampshire', 'ayrshire', 'roxburghshire',
        'dumfries', 'galloway', 'clackmannanshire', 'kensington', 'jamaica',
        'barbados', 'madeira', 'skye', 'isle of skye', 'ross-shire',
        'tynemouth', 'peterhead', 'blythswood', 'glasgow', 'edinburgh',
        'evanton',
    ]
    for q in non_shetland_qualifiers:
        if q in lower:
            return 0

    # Check each comma-separated part
    parts = [p.strip().lower() for p in place_str.split(',')]
    for part in parts:
        if part in SHETLAND_PLACES:
            return 1

    # Check if the whole string (lowered) matches
    if lower in SHETLAND_PLACES:
        return 1

    # Check if first part (before comma) is a Shetland place
    first_part = parts[0]
    # Handle "X Street, Lerwick" type addresses
    if len(parts) > 1 and parts[-1].strip() in SHETLAND_PLACES:
        return 1

    # Known non-Shetland places
    non_shetland = {
        'aberdeen', 'adelaide', 'arbirlot', 'auldearn', 'bellshill', 'bolton',
        'bombay', 'bourne', 'braintree', 'caithness', 'colmonell', 'douglas',
        'doune', 'dundee', 'dunfermline', 'east linton', 'edinburgh',
        'elderslie', 'forfar', 'fraserburgh', 'glasgow', 'golspie', 'greenock',
        'halifax', 'hamilton', 'huddersfield', 'huyton', 'inverarity',
        'ireland', 'islay', 'keiss', 'kenmore', 'kilmacolm', 'kilwinning',
        'kirkcaldy', 'kirkintilloch', 'kirkwall', 'lasswade', 'leith',
        'lochee', 'manchester', 'melbourne', 'mortlake', 'muthil',
        'old machar', 'orkney', 'oxford', 'paisley', 'peterhead', 'ryde',
        'scoonie', 'shandon', 'shapinsay', 'south ronaldsay', 'south shields',
        'st ola', 'stromness', 'tain', 'wallacetown', 'walney island', 'wick',
        'aboyne', 'banchory', 'bathgate', 'blairgowrie', 'bothwell',
        'bournemouth', 'coupar angus', 'darlington', 'dumfries', 'ellon',
        'hawick', 'lockerbie', 'montrose', 'musselburgh', 'peebles', 'perth',
        'portree', 'rubislaw', 'sanquhar', 'selkirk', 'sydenham',
        'tillicoulty', 'torquay', 'troon', 'whitburn',
        'new york', 'vancouver', 'victoria', 'dunedin', 'tauranga',
        'gisbourne', 'pahiatua', 'hagersville', 'bridgewater', 'brandon',
        'newhaven', 'pittenweem', 'burma', 'stratford-on-avon', 'st machars',
    }
    if first_part in non_shetland:
        return 0

    # Street addresses with Lerwick/Shetland context
    if any(s in lower for s in ['commercial street', 'john street', 'magnus street',
                                  'harbour street', 'carlton place', 'st. olaf street',
                                  'mounthooly street', 'landale road',
                                  'near lerwick', 'middlebie house']):
        # Most of these are Lerwick streets
        if 'lerwick' in lower or 'peterhead' not in lower:
            return 1

    # Data errors
    if lower.startswith('d. '):
        return None

    # M.V. Hrossey (ferry) - unknown
    if 'hrossey' in lower:
        return None

    return None


def main():
    db_paths = ['shetland.db']
    for db_path in db_paths:
        try:
            db = sqlite3.connect(db_path)
        except Exception as e:
            print(f"Skipping {db_path}: {e}")
            continue

        cur = db.cursor()

        # Add columns if missing
        cols = [row[1] for row in cur.execute("PRAGMA table_info(people)")]
        if 'born_in_shetland' not in cols:
            cur.execute("ALTER TABLE people ADD COLUMN born_in_shetland INTEGER")
            print(f"[{db_path}] Added born_in_shetland column")
        if 'died_in_shetland' not in cols:
            cur.execute("ALTER TABLE people ADD COLUMN died_in_shetland INTEGER")
            print(f"[{db_path}] Added died_in_shetland column")

        # Populate from birth_place/death_place
        people = cur.execute("SELECT id, name, birth_place, death_place FROM people").fetchall()
        updated = 0
        for pid, name, bp, dp in people:
            born_val = is_shetland(bp)
            died_val = is_shetland(dp)
            cur.execute("UPDATE people SET born_in_shetland = ?, died_in_shetland = ? WHERE id = ?",
                        (born_val, died_val, pid))
            updated += 1

        db.commit()

        # Summary
        stats = cur.execute("""
            SELECT
                SUM(CASE WHEN born_in_shetland = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN born_in_shetland = 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN born_in_shetland IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN died_in_shetland = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN died_in_shetland = 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN died_in_shetland IS NULL THEN 1 ELSE 0 END)
            FROM people
        """).fetchone()
        print(f"[{db_path}] Updated {updated} people")
        print(f"  Born in Shetland: {stats[0]} | Born elsewhere: {stats[1]} | Unknown: {stats[2]}")
        print(f"  Died in Shetland: {stats[3]} | Died elsewhere: {stats[4]} | Unknown: {stats[5]}")

        # Show unclassified places for review
        unclass_birth = cur.execute(
            "SELECT DISTINCT birth_place FROM people WHERE birth_place IS NOT NULL AND born_in_shetland IS NULL ORDER BY birth_place"
        ).fetchall()
        if unclass_birth:
            print(f"  Unclassified birth places: {[r[0] for r in unclass_birth]}")

        unclass_death = cur.execute(
            "SELECT DISTINCT death_place FROM people WHERE death_place IS NOT NULL AND died_in_shetland IS NULL ORDER BY death_place"
        ).fetchall()
        if unclass_death:
            print(f"  Unclassified death places: {[r[0] for r in unclass_death]}")

        db.close()


if __name__ == '__main__':
    main()
