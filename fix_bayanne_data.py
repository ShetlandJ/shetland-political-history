#!/usr/bin/env python3
"""
Fix people data using Bayanne cross-reference results.
1. Remove wrong Bayanne IDs (pointing to wrong person)
2. Update dates where Bayanne differs (trust Bayanne for genealogical data)
3. Fill missing dates/places from Bayanne
4. Fix Charlotte Nicol's birth_place containing death info
"""

import sqlite3, json

DB_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'
CROSSREF_PATH = '/private/tmp/claude-501/bayanne_crossref.json'

def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    with open(CROSSREF_PATH) as f:
        data = json.load(f)

    # === 1. Remove wrong Bayanne IDs ===
    print("=== Removing wrong Bayanne IDs ===")
    wrong_ids = [
        ('Balfour Spence', 'Returns Charlotte OGILVY'),
        ('Gilbert Duncan', 'Returns Elizabeth OGILVY'),
        ('William Hay', 'Returns Margaret OGILVY'),
        ('Andrew Garriock', 'Returns Peter GARRIOCK'),
        ('James Morrison', 'Returns Magnus Peter MORRISON - different person'),
        ('Magnus Flaws (ii)', 'Dates wildly different: 1947 vs 1893'),
        ('John Irvine (i)', 'Dates very different: 1873 vs 1877, 1945 vs 1959'),
    ]
    for name, reason in wrong_ids:
        c.execute("UPDATE people SET bayanne_id = NULL WHERE name = ?", (name,))
        if c.rowcount:
            print(f"  Removed: {name} ({reason})")

    # === 2. Update dates from Bayanne ===
    print("\n=== Updating dates from Bayanne ===")

    # These are people where Bayanne has different dates and Bayanne is the authority
    # Excluding "Living" people (Bayanne hides their data) and wrong-ID people
    date_updates = {
        # (name): {field: new_value, ...}
        'Adam Jamieson': {'born_date': '1861-10-04'},
        'Adam Thomson': {'born_date': '1911-10-09'},
        'Archibald Garriock': {'died_date': '1899-02-28'},
        'Arthur Anderson': {'born_date': '1792-02-19'},
        'Basil Neven-Spence': {'born_date': '1888-05-12'},
        'Catherine Anderson': {'born_date': '1917-02-17'},
        'Charles Duncan': {'died_date': '1884-11-10'},
        'Charles Merrylees': {'born_date': '1843-10-23'},
        'Charles Stout': {'born_date': '1845-11-26'},
        'Charlotte Nicol': {'born_date': '1864', 'birth_place': None, 'died_date': '1954-11-25'},
        'David Gray (i)': {'born_date': '1842', 'died_date': '1901-05-29'},
        'David Murray': {'died_date': '1961-03-21'},
        'Edwyn Tait': {'born_date': '1884-12-14'},
        'George Laurence': {'died_date': '1885-09-02'},
        'George Smith (i)': {'born_date': '1822-12-01'},
        'George Tait': {'born_date': '1829', 'died_date': '1889-07-08'},
        'James Anderson (iv)': {'born_date': '1910-01-26'},
        'James Brownlie': {'died_date': '1968-11-21'},
        'James Hunter (iii)': {'born_date': '1872-02-06'},
        'James Shearer': {'died_date': '1949-03-28'},
        'James Smith (i)': {'born_date': '1877-07-07'},
        'John Nicolson': {'born_date': '1937-09-26'},
        'John Robertson (iii)': {'born_date': '1841-05-25'},
        'John Stewart': {'died_date': '1956-12-12'},
        'Joseph Peterson (i)': {'died_date': '1953-04-24'},
        'Robert Hicks': {'born_date': '1809'},
        'Robert Sinclair': {'born_date': '1814', 'died_date': '1891-07-16'},
        'Robert Strachan': {'born_date': '1925-09-12'},
        'Samuel Fordyce': {'born_date': '1852'},
        'Theodore Andrew': {'died_date': '1960-09-05'},
        'Thomas Irvine (ii)': {'died_date': '1946-06-06'},
        'Thomas Sinclair': {'born_date': '1899-07-12'},
        'William Adie': {'born_date': '1839-04-02'},
        'William Carson': {'born_date': '1890-12-14'},
        'William Hamilton': {'born_date': '1905-12-01'},
        'William Henry': {'born_date': '1878-05-25'},
        'William Jamieson': {'died_date': '1937-01-21'},
    }

    update_count = 0
    for name, updates in date_updates.items():
        set_clauses = []
        values = []
        for field, value in updates.items():
            set_clauses.append(f"{field} = ?")
            values.append(value)
        values.append(name)
        sql = f"UPDATE people SET {', '.join(set_clauses)} WHERE name = ?"
        c.execute(sql, values)
        if c.rowcount:
            update_count += 1
            print(f"  Updated {name}: {updates}")
    print(f"  Total: {update_count} people updated")

    # === 3. Fill missing dates/places from Bayanne ===
    print("\n=== Filling missing data from Bayanne ===")

    fills = {
        'Alexander Manson': {'born_date': '1883-07-18', 'died_date': '1938-07-22', 'birth_place': 'Clumly, Dunrossness', 'death_place': 'Lerwick'},
        'Amanda Youngman': {'born_date': '1914-01-31', 'birth_place': 'Greenock'},
        'Andrew Dick': {'born_date': '1637'},
        'Arthur Hay': {'died_date': '1896-12-25', 'death_place': 'Lerwick'},
        'Arthur Nicolson (i)': {'died_date': '1917-05-27', 'death_place': 'Fetlar'},
        'Cecil Eunson (i)': {'born_date': '1928-12-30', 'died_date': '2007-12-25', 'birth_place': 'Lerwick', 'death_place': 'Aberdeen'},
        'Edward Knight': {'died_date': '2022-10-15', 'death_place': 'Lerwick'},
        'Edwin Hyde': {'born_date': '1873'},
        'Erling Clausen': {'died_date': '1984-06-28'},
        'Florence Grains': {'died_date': '2025-03-05', 'death_place': 'Walls'},
        'George Jamieson': {'born_date': '1870-12-01'},
        'George Johnston': {'born_date': '1869-03-21', 'died_date': '1909-06-08'},
        'Gordon Walterson': {'died_date': '2019-08-27'},
        'Hugh Robertson': {'died_date': '1932-01-15'},
        'James Henry': {'died_date': '2018-07-31'},
        'James Irvine (ii)': {'died_date': '2021-09-04'},
        'James Pottinger (i)': {'born_date': '1790'},
        'James Scott': {'died_date': '1859-12-20'},
        'John Inkster (iii)': {'died_date': '2021-12-12'},
        'John Nicolson': {'died_date': '2019-10-09'},
        'John Rae': {'born_date': '1904-12-22', 'died_date': '1985-11-14'},
        'John Smith (ii)': {'died_date': '1978'},
        'Laurence Smith': {'died_date': '1964-05-27'},
        'Leslie Angus': {'died_date': '2019-10-01'},
        'Loretta Hutchison': {'died_date': '2024-02-17'},
        'Mary Colligan': {'died_date': '2025-07-13'},
        'Norman Cameron': {'died_date': '1967-04-09'},
        'Peter Goodlad': {'born_date': '1857-12-02', 'died_date': '1936-11-13'},
        'Robert Haldane': {'born_date': '1848-07-20'},
        'Robert Johnson (ii)': {'born_date': '1929-03-09', 'died_date': '2014-08-12'},
        'Robert Scott': {'born_date': '1840-11-10', 'died_date': '1906-10-14'},
        'Thomas Nicolson': {'born_date': '1793-11-16'},
        'William Anderson (ii)': {'born_date': '1919-12-22'},
        'William Duncan (ii)': {'died_date': '1945-06-21'},
        'William Levie': {'died_date': '1901-01-27'},
        'William Peterson': {'born_date': '1923-11-22', 'died_date': '1994-07-17'},
        'William Playfair': {'born_date': '1926'},
        'William Robertson': {'born_date': '1828-11-10'},
        'William Sievwright (i)': {'died_date': '1870-06-26'},
        'William Tait': {'died_date': '2021-03-19'},
    }

    fill_count = 0
    for name, updates in fills.items():
        set_clauses = []
        values = []
        for field, value in updates.items():
            # Only fill if currently NULL
            set_clauses.append(f"{field} = COALESCE({field}, ?)")
            values.append(value)
        values.append(name)
        sql = f"UPDATE people SET {', '.join(set_clauses)} WHERE name = ?"
        c.execute(sql, values)
        if c.rowcount:
            fill_count += 1
    print(f"  Filled missing data for {fill_count} people")

    # === 4. Fix Charlotte Nicol's birth_place ===
    print("\n=== Fixing Charlotte Nicol birth_place ===")
    c.execute("UPDATE people SET birth_place = NULL WHERE name = 'Charlotte Nicol' AND birth_place LIKE 'd.%'")
    if c.rowcount:
        print("  Fixed: removed 'd. after 1950' from birth_place")

    db.commit()

    # === Summary ===
    print("\n=== Summary ===")
    c.execute("SELECT COUNT(*) FROM people WHERE born_date IS NOT NULL")
    print(f"  People with birth date: {c.fetchone()[0]}")
    c.execute("SELECT COUNT(*) FROM people WHERE died_date IS NOT NULL")
    print(f"  People with death date: {c.fetchone()[0]}")
    c.execute("SELECT COUNT(*) FROM people WHERE bayanne_id IS NOT NULL")
    print(f"  People with Bayanne ID: {c.fetchone()[0]}")

    db.close()
    print("\nDone!")

if __name__ == '__main__':
    main()
