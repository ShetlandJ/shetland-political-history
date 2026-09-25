#!/usr/bin/env python3
"""
1. Record who each modern SIC by-election replaced. add_modern_sic.py added these by-elections
without the vacating councillor, so the council's membership couldn't be traced past them.
Idempotent: re-running does nothing once the corrections are in place.

Sources (Wikipedia, the same source add_modern_sic.py used), checked 2026-09-25:
  2017 Shetland Islands Council election, "By-elections since 2017":
    - Lerwick South, 7 Nov 2019: "Beatrice Wishart was subsequently elected as Member of the
      Scottish Parliament for Shetland at the 2019 Shetland by-election".
    - Shetland Central, 7 Nov 2019: "Mark Burgess resigned his seat on 20 September 2019 for
      personal reasons."
  2022 Shetland Islands Council election:
    - North Isles, 4 Aug 2022: filled the third seat, left vacant at the May 2022 election
      through insufficient nominations. Nobody was replaced.
    - Shetland West, 17 Nov 2022: John Leask resigned in August 2022.
    - Shetland North, 23 Jan 2025: Tom Morton stepped down in October 2024.

2. Put SIC by-elections in the ward their own title names. Six had no ward, and four were on
   an older ward of the same area ("Delting", "Yell") that wasn't in use at the time. Each
   title matches exactly one ward at the preceding general election, which is used here.
   Without this their seats couldn't be traced (e.g. 1996 showed 27 members for 26 seats).
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

# wiki_page_title -> slug of the councillor replaced, or None for a seat that was never filled
REPLACED = {
    'Lerwick South By-Election November 2019': 'beatrice-wishart',
    'Shetland Central By-Election November 2019': 'mark-burgess',
    'North Isles By-Election August 2022': None,
    'Shetland West By-Election November 2022': 'john-leask',
    'Shetland North By-Election January 2025': 'tom-morton',
}

UNFILLED = '[unfilled seat]'  # the marker the ZCC by-elections already use

# wiki_page_title -> (constituency_id it had, constituency_id of the ward in its title)
WARDS = {
    'Lerwick North By-Election December 1980': (None, 65),         # Lerwick North
    'Delting North By-Election August 1982': (9, 78),              # Delting -> Delting North
    'Delting North By-Election November 1983': (9, 78),
    'Sandwick By-Election May 1987': (None, 74),                   # Sandwick
    'Yell South By-Election February 1993': (61, 77),              # Yell -> Yell South
    'Whalsay and Skerries By-Election March 1993': (None, 75),     # Whalsay And Skerries
    'Whiteness, Weisdale And Tingwall By-Election September 1993': (None, 76),
    'Yell South By-Election November 1996': (61, 77),
    'Whalsay and Skerries By-Election March 2002': (None, 75),
    'Lerwick South By-Election February 2008': (None, 31),         # Lerwick South (SIC Constituency)
}


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    print("=== Modern SIC by-elections: replaced councillor ===")
    for title, slug in REPLACED.items():
        c.execute("SELECT id, replaced_person, replaced_person_id FROM elections "
                  "WHERE wiki_page_title = ? AND council_id = 3", (title,))
        rows = c.fetchall()
        if len(rows) != 1:
            raise SystemExit(f"expected 1 election '{title}', got {len(rows)}")
        e = rows[0]

        if slug is None:
            name, pid = UNFILLED, None
        else:
            c.execute("SELECT id, name FROM people WHERE slug = ?", (slug,))
            p = c.fetchone()
            if p is None:
                raise SystemExit(f"people.{slug} not found")
            name, pid = p['name'], p['id']

        if (e['replaced_person'], e['replaced_person_id']) == (name, pid):
            print(f"  {title}: already {name}")
        elif e['replaced_person'] is None and e['replaced_person_id'] is None:
            c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = ? WHERE id = ?",
                      (name, pid, e['id']))
            print(f"  {title} (id {e['id']}): replaced_person = {name}")
        else:
            raise SystemExit(f"'{title}' already has replaced_person {e['replaced_person']}, not overwriting")

    print("\n=== SIC by-elections: ward from the title ===")
    for title, (wrong, right) in WARDS.items():
        c.execute("SELECT id, constituency_id FROM elections WHERE wiki_page_title = ? AND council_id = 3", (title,))
        rows = c.fetchall()
        if len(rows) != 1:
            raise SystemExit(f"expected 1 election '{title}', got {len(rows)}")
        e = rows[0]
        c.execute("SELECT name FROM constituencies WHERE id = ? AND council_id = 3", (right,))
        ward = c.fetchone()
        if ward is None:
            raise SystemExit(f"constituency {right} is not an SIC ward")
        if e['constituency_id'] == right:
            print(f"  {title}: already {ward['name']}")
        elif e['constituency_id'] == wrong:
            c.execute("UPDATE elections SET constituency_id = ? WHERE id = ?", (right, e['id']))
            print(f"  {title} (id {e['id']}): ward -> {ward['name']}")
        else:
            raise SystemExit(f"'{title}' has unexpected constituency_id {e['constituency_id']}")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
