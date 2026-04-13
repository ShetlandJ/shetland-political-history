#!/usr/bin/env python3
"""
Create constituency records for unmatched ward names in elections, then link
the elections to them. Runs after parse_wiki.py + add_modern_sic.py.

Many SIC wards (1974-2002) appear in election pages as ward headings but
have no matching constituency record. This creates them so they can be
linked from election pages and have their own constituency pages.
"""
import sqlite3
import re
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'shetland.db')


def normalize_name(name):
    """Decode HTML entities and normalize ampersands to 'And'."""
    name = name.replace('&amp;', '&')
    name = re.sub(r'\s*&\s*', ' And ', name)
    return name.strip()


def slugify(name):
    """Match the slug style used elsewhere: lowercase, strip punctuation, hyphens."""
    s = name.lower()
    s = re.sub(r"[,\.\(\)']", '', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'-+', '-', s).strip('-')
    return s


def main():
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # Find all unmatched display names per council
    rows = cur.execute("""
        SELECT DISTINCT e.council_id, e.constituency_display_name
        FROM elections e
        WHERE e.constituency_id IS NULL
          AND e.constituency_display_name IS NOT NULL
          AND e.constituency_display_name != ''
    """).fetchall()

    created = 0
    linked = 0
    skipped_existing = 0

    # Pre-load existing constituencies. Two lookups per council:
    #   names_by_council[council_id][name_lower] = constituency_id
    #   slugs_by_council[council_id] = set of slugs (for uniqueness)
    names_by_council = {}
    slugs_by_council = {}
    for con_id, council_id, name, slug in cur.execute(
        "SELECT id, council_id, name, slug FROM constituencies"
    ):
        names_by_council.setdefault(council_id, {})[name.lower()] = con_id
        slugs_by_council.setdefault(council_id, set()).add(slug)

    # Group display names by their normalized form per council
    name_to_constituency_id = {}  # (council_id, normalized_name) -> constituency_id

    for council_id, display_name in rows:
        normalized = normalize_name(display_name)
        key = (council_id, normalized.lower())

        if key in name_to_constituency_id:
            continue

        # Check if a constituency with this name already exists for the council
        if normalized.lower() in names_by_council.get(council_id, {}):
            cid = names_by_council[council_id][normalized.lower()]
            name_to_constituency_id[key] = cid
            skipped_existing += 1
            continue

        # Create new constituency
        slug = slugify(normalized)
        original_slug = slug
        n = 2
        while slug in slugs_by_council.get(council_id, set()):
            slug = f"{original_slug}-{n}"
            n += 1

        cur.execute("""
            INSERT INTO constituencies (council_id, name, slug)
            VALUES (?, ?, ?)
        """, (council_id, normalized, slug))
        new_id = cur.lastrowid
        name_to_constituency_id[key] = new_id
        names_by_council.setdefault(council_id, {})[normalized.lower()] = new_id
        slugs_by_council.setdefault(council_id, set()).add(slug)
        created += 1

    # Now link elections to these constituencies
    rows = cur.execute("""
        SELECT id, council_id, constituency_display_name
        FROM elections
        WHERE constituency_id IS NULL
          AND constituency_display_name IS NOT NULL
          AND constituency_display_name != ''
    """).fetchall()

    for eid, council_id, display_name in rows:
        normalized = normalize_name(display_name)
        key = (council_id, normalized.lower())
        if key in name_to_constituency_id:
            cid = name_to_constituency_id[key]
            # If display_name == new constituency name, clear the display field
            # (no need to show it as a "now ..." override)
            if normalized == display_name:
                cur.execute("""
                    UPDATE elections SET constituency_id = ?, constituency_display_name = NULL
                    WHERE id = ?
                """, (cid, eid))
            else:
                cur.execute("""
                    UPDATE elections SET constituency_id = ?
                    WHERE id = ?
                """, (cid, eid))
            linked += 1

    db.commit()
    print(f"Created {created} new constituencies, linked {linked} elections")
    print(f"  ({skipped_existing} display names matched existing constituencies)")
    db.close()


if __name__ == '__main__':
    main()
