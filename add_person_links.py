#!/usr/bin/env python3
"""
Add person links to intro/biography text in SQLite.

Strategy: Read wiki source, find [[Person|Display]] links, determine surrounding
context words. Use that context to find the exact position in the clean SQLite text.
"""

import sqlite3
import re
import mysql.connector

SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'
MYSQL_DB = 'shetland_history2'
MYSQL_PREFIX = 'mwfn_'


def strip_all_markup(t):
    """Strip all wiki markup to plain text."""
    t = re.sub(r'\[\[(?:File|Image):[^\]]*(?:\[\[[^\]]*\]\][^\]]*)*\]\]', '', t)
    t = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', t)
    t = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', t)
    t = re.sub(r'\[https?://[^\s\]]+\s+([^\]]+)\]', r'\1', t)
    t = re.sub(r'\[https?://[^\]]+\]', '', t)
    t = re.sub(r"'{2,3}", '', t)
    t = re.sub(r'<[^>]+>', '', t)
    t = re.sub(r'\{[^}]*\}', '', t)
    t = re.sub(r'\\n', '\n', t)
    t = re.sub(r'\s+,', ',', t)
    return t


def main():
    db = sqlite3.connect(SQLITE_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    mysql_db = mysql.connector.connect(host='localhost', user='root', database=MYSQL_DB)
    mc = mysql_db.cursor(dictionary=True)

    # Build person lookup
    c.execute("SELECT wiki_page_title, slug FROM people WHERE wiki_page_title IS NOT NULL")
    person_by_title = {}
    for row in c.fetchall():
        person_by_title[row['wiki_page_title']] = row['slug']
        person_by_title[row['wiki_page_title'].replace('_', ' ')] = row['slug']

    # Build redirect map
    mc.execute(f"""
        SELECT p.page_title, CONVERT(t.old_text USING utf8) as text
        FROM {MYSQL_PREFIX}page p
        JOIN {MYSQL_PREFIX}revision r ON r.rev_page = p.page_id
        JOIN {MYSQL_PREFIX}text t ON t.old_id = r.rev_text_id
        WHERE p.page_namespace = 0
        AND CONVERT(t.old_text USING utf8) LIKE '#REDIRECT%'
    """)
    redirect_map = {}
    for row in mc.fetchall():
        m = re.search(r'#REDIRECT\s*\[\[([^\]]+)\]\]', row['text'], re.IGNORECASE)
        if m:
            target = m.group(1).strip().replace(' ', '_')
            title = row['page_title'].decode('utf-8') if isinstance(row['page_title'], bytes) else row['page_title']
            redirect_map[title] = target
            redirect_map[title.replace('_', ' ')] = target

    def resolve_to_slug(wiki_link):
        for key in [wiki_link, wiki_link.replace(' ', '_'), wiki_link.replace('_', ' ')]:
            if key in person_by_title:
                return person_by_title[key]
        for key in [wiki_link, wiki_link.replace(' ', '_')]:
            target = redirect_map.get(key)
            if target:
                for tkey in [target, target.replace('_', ' ')]:
                    if tkey in person_by_title:
                        return person_by_title[tkey]
        return None

    skip_prefixes = ['Image:', 'File:', 'Template:', 'Category:', 'Wikipedia:']
    skip_contains = ['Constituency', 'Council', 'Election', 'By-Election',
                     'Parliament', 'Referendum', 'Referenda']

    def get_links_with_context(wiki_section):
        """Extract person links with surrounding plain-text context."""
        if not wiki_section:
            return []

        results = []

        # Find all person wiki links in the raw source
        for m in re.finditer(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', wiki_section):
            target = m.group(1).strip()
            display = (m.group(2) or target).strip()

            if any(target.startswith(p) for p in skip_prefixes):
                continue
            if any(kw in target for kw in skip_contains):
                continue
            if re.match(r'^\d{4}$', target):
                continue
            # Skip if inside a File/Image tag (check backwards for [[File: or [[Image:)
            before = wiki_section[:m.start()]
            # Count unmatched [[ for File/Image
            open_file = len(re.findall(r'\[\[(?:File|Image):', before)) - len(re.findall(r'\]\]', before[before.rfind('[[File:') if '[[File:' in before else before.rfind('[[Image:') if '[[Image:' in before else 0:]))
            # Simpler check: is the link inside a File/Image block?
            last_file_start = max(before.rfind('[[File:'), before.rfind('[[Image:'))
            if last_file_start != -1:
                # Check if that File/Image block is closed before our link
                between = wiki_section[last_file_start:m.start()]
                # Count [[ and ]] — if unbalanced, we're inside
                opens = len(re.findall(r'\[\[', between))
                closes = len(re.findall(r'\]\]', between))
                if opens > closes:
                    continue

            slug = resolve_to_slug(target)
            if not slug:
                continue

            # Extract context: plain text around this link in the wiki source
            # Get ~30 chars before and after, stripped of markup
            ctx_start = max(0, m.start() - 80)
            ctx_end = min(len(wiki_section), m.end() + 80)
            before_raw = wiki_section[ctx_start:m.start()]
            after_raw = wiki_section[m.end():ctx_end]
            before_plain = strip_all_markup(before_raw).strip()
            after_plain = strip_all_markup(after_raw).strip()

            # Get the last few words before and first few words after
            before_words = before_plain.split()[-3:] if before_plain else []
            after_words = after_plain.split()[:3] if after_plain else []

            results.append({
                'display': display,
                'slug': slug,
                'before': ' '.join(before_words),
                'after': ' '.join(after_words),
            })

        return results

    def apply_links(text, links, page_slug):
        """Apply person links using context matching."""
        if not text or not links:
            return text, 0

        count = 0
        used_positions = set()

        for link in links:
            display = link['display']
            slug = link['slug']
            if slug == page_slug:
                continue

            marker = f'[person:{slug}:{display}]'
            if marker in text:
                continue

            # Try to find the display text with matching context
            best_pos = -1
            best_score = -1

            idx = 0
            while idx < len(text):
                pos = text.find(display, idx)
                if pos == -1:
                    break

                # Skip if already used
                if any(s <= pos < e for s, e in used_positions):
                    idx = pos + 1
                    continue

                # Check word boundary for short names
                if len(display) < 6:
                    before_ok = pos == 0 or not text[pos - 1].isalpha()
                    after_pos = pos + len(display)
                    after_ok = after_pos >= len(text) or not text[after_pos].isalpha()
                    if not (before_ok and after_ok):
                        idx = pos + 1
                        continue

                # Score this position by context match
                score = 0
                text_before = text[max(0, pos - 50):pos]
                text_after = text[pos + len(display):pos + len(display) + 50]

                if link['before']:
                    for word in link['before'].split():
                        if len(word) > 2 and word in text_before:
                            score += 1
                if link['after']:
                    for word in link['after'].split():
                        if len(word) > 2 and word in text_after:
                            score += 1

                if score > best_score:
                    best_score = score
                    best_pos = pos

                idx = pos + 1

            if best_pos == -1:
                continue

            # Apply the replacement
            text = text[:best_pos] + marker + text[best_pos + len(display):]
            used_positions.add((best_pos, best_pos + len(marker)))
            count += 1

        return text, count

    # Process each person
    c.execute("SELECT id, name, slug, wiki_page_title, intro, biography FROM people WHERE wiki_page_title IS NOT NULL")
    people = c.fetchall()

    total_links = 0
    pages_updated = 0

    for person in people:
        wiki_title = person['wiki_page_title']

        mc.execute(f"""
            SELECT CONVERT(t.old_text USING utf8) as text
            FROM {MYSQL_PREFIX}page p
            JOIN {MYSQL_PREFIX}revision r ON r.rev_page = p.page_id
            JOIN {MYSQL_PREFIX}text t ON t.old_id = r.rev_text_id
            WHERE p.page_title = %s AND p.page_namespace = 0
            ORDER BY r.rev_id DESC LIMIT 1
        """, (wiki_title,))
        row = mc.fetchone()
        if not row:
            continue

        wiki_text = row['text']

        # Split into intro and biography
        bio_split = re.split(r'==\s*Biography\s*==', wiki_text, maxsplit=1)
        intro_wiki = bio_split[0]
        intro_wiki = re.split(r'==\s*\w', intro_wiki)[0]

        bio_wiki = None
        if len(bio_split) > 1:
            bio_wiki = re.split(r'\n==(?!=)', bio_split[1], maxsplit=1)[0]

        intro_links = get_links_with_context(intro_wiki)
        bio_links = get_links_with_context(bio_wiki)

        new_intro, ic = apply_links(person['intro'], intro_links, person['slug'])
        new_bio, bc = apply_links(person['biography'], bio_links, person['slug'])

        if ic > 0 or bc > 0:
            c.execute("UPDATE people SET intro = ?, biography = ? WHERE id = ?",
                      (new_intro, new_bio, person['id']))
            total_links += ic + bc
            pages_updated += 1

    db.commit()
    mysql_db.close()

    print(f"Updated {pages_updated} people with {total_links} person links")

    # Verify key examples
    for slug in ['magnus-shearer-i', 'adam-halcrow-i', 'balfour-spence']:
        c.execute("SELECT name, intro FROM people WHERE slug = ?", (slug,))
        row = c.fetchone()
        if row:
            links = re.findall(r'\[person:([^\]:]+):([^\]]+)\]', row['intro'] or '')
            if links:
                print(f"\n{row['name']}: {', '.join(f'{d} → {s}' for s, d in links)}")
            # Show snippet around each link
            for m in re.finditer(r'\[person:[^\]]+\]', row['intro'] or ''):
                start = max(0, m.start() - 20)
                end = min(len(row['intro']), m.end() + 20)
                print(f"  ...{row['intro'][start:end]}...")

    db.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
