#!/usr/bin/env python3
"""
Parse Shetland political history from MediaWiki MySQL database into structured SQLite.

Uses the navigation templates as the canonical list of elections, then parses
each election page to extract candidates, votes, and results. Also imports
councillor biographies and constituency pages.
"""

import re
import sqlite3
import mysql.connector
from datetime import datetime
from collections import defaultdict

# --- Config ---
MYSQL_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'database': 'shetland_history2',
}
SQLITE_PATH = '/Users/james/projects/shetland_history/new-site/shetland.db'
SCHEMA_PATH = '/Users/james/projects/shetland_history/new-site/schema.sql'

# --- Helpers ---

def slugify(text):
    """Convert text to URL-friendly slug."""
    s = text.lower().strip()
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    s = re.sub(r'[\s]+', '-', s)
    s = re.sub(r'-+', '-', s)
    return s.strip('-')


def get_wiki_page(cursor, page_title, namespace=0):
    """Fetch the latest revision text for a wiki page."""
    title = page_title.replace(' ', '_')
    cursor.execute("""
        SELECT CAST(old_text AS CHAR CHARACTER SET utf8mb4)
        FROM mwfn_text t
        JOIN mwfn_revision r ON r.rev_text_id = t.old_id
        JOIN mwfn_page p ON p.page_latest = r.rev_id
        WHERE p.page_title = %s AND p.page_namespace = %s
        LIMIT 1
    """, (title, namespace))
    row = cursor.fetchone()
    if row and row[0]:
        val = row[0]
        if isinstance(val, (bytes, bytearray)):
            val = val.decode('utf-8', errors='replace')
        return val
    return None


def get_template_text(cursor, template_name):
    """Fetch a template's text (namespace 10)."""
    return get_wiki_page(cursor, template_name, namespace=10)


def extract_links_from_template(text):
    """Extract all [[Page Title|Display]] links from a template."""
    links = []
    for match in re.finditer(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', text):
        page_title = match.group(1).strip()
        display = match.group(2).strip() if match.group(2) else page_title
        links.append((page_title, display))
    return links


def parse_election_date_from_title(title):
    """Extract date from election page title like 'Lerwick_Town_Council_Election_May_1958'."""
    # Try to extract month and year
    m = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})', title.replace('_', ' '))
    if m:
        month_str, year = m.group(1), m.group(2)
        try:
            dt = datetime.strptime(f"1 {month_str} {year}", "%d %B %Y")
            return dt.strftime("%Y-%m-01")
        except ValueError:
            pass

    # Just year
    m = re.search(r'(\d{4})', title)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def parse_election_date_from_text(text, title):
    """Try to extract the exact date from the election page text."""
    # "occurred on Tuesday 5 May" or "occurred on April 2"
    m = re.search(r'occurred\s+on\s+(?:\w+day\s+)?(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)', text)
    if m:
        day, month_str = m.group(1), m.group(2)
        year_m = re.search(r'(\d{4})', title)
        if year_m:
            try:
                dt = datetime.strptime(f"{day} {month_str} {year_m.group(1)}", "%d %B %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    m = re.search(r'occurred\s+on\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})', text)
    if m:
        month_str, day = m.group(1), m.group(2)
        year_m = re.search(r'(\d{4})', title)
        if year_m:
            try:
                dt = datetime.strptime(f"{day} {month_str} {year_m.group(1)}", "%d %B %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # "on 14 December 1918"
    m = re.search(r'on\s+(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', text)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return parse_election_date_from_title(title)


def parse_electorate_turnout(text):
    """Extract electorate and turnout from election page text."""
    electorate = None
    turnout = None
    turnout_pct = None

    m = re.search(r'Electorate:\s*([\d,]+)', text)
    if m:
        electorate = int(m.group(1).replace(',', ''))

    m = re.search(r'Turnout:\s*([\d,]+)\s*\((\d+\.?\d*)%\)', text)
    if m:
        turnout = int(m.group(1).replace(',', ''))
        turnout_pct = float(m.group(2))
    elif re.search(r'Turnout:\s*([\d,]+)', text):
        m2 = re.search(r'Turnout:\s*([\d,]+)', text)
        turnout = int(m2.group(1).replace(',', ''))

    return electorate, turnout, turnout_pct


def parse_candidates_from_table(table_text, has_party_column=False):
    """Parse candidates from a wikitable block."""
    candidates = []
    # Split into rows by |-
    rows = re.split(r'\|-', table_text)

    for row in rows:
        row_stripped = row.strip()

        # Skip table open/close, empty rows
        if not row_stripped or row_stripped == '|}' or row_stripped.startswith('{|'):
            continue

        # Skip header rows - contain ! (header cells) or '''Candidate''' / '''Party'''
        if "'''Candidate'''" in row or "'''Party'''" in row or "'''Votes'''" in row:
            continue
        if re.match(r'\s*!', row_stripped):
            continue
        # Skip rows that are purely styling/header (contain !style but no candidate data)
        if '!style' in row:
            continue
        # Skip rows that only contain background color styling and header text
        if 'background:' in row and ('colspan' in row or "'''" in row):
            continue

        # Extract cells - split by ||
        cells = re.split(r'\|\|', row)
        if not cells:
            continue

        # First cell might have leading | from the row
        first_cell = cells[0]
        # Remove leading | and whitespace
        first_cell = re.sub(r'^\s*\|', '', first_cell).strip()

        if not first_cell:
            continue

        # Skip if first cell looks like table markup, not a candidate name
        if first_cell.startswith('{|') or first_cell.startswith('!') or 'class="wikitable"' in first_cell:
            continue

        # Parse candidate name from first cell - could be [[Name]] or [[Name (i)|Name]] or plain text
        name_match = re.search(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', first_cell)
        if name_match:
            wiki_link = name_match.group(1).strip()
            display_name = name_match.group(2).strip() if name_match.group(2) else wiki_link
            # Skip if the link is an Image
            if wiki_link.startswith('Image:') or wiki_link.startswith('File:'):
                continue
        else:
            # Plain text name (no wiki link - person doesn't have a page)
            display_name = re.sub(r"'''", '', first_cell).strip()
            display_name = re.sub(r'\s+', ' ', display_name).strip()
            wiki_link = None

        if not display_name or len(display_name) < 2:
            continue

        # Skip if the "name" looks like markup rather than a real name
        if any(marker in display_name.lower() for marker in ['wikitable', 'style=', 'background', 'colspan', '{|', '|}']):
            continue

        # Determine which cells are what based on column count
        party = None
        votes_text = None
        votes = None
        elected = False

        if has_party_column and len(cells) >= 3:
            # Format: Name || Party || Votes || Elected
            party_cell = cells[1].strip() if len(cells) > 1 else ''
            party = re.sub(r'\s+', ' ', party_cell).strip()
            party = re.sub(r'^\|?\s*', '', party)
            if party and party.startswith('style='):
                # Color-coded party cell like |style="background: #ffd700"| || Liberal
                party = ''

            votes_cell = cells[2].strip() if len(cells) > 2 else ''
            votes_cell = re.sub(r'^\|?\s*', '', votes_cell)
        elif has_party_column and len(cells) == 2:
            # Might be color | party in first cell, then rest
            party = ''
            votes_cell = cells[1].strip() if len(cells) > 1 else ''
            votes_cell = re.sub(r'^\|?\s*', '', votes_cell)
        else:
            # Format: Name || Votes || Elected
            votes_cell = cells[1].strip() if len(cells) > 1 else ''
            votes_cell = re.sub(r'^\|?\s*', '', votes_cell)

        # Parse votes
        votes_clean = votes_cell.strip()
        votes_num_match = re.search(r'([\d,]+)', votes_clean)
        if votes_num_match and not any(kw in votes_clean.lower() for kw in ['unopposed', 'appointed', 'elected', 'image']):
            votes = int(votes_num_match.group(1).replace(',', ''))
            votes_text = None
        elif votes_clean:
            votes_text = re.sub(r'\[\[Image:[^\]]+\]\]', '', votes_clean).strip()
            votes_text = re.sub(r"'''", '', votes_text).strip()
            if not votes_text:
                votes_text = None

        # Check if elected - look for tick.gif in the entire row
        if 'tick.gif' in row or 'tick.png' in row:
            elected = True

        # Extract role from votes_text
        role = None
        if votes_text:
            role_match = re.search(r'(?:Appointed|Elected)\s+as\s+(.+)', votes_text)
            if role_match:
                role = role_match.group(1).strip()

        candidates.append({
            'name': display_name,
            'wiki_link': wiki_link,
            'party': party if party else None,
            'votes': votes,
            'votes_text': votes_text,
            'elected': elected,
            'role': role,
        })

    return candidates


def detect_party_column(text):
    """Check if the wikitable has a Party column."""
    return bool(re.search(r"'''Party'''", text))


def parse_election_page(text, title):
    """Parse a full election page and return structured data."""
    if not text:
        return None

    election_date = parse_election_date_from_text(text, title)
    is_by_election = 'by-election' in title.lower() or 'by_election' in title.lower() or 'By-Election' in title

    # Check for sub-sections (constituency-level results within a general election)
    # County Council general elections have === Constituency === sub-sections
    sections = re.split(r'===\s*\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]\s*===', text)

    has_party = detect_party_column(text)

    results = []

    if len(sections) > 1:
        # Multi-constituency election (e.g. County Council general election)
        # sections[0] is pre-content, then pairs of (link, display, content)
        i = 1
        while i < len(sections):
            constituency_link = sections[i].strip() if i < len(sections) else None
            constituency_display = sections[i+1].strip() if i+1 < len(sections) else constituency_link
            section_text = sections[i+2] if i+2 < len(sections) else ''

            # Parse electorate/turnout for this constituency
            electorate, turnout, turnout_pct = parse_electorate_turnout(section_text)

            # Find wikitable in this section
            table_match = re.search(r'(\{\|\s*class="wikitable".*?\|\})', section_text, re.DOTALL)

            candidates = []
            if table_match:
                has_party_section = detect_party_column(section_text)
                candidates = parse_candidates_from_table(table_match.group(1), has_party_section)

            # Check for notes (no nomination, election voided, etc.)
            notes = None
            if not candidates:
                # Look for explanatory text
                note_match = re.search(r'(?:There was no|No candidate|Election voided|see \[\[)(.+?)(?:\n|$)', section_text)
                if note_match:
                    notes = note_match.group(0).strip()
                    notes = re.sub(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', lambda m: m.group(2) or m.group(1), notes)

            results.append({
                'constituency_link': constituency_link,
                'constituency_name': constituency_display or constituency_link,
                'electorate': electorate,
                'turnout': turnout,
                'turnout_pct': turnout_pct,
                'candidates': candidates,
                'notes': notes,
            })
            i += 3
    else:
        # Single-constituency election (LTC, by-elections, UK elections)
        electorate, turnout, turnout_pct = parse_electorate_turnout(text)

        # Find all wikitables
        table_matches = list(re.finditer(r'(\{\|\s*class="wikitable".*?\|\})', text, re.DOTALL))
        candidates = []
        for table_match in table_matches:
            has_party_table = detect_party_column(table_match.group(1))
            candidates.extend(parse_candidates_from_table(table_match.group(1), has_party_table))

        results.append({
            'constituency_link': None,
            'constituency_name': None,
            'electorate': electorate,
            'turnout': turnout,
            'turnout_pct': turnout_pct,
            'candidates': candidates,
            'notes': None,
        })

    return {
        'date': election_date,
        'type': 'by-election' if is_by_election else 'general',
        'results': results,
    }


def parse_uk_election_page(text, title):
    """Parse a UK/GB Westminster election page.

    UK election tables have this format:
    |style="background: #color"| || Party || [[Candidate]] || 6,304 || 36.0 || ±
    """
    if not text:
        return None

    election_date = parse_election_date_from_text(text, title)
    is_by_election = 'by-election' in title.lower() or 'by_election' in title.lower()

    electorate, turnout, turnout_pct = parse_electorate_turnout(text)

    candidates = []

    table_match = re.search(r'(\{\|\s*class="wikitable".*?\|\})', text, re.DOTALL)
    if table_match:
        table = table_match.group(1)
        rows = re.split(r'\|-', table)

        for row in rows:
            row_stripped = row.strip()
            # Skip header rows, empty rows, table open/close
            if not row_stripped or row_stripped == '|}' or row_stripped.startswith('{|'):
                continue
            if "'''Candidate'''" in row or "'''Party'''" in row or "'''Votes'''" in row:
                continue
            if '!style' in row or re.match(r'\s*!', row_stripped):
                continue

            # Split by || to get cells
            cells = re.split(r'\|\|', row)

            # Clean each cell
            clean_cells = []
            for cell in cells:
                c = cell.strip()
                # Remove leading | and whitespace
                c = re.sub(r'^\s*\|', '', c).strip()
                clean_cells.append(c)

            # The typical format is:
            # [color_style] || [party] || [candidate] || [votes] || [%] || [swing]
            # But some elections have: [candidate] || [party] || [votes] || [%] || [swing]
            # Or simple: [candidate] || Unopposed

            party = None
            candidate_name = None
            wiki_link = None
            votes = None
            votes_text = None
            vote_pct = None
            elected = 'tick.gif' in row or 'tick.png' in row

            # Strategy: find the cell with [[wiki link]] — that's the candidate
            candidate_cell_idx = None
            for idx, c in enumerate(clean_cells):
                if re.search(r'\[\[', c) and not c.startswith('Image:'):
                    candidate_cell_idx = idx
                    break

            if candidate_cell_idx is None:
                # No wiki link — might be a plain text candidate
                # Look for cells that look like names (not numbers, not style, not party keywords)
                for idx, c in enumerate(clean_cells):
                    if not c or c.startswith('style=') or c.startswith('{|'):
                        continue
                    if re.match(r'^[\d,.%±<>span/\s\-]+$', c):
                        continue
                    # Check it's not a known party
                    c_plain = re.sub(r"'''", '', c).strip()
                    if c_plain and len(c_plain) > 2 and not any(mk in c_plain.lower() for mk in ['wikitable', 'background', 'colspan']):
                        # Could be name or party - if we haven't found candidate yet
                        if candidate_cell_idx is None:
                            candidate_cell_idx = idx

            if candidate_cell_idx is None:
                continue

            # Extract candidate name
            cand_cell = clean_cells[candidate_cell_idx]
            name_match = re.search(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', cand_cell)
            if name_match:
                wiki_link = name_match.group(1).strip()
                candidate_name = name_match.group(2).strip() if name_match.group(2) else wiki_link
            else:
                candidate_name = re.sub(r"'''", '', cand_cell).strip()

            if not candidate_name or len(candidate_name) < 2:
                continue

            # Skip if it looks like markup
            if any(mk in candidate_name.lower() for mk in ['wikitable', 'style=', 'background', 'colspan', '{|']):
                continue

            # Look at cells before candidate for party
            for idx in range(candidate_cell_idx):
                c = clean_cells[idx]
                if not c or c.startswith('style='):
                    continue
                c_clean = re.sub(r"'''", '', c).strip()
                if c_clean and len(c_clean) > 1:
                    party = c_clean

            # Look at cells after candidate for votes, %, swing
            remaining = clean_cells[candidate_cell_idx + 1:]
            for c in remaining:
                if not c:
                    continue
                c_clean = c.strip()
                # Check for Unopposed
                if 'Unopposed' in c_clean or 'unopposed' in c_clean:
                    votes_text = 'Unopposed'
                    continue
                # Check for number (votes)
                num_match = re.match(r'^([\d,]+)$', c_clean)
                if num_match and votes is None:
                    votes = int(num_match.group(1).replace(',', ''))
                    continue
                # Check for percentage
                pct_match = re.match(r'^([\d.]+)%?$', c_clean)
                if pct_match and vote_pct is None:
                    vote_pct = float(pct_match.group(1))
                    continue

            if candidate_name:
                candidates.append({
                    'name': candidate_name,
                    'wiki_link': wiki_link,
                    'party': party,
                    'votes': votes,
                    'votes_text': votes_text,
                    'vote_pct': vote_pct,
                    'elected': elected,
                    'role': None,
                })

    # If none marked elected and we have candidates, mark the first (winner)
    if candidates and not any(c['elected'] for c in candidates):
        candidates[0]['elected'] = True

    return {
        'date': election_date,
        'type': 'by-election' if is_by_election else 'general',
        'electorate': electorate,
        'turnout': turnout,
        'turnout_pct': turnout_pct,
        'candidates': candidates,
    }


def parse_person_page(text, title):
    """Parse a councillor/politician biography page."""
    if not text:
        return None

    born_date = None
    died_date = None
    birth_place = None
    image_ref = None

    # Strip wiki links for date/place parsing: [[Page|Display]] -> Display, [[Page]] -> Page
    text_clean = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', text)
    text_clean = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', text_clean)

    # Extract birth/death from text like "(b. 10 November 1775, Lerwick, d. 17 February 1841, Lerwick)"
    bd_match = re.search(r'\(b\.\s*(\d{1,2}\s+\w+\s+\d{4})(?:,\s*([^,)]+))?', text_clean)
    if bd_match:
        try:
            dt = datetime.strptime(bd_match.group(1), "%d %B %Y")
            born_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        if bd_match.group(2):
            bp = bd_match.group(2).strip()
            # Strip wiki link markup: [[Place|Display]] -> Display, [[Place]] -> Place
            bp = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', bp)
            bp = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', bp)
            birth_place = bp

    dd_match = re.search(r'd\.\s*(\d{1,2}\s+\w+\s+\d{4})', text_clean)
    if dd_match:
        try:
            dt = datetime.strptime(dd_match.group(1), "%d %B %Y")
            died_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Year-only fallbacks: "b. 1850" or "d. 1927"
    if not born_date:
        m = re.search(r'\(b\.\s*(\d{4})', text_clean)
        if m:
            born_date = m.group(1)

    if not died_date:
        m = re.search(r'd\.\s*(\d{4})', text_clean)
        if m and not re.search(r'd\.\s*\d{1,2}\s+\w+\s+\d{4}', text_clean):
            died_date = m.group(1)

    # Also try "born on the Xth of Month, Year" patterns
    if not born_date:
        m = re.search(r'born\s+(?:on\s+)?(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?(January|February|March|April|May|June|July|August|September|October|November|December),?\s+(\d{4})', text_clean)
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
                born_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    if not died_date:
        m = re.search(r'died\s+(?:on\s+)?(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?(January|February|March|April|May|June|July|August|September|October|November|December),?\s+(\d{4})', text_clean)
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
                died_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Extract image
    img_match = re.search(r'\[\[(?:Image|File):([^\]|]+)', text)
    if img_match:
        image_ref = img_match.group(1).strip()

    # Extract Bayanne ID
    bayanne_id = None
    bayanne_match = re.search(r'personID=(I\d+)', text)
    if bayanne_match:
        bayanne_id = bayanne_match.group(1)

    # Split into intro (before any ==section==) and biography (==Biography== section)
    clean = text
    # Remove image/file tags - these can contain nested [[links]] so we can't use non-greedy .*?
    # Instead, match from [[Image: or [[File: to the final ]] that closes the tag
    def strip_image_tags(t):
        while True:
            m = re.search(r'\[\[(?:Image|File):', t)
            if not m:
                break
            # Find the matching ]] by counting bracket depth
            start = m.start()
            depth = 0
            i = start
            while i < len(t) - 1:
                if t[i:i+2] == '[[':
                    depth += 1
                    i += 2
                elif t[i:i+2] == ']]':
                    depth -= 1
                    i += 2
                    if depth == 0:
                        break
                else:
                    i += 1
            t = t[:start] + t[i:]
        return t
    clean = strip_image_tags(clean)
    clean = re.sub(r'\{\{[^}]+\}\}', '', clean)
    clean = re.sub(r'\[\[Category:[^\]]+\]\]', '', clean)

    # Split on ==Biography== to separate intro from biography
    bio_split = re.split(r'==\s*Biography\s*==', clean, maxsplit=1)
    intro_raw = bio_split[0]
    bio_raw = bio_split[1] if len(bio_split) > 1 else None

    # Cut intro at the first == section header
    intro_raw = re.split(r'==\s*\w', intro_raw)[0]

    # Cut biography at the next == section header
    if bio_raw:
        bio_raw = re.split(r'==\s*\w', bio_raw)[0]

    def clean_wiki_markup(t):
        t = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', t)
        t = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', t)
        # Strip external links: [http://... display text] -> display text, [http://...] -> ''
        t = re.sub(r'\[https?://[^\s\]]+\s+([^\]]+)\]', r'\1', t)
        t = re.sub(r'\[https?://[^\]]+\]', '', t)
        t = re.sub(r"'{2,3}", '', t)
        t = re.sub(r'<[^>]+>', '', t)
        t = re.sub(r'\{[^}]*\}', '', t)
        t = re.sub(r'\\n', '\n', t)
        t = re.sub(r'\n{3,}', '\n\n', t)
        return t.strip()

    intro_text = clean_wiki_markup(intro_raw) or None
    bio_text = clean_wiki_markup(bio_raw) if bio_raw else None

    # Strip the "(b. ..., d. ...)" parenthetical from intro since we show it structured
    if intro_text:
        intro_text = re.sub(r'\s*\(b\.\s*[^)]+\)\s*', ' ', intro_text)
        intro_text = re.sub(r'\s{2,}', ' ', intro_text).strip()

    # Extract categories
    categories = re.findall(r'\[\[Category:\s*([^\]]+?)\s*\]\]', text)

    # Extract political career links
    career_links = []
    career_match = re.search(r'==\s*Political Career\s*==(.*?)(?===|$)', text, re.DOTALL)
    if career_match:
        for link_match in re.finditer(r'\[\[([^\]|]+?)(?:\|[^\]]+?)?\]\]', career_match.group(1)):
            career_links.append(link_match.group(1).strip())

    return {
        'name': title.replace('_', ' '),
        'born_date': born_date,
        'died_date': died_date,
        'birth_place': birth_place,
        'intro': intro_text,
        'biography': bio_text,
        'image_ref': image_ref,
        'bayanne_id': bayanne_id,
        'categories': categories,
        'career_links': career_links,
    }


def get_pages_in_categories(cursor, categories):
    """Get all page titles in given categories."""
    placeholders = ','.join(['%s'] * len(categories))
    cursor.execute(f"""
        SELECT DISTINCT p.page_title
        FROM mwfn_page p
        JOIN mwfn_categorylinks cl ON p.page_id = cl.cl_from
        WHERE cl.cl_to IN ({placeholders})
        AND p.page_namespace = 0
        ORDER BY p.page_title
    """, categories)
    results = []
    for row in cursor.fetchall():
        val = row[0]
        if isinstance(val, (bytes, bytearray)):
            val = val.decode('utf-8', errors='replace')
        results.append(val)
    return results


def main():
    print("Connecting to MySQL...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()

    print("Creating SQLite database...")
    import os
    if os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_cursor = sqlite_conn.cursor()

    with open(SCHEMA_PATH) as f:
        sqlite_cursor.executescript(f.read())

    # --- Step 1: Create councils ---
    print("\n=== Creating councils ===")
    councils = {
        'Lerwick Town Council': ('lerwick-town-council', 'local'),
        'Zetland County Council': ('zetland-county-council', 'county'),
        'Shetland Islands Council': ('shetland-islands-council', 'regional'),
        'Parliament of Great Britain': ('parliament-great-britain', 'westminster'),
        'Parliament of the United Kingdom': ('parliament-uk', 'westminster'),
    }
    council_ids = {}
    for name, (slug, level) in councils.items():
        sqlite_cursor.execute("INSERT INTO councils (name, slug, level) VALUES (?, ?, ?)", (name, slug, level))
        council_ids[name] = sqlite_cursor.lastrowid
        print(f"  Created council: {name} (id={council_ids[name]})")

    # --- Step 2: Extract elections from templates ---
    print("\n=== Extracting elections from templates ===")

    # Map template -> council
    template_council_map = {
        'ElectionResultsLTC': 'Lerwick Town Council',
        'ElectionResultsZCC': 'Zetland County Council',
        'ElectionResultsSIC': 'Shetland Islands Council',
        'ElectionResultsGB': 'Parliament of Great Britain',
        'ElectionResultsUK': 'Parliament of the United Kingdom',
    }

    election_links = {}  # council_name -> [(page_title, display)]
    for template_name, council_name in template_council_map.items():
        text = get_template_text(mysql_cursor, template_name)
        if text:
            links = extract_links_from_template(text)
            # Filter to actual election pages (not template navigation links)
            election_pages = [(t, d) for t, d in links
                              if (('Election' in t or 'By-Election' in t or 'by-election' in t or 'General' in t)
                                  and not t.startswith('Template:')
                                  and not t.startswith('template:'))]
            election_links[council_name] = election_pages
            print(f"  {template_name}: {len(election_pages)} elections")
        else:
            print(f"  WARNING: Template {template_name} not found")

    # Also get the combined ElectionResults template for Westminster elections
    # that include by-elections not in the specific GB/UK templates
    combined_text = get_template_text(mysql_cursor, 'ElectionResults')
    if combined_text:
        all_westminster_links = extract_links_from_template(combined_text)
        for title, display in all_westminster_links:
            # Skip template cross-references
            if title.startswith('Template:') or title.startswith('template:'):
                continue
            title_norm = title.replace('_', ' ')
            if 'British_General_Election' in title or 'British General Election' in title_norm:
                if 'Parliament of Great Britain' not in election_links:
                    election_links['Parliament of Great Britain'] = []
                if not any(t == title for t, d in election_links.get('Parliament of Great Britain', [])):
                    election_links['Parliament of Great Britain'].append((title, display))
            elif 'UK_General_Election' in title or 'UK General Election' in title_norm:
                if 'Parliament of the United Kingdom' not in election_links:
                    election_links['Parliament of the United Kingdom'] = []
                if not any(t == title for t, d in election_links.get('Parliament of the United Kingdom', [])):
                    election_links['Parliament of the United Kingdom'].append((title, display))
            elif 'by-election' in title.lower():
                # Westminster by-elections
                if 'Parliament of the United Kingdom' not in election_links:
                    election_links['Parliament of the United Kingdom'] = []
                if not any(t == title for t, d in election_links.get('Parliament of the United Kingdom', [])):
                    election_links['Parliament of the United Kingdom'].append((title, display))

    # --- Step 3: Create constituencies ---
    print("\n=== Creating constituencies ===")
    constituency_ids = {}  # (council_id, name) -> id

    # Get constituency pages
    constituency_pages = get_pages_in_categories(mysql_cursor, ['Constituencies'])
    for page_title in constituency_pages:
        text = get_wiki_page(mysql_cursor, page_title)
        name = page_title.replace('_', ' ')
        # Remove "(Constituency)" suffix
        clean_name = re.sub(r'\s*\(Constituency\)\s*$', '', name)

        # Determine which council this constituency belongs to
        # Check the page text for clues
        council_id = None
        if text:
            if 'County Council' in text:
                council_id = council_ids['Zetland County Council']
            elif 'Shetland Islands Council' in text or 'SIC' in text:
                council_id = council_ids['Shetland Islands Council']
            elif 'Town Council' in text:
                council_id = council_ids['Lerwick Town Council']

        if not council_id:
            # Default to County Council for constituency pages
            council_id = council_ids['Zetland County Council']

        slug = slugify(clean_name)
        sqlite_cursor.execute(
            "INSERT OR IGNORE INTO constituencies (council_id, name, slug, wiki_page_title) VALUES (?, ?, ?, ?)",
            (council_id, clean_name, slug, page_title)
        )
        cid = sqlite_cursor.lastrowid
        if cid:
            constituency_ids[(council_id, clean_name)] = cid
            # Also index by variations
            constituency_ids[(council_id, page_title.replace('_', ' '))] = cid

    print(f"  Created {len(constituency_ids)} constituencies")

    # --- Step 4: Import people ---
    print("\n=== Importing people ===")
    person_categories = [
        'Lerwick_Town_Councillors', 'Zetland_County_Councillors',
        'Shetland_Islands_Councillors',
        'Members_of_the_Parliament_of_the_United_Kingdom',
        'Members_of_the_Parliament_of_Great_Britain',
        'Burgh_and_Shire_Commissioners_to_the_Parliament_of_Scotland',
    ]
    person_pages = get_pages_in_categories(mysql_cursor, person_categories)

    person_ids = {}  # wiki_page_title -> person_id
    person_name_map = {}  # display_name -> person_id (for matching candidates)
    import json

    for page_title in person_pages:
        text = get_wiki_page(mysql_cursor, page_title)
        parsed = parse_person_page(text, page_title)
        if not parsed:
            continue

        name = parsed['name']
        slug = slugify(name)

        # Handle duplicate slugs
        base_slug = slug
        counter = 1
        while True:
            sqlite_cursor.execute("SELECT id FROM people WHERE slug = ?", (slug,))
            if not sqlite_cursor.fetchone():
                break
            counter += 1
            slug = f"{base_slug}-{counter}"

        sqlite_cursor.execute("""
            INSERT INTO people (name, slug, born_date, died_date, birth_place, intro, biography, image_ref, bayanne_id, wiki_page_title, categories)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, slug, parsed['born_date'], parsed['died_date'],
            parsed['birth_place'], parsed['intro'], parsed['biography'], parsed['image_ref'],
            parsed['bayanne_id'], page_title, json.dumps(parsed['categories'])
        ))
        pid = sqlite_cursor.lastrowid
        person_ids[page_title] = pid
        person_name_map[name] = pid
        # Also map without disambiguation
        clean = re.sub(r'\s*\([^)]+\)\s*$', '', name)
        if clean != name:
            person_name_map[clean] = pid

    print(f"  Imported {len(person_ids)} people")

    # --- Step 4b: Build redirect map ---
    print("\n=== Building redirect map ===")
    mysql_cursor.execute("""
        SELECT p.page_title, CAST(old_text AS CHAR CHARACTER SET utf8mb4)
        FROM mwfn_page p
        JOIN mwfn_revision r ON p.page_latest = r.rev_id
        JOIN mwfn_text t ON r.rev_text_id = t.old_id
        WHERE p.page_is_redirect = 1 AND p.page_namespace = 0
    """)
    redirect_count = 0
    for row in mysql_cursor.fetchall():
        redirect_from = row[0]
        if isinstance(redirect_from, (bytes, bytearray)):
            redirect_from = redirect_from.decode('utf-8', errors='replace')
        redirect_text = row[1]
        if isinstance(redirect_text, (bytes, bytearray)):
            redirect_text = redirect_text.decode('utf-8', errors='replace')

        # Extract redirect target: #REDIRECT [[Target Page]]
        redir_match = re.search(r'#REDIRECT\s*\[\[([^\]]+)\]\]', redirect_text, re.IGNORECASE)
        if redir_match:
            target = redir_match.group(1).strip().replace(' ', '_')
            redirect_from_norm = redirect_from.replace(' ', '_')

            # If target is a person we imported, map the redirect source to that person
            if target in person_ids:
                pid = person_ids[target]
                # Add redirect name to person_ids and person_name_map
                person_ids[redirect_from_norm] = pid
                person_name_map[redirect_from.replace('_', ' ')] = pid
                redirect_count += 1

    print(f"  Mapped {redirect_count} redirects to people")

    # --- Step 4c: Fix Bayanne IDs ---
    # Cross-referenced against live Bayanne site on 2026-03-26.
    # DB2 has wrong IDs for 30 people (DB1 was correct); DB2 was correct for 4.
    print("\n=== Fixing Bayanne IDs ===")
    bayanne_corrections = {
        'Adam_Jamieson': 'I102720',
        'Alexander_Manson': 'I168141',
        'Andrew_Clark': 'I71829',
        'Arthur_White': 'I134528',
        'Charles_Robertson': 'I11326',
        'Charles_Stout': 'I25918',
        'Christopher_Sandison': 'I11970',
        'George_Tait': 'I10054',
        'James_Goodlad': 'I45507',
        'James_Goudie': 'I68965',
        'James_Irvine_(i)': 'I154415',
        'James_Jamieson_(i)': 'I173602',
        'James_Pottinger_(iii)': 'I28933',      # DB2 correct
        'John_Anderson': 'I87228',
        'John_Irvine_(iii)': 'I92038',
        'John_Leisk': 'I10067',
        'John_Meiklejohn': 'I268290',
        'John_Robertson_(iii)': 'I120463',       # DB2 correct
        'John_Sinclair': 'I119064',
        'John_Stewart': 'I23844',
        'Laurence_Sandison': 'I93633',
        'Laurence_Smith': 'I225276',
        'Robert_Deans': 'I45555',
        'Robert_Ganson_(i)': 'I84530',
        'Robert_Johnson_(i)': 'I34247',
        'Robert_Ollason': 'I27257',
        'Robert_Stout_(i)': 'I86036',
        'Sinclair_Johnson': 'I44151',
        'Thomas_Gifford': 'I10610',              # DB2 correct (actually same value)
        'William_Adie': 'I9136',
        'William_Bruce_(ii)': 'I34310',
        'William_Greig': 'I26112',
        'William_Smith_(ii)': 'I11002',
        'William_Tait': 'I55663',                # DB2 correct
    }
    fix_count = 0
    for wiki_title, correct_id in bayanne_corrections.items():
        sqlite_cursor.execute(
            "UPDATE people SET bayanne_id = ? WHERE wiki_page_title = ? AND bayanne_id != ?",
            (correct_id, wiki_title, correct_id)
        )
        if sqlite_cursor.rowcount > 0:
            fix_count += 1
    print(f"  Fixed {fix_count} Bayanne IDs")

    # --- Step 5: Import elections and candidacies ---
    print("\n=== Importing elections ===")
    election_count = 0
    candidacy_count = 0
    skipped = []
    processed_pages = set()  # Track pages already processed to avoid duplicates

    for council_name, links in election_links.items():
        council_id = council_ids[council_name]
        is_westminster = council_name in ('Parliament of Great Britain', 'Parliament of the United Kingdom')

        for page_title, display in links:
            # Skip pages we've already processed (same page in multiple templates)
            page_key = page_title.replace(' ', '_')
            if page_key in processed_pages:
                continue
            processed_pages.add(page_key)

            text = get_wiki_page(mysql_cursor, page_title)
            if not text:
                skipped.append(page_title)
                continue

            if is_westminster:
                parsed = parse_uk_election_page(text, page_title)
                if not parsed:
                    skipped.append(page_title)
                    continue

                # For Westminster, determine constituency from title
                constituency_name = 'Orkney and Shetland'

                sqlite_cursor.execute("""
                    INSERT INTO elections (council_id, election_date, election_type, electorate, turnout, turnout_pct, wiki_page_title)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    council_id, parsed['date'], parsed['type'],
                    parsed.get('electorate'), parsed.get('turnout'), parsed.get('turnout_pct'),
                    page_title
                ))
                election_id = sqlite_cursor.lastrowid
                election_count += 1

                for pos, cand in enumerate(parsed['candidates'], 1):
                    # Try to resolve person
                    person_id = None
                    if cand.get('wiki_link'):
                        person_id = person_ids.get(cand['wiki_link'].replace(' ', '_'))
                    if not person_id:
                        person_id = person_name_map.get(cand['name'])

                    sqlite_cursor.execute("""
                        INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes, votes_text, elected, position, role)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        election_id, person_id, cand['name'], cand.get('party'),
                        cand.get('votes'), cand.get('votes_text'),
                        1 if cand['elected'] else 0, pos, cand.get('role'),
                    ))
                    candidacy_count += 1
            else:
                parsed = parse_election_page(text, page_title)
                if not parsed:
                    skipped.append(page_title)
                    continue

                for result in parsed['results']:
                    # Resolve constituency
                    constituency_id = None
                    if result.get('constituency_link'):
                        clink = result['constituency_link'].replace('_', ' ')
                        # Try exact match
                        constituency_id = constituency_ids.get((council_id, clink))
                        # Try without "(Constituency)"
                        if not constituency_id:
                            clean = re.sub(r'\s*\(Constituency\)\s*$', '', clink)
                            constituency_id = constituency_ids.get((council_id, clean))
                        # Try the display name
                        if not constituency_id and result.get('constituency_name'):
                            constituency_id = constituency_ids.get((council_id, result['constituency_name']))

                    # For by-elections, extract constituency from page title
                    if not constituency_id and parsed['type'] == 'by-election':
                        # "Aithsting County Council By-Election April 1890"
                        title_clean = page_title.replace('_', ' ')
                        for (cid_key, cname), cid_val in constituency_ids.items():
                            if cid_key == council_id and cname in title_clean:
                                constituency_id = cid_val
                                break

                    sqlite_cursor.execute("""
                        INSERT INTO elections (council_id, constituency_id, election_date, election_type,
                            electorate, turnout, turnout_pct, notes, wiki_page_title)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        council_id, constituency_id, parsed['date'], parsed['type'],
                        result.get('electorate'), result.get('turnout'), result.get('turnout_pct'),
                        result.get('notes'), page_title
                    ))
                    election_id = sqlite_cursor.lastrowid

                    if not result.get('candidates'):
                        # No candidates but might have notes
                        continue

                    election_count_for_page = 0
                    for pos, cand in enumerate(result['candidates'], 1):
                        person_id = None
                        if cand.get('wiki_link'):
                            person_id = person_ids.get(cand['wiki_link'].replace(' ', '_'))
                        if not person_id:
                            person_id = person_name_map.get(cand['name'])

                        sqlite_cursor.execute("""
                            INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes, votes_text, elected, position, role)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            election_id, person_id, cand['name'], cand.get('party'),
                            cand.get('votes'), cand.get('votes_text'),
                            1 if cand['elected'] else 0, pos, cand.get('role'),
                        ))
                        candidacy_count += 1

                election_count += 1

    print(f"  Imported {election_count} elections")
    print(f"  Imported {candidacy_count} candidacies")
    if skipped:
        print(f"  Skipped {len(skipped)} pages (no content found):")
        for s in skipped[:20]:
            print(f"    - {s}")
        if len(skipped) > 20:
            print(f"    ... and {len(skipped) - 20} more")

    # --- Step 6: Validate person-candidacy links ---
    print("\n=== Validating person-candidacy links ===")

    # Unlink candidacies where the person died before or was born after the election
    sqlite_cursor.execute("""
        SELECT c.id, p.name, p.born_date, p.died_date, e.election_date, c.candidate_name, e.wiki_page_title
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN people p ON c.person_id = p.id
        WHERE p.died_date IS NOT NULL
          AND e.election_date IS NOT NULL
          AND CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) > CAST(SUBSTR(p.died_date, 1, 4) AS INTEGER)
    """)
    dead_at_election = sqlite_cursor.fetchall()
    for row in dead_at_election:
        cid, pname, born, died, edate, cname, wiki_page = row
        print(f"  UNLINK (dead): {pname} (d.{died}) linked to {edate} election '{wiki_page}' as '{cname}'")
        sqlite_cursor.execute("UPDATE candidacies SET person_id = NULL WHERE id = ?", (cid,))

    sqlite_cursor.execute("""
        SELECT c.id, p.name, p.born_date, p.died_date, e.election_date, c.candidate_name, e.wiki_page_title
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN people p ON c.person_id = p.id
        WHERE p.born_date IS NOT NULL
          AND e.election_date IS NOT NULL
          AND CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) < CAST(SUBSTR(p.born_date, 1, 4) AS INTEGER)
    """)
    born_after_election = sqlite_cursor.fetchall()
    for row in born_after_election:
        cid, pname, born, died, edate, cname, wiki_page = row
        print(f"  UNLINK (not born): {pname} (b.{born}) linked to {edate} election '{wiki_page}' as '{cname}'")
        sqlite_cursor.execute("UPDATE candidacies SET person_id = NULL WHERE id = ?", (cid,))

    # Also unlink candidacies where the person was under 18 at the election
    sqlite_cursor.execute("""
        SELECT c.id, p.name, p.born_date, e.election_date, c.candidate_name, e.wiki_page_title,
               CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) - CAST(SUBSTR(p.born_date, 1, 4) AS INTEGER) as age
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        JOIN people p ON c.person_id = p.id
        WHERE p.born_date IS NOT NULL
          AND e.election_date IS NOT NULL
          AND CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) - CAST(SUBSTR(p.born_date, 1, 4) AS INTEGER) < 18
    """)
    too_young = sqlite_cursor.fetchall()
    for row in too_young:
        cid, pname, born, edate, cname, wiki_page, age = row
        print(f"  UNLINK (age {age}): {pname} (b.{born}) linked to {edate} election '{wiki_page}' as '{cname}'")
        sqlite_cursor.execute("UPDATE candidacies SET person_id = NULL WHERE id = ?", (cid,))

    total_unlinked = len(dead_at_election) + len(born_after_election) + len(too_young)
    print(f"  Unlinked {total_unlinked} impossible candidacy links")

    # --- Step 6b: Hide erroneous elections ---
    print("\n=== Hiding erroneous elections ===")
    hidden_elections = [
        'Lerwick_Town_Council_By-Election_May_1844',  # Not actually a by-election
    ]
    hidden_count = 0
    for wiki_title in hidden_elections:
        # Try with both spaces and underscores
        for variant in [wiki_title, wiki_title.replace('_', ' ')]:
            sqlite_cursor.execute(
                "UPDATE elections SET hidden = 1 WHERE wiki_page_title = ?", (variant,)
            )
            hidden_count += sqlite_cursor.rowcount
    print(f"  Marked {hidden_count} elections as hidden")

    # --- Step 7: Stats ---
    print("\n=== Final stats ===")
    for table in ['councils', 'constituencies', 'people', 'elections', 'candidacies']:
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = sqlite_cursor.fetchone()[0]
        print(f"  {table}: {count} rows")

    # Check person linkage
    sqlite_cursor.execute("SELECT COUNT(*) FROM candidacies WHERE person_id IS NOT NULL")
    linked = sqlite_cursor.fetchone()[0]
    sqlite_cursor.execute("SELECT COUNT(*) FROM candidacies WHERE person_id IS NULL")
    unlinked = sqlite_cursor.fetchone()[0]
    print(f"\n  Candidacies linked to people: {linked}")
    print(f"  Candidacies not linked: {unlinked}")

    # Sample verification
    print("\n=== Sample verification: Lerwick 1958 ===")
    sqlite_cursor.execute("""
        SELECT c.candidate_name, c.party, c.votes, c.elected, p.name as person_name
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        LEFT JOIN people p ON c.person_id = p.id
        WHERE e.wiki_page_title = 'Lerwick_Town_Council_Election_May_1958'
        ORDER BY c.position
    """)
    for row in sqlite_cursor.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]} votes | {'ELECTED' if row[3] else 'not elected'} | person: {row[4]}")

    print("\n=== Sample verification: Arthur Edmondston ===")
    sqlite_cursor.execute("""
        SELECT p.name, p.born_date, p.died_date, e.wiki_page_title, c.votes_text, c.elected
        FROM people p
        JOIN candidacies c ON c.person_id = p.id
        JOIN elections e ON c.election_id = e.id
        WHERE p.wiki_page_title = 'Arthur_Edmondston'
        ORDER BY e.election_date
    """)
    for row in sqlite_cursor.fetchall():
        print(f"  {row[0]} (b.{row[1]}, d.{row[2]}) - {row[3]} | {row[4]} | {'ELECTED' if row[5] else ''}")

    sqlite_conn.commit()
    sqlite_conn.close()
    mysql_conn.close()
    print("\nDone! Database saved to:", SQLITE_PATH)


if __name__ == '__main__':
    main()
