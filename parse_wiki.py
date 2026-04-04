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
    """Extract electorate, electorate detail, and turnout from election page text."""
    electorate = None
    electorate_detail = None
    turnout = None
    turnout_pct = None

    m = re.search(r'Electorate:\s*([\d,]+)(?:\s*\(([^)]+)\))?', text)
    if m:
        electorate = int(m.group(1).replace(',', ''))
        if m.group(2):
            electorate_detail = m.group(2).strip()

    m = re.search(r'Turnout:\s*([\d,]+)\s*\((\d+\.?\d*)%\)', text)
    if m:
        turnout = int(m.group(1).replace(',', ''))
        turnout_pct = float(m.group(2))
    elif re.search(r'Turnout:\s*([\d,]+)', text):
        m2 = re.search(r'Turnout:\s*([\d,]+)', text)
        turnout = int(m2.group(1).replace(',', ''))

    return electorate, electorate_detail, turnout, turnout_pct


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

    # Extract who was replaced (for by-elections)
    # Many patterns in the wiki text:
    #   "Following the resignation of [[Person]]"
    #   "Following the death of councillor [[Person]]"
    #   "because [[Person]] had resigned"
    #   "because [[Person]] resigned"
    #   "after Reverend [[Person]] resigned"
    #   "Previous councillor Mr [[Person]] announced..."
    #   "Councillor [[Person]] lost his seat due to..."
    #   "[[Person]]'s resignation"
    #   "seat received no nomination at the previous election" (no specific person)
    replaced_person = None
    replaced_wiki_link = None
    if is_by_election:
        # Strip wiki link markup for cleaner matching, but keep originals for link extraction
        text_for_match = text

        # Pattern 1: "Following the resignation/death of [title] [[Person]]"
        rp_match = re.search(
            r'[Ff]ollowing the (?:resignation|death|removal|departure) of (?:councillor |Councillor |Provost |Captain |Mr\.? |Mrs\.? |Dr\.? |Rev(?:erend)?\.? )?'
            r'(?:both )?\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]',
            text_for_match
        )
        if rp_match:
            replaced_wiki_link = rp_match.group(1).strip()
            replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 2: "because [[Person]] had resigned / resigned"
        if not replaced_person:
            rp_match = re.search(
                r'because \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] (?:had )?resigned',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 3: "after [title] [[Person]] resigned"
        if not replaced_person:
            rp_match = re.search(
                r'after (?:Reverend |Rev\.? |Mr\.? |Councillor )?\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] resigned',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 4: "Previous councillor Mr [[Person]] announced"
        if not replaced_person:
            rp_match = re.search(
                r'[Pp]revious councillor (?:Mr\.? )?\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 5: "Councillor [[Person]] lost his seat"
        if not replaced_person:
            rp_match = re.search(
                r'[Cc]ouncillor \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] lost',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 6: "[[Person]]'s resignation" or "[[Person]] had resigned"
        if not replaced_person:
            rp_match = re.search(
                r"\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]'s (?:resignation|death)",
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 7: "Following the death of councillor Robert..." (no wiki link)
        # First strip titles to avoid "Rev." period cutting off the name
        if not replaced_person:
            text_no_titles = re.sub(r'\b(?:Rev\.?|Mr\.?|Mrs\.?|Dr\.?|Captain|Councillor|Provost|Reverend)\s+', '', text_for_match)
            rp_match = re.search(
                r'[Ff]ollowing the (?:resignation|death|removal) of (?:councillor |Councillor |Provost |Captain |Mr\.? |Rev\.? )?'
                r'([A-Z][a-zA-Z. ]+?)(?:,|\.|\\n| was| owing)',
                text_no_titles
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()
            # Also try on original text if that failed
            if not replaced_person:
                rp_match = re.search(
                    r'[Ff]ollowing the (?:resignation|death|removal) of (?:councillor |Councillor |Provost |Captain |Mr\.? |Rev\.? )?'
                    r'([A-Z][a-zA-Z. ]+?)(?:,|\.|\\n| was| owing)',
                    text_for_match
                )
                if rp_match:
                    replaced_person = rp_match.group(1).strip()

        # Pattern 8: "because Person had resigned" (no wiki link)
        if not replaced_person:
            rp_match = re.search(
                r'because ([A-Z][a-zA-Z. ]+?) (?:had )?resigned',
                text_for_match
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()

        # Pattern 9: "after Person resigned" (no wiki link)
        if not replaced_person:
            rp_match = re.search(
                r'after (?:Reverend |Rev\.? |Mr\.? )?([A-Z][a-zA-Z. ]+?) resigned',
                text_for_match
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()

        # Pattern 10: "Since [[Person]] was elected to the Town/County Council"
        if not replaced_person:
            rp_match = re.search(
                r'[Ss]ince \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] was elected to',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 11: "As [[Person]] was elected to" / "as [[Person]] was elected to"
        if not replaced_person:
            rp_match = re.search(
                r'[Aa]s \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] was elected to',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 12: "election of [[Person]] to the Town Council"
        if not replaced_person:
            rp_match = re.search(
                r'election of \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] to the',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 13: "after Name retired" / "Name retired"
        if not replaced_person:
            rp_match = re.search(
                r'after (?:\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]|([A-Z][a-zA-Z. ]+?)) retired',
                text_for_match
            )
            if rp_match:
                if rp_match.group(1):
                    replaced_wiki_link = rp_match.group(1).strip()
                    replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link
                elif rp_match.group(3):
                    replaced_person = rp_match.group(3).strip()

        # Pattern 14: "Person felt that he should retire" / "Person resigned and"
        if not replaced_person:
            rp_match = re.search(
                r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\] (?:felt|resigned|chose)',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 15: "caused by the death of [[Person]]" / "result of the death of"
        if not replaced_person:
            rp_match = re.search(
                r'(?:caused by|result of) the (?:death|resignation) of \[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 16: "caused by the death of Name" (no wiki link)
        if not replaced_person:
            rp_match = re.search(
                r'(?:caused by|result of) the (?:death|resignation) of ([A-Z][a-zA-Z. ]+?)(?:\.|,|\\n)',
                text_for_match
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()

        # Pattern 17: "vacancy was caused by the death of Name"
        if not replaced_person:
            rp_match = re.search(
                r'vacancy was caused by the death of ([A-Z][a-zA-Z. ]+)',
                text_for_match
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()

        # Pattern 18: "whose resignation" with nearby [[Person]]
        if not replaced_person:
            rp_match = re.search(
                r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\],?\s*whose (?:resignation|death)',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 19: "Person resigned" plain (Robert H. W. Bruce resigned)
        if not replaced_person:
            rp_match = re.search(
                r'([A-Z][a-zA-Z. ]+?(?:[A-Z][a-zA-Z]+)) resigned',
                text_for_match
            )
            if rp_match:
                name = rp_match.group(1).strip()
                # Filter out false positives
                if name not in ('The', 'A', 'This', 'He', 'She') and len(name.split()) >= 2:
                    replaced_person = name

        # Pattern 20: "resignation of sitting member [[Person]]"
        if not replaced_person:
            rp_match = re.search(
                r'resignation of (?:sitting member |councillor )?\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]',
                text_for_match
            )
            if rp_match:
                replaced_wiki_link = rp_match.group(1).strip()
                replaced_person = rp_match.group(2).strip() if rp_match.group(2) else replaced_wiki_link

        # Pattern 21: "Sandy Cluness resigned following"
        if not replaced_person:
            rp_match = re.search(
                r'([A-Z][a-zA-Z]+ [A-Z][a-zA-Z]+) resigned following',
                text_for_match
            )
            if rp_match:
                replaced_person = rp_match.group(1).strip()

        # Pattern 22: "no nomination at the previous election" — unfilled seat, not a replacement
        if not replaced_person:
            if re.search(r'no nomination|received no nomination', text_for_match, re.IGNORECASE):
                replaced_person = '[unfilled seat]'

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
            electorate, electorate_detail, turnout, turnout_pct = parse_electorate_turnout(section_text)

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
                'electorate_detail': electorate_detail,
                'turnout': turnout,
                'turnout_pct': turnout_pct,
                'candidates': candidates,
                'notes': notes,
            })
            i += 3
    else:
        # Check for plain-text sub-sections (e.g. 1874 LTC with two competing council groups)
        plain_sections = re.split(r'===\s*([^=\[\]]+?)\s*===', text)

        if len(plain_sections) > 1:
            # plain_sections: [pre-content, heading1, content1, heading2, content2, ...]
            # Extract descriptive intro text before ==Results== for notes
            intro_text = plain_sections[0]
            intro_notes = None
            # Look for paragraph text before ==Results== (after templates/TOC)
            intro_match = re.search(r'\n\n((?:The |Two |In ).+?)(?:\n==|\n\n\{)', intro_text, re.DOTALL)
            if not intro_match:
                intro_match = re.search(r"\n(The '''.*?)\n==", intro_text, re.DOTALL)
            if intro_match:
                note_text = intro_match.group(1).strip()
                # Clean wiki markup from notes
                note_text = re.sub(r"\[\[(\d{4})\]\]", r"\1", note_text)
                note_text = re.sub(r"\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]", lambda m: m.group(2) or m.group(1), note_text)
                note_text = re.sub(r"'''(.+?)'''", r"\1", note_text)
                if note_text:
                    intro_notes = note_text

            i = 1
            while i < len(plain_sections):
                heading = plain_sections[i].strip()
                section_text = plain_sections[i+1] if i+1 < len(plain_sections) else ''

                electorate, electorate_detail, turnout, turnout_pct = parse_electorate_turnout(section_text)

                table_match = re.search(r'(\{\|\s*class="wikitable".*?\|\})', section_text, re.DOTALL)
                candidates = []
                if table_match:
                    has_party_section = detect_party_column(section_text)
                    candidates = parse_candidates_from_table(table_match.group(1), has_party_section)

                # Build note from heading + intro context
                section_note = heading
                if intro_notes:
                    section_note = f"{heading}. {intro_notes}"

                results.append({
                    'constituency_link': None,
                    'constituency_name': None,
                    'electorate': electorate,
                    'electorate_detail': electorate_detail,
                    'turnout': turnout,
                    'turnout_pct': turnout_pct,
                    'candidates': candidates,
                    'notes': section_note,
                })
                i += 2
        else:
            # Single-constituency election (LTC, by-elections, UK elections)
            electorate, electorate_detail, turnout, turnout_pct = parse_electorate_turnout(text)

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
                'electorate_detail': electorate_detail,
                'turnout': turnout,
                'turnout_pct': turnout_pct,
                'candidates': candidates,
                'notes': None,
            })

    return {
        'date': election_date,
        'type': 'by-election' if is_by_election else 'general',
        'replaced_person': replaced_person,
        'replaced_wiki_link': replaced_wiki_link,
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

    electorate, electorate_detail, turnout, turnout_pct = parse_electorate_turnout(text)

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
                'electorate_detail': electorate_detail,
        'turnout': turnout,
        'turnout_pct': turnout_pct,
        'candidates': candidates,
    }


def strip_election_result_sentences(text):
    """Remove sentences describing election results — now shown in structured data."""
    if not text:
        return text
    # "He/She/Name (also) unsuccessfully contested the X election(s)." (with or without trailing period)
    text = re.sub(r'\w+ (?:also )?unsuccessfully contested [^.]+\.?\s*', '', text)
    # "He/She (also) stood for X but was not elected."
    text = re.sub(r'(?:He|She) (?:also )?stood for [^.]+but was not elected\.\s*', '', text)
    # "He tried again in YYYY for X but failed again."
    text = re.sub(r'(?:He|She) tried again [^.]+but failed again\.\s*', '', text)
    # ", where he/she lost his/her bid for re-election" (clause within a sentence)
    text = re.sub(r',\s*where (?:he|she) lost (?:his|her) bid for re-election', '', text)
    # ", but had also stood for X where he/she was elected" (redundant clause)
    text = re.sub(r',\s*but had also stood for [^.]+where (?:he|she) was elected', '', text)
    # "In YYYY, he also stood for the X seat but was not elected."
    text = re.sub(r'In \d{4}, (?:he|she) also stood for [^.]+but was not elected\.\s*', '', text)
    # Clean up double spaces and trailing whitespace
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text or None


def parse_person_page(text, title):
    """Parse a councillor/politician biography page."""
    if not text:
        return None

    born_date = None
    died_date = None
    birth_place = None
    death_place = None
    image_ref = None

    # Strip wiki links for date/place parsing: [[Page|Display]] -> Display, [[Page]] -> Page
    text_clean = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', text)
    text_clean = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', text_clean)

    # Extract birth/death from text like "(b. 10 November 1775, Lerwick, d. 17 February 1841, Lerwick)"
    # Full pattern with both places
    full_match = re.search(
        r'\(b\.\s*(\d{1,2}\s+\w+\s+\d{4})(?:,\s*([^,)]+))?,\s*d\.\s*(\d{1,2}\s+\w+\s+\d{4})(?:,\s*([^)]+))?\)',
        text_clean
    )
    if full_match:
        try:
            dt = datetime.strptime(full_match.group(1), "%d %B %Y")
            born_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        if full_match.group(2):
            birth_place = full_match.group(2).strip()
        try:
            dt = datetime.strptime(full_match.group(3), "%d %B %Y")
            died_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        if full_match.group(4):
            death_place = full_match.group(4).strip()
    else:
        # Try birth only
        bd_match = re.search(r'\(b\.\s*(\d{1,2}\s+\w+\s+\d{4})(?:,\s*([^,)]+))?', text_clean)
        if bd_match:
            try:
                dt = datetime.strptime(bd_match.group(1), "%d %B %Y")
                born_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
            if bd_match.group(2):
                birth_place = bd_match.group(2).strip()

        # Try death only
        dd_match = re.search(r'd\.\s*(\d{1,2}\s+\w+\s+\d{4})(?:,\s*([^)]+))?\)', text_clean)
        if dd_match:
            try:
                dt = datetime.strptime(dd_match.group(1), "%d %B %Y")
                died_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
            if dd_match.group(2):
                death_place = dd_match.group(2).strip()
        elif not died_date:
            dd_match2 = re.search(r'd\.\s*(\d{1,2}\s+\w+\s+\d{4})', text_clean)
            if dd_match2:
                try:
                    dt = datetime.strptime(dd_match2.group(1), "%d %B %Y")
                    died_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

    # Handle year-only birth with full or year-only death, plus places:
    # "(b. 1776 Anness, Cunningsburgh, d. 20 April 1847, Lerwick)"
    # "(b. 1850, North Roe, d. 12 January 1927, Lerwick)"
    # "(b. 1850, d. 1927)"
    if not born_date or not birth_place or not death_place:
        # Match the full parenthetical for year-only births
        paren_match = re.search(r'\(b\.\s*(\d{4})\b(.*?)d\.\s*(.*?)\)', text_clean)
        if paren_match:
            byear = paren_match.group(1)
            between = paren_match.group(2).strip().rstrip(',').strip()
            after_d = paren_match.group(3).strip()

            if not born_date:
                born_date = byear

            # Between birth year and "d." is the birth place
            if not birth_place and between:
                # Clean up: might have leading/trailing commas
                bp = between.strip().strip(',').strip()
                if bp and not re.match(r'^\d', bp):
                    birth_place = bp

            # After "d." is: [date, ]place
            if after_d:
                # Try to extract date
                d_date_match = re.match(r'(\d{1,2}\s+\w+\s+)?(\d{4})[,\s]*(.*)', after_d)
                if d_date_match:
                    if not died_date:
                        if d_date_match.group(1):
                            try:
                                dt = datetime.strptime(d_date_match.group(1).strip() + ' ' + d_date_match.group(2), "%d %B %Y")
                                died_date = dt.strftime("%Y-%m-%d")
                            except ValueError:
                                died_date = d_date_match.group(2)
                        else:
                            died_date = d_date_match.group(2)
                    if not death_place and d_date_match.group(3):
                        dp = d_date_match.group(3).strip().rstrip(',').strip()
                        if dp:
                            death_place = dp

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

    # Extract image — only from the main page content, NOT from {{ }} templates
    # First, strip all templates to find images that are in the page body
    text_no_templates = re.sub(r'\{\{[^}]+\}\}', '', text)
    img_match = re.search(r'\[\[(?:Image|File):([^\]|]+)', text_no_templates)
    if img_match:
        image_ref = img_match.group(1).strip()

    # Extract Bayanne ID — prefer the one in ==External Links== section (the page subject's ID)
    # rather than the first on the page (which may be from succession templates or body references)
    bayanne_id = None
    ext_links_match = re.search(r'==\s*External\s+Links?\s*==(.*?)(?===|$)', text, re.DOTALL)
    if ext_links_match:
        bayanne_match = re.search(r'personID=(I\d+)', ext_links_match.group(1))
        if bayanne_match:
            bayanne_id = bayanne_match.group(1)
    if not bayanne_id:
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
    # Strip orphaned image caption lines (leftover from image tags not prefixed with [[File:)
    # These start with image parameters like "thumb|", "right|", "left|", "center|", or dimension specs
    clean = re.sub(r'^(?:thumb|right|left|center)\|.*$', '', clean, flags=re.MULTILINE)
    clean = re.sub(r'^\d+px\|.*$', '', clean, flags=re.MULTILINE)

    # Split on biography-type section headings to separate intro from biography
    # Matches: Biography, Profile, Background, Early life, Life, Personal Life, Abridged Biography, etc.
    bio_headings = r'==\s*(?:Biography|Profile|Background(?:\s+and\s+\w+)?|Early [Ll]ife(?:\s+and\s+\w+)?|Life|Personal Life|Abridged Biography|Naval [Cc]areer)\s*=*'
    bio_split = re.split(bio_headings, clean, maxsplit=1)
    intro_raw = bio_split[0]
    bio_raw = bio_split[1] if len(bio_split) > 1 else None

    # Cut intro at the first == section header
    intro_raw = re.split(r'==\s*\w', intro_raw)[0]

    # Cut biography at the next top-level == section header (not === sub-sections)
    # Match == followed by a word char, but NOT preceded by = (to exclude ===)
    if bio_raw:
        bio_raw = re.split(r'(?<!=)==\s*\w', bio_raw)[0]

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
        # Fix "Name , description" → "Name, description" (stray spaces before commas in wiki source)
        t = re.sub(r'\s+,', ',', t)
        return t.strip()

    intro_text = clean_wiki_markup(intro_raw) or None
    bio_text = clean_wiki_markup(bio_raw) if bio_raw else None

    # Strip orphaned wiki image parameters (e.g. "right|250px|thumb|Caption text")
    # left over from image tags where parameters leaked into the intro text
    if intro_text:
        intro_text = re.sub(r'^(?:(?:right|left|center|thumb|thumbnail|frame|frameless|\d+px)\|)+', '', intro_text)

    # Strip disambiguation notices and magic words from intro
    if intro_text:
        intro_text = re.sub(r'__\w+__\s*', '', intro_text)
        intro_text = re.sub(r"For other people (?:named )?with the same name,?\s*(?:please\s+)?see\s+[^.]+\.\s*", '', intro_text)
        intro_text = re.sub(r"Not to be confused with [^.]+\.\s*", '', intro_text)
        # Strip the "(b. ..., d. ...)" parenthetical from intro since we show it structured
        intro_text = re.sub(r'\s*\(b\.\s*[^)]+\)\s*', ' ', intro_text)
        intro_text = re.sub(r'\s{2,}', ' ', intro_text).strip()

    # Strip redundant election result sentences (shown in structured data)
    intro_text = strip_election_result_sentences(intro_text)
    bio_text = strip_election_result_sentences(bio_text)

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
        'death_place': death_place,
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
            INSERT INTO people (name, slug, born_date, died_date, birth_place, death_place, intro, biography, image_ref, headshot_ref, bayanne_id, wiki_page_title, categories)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        """, (
            name, slug, parsed['born_date'], parsed['died_date'],
            parsed['birth_place'], parsed['death_place'], parsed['intro'], parsed['biography'], parsed['image_ref'],
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

    # --- Step 4a2: Extract headshots from succession templates ---
    # Headshots appear on OTHER people's pages in {{template}} blocks,
    # e.g. {{CountyCouncillors|Preceded by<br>'''[[Person]]'''<br>[[Image:headshot.png|60px]]|...}}
    print("\n=== Extracting headshots from templates ===")
    mysql_cursor.execute("""
        SELECT p.page_title, CAST(old_text AS CHAR CHARACTER SET utf8mb4)
        FROM mwfn_text t
        JOIN mwfn_revision r ON r.rev_text_id = t.old_id
        JOIN mwfn_page p ON p.page_latest = r.rev_id
        WHERE p.page_namespace = 0
    """)
    headshot_map = {}  # person_page_title -> headshot_filename
    for row in mysql_cursor.fetchall():
        text = row[1]
        if isinstance(text, (bytes, bytearray)):
            text = text.decode('utf-8', errors='replace')
        # Find all templates
        for tmatch in re.finditer(r'\{\{[^}]+\}\}', text, re.DOTALL):
            template = tmatch.group()
            # Protect wiki links from being split by | — replace [[ ]] content temporarily
            protected = re.sub(r'\[\[([^\]]+)\]\]', lambda m: '[[' + m.group(1).replace('|', '\x00') + ']]', template)
            cells = protected.split('|')
            # Restore pipe chars
            cells = [c.replace('\x00', '|') for c in cells]
            for cell in cells:
                plinks = re.findall(r'\[\[([^\]|]+?)(?:\|[^\]]+?)?\]\]', cell)
                images = re.findall(r'\[\[(?:Image|File):([^\]|]+)', cell)
                if not images:
                    continue
                for plink in plinks:
                    plink_norm = plink.strip().replace(' ', '_')
                    if any(plink_norm.startswith(p) for p in ('Image:', 'File:', 'Category:')):
                        continue
                    if '(Constituency)' in plink_norm:
                        continue
                    for img in images:
                        img_clean = img.strip()
                        if 'headshot' in img_clean.lower():
                            if plink_norm not in headshot_map:
                                headshot_map[plink_norm] = img_clean

    headshot_count = 0
    for wiki_title, headshot_file in headshot_map.items():
        sqlite_cursor.execute(
            "UPDATE people SET headshot_ref = ? WHERE wiki_page_title = ?",
            (headshot_file, wiki_title)
        )
        if sqlite_cursor.rowcount > 0:
            headshot_count += 1

    print(f"  Mapped {headshot_count} headshots to people")

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

    # --- Step 4d: Manual person data corrections ---
    # Bayanne cross-reference verified 2026-03-27 for all 457 people with Bayanne IDs.
    print("\n=== Applying manual person data corrections ===")
    person_corrections = [
        # (name, {field: value, ...})
        ('David Harbison', {'born_date': '1933-08-26', 'died_date': '2017-10-25', 'birth_place': 'Greenock'}),
        # Bayanne ID extraction now prefers ==External Links== section, fixing these 7 cases
        # Date corrections from Bayanne cross-reference
        ('Adam Jamieson', {'born_date': '1861-10-04'}),
        ('Adam Thomson', {'born_date': '1911-10-09'}),
        ('Archibald Garriock', {'died_date': '1899-02-28'}),
        ('Arthur Anderson', {'born_date': '1792-02-19'}),
        ('Basil Neven-Spence', {'born_date': '1888-05-12'}),
        ('Catherine Anderson', {'born_date': '1917-02-17'}),
        ('Charles Duncan', {'died_date': '1884-11-10'}),
        ('Charles Merrylees', {'born_date': '1843-10-23'}),
        ('Charles Stout', {'born_date': '1845-11-26'}),
        ('Charlotte Nicol', {'born_date': '1864', 'birth_place': None, 'died_date': '1954-11-25'}),
        ('David Gray (i)', {'born_date': '1842', 'died_date': '1901-05-29'}),
        ('David Murray', {'died_date': '1961-03-21'}),
        ('Edwyn Tait', {'born_date': '1884-12-14'}),
        ('George Laurence', {'died_date': '1885-09-02'}),
        ('George Smith (i)', {'born_date': '1822-12-01'}),
        ('George Tait', {'born_date': '1829', 'died_date': '1889-07-08'}),
        ('James Anderson (iv)', {'born_date': '1910-01-26'}),
        ('James Brownlie', {'died_date': '1968-11-21'}),
        ('James Hunter (iii)', {'born_date': '1872-02-06'}),
        ('James Shearer', {'died_date': '1949-03-28'}),
        ('James Smith (i)', {'born_date': '1877-07-07'}),
        ('John Nicolson', {'born_date': '1937-09-26'}),
        ('John Ogilvy', {'death_place': 'London'}),
        ('John Robertson (iii)', {'born_date': '1841-05-25'}),
        ('John Stewart', {'died_date': '1956-12-12'}),
        ('Joseph Peterson (i)', {'died_date': '1953-04-24'}),
        ('Robert Hicks', {'born_date': '1809'}),
        ('Robert Sinclair', {'born_date': '1814', 'died_date': '1891-07-16'}),
        ('Robert Strachan', {'born_date': '1925-09-12'}),
        ('Samuel Fordyce', {'born_date': '1852'}),
        ('Theodore Andrew', {'died_date': '1960-09-05'}),
        ('Thomas Irvine (ii)', {'died_date': '1946-06-06'}),
        ('Thomas Sinclair', {'born_date': '1899-07-12'}),
        ('William Adie', {'born_date': '1839-04-02'}),
        ('William Carson', {'born_date': '1890-12-14'}),
        ('William Hamilton', {'born_date': '1905-12-01'}),
        ('William Henry', {'born_date': '1878-05-25'}),
        ('William Jamieson', {'died_date': '1937-01-21'}),
    ]

    # Fill missing data from Bayanne (only set if currently NULL)
    person_fills = [
        ('Alexander Manson', {'born_date': '1883-07-18', 'died_date': '1938-07-22', 'birth_place': 'Clumly, Dunrossness', 'death_place': 'Lerwick'}),
        ('Amanda Youngman', {'born_date': '1914-01-31', 'birth_place': 'Greenock'}),
        ('Andrew Dick', {'born_date': '1637'}),
        ('Arthur Hay', {'died_date': '1896-12-25', 'death_place': 'Lerwick'}),
        ('Arthur Nicolson (i)', {'died_date': '1917-05-27', 'death_place': 'Fetlar'}),
        ('Cecil Eunson (i)', {'born_date': '1928-12-30', 'died_date': '2007-12-25', 'birth_place': 'Lerwick', 'death_place': 'Aberdeen'}),
        ('Edward Knight', {'died_date': '2022-10-15', 'death_place': 'Lerwick'}),
        ('Edwin Hyde', {'born_date': '1873'}),
        ('Erling Clausen', {'died_date': '1984-06-28'}),
        ('Florence Grains', {'died_date': '2025-03-05', 'death_place': 'Walls'}),
        ('George Jamieson', {'born_date': '1870-12-01'}),
        ('George Johnston', {'born_date': '1869-03-21', 'died_date': '1909-06-08'}),
        ('Gordon Walterson', {'died_date': '2019-08-27'}),
        ('Hugh Robertson', {'died_date': '1932-01-15'}),
        ('James Henry', {'died_date': '2018-07-31'}),
        ('James Irvine (ii)', {'died_date': '2021-09-04'}),
        ('James Ogilvy', {'death_place': 'New Orleans'}),
        ('James Pottinger (i)', {'born_date': '1790'}),
        ('James Scott', {'died_date': '1859-12-20'}),
        ('John Inkster (iii)', {'died_date': '2021-12-12'}),
        ('John Nicolson', {'died_date': '2019-10-09'}),
        ('John Rae', {'born_date': '1904-12-22', 'died_date': '1985-11-14'}),
        ('John Smith (ii)', {'died_date': '1978'}),
        ('Laurence Smith', {'died_date': '1964-05-27'}),
        ('Leslie Angus', {'died_date': '2019-10-01'}),
        ('Loretta Hutchison', {'died_date': '2024-02-17'}),
        ('Mary Colligan', {'died_date': '2025-07-13'}),
        ('Norman Cameron', {'died_date': '1967-04-09'}),
        ('Peter Goodlad', {'born_date': '1857-12-02', 'died_date': '1936-11-13'}),
        ('Robert Haldane', {'born_date': '1848-07-20'}),
        ('Robert Johnson (ii)', {'born_date': '1929-03-09', 'died_date': '2014-08-12'}),
        ('Robert Scott', {'born_date': '1840-11-10', 'died_date': '1906-10-14'}),
        ('Thomas Nicolson', {'born_date': '1793-11-16'}),
        ('William Anderson (ii)', {'born_date': '1919-12-22'}),
        ('William Duncan (ii)', {'died_date': '1945-06-21'}),
        ('William Levie', {'died_date': '1901-01-27'}),
        ('William Peterson', {'born_date': '1923-11-22', 'died_date': '1994-07-17'}),
        ('William Playfair', {'born_date': '1926'}),
        ('William Robertson', {'born_date': '1828-11-10'}),
        ('William Sievwright (i)', {'died_date': '1870-06-26'}),
        ('William Tait', {'died_date': '2021-03-19'}),
    ]

    pfix_count = 0
    for name, updates in person_corrections:
        set_clauses = [f"{k} = ?" for k in updates]
        params = list(updates.values()) + [name]
        sqlite_cursor.execute(f"UPDATE people SET {', '.join(set_clauses)} WHERE name = ?", params)
        if sqlite_cursor.rowcount > 0:
            pfix_count += 1
    print(f"  Applied {pfix_count} corrections")

    pfill_count = 0
    for name, fills in person_fills:
        set_clauses = [f"{k} = COALESCE({k}, ?)" for k in fills]
        params = list(fills.values()) + [name]
        sqlite_cursor.execute(f"UPDATE people SET {', '.join(set_clauses)} WHERE name = ?", params)
        if sqlite_cursor.rowcount > 0:
            pfill_count += 1
    print(f"  Filled missing data for {pfill_count} people")

    # --- Step 4e: Resolve person links in intros and biographies ---
    # Wiki links like [[Charles Ogilvy (i)|Charles]] were stripped to plain "Charles"
    # by clean_wiki_markup. We re-read the wiki source and replace those plain-text
    # occurrences with [person:slug:Display] markers, matching by position.
    print("\n=== Resolving person links in text ===")
    # Build wiki_page_title -> slug map (includes redirects via person_ids)
    title_to_slug = {}
    for wiki_title, pid in person_ids.items():
        row = sqlite_cursor.execute("SELECT slug FROM people WHERE id = ?", (pid,)).fetchone()
        if row:
            title_to_slug[wiki_title.replace(' ', '_')] = row[0]

    def resolve_person_links_in_wiki(wiki_text):
        """Replace [[PersonPage|Display]] wiki links with [person:slug:Display] markers
        in one pass, leaving non-person links as plain display text."""
        def replace_link(m):
            target = m.group(1).strip()
            display = m.group(2).strip() if m.group(2) else target.replace('_', ' ')
            target_norm = target.replace(' ', '_')
            slug = title_to_slug.get(target_norm)
            if slug:
                return f'[person:{slug}:{display}]'
            return display
        return re.sub(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]', replace_link, wiki_text)

    link_count = 0
    for page_title in person_pages:
        text = get_wiki_page(mysql_cursor, page_title)
        if not text:
            continue

        pid = person_ids.get(page_title.replace(' ', '_'))
        if not pid:
            continue

        # Re-parse intro/bio sections from wiki source, but this time resolve person links
        # before stripping other markup
        intro_raw = text
        bio_raw = None
        bio_match = re.search(r'==\s*Biography\s*==', text, re.IGNORECASE)
        if bio_match:
            intro_raw = text[:bio_match.start()]
            bio_raw = text[bio_match.end():]
        intro_raw = re.split(r'==\s*\w', intro_raw)[0]
        if bio_raw:
            bio_raw = re.split(r'(?<!=)==\s*\w', bio_raw)[0]

        # Resolve person links first, then clean remaining wiki markup
        intro_resolved = resolve_person_links_in_wiki(intro_raw)
        bio_resolved = resolve_person_links_in_wiki(bio_raw) if bio_raw else None

        # Check if any person links were found
        has_person_links = '[person:' in intro_resolved or (bio_resolved and '[person:' in bio_resolved)
        if not has_person_links:
            continue

        # Clean remaining wiki markup (but preserve [person:...] markers)
        def clean_wiki_preserving_persons(t):
            # Temporarily protect person markers
            markers = []
            def save_marker(m):
                markers.append(m.group(0))
                return f'\x00PERSON{len(markers) - 1}\x00'
            t = re.sub(r'\[person:[^\]]+\]', save_marker, t)

            # Standard wiki markup cleanup
            t = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', t)
            t = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', t)
            t = re.sub(r'\[https?://[^\s\]]+\s+([^\]]+)\]', r'\1', t)
            t = re.sub(r'\[https?://[^\]]+\]', '', t)
            t = re.sub(r"'{2,3}", '', t)
            t = re.sub(r'<[^>]+>', '', t)
            t = re.sub(r'\{[^}]*\}', '', t)
            t = re.sub(r'\\n', '\n', t)
            t = re.sub(r'\n{3,}', '\n\n', t)
            t = re.sub(r'\s+,', ',', t)

            # Restore person markers
            for i, marker in enumerate(markers):
                t = t.replace(f'\x00PERSON{i}\x00', marker)
            return t.strip()

        intro_text = clean_wiki_preserving_persons(intro_resolved) or None
        bio_text = clean_wiki_preserving_persons(bio_resolved) if bio_resolved else None

        # Apply same intro cleanup as step 4
        if intro_text:
            intro_text = re.sub(r'__\w+__\s*', '', intro_text)
            intro_text = re.sub(r"For other people (?:named )?with the same name,?\s*(?:please\s+)?see\s+[^.]+\.\s*", '', intro_text)
            intro_text = re.sub(r"Not to be confused with [^.]+\.\s*", '', intro_text)
            intro_text = re.sub(r'\s*\(b\.\s*[^)]+\)\s*', ' ', intro_text)
            intro_text = re.sub(r'\s{2,}', ' ', intro_text).strip()

        # Strip redundant election result sentences (shown in structured data)
        intro_text = strip_election_result_sentences(intro_text)
        bio_text = strip_election_result_sentences(bio_text)

        sqlite_cursor.execute("UPDATE people SET intro = ?, biography = ? WHERE id = ?",
                              (intro_text, bio_text, pid))
        link_count += 1

    print(f"  Resolved person links in {link_count} people")

    def clean_candidate_name(name):
        """Strip external wiki links from candidate names, e.g. '[https://...url Display Name]' -> 'Display Name'"""
        cleaned = re.sub(r'\[https?://[^\s\]]+\s+([^\]]+)\]', r'\1', name)
        return cleaned.strip()

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
                    INSERT INTO elections (council_id, election_date, election_type, electorate, electorate_detail, turnout, turnout_pct, wiki_page_title)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    council_id, parsed['date'], parsed['type'],
                    parsed.get('electorate'), parsed.get('electorate_detail'), parsed.get('turnout'), parsed.get('turnout_pct'),
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

                    # Store historical display name if different from constituency's current name
                    display_name = result.get('constituency_name')
                    constituency_display = None
                    if constituency_id and display_name:
                        # Look up the current constituency name
                        sqlite_cursor.execute("SELECT name FROM constituencies WHERE id = ?", (constituency_id,))
                        current_name = sqlite_cursor.fetchone()
                        if current_name and current_name[0] != display_name:
                            constituency_display = display_name

                    # Resolve replaced person for by-elections
                    replaced_name = parsed.get('replaced_person')
                    replaced_pid = None
                    if replaced_name and parsed.get('replaced_wiki_link'):
                        replaced_pid = person_ids.get(parsed['replaced_wiki_link'].replace(' ', '_'))
                    if replaced_name and not replaced_pid:
                        replaced_pid = person_name_map.get(replaced_name)

                    sqlite_cursor.execute("""
                        INSERT INTO elections (council_id, constituency_id, constituency_display_name, election_date, election_type,
                            electorate, electorate_detail, turnout, turnout_pct, notes, replaced_person, replaced_person_id, wiki_page_title)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        council_id, constituency_id, constituency_display, parsed['date'], parsed['type'],
                        result.get('electorate'), result.get('electorate_detail'), result.get('turnout'), result.get('turnout_pct'),
                        result.get('notes'), replaced_name, replaced_pid, page_title
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

    # --- Step 6a2: Manual candidacy corrections ---
    # Fix cases where name-matching linked to the wrong disambiguated person
    print("\n=== Applying manual candidacy corrections ===")
    candidacy_corrections = [
        # (election_wiki_page, candidate_name, correct_person_wiki_page)
        ('Lerwick_Town_Council_Election_May_1956', 'Robert Anderson', 'Robert_Anderson_(i)'),
        ('Lerwick Town Council Election May 1956', 'Robert Anderson', 'Robert_Anderson_(i)'),
    ]
    fix_count = 0
    for election_page, cand_name, correct_person_page in candidacy_corrections:
        sqlite_cursor.execute("SELECT id FROM people WHERE wiki_page_title = ?", (correct_person_page,))
        person_row = sqlite_cursor.fetchone()
        if not person_row:
            continue
        correct_pid = person_row[0]
        sqlite_cursor.execute("""
            UPDATE candidacies SET person_id = ?
            WHERE candidate_name = ? AND election_id IN (
                SELECT id FROM elections WHERE wiki_page_title = ?
            ) AND person_id != ?
        """, (correct_pid, cand_name, election_page, correct_pid))
        if sqlite_cursor.rowcount > 0:
            fix_count += sqlite_cursor.rowcount
            print(f"  Fixed: '{cand_name}' in '{election_page}' -> person {correct_person_page}")
    print(f"  Applied {fix_count} corrections")

    # --- Step 6a3: Fix elected/not-elected swaps ---
    print("\n=== Fixing elected status swaps ===")
    elected_swaps = [
        # (election_wiki_page, should_be_elected_name, should_be_not_elected_name)
        # Delting South 1919: Joseph Peterson won (78 votes), not John T. J. Sinclair (30)
        ('County_Council_Election_December_1919', 'Joseph Peterson', 'John T. J. Sinclair'),
    ]
    swap_count = 0
    for election_page, winner, loser in elected_swaps:
        for variant in [election_page, election_page.replace('_', ' ')]:
            sqlite_cursor.execute("""
                UPDATE candidacies SET elected = 1
                WHERE candidate_name = ? AND elected = 0
                AND election_id IN (SELECT id FROM elections WHERE wiki_page_title = ?)
            """, (winner, variant))
            if sqlite_cursor.rowcount > 0:
                sqlite_cursor.execute("""
                    UPDATE candidacies SET elected = 0
                    WHERE candidate_name = ? AND elected = 1
                    AND election_id IN (SELECT id FROM elections WHERE wiki_page_title = ?)
                """, (loser, variant))
                swap_count += 1
                print(f"  Swapped: {winner} elected, {loser} not elected in {election_page}")
                break
    print(f"  Fixed {swap_count} swaps")

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

    # --- Step 6b3: Fix redirect elections with missing data ---
    print("\n=== Fixing redirect elections ===")
    # These elections were imported from redirect pages and have no candidacies.
    # Fix by re-reading the actual target page and importing candidates.
    redirect_fixes = [
        # (stored_wiki_title, actual_wiki_title, correct_constituency_name)
        ('Northmavine South County Council By-Election February 1951',
         'Northmavine_North_County_Council_By-Election_February_1951', 'Northmavine North'),
    ]
    for stored_title, actual_title, correct_constit in redirect_fixes:
        # Get the actual page content
        actual_text = get_wiki_page(mysql_cursor, actual_title)
        if not actual_text:
            print(f"  Skipped (no content): {actual_title}")
            continue

        # Find the election record
        sqlite_cursor.execute("SELECT id, constituency_id FROM elections WHERE wiki_page_title = ? OR wiki_page_title = ?",
                            (stored_title, stored_title.replace(' ', '_')))
        erow = sqlite_cursor.fetchone()
        if not erow:
            print(f"  Skipped (no election): {stored_title}")
            continue
        eid = erow[0]

        # Fix constituency if wrong
        if correct_constit:
            sqlite_cursor.execute("SELECT id FROM constituencies WHERE name = ?", (correct_constit,))
            crow = sqlite_cursor.fetchone()
            if crow:
                sqlite_cursor.execute("UPDATE elections SET constituency_id = ? WHERE id = ?", (crow[0], eid))

        # Parse candidates from the actual page
        parsed = parse_election_page(actual_text, actual_title)
        if parsed and parsed['results']:
            for result in parsed['results']:
                for pos, cand in enumerate(result.get('candidates', []), 1):
                    pid = None
                    if cand.get('wiki_link'):
                        pid = person_ids.get(cand['wiki_link'].replace(' ', '_'))
                    if not pid:
                        pid = person_name_map.get(cand['name'])
                    sqlite_cursor.execute("""
                        INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes, votes_text, elected, position, role)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (eid, pid, cand['name'], cand.get('party'), cand.get('votes'),
                          cand.get('votes_text'), 1 if cand['elected'] else 0, pos, cand.get('role')))
            print(f"  Fixed: {stored_title} -> {actual_title}")

    # Hide the 1902 redirect (actual page imported separately via different title)
    sqlite_cursor.execute("UPDATE elections SET hidden = 1 WHERE wiki_page_title IN ('1902 Orkney and Shetland by-election', '1902_Orkney_and_Shetland_by-election')")
    if sqlite_cursor.rowcount:
        print(f"  Hidden: 1902 Orkney and Shetland by-election (redirect)")

    # --- Step 6b2: Fix by-election replacement data ---
    # Manually verified replacements where the parser couldn't extract the name correctly.
    # Confirmed with James Stewart 2026-03-27.
    print("\n=== Fixing by-election replacements ===")
    replacement_fixes = [
        # (by-election wiki_page_title, replaced_person_name, replaced_person_wiki_page_title)
        ('Lerwick Town Council By-Election November 1880', 'James Mouat Goudie', 'James_M._Goudie'),
        ('Lerwick Town Council By-Election November 1884', 'Arthur Hay', 'Arthur_Hay'),
        # Nov 1886: 2 co-opted — unfilled seats, source unknown. Needs further research.
        ('Lerwick Town Council By-Election June 1921', 'James Pottinger (iii)', 'James_Pottinger_(iii)'),
        ('Lerwick Town Council By-Election May 1924', 'James Goodlad', 'James_Goodlad'),
        ('Lerwick Town Council By-Election August 1936', 'George Duffin', 'George_Duffin'),
        ('Lerwick Town Council By-Election November 1936', 'Laurence Cogle', 'Laurence_Cogle'),  # Needs confirmation
        ('Lerwick Town Council By-Election February 1938', 'Erling Clausen', 'Erling_Clausen'),
        ('Lerwick Town Council By-Election January 1941', 'Adam Halcrow (i)', 'Adam_Halcrow_(i)'),
        ('Lerwick Town Council By-Election July 1941', 'James A. Smith', 'James_A._Smith'),
        # Oct 1941: TWO replacements - Thomas Irvine (ii) and Joseph Linklater
        ('Lerwick Town Council By-Election October 1941', 'Thomas Irvine (ii)', 'Thomas_Irvine_(ii)'),
        ('Lerwick Town Council By-Election September 1950', 'James Jamieson (ii)', 'James_Jamieson_(ii)'),
        ('Lerwick Town Council By-Election June 1958', 'James Tait', 'James_Tait'),
        ('Lerwick Town Council By-Election September 1959', 'Magnus Sandison', 'Magnus_Sandison'),
        ('Lerwick Town Council By-Election May 1961', 'Kenneth Thomson', 'Kenneth_Thomson'),
        ('Lerwick Town Council By-Election May 1967', 'Andrew Nicolson', 'Andrew_Nicolson'),
        ('Lerwick Town Council By-Election August 1967', 'Robert Anderson (i)', 'Robert_Anderson_(i)'),
        # County Council by-elections — names resolved from wiki source + people search
        ('Tingwall County Council By-Election March 1909', 'William Duncan (ii)', 'William_Duncan_(ii)'),
        ('Whalsay And Skerries County Council By-Election July 1910', 'Charles Stobie', 'Charles_Stobie'),
        ('Dunrossness South County Council By-Election June 1919', 'William Fotheringham', 'William_Fotheringham'),
        ('Walls_County_Council_By-Election_October_1921', 'Alexander Groundwater', 'Alexander_Groundwater'),
        ('Whalsay And Skerries County Council By-Election February 1937', 'Magnus Shearer (i)', 'Magnus_Shearer_(i)'),
        ('Dunrossness North County Council By-Election February 1952', 'Robert Bruce', 'Robert_Bruce'),
        ('Dunrossness North County Council By-Election September 1963', 'Tom Henderson', 'Tom_Henderson'),
        ('Dunrossness North County Council By-Election June 1907', 'Robert Isbister', 'Robert_Isbister'),
        ('Cunningsburgh_County_Council_By-Election_February_1919', 'Francis Pottinger', 'Francis_Pottinger'),
        ('Aithsting County Council By-Election February 1921', 'Thomas Anderson (iii)', 'Thomas_Anderson_(iii)'),
        ('Lerwick Central County Council By-Election May 1921', 'James Pottinger (iii)', 'James_Pottinger_(iii)'),
        ('Northmavine_South_County_Council_By-Election_February_1924', 'John Robertson (iv)', 'John_Robertson_(iv)'),
        ('Northmavine South County Council By-Election July 1927', 'John Robertson (iv)', 'John_Robertson_(iv)'),
        ('Dunrossness North County Council By-Election June 1931', 'John Irvine (i)', 'John_Irvine_(i)'),
        ('Fetlar County Council By-Election October 1937', 'William Carson', 'William_Carson'),
        ('Yell South County Council By-Election December 1940', 'Thomas Manson (i)', 'Thomas_Manson_(i)'),
        ('Unst South County Council By-Election December 1942', 'Andrew Irvine (i)', 'Andrew_Irvine_(i)'),
        ('Gulberwick County Council By-Election May 1951', 'John Williamson (iii)', 'John_Williamson_(iii)'),
        ('Aithsting County Council By-Election February 1959', 'Frederick Tait', 'Frederick_Tait'),
        ('Walls County Council By-Election February 1960', 'Peter Henry', 'Peter_Henry'),
        ('Yell North County Council By-Election December 1962', 'Andrew Peterson', 'Andrew_Peterson'),
        ('Yell South County Council By-Election March 1966', 'John Tulloch (ii)', 'John_Tulloch_(ii)'),
        ('Gulberwick County Council By-Election October 1970', 'Robert Johnson (i)', 'Robert_Johnson_(i)'),
        ('Dunrossness North County Council By-Election October 1971', 'Iain Campbell', 'Iain_Campbell'),
        ('Northmavine South County Council By-Election June 1972', 'Hugh Sutherland', 'Hugh_Sutherland'),
        ('Dunrossness By-Election July 1977', 'James Leask', 'James_Leask'),
        # ZCC by-elections — resolved from wiki text analysis 2026-03-27
        ('Aithsting County Council By-Election April 1890', '[voided election re-run]', None),
        ('Sandsting County Council By-Election April 1890', '[voided election re-run]', None),
        ('Burra County Council By-Election February 1893', '[unfilled seat]', None),
        ('Aithsting County Council By-Election January 1896', '[unfilled seat]', None),
        ('Burra County Council By-Election August 1909', 'Charles Lennie', 'Charles_Lennie'),
        ('Whiteness And Weisdale County Council By-Election June 1914', 'Peter Anderson', 'Peter_Anderson'),
        ('Burra County Council By-Election April 1920', 'William Sinclair', 'William_Sinclair'),
        ('Bressay County Council By-Election December 1934', 'James A. Smith', 'James_A._Smith'),
        ('Dunrossness North County Council By-Election December 1934', 'John Robertson (iv)', 'John_Robertson_(iv)'),
        ('Dunrossness North County Council By-Election December 1946', 'John Goudie', None),  # John J. Goudie — no person page
        ('Bressay County Council By-Election June 1966', 'John Smith', 'John_Smith_(ii)'),
        ('Shetland Central By-Election December 2011', 'Iris Hawkins', None),  # no person page
        ('Northmavine South County Council By-Election February 1951', 'David Walker', None),  # redirect from South to North; Rev. David A. Walker resigned
    ]
    # Oct 1941 also replaced Joseph Linklater — handle as second update
    replacement_fixes_extra = [
        ('Lerwick Town Council By-Election October 1941', 'Joseph Linklater', 'Joseph_Linklater'),
    ]

    repl_count = 0
    for wiki_title, repl_name, repl_wiki in replacement_fixes:
        pid = person_ids.get(repl_wiki)
        if not pid:
            pid = person_name_map.get(repl_name)
        for variant in [wiki_title, wiki_title.replace(' ', '_')]:
            sqlite_cursor.execute("""
                UPDATE elections SET replaced_person = ?, replaced_person_id = ?
                WHERE wiki_page_title = ? AND (replaced_person IS NULL OR replaced_person_id IS NULL)
            """, (repl_name, pid, variant))
            if sqlite_cursor.rowcount > 0:
                repl_count += 1
                break

    # For Oct 1941 with two replacements, we store the second one in notes
    # (the first is already in replaced_person)
    for wiki_title, repl_name, repl_wiki in replacement_fixes_extra:
        pid = person_ids.get(repl_wiki)
        for variant in [wiki_title, wiki_title.replace(' ', '_')]:
            sqlite_cursor.execute("""
                UPDATE elections SET notes = COALESCE(notes, '') || ' Also replaced: ' || ?
                WHERE wiki_page_title = ? AND replaced_person IS NOT NULL
            """, (repl_name, variant))

    print(f"  Fixed {repl_count} by-election replacements")

    # --- Step 6c: Middle-name / abbreviation matching ---
    print("\n=== Resolving middle-name matches ===")
    sqlite_cursor.execute("SELECT id, name, slug, intro, born_date, died_date FROM people")
    all_people_for_mn = sqlite_cursor.fetchall()

    mn_surname_map = {}
    for pid, pname, pslug, pintro, pborn, pdied in all_people_for_mn:
        clean = re.sub(r'\s*\([^)]*\)\s*$', '', pname)
        parts = clean.split()
        if len(parts) >= 2:
            surname = parts[-1].lower()
            if surname not in mn_surname_map:
                mn_surname_map[surname] = []
            mn_surname_map[surname].append({
                'id': pid, 'name': pname, 'clean': clean,
                'first': parts[0].lower(), 'last': parts[-1].lower(),
                'intro': (pintro or '').lower(), 'born': pborn, 'died': pdied
            })

    sqlite_cursor.execute("""
        SELECT c.id, c.candidate_name, e.election_date
        FROM candidacies c JOIN elections e ON c.election_id = e.id
        WHERE c.person_id IS NULL
    """)
    mn_unlinked = sqlite_cursor.fetchall()
    mn_count = 0

    for cid, cname, edate in mn_unlinked:
        parts = cname.strip().split()
        if len(parts) < 2:
            continue
        csurname = parts[-1].lower()
        cfirst = parts[0].lower()
        if csurname not in mn_surname_map:
            continue

        eyear = int(edate[:4]) if edate and len(edate) >= 4 else 0
        candidates = []
        for p in mn_surname_map[csurname]:
            if cfirst != p['first']:
                if not (len(cfirst) <= 2 and p['first'].startswith(cfirst.rstrip('.'))):
                    continue
            if eyear:
                by = int(p['born'][:4]) if p['born'] and len(p['born']) >= 4 else 0
                dy = int(p['died'][:4]) if p['died'] and len(p['died']) >= 4 else 9999
                if by and eyear < by - 5: continue
                if dy < 9999 and eyear > dy + 1: continue
                if by and eyear - by < 18: continue
            if cname.lower() == p['clean'].lower():
                continue
            matched = False
            if cname.lower() in p['intro']:
                matched = True
            if not matched and len(parts) > 2 and cfirst == p['first'] and csurname == p['last']:
                matched = True
            if not matched and len(cfirst) <= 2 and csurname == p['last'] and p['first'].startswith(cfirst.rstrip('.')):
                matched = True
            if matched:
                candidates.append(p)

        if len(candidates) == 1:
            sqlite_cursor.execute("UPDATE candidacies SET person_id = ? WHERE id = ?",
                                 (candidates[0]['id'], cid))
            mn_count += 1

    print(f"  Linked {mn_count} candidacies via middle-name matching")

    # --- Step 6d: Propagate person_id by exact candidate name ---
    # If "William Arthur Bruce" is linked in 4 elections but not a 5th,
    # propagate the person_id to the unlinked one (with alive check).
    print("\n=== Propagating person links by name ===")
    sqlite_cursor.execute("""
        SELECT c.id, c.candidate_name, e.election_date
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        WHERE c.person_id IS NULL
    """)
    still_unlinked = sqlite_cursor.fetchall()

    # Build lookup: candidate_name -> person_id (from linked candidacies)
    sqlite_cursor.execute("""
        SELECT DISTINCT c.candidate_name, c.person_id, p.born_date, p.died_date
        FROM candidacies c
        JOIN people p ON c.person_id = p.id
        WHERE c.person_id IS NOT NULL
    """)
    name_to_person = {}
    for cname, pid, pborn, pdied in sqlite_cursor.fetchall():
        if cname not in name_to_person:
            name_to_person[cname] = []
        name_to_person[cname].append({'id': pid, 'born': pborn, 'died': pdied})

    prop_count = 0
    for cid, cname, edate in still_unlinked:
        if cname not in name_to_person:
            continue
        matches = name_to_person[cname]
        eyear = int(edate[:4]) if edate and len(edate) >= 4 else 0

        alive = []
        for m in matches:
            by = int(m['born'][:4]) if m['born'] and len(m['born']) >= 4 else 0
            dy = int(m['died'][:4]) if m['died'] and len(m['died']) >= 4 else 9999
            if by and eyear < by - 5: continue
            if dy < 9999 and eyear > dy + 1: continue
            if by and eyear - by < 18: continue
            alive.append(m)

        if len(alive) == 1:
            sqlite_cursor.execute("UPDATE candidacies SET person_id = ? WHERE id = ?",
                                 (alive[0]['id'], cid))
            prop_count += 1

    print(f"  Propagated {prop_count} person links by exact name match")

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

    # --- Step 7: Import referenda ---
    print("\n=== Importing referenda ===")
    referendum_pages = [
        '1975_European_Economic_Community_Membership_Referendum',
        '1979_Scottish_Devolution_Referendum',
        '1997_Scottish_Devolution_Referendum',
        '2011_Alternative_Vote_Referendum',
        '2014_Scottish_Independence_Referendum',
        '2016_European_Union_Membership_Referendum',
    ]

    for page_title in referendum_pages:
        text = get_wiki_page(mysql_cursor, page_title)
        if not text:
            print(f"  Skipped: {page_title}")
            continue

        title = page_title.replace('_', ' ')
        slug = slugify(title)

        # Extract date from title
        year_match = re.search(r'^(\d{4})', title)
        ref_date = f"{year_match.group(1)}-01-01" if year_match else None

        # Extract question
        question = None
        q_match = re.search(r'asked\s*["\u201c]([^"\u201d]+)["\u201d]', text)
        if q_match:
            question = q_match.group(1).strip()
        # Also try "vote on was" pattern
        if not question:
            q_match = re.search(r'vote on was\s*["\u201c]([^"\u201d]+)["\u201d]', text)
            if q_match:
                question = q_match.group(1).strip()

        # Extract description (text before ==Shetland Result==)
        desc_text = text
        desc_text = re.sub(r'\[\[(?:Image|File):.*?\]\]', '', desc_text, flags=re.DOTALL)
        desc_text = re.sub(r'\{\{[^}]+\}\}', '', desc_text)
        desc_text = re.sub(r'\[\[Category:[^\]]+\]\]', '', desc_text)
        desc_parts = re.split(r'==\s*Shetland Result\s*==', desc_text)
        desc_raw = desc_parts[0] if desc_parts else ''
        # Also handle TOC
        desc_raw = re.sub(r'\{\|[^}]*__TOC__[^}]*\|\}', '', desc_raw)

        def clean_markup(t):
            t = re.sub(r'\[\[([^\]|]+?)\|([^\]]+?)\]\]', r'\2', t)
            t = re.sub(r'\[\[([^\]]+?)\]\]', r'\1', t)
            t = re.sub(r"'{2,3}", '', t)
            t = re.sub(r'<[^>]+>', '', t)
            t = re.sub(r'&quot;', '"', t)
            t = re.sub(r'\\n', '\n', t)
            t = re.sub(r'\n{3,}', '\n\n', t)
            return t.strip()

        description = clean_markup(desc_raw) or None

        # Extract turnout
        turnout_pct = None
        tp_match = re.search(r'Turnout:\s*(?:[\d,]+\s*\()?([\d.]+)%', text)
        if tp_match:
            turnout_pct = float(tp_match.group(1))

        sqlite_cursor.execute("""
            INSERT OR IGNORE INTO referenda (title, slug, date, question, description, turnout_pct, wiki_page_title)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (title, slug, ref_date, question, description, turnout_pct, page_title))
        ref_id = sqlite_cursor.lastrowid
        if not ref_id:
            print(f"  Skipped (exists): {title}")
            continue

        # Parse result tables - may have multiple questions (1997)
        # Split by === sub-sections for multi-question referenda
        result_section = text
        shetland_split = re.split(r'==\s*Shetland Result\s*==', result_section, maxsplit=1)
        if len(shetland_split) > 1:
            result_section = shetland_split[1]

        # Check for sub-questions (=== sections ===)
        sub_questions = re.split(r'===\s*([^=]+?)\s*===', result_section)

        if len(sub_questions) > 1:
            # Multi-question: pairs of (question_label, content)
            i = 1
            while i < len(sub_questions):
                q_label = clean_markup(sub_questions[i].strip()) if i < len(sub_questions) else None
                content = sub_questions[i+1] if i+1 < len(sub_questions) else ''

                # Parse the wikitable in this section
                table_match = re.search(r'\{\|\s*class="wikitable"(.*?)\|\}', content, re.DOTALL)
                if table_match:
                    rows = re.split(r'\|-', table_match.group(1))
                    for row in rows:
                        if "'''Option'''" in row or not row.strip() or row.strip() == '|}':
                            continue
                        cells = re.split(r'\|\|', row)
                        if len(cells) < 2:
                            continue
                        option = re.sub(r'^\s*\|', '', cells[0]).strip()
                        option = re.sub(r"'''", '', option).strip()
                        if not option or 'background' in option:
                            continue
                        votes_str = cells[1].strip() if len(cells) > 1 else ''
                        votes = None
                        v_match = re.search(r'([\d,]+)', votes_str)
                        if v_match:
                            votes = int(v_match.group(1).replace(',', ''))
                        pct = None
                        if len(cells) > 2:
                            p_match = re.search(r'([\d.]+)%', cells[2])
                            if p_match:
                                pct = float(p_match.group(1))
                        won = 1 if 'tick.gif' in row else 0

                        sqlite_cursor.execute("""
                            INSERT INTO referendum_results (referendum_id, question_label, option_name, votes, percentage, won)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (ref_id, q_label, option, votes, pct, won))
                i += 2
        else:
            # Single question - parse the one wikitable
            table_match = re.search(r'\{\|\s*class="wikitable"(.*?)\|\}', result_section, re.DOTALL)
            if table_match:
                rows = re.split(r'\|-', table_match.group(1))
                for row in rows:
                    if "'''Option'''" in row or not row.strip() or row.strip() == '|}':
                        continue
                    cells = re.split(r'\|\|', row)
                    if len(cells) < 2:
                        continue
                    option = re.sub(r'^\s*\|', '', cells[0]).strip()
                    option = re.sub(r"'''", '', option).strip()
                    if not option or 'background' in option:
                        continue
                    votes_str = cells[1].strip()
                    votes = None
                    v_match = re.search(r'([\d,]+)', votes_str)
                    if v_match:
                        votes = int(v_match.group(1).replace(',', ''))
                    pct = None
                    if len(cells) > 2:
                        p_match = re.search(r'([\d.]+)%', cells[2])
                        if p_match:
                            pct = float(p_match.group(1))
                    won = 1 if 'tick.gif' in row else 0

                    sqlite_cursor.execute("""
                        INSERT INTO referendum_results (referendum_id, question_label, option_name, votes, percentage, won)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (ref_id, None, option, votes, pct, won))

        print(f"  Imported: {title}")

    # --- Step 9: Import leadership roles (Provosts, Conveners) ---
    print("\n=== Importing leadership roles ===")

    def parse_leadership_list(template_text, council_name, role):
        """Parse a leadership list from a template like 'Name (YYYY-YYYY) • Name (YYYY-YYYY)'"""
        council_id_local = council_ids[council_name]
        entries = re.findall(r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]\s*\((\d{4})-(\d{4})\)', template_text)
        count = 0
        for wiki_link, display_name, start, end in entries:
            name = display_name.strip() if display_name else wiki_link.strip()
            wiki_title = wiki_link.strip().replace(' ', '_')
            # Try to find person_id
            pid = person_ids.get(wiki_title)
            if not pid:
                pid = person_name_map.get(name)
            sqlite_cursor.execute("""
                INSERT INTO leadership_roles (council_id, person_id, person_name, role, start_year, end_year)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (council_id_local, pid, name, role, start, end))
            count += 1
        return count

    # LTC Provosts (called "Senior Bailie" until 1877, then "Provost")
    ltc_provosts_text = get_template_text(mysql_cursor, 'LTCProvosts')
    if ltc_provosts_text:
        count = parse_leadership_list(ltc_provosts_text, 'Lerwick Town Council', 'Provost')
        print(f"  LTC Provosts: {count}")

    # ZCC Conveners
    zcc_conveners_text = get_template_text(mysql_cursor, 'ZCCConveners')
    if zcc_conveners_text:
        count = parse_leadership_list(zcc_conveners_text, 'Zetland County Council', 'Convener')
        print(f"  ZCC Conveners: {count}")

    # SIC Conveners — not in a single template, extract from person pages
    # Look for {{Conveners|...|Convener of the Shetland Islands Council...|...}} or
    # {{SICConveners}} usage on person pages
    print("  Extracting SIC Conveners from person pages...")
    sic_convener_count = 0
    sic_council_id = council_ids['Shetland Islands Council']
    mysql_cursor.execute("""
        SELECT p.page_title, CAST(old_text AS CHAR CHARACTER SET utf8mb4)
        FROM mwfn_text t
        JOIN mwfn_revision r ON r.rev_text_id = t.old_id
        JOIN mwfn_page p ON p.page_latest = r.rev_id
        WHERE p.page_namespace = 0
    """)
    for row in mysql_cursor.fetchall():
        page_title = row[0]
        if isinstance(page_title, (bytes, bytearray)):
            page_title = page_title.decode('utf-8', errors='replace')
        text = row[1]
        if isinstance(text, (bytes, bytearray)):
            text = text.decode('utf-8', errors='replace')

        # Look for Convener templates referencing SIC
        for tmatch in re.finditer(r'\{\{(?:Conveners|SICConveners)[^}]*\}\}', text, re.DOTALL):
            template = tmatch.group()
            if 'Shetland Islands Council' in template or 'SICConveners' in template:
                # Extract the tenure: "Convener of the\nShetland Islands Council\nYYYY-YYYY"
                tenure_match = re.search(r'Convener.*?(\d{4})-(\d{4})', template, re.DOTALL)
                if tenure_match:
                    start = tenure_match.group(1)
                    end = tenure_match.group(2)
                    name = page_title.replace('_', ' ')
                    # Remove disambiguation
                    display = re.sub(r'\s*\([^)]*\)\s*$', '', name)
                    pid = person_ids.get(page_title.replace(' ', '_'))
                    sqlite_cursor.execute("""
                        INSERT INTO leadership_roles (council_id, person_id, person_name, role, start_year, end_year)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (sic_council_id, pid, display, 'Convener', start, end))
                    sic_convener_count += 1

    print(f"  SIC Conveners: {sic_convener_count}")

    sqlite_conn.commit()
    sqlite_conn.close()
    mysql_conn.close()
    print("\nDone! Database saved to:", SQLITE_PATH)


if __name__ == '__main__':
    main()
