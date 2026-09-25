#!/usr/bin/env python3
"""
MCP server for Shetland Political History database.
Exposes read-only SQL access + convenience tools for Claude Desktop.

Usage:
  pip install mcp sqlite3
  Add to Claude Desktop config:
  {
    "mcpServers": {
      "shetland-history": {
        "command": "python3",
        "args": ["/Users/james/projects/shetland_history/new-site/mcp-server.py"]
      }
    }
  }
"""

import sqlite3
import json
from mcp.server.fastmcp import FastMCP

DB_PATH = "/Users/james/projects/shetland_history/new-site/shetland.db"

mcp = FastMCP("Shetland Political History")


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


@mcp.tool()
def query(sql: str) -> str:
    """Run a read-only SQL query against the Shetland political history database.

    Tables: councils, constituencies, people, elections, candidacies,
    referenda, referendum_results, leadership_roles, council_terms, term_issues.

    Key columns:
    - people: id, name, slug, born_date, died_date, birth_place, death_place, intro, biography, bayanne_id
    - elections: id, council_id, constituency_id, election_date, election_type, electorate, turnout, replaced_person, wiki_page_title, hidden
    - candidacies: id, election_id, person_id, candidate_name, party, votes, votes_text, elected, role
    - leadership_roles: id, council_id, person_id, person_name, role, start_year, end_year
    - council_terms: council_id, person_id, person_name, candidacy_id (party comes from this candidacy),
      constituency_id, start_date, end_date (exclusive, NULL = still serving), start_reason, end_reason,
      confirmed, source. Serving on date X: start_date <= X AND (end_date IS NULL OR end_date > X).
    - term_issues: council_id, kind, date_from, date_to, person_name, detail (unresolved membership checks)
    """
    db = get_db()
    try:
        # Block writes
        sql_upper = sql.strip().upper()
        if any(sql_upper.startswith(kw) for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE']):
            return "Error: read-only access. Only SELECT queries are allowed."

        rows = db.execute(sql).fetchall()
        if not rows:
            return "No results."

        # Convert to list of dicts
        result = [dict(row) for row in rows]
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        return f"SQL Error: {e}"
    finally:
        db.close()


@mcp.tool()
def find_person(name: str) -> str:
    """Search for a person by name (partial match). Returns their details and election history."""
    db = get_db()
    people = db.execute(
        "SELECT * FROM people WHERE name LIKE ? ORDER BY name LIMIT 10",
        (f"%{name}%",)
    ).fetchall()

    results = []
    for p in people:
        elections = db.execute("""
            SELECT c.candidate_name, c.party, c.votes, c.elected, c.role,
                   e.election_date, e.election_type, e.wiki_page_title,
                   co.name as council_name, con.name as constituency_name
            FROM candidacies c
            JOIN elections e ON c.election_id = e.id
            JOIN councils co ON e.council_id = co.id
            LEFT JOIN constituencies con ON e.constituency_id = con.id
            WHERE c.person_id = ? AND e.hidden = 0
            ORDER BY e.election_date
        """, (p['id'],)).fetchall()

        results.append({
            "person": dict(p),
            "elections": [dict(e) for e in elections]
        })

    db.close()
    return json.dumps(results, indent=2, default=str) if results else "No person found."


@mcp.tool()
def council_composition(council_slug: str, date: str, party: str = "") -> str:
    """Who was sitting on a council on a given date (YYYY-MM-DD), from council_terms.

    council_slug: lerwick-town-council, zetland-county-council or shetland-islands-council.
    party: optional exact party label to filter on (e.g. "Labour"); party is the label the
    member stood under when they won the seat.
    """
    db = get_db()
    rows = db.execute("""
        SELECT ct.person_name, p.slug, k.name AS ward, c.party, ct.start_date, ct.end_date,
               ct.start_reason, ct.end_reason, ct.confirmed
        FROM council_terms ct
        JOIN councils co ON co.id = ct.council_id
        LEFT JOIN people p ON p.id = ct.person_id
        LEFT JOIN constituencies k ON k.id = ct.constituency_id
        LEFT JOIN candidacies c ON c.id = ct.candidacy_id
        WHERE co.slug = ? AND ct.start_date <= ? AND (ct.end_date IS NULL OR ct.end_date > ?)
          AND (? = '' OR c.party = ?)
        ORDER BY ward, ct.person_name
    """, (council_slug, date, date, party, party)).fetchall()
    issues = db.execute("""
        SELECT ti.kind, ti.date_from, ti.date_to, ti.detail FROM term_issues ti
        JOIN councils co ON co.id = ti.council_id
        WHERE co.slug = ? AND ti.date_from <= ? AND (ti.date_to IS NULL OR ti.date_to > ?)
    """, (council_slug, date, date)).fetchall()
    db.close()
    return json.dumps({"members": [dict(r) for r in rows],
                       "open_issues_on_this_date": [dict(i) for i in issues]}, indent=2, default=str)


@mcp.tool()
def schema() -> str:
    """Get the full database schema."""
    db = get_db()
    tables = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    db.close()
    return "\n\n".join(row['sql'] for row in tables if row['sql'])


if __name__ == "__main__":
    mcp.run()
