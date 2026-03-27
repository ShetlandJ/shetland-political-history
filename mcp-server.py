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
    referenda, referendum_results, leadership_roles.

    Key columns:
    - people: id, name, slug, born_date, died_date, birth_place, death_place, intro, biography, bayanne_id
    - elections: id, council_id, constituency_id, election_date, election_type, electorate, turnout, replaced_person, wiki_page_title, hidden
    - candidacies: id, election_id, person_id, candidate_name, party, votes, votes_text, elected, role
    - leadership_roles: id, council_id, person_id, person_name, role, start_year, end_year
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
def council_composition(year: int) -> str:
    """Get the Lerwick Town Council composition at a given year (approximate).
    Shows who was on the council based on election records."""
    db = get_db()

    # Get the most recent elections before this year for LTC
    recent = db.execute("""
        SELECT c.candidate_name, c.person_id, p.name as person_name,
               e.election_date, e.election_type
        FROM candidacies c
        JOIN elections e ON c.election_id = e.id
        LEFT JOIN people p ON c.person_id = p.id
        WHERE e.council_id = 1 AND c.elected = 1 AND e.hidden = 0
        AND CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) <= ?
        AND CAST(SUBSTR(e.election_date, 1, 4) AS INTEGER) >= ? - 3
        ORDER BY e.election_date DESC
    """, (year, year)).fetchall()

    db.close()
    return json.dumps([dict(r) for r in recent], indent=2, default=str)


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
