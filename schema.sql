-- Shetland Political History Database Schema

CREATE TABLE IF NOT EXISTS councils (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    level TEXT NOT NULL  -- 'local', 'county', 'regional', 'westminster'
);

CREATE TABLE IF NOT EXISTS constituencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    council_id INTEGER NOT NULL REFERENCES councils(id),
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    description TEXT,
    wiki_page_title TEXT,
    UNIQUE(council_id, slug)
);

CREATE TABLE IF NOT EXISTS people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    born_date TEXT,          -- ISO date or partial e.g. '1775-11-10'
    died_date TEXT,
    birth_place TEXT,
    intro TEXT,              -- opening text before any section
    biography TEXT,          -- content from ==Biography== section
    image_ref TEXT,          -- image filename from wiki
    bayanne_id TEXT,         -- bayanne.info person ID e.g. 'I18328'
    wiki_page_title TEXT,
    categories TEXT           -- JSON array of original wiki categories
);

CREATE TABLE IF NOT EXISTS elections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    council_id INTEGER NOT NULL REFERENCES councils(id),
    constituency_id INTEGER REFERENCES constituencies(id),
    election_date TEXT,       -- ISO date or partial
    election_type TEXT NOT NULL,  -- 'general', 'by-election'
    electorate INTEGER,
    turnout INTEGER,
    turnout_pct REAL,
    notes TEXT,
    wiki_page_title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidacies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    election_id INTEGER NOT NULL REFERENCES elections(id),
    person_id INTEGER REFERENCES people(id),
    candidate_name TEXT NOT NULL,
    party TEXT,
    votes INTEGER,
    votes_text TEXT,          -- 'Unopposed', 'Appointed as Senior Bailie', etc.
    elected INTEGER NOT NULL DEFAULT 0,  -- boolean
    position INTEGER,         -- order in results table
    role TEXT                 -- 'Senior Bailie', 'Junior Bailie', 'councillor', etc.
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_elections_council ON elections(council_id);
CREATE INDEX IF NOT EXISTS idx_elections_constituency ON elections(constituency_id);
CREATE INDEX IF NOT EXISTS idx_candidacies_election ON candidacies(election_id);
CREATE INDEX IF NOT EXISTS idx_candidacies_person ON candidacies(person_id);
CREATE INDEX IF NOT EXISTS idx_constituencies_council ON constituencies(council_id);
CREATE INDEX IF NOT EXISTS idx_people_slug ON people(slug);
CREATE INDEX IF NOT EXISTS idx_elections_date ON elections(election_date);
