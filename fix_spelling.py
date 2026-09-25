#!/usr/bin/env python3
"""
Spelling and typo corrections in the wiki's prose (biographies, intros, election notes).
Idempotent: each fix is guarded on the old text, and re-running does nothing once applied.

Source: a spell check of the site text, 2026-09-25 (prompted by "newphie" on James Mouat (ii)).
Only unambiguous slips are corrected here: misspellings, doubled words, dropped or swapped
letters, and wrong-word typos whose meaning is clear from the sentence. Left as they are:
quotations from historic documents (e.g. the Thomas Dundas plaque, the 17th-century records),
dialect, variant spellings (licenced, Convenor, Baillie), and anything where the intended word
or fact isn't certain.

Place and ship names were corrected only to a spelling used elsewhere on the site or to the
real name: Cunningsburgh (the constituency), Tingwall, Kirkcaldy, Coupar Angus, Tillicoultry,
Dumfries, Gisborne NZ, Hagersville (Ontario, spelt so elsewhere in the same biography), Libya,
Halyburton of Pitcur, HMS Euryalus, the emigrant ship Metagama, St Columba's (Lerwick),
Valentine & Sons (Dundee), the Lyon Office.
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

# (table, id, column, old, new). `old` must occur exactly once in the field.
FIXES = [
    ('people', 1, 'biography', 'elected toLerwick Town Council', 'elected to Lerwick Town Council'),
    ('people', 2, 'intro', 'for Cunningburgh between', 'for Cunningsburgh between'),
    ('people', 4, 'biography', 'Presbyterian in Hagarsville', 'Presbyterian in Hagersville'),
    ('people', 13, 'biography', 'Couper Angus', 'Coupar Angus'),
    ('people', 14, 'biography', "Fishermen's Windows' Relief", "Fishermen's Widows' Relief"),
    ('people', 17, 'intro', 'was the a County Councillor', 'was a County Councillor'),
    ('people', 19, 'biography', 'post-were period', 'post-war period'),
    ('people', 20, 'intro', 'Sheriff clerk deputee', 'Sheriff clerk depute'),
    ('people', 35, 'intro', 'he complied a population census', 'he compiled a population census'),
    ('people', 37, 'intro', 'family esate', 'family estate'),
    ('people', 38, 'biography', 'the Greirson lairds', 'the Grierson lairds'),
    ('people', 39, 'biography', 'in a office based role', 'in an office based role'),
    ('people', 46, 'biography', 'He was was returning', 'He was returning'),
    ('people', 48, 'biography', 'he was the an assistant', 'he was an assistant'),
    ('people', 50, 'biography', 'Shetladn Salmon', 'Shetland Salmon'),
    ('people', 57, 'biography', 'at St Columbus until', "at St Columba's until"),
    ('people', 59, 'intro', 'He a founded a school', 'He founded a school'),
    ('people', 61, 'intro', 'treatises on opthalmia', 'treatises on ophthalmia'),
    ('people', 61, 'intro', 'notoriously litigous', 'notoriously litigious'),
    ('people', 65, 'biography', 'would not approved such', 'would not approve such'),
    ('people', 66, 'biography', 'late moving to Greenwich', 'later moving to Greenwich'),
    ('people', 78, 'intro', 'former Shetland Islands Council and record shop owner',
     'former Shetland Islands Councillor and record shop owner'),
    ('people', 88, 'biography', 'herring station, cooperate and', 'herring station, cooperage and'),
    ('people', 92, 'biography', 'and whom had trained Charles', 'and who had trained Charles'),
    ('people', 100, 'intro', 'had been know as', 'had been known as'),
    ('people', 103, 'biography', 'as Chaplin to the Royal Infirmary', 'as Chaplain to the Royal Infirmary'),
    ('people', 104, 'biography', 'in 1926, by his son Charles carried on', 'in 1926, but his son Charles carried on'),
    ('people', 105, 'biography', 'Presbyterian University in American', 'Presbyterian University in America'),
    ('people', 106, 'biography', 'Banff, Abedeen and', 'Banff, Aberdeen and'),
    ('people', 109, 'biography', 'travelling stationary salesman', 'travelling stationery salesman'),
    ('people', 124, 'biography', 'the seven child of eleven', 'the seventh child of eleven'),
    ('people', 132, 'biography', 'Royal Tank REgiment', 'Royal Tank Regiment'),
    ('people', 146, 'intro', "until it's dissolution", 'until its dissolution'),
    ('people', 150, 'biography', 'remaining as the a Manager', 'remaining as the Manager'),
    ('people', 154, 'biography', 'his retirenemt in 1925', 'his retirement in 1925'),
    ('people', 189, 'biography', 'with the form J & J Tod', 'with the firm J & J Tod'),
    ('people', 189, 'biography', 'one of the principle suppliers', 'one of the principal suppliers'),
    ('people', 207, 'biography', 'the British convoy was attached', 'the British convoy was attacked'),
    ('people', 211, 'biography', 'until his retiral until around 1961', 'until his retiral around 1961'),
    ('people', 221, 'intro', 'fill the a vacancy', 'fill a vacancy'),
    ('people', 225, 'intro', 'Halyburton of Pitcut', 'Halyburton of Pitcur'),
    ('people', 233, 'biography', 'a four year apprentice in', 'a four year apprenticeship in'),
    ('people', 233, 'biography', 'when the returned to Lerwick', 'when he returned to Lerwick'),
    ('people', 235, 'biography', 'They latter added', 'They later added'),
    ('people', 235, 'biography', 'In on 31 May 1919', 'On 31 May 1919'),
    ('people', 244, 'biography', 'aboard the Matagama', 'aboard the Metagama'),
    ('people', 246, 'biography', 'at the form of P & W. Anderson', 'at the firm of P & W. Anderson'),
    ('people', 246, 'biography', 'on Commericial Road', 'on Commercial Road'),
    ('people', 246, 'biography', 'In political, he was', 'In politics, he was'),
    ('people', 248, 'intro', 'His newphie,', 'His nephew,'),
    ('people', 266, 'biography', 'the Post Post Office', 'the Post Office'),
    ('people', 260, 'biography', 'joined the Euryallus', 'joined the Euryalus'),
    ('people', 260, 'biography', 'twice MPs for Orkney', 'twice MP for Orkney'),
    ('people', 267, 'biography', 'expended its offering', 'expanded its offering'),
    ('people', 286, 'biography', 'as heads postmaster', 'as head postmaster'),
    ('people', 291, 'biography', 'left Shetland as a young age', 'left Shetland at a young age'),
    ('people', 295, 'biography', 'At a boy he served', 'As a boy he served'),
    ('people', 312, 'biography', 'his wife margaret', 'his wife Margaret'),
    ('people', 324, 'biography', 'and after the was focussed', 'and after the war focussed'),
    ('people', 325, 'intro', 'was was a County Councillor', 'was a County Councillor'),
    ('people', 326, 'biography', 'this business is 1873', 'this business in 1873'),
    ('people', 329, 'biography', 'a partner in a a firm', 'a partner in a firm'),
    ('people', 329, 'biography', 'the Congressional Church', 'the Congregational Church'),
    ('people', 337, 'biography', 'among later day Faroe skippers', 'among latter-day Faroe skippers'),
    ('people', 339, 'biography', 'Shetland woolen products', 'Shetland woollen products'),
    ('people', 340, 'intro', 'Shetland Islands Counciller', 'Shetland Islands Councillor'),
    ('people', 341, 'death_place', 'Dunfries and Galloway', 'Dumfries and Galloway'),
    ('people', 342, 'death_place', 'Tillicoulty', 'Tillicoultry'),
    ('people', 346, 'biography', 'centered around Victoria Pier', 'centred around Victoria Pier'),
    ('people', 347, 'biography', 'Laurence believe that the Vega', 'Laurence believed that the Vega'),
    ('people', 347, 'biography', 'the who was elected to lead', 'who was elected to lead'),
    ('people', 350, 'biography', 'still a boy, the his family', 'still a boy, his family'),
    ('people', 350, 'biography', 'before coming a qualified sergeart', 'before becoming a qualified sergeant'),
    ('people', 353, 'biography', 'he server in the anti-aircraft', 'he served in the anti-aircraft'),
    ('people', 360, 'intro', 'a distance relative', 'a distant relative'),
    ('people', 366, 'intro', 'a distance relation', 'a distant relation'),
    ('people', 370, 'biography', 'he was against called for duty', 'he was again called for duty'),
    ('people', 381, 'biography', 'rejoin his Batallion', 'rejoin his Battalion'),
    ('people', 381, 'biography', 'completing his studied worked', 'completing his studies worked'),
    ('people', 381, 'biography', 'and as as adjutant', 'and as adjutant'),
    ('people', 383, 'biography', 'cattle and pony deals in Shetland', 'cattle and pony dealers in Shetland'),
    ('people', 389, 'biography', 'high lass boots', 'high class boots'),
    ('people', 389, 'biography', 'The Shetland News describe him', 'The Shetland News described him'),
    ('people', 397, 'biography', 'headed downstairs and check if', 'headed downstairs and checked if'),
    ('people', 397, 'biography', 'made an attempt as escaping', 'made an attempt at escaping'),
    ('people', 398, 'biography', 'until his retrial.', 'until his retiral.'),
    ('people', 403, 'biography', 'to have had held this title', 'to have held this title'),
    ('people', 406, 'biography', 'Baikie was oldest the son', 'Baikie was the oldest son'),
    ('people', 416, 'biography', 'the Procurate-Fiscal', 'the Procurator-Fiscal'),
    ('people', 416, 'biography', 'he continued extend his farmland', 'he continued to extend his farmland'),
    ('people', 417, 'biography', 'as he was call up to serve', 'as he was called up to serve'),
    ('people', 421, 'biography', 'at the Lyons Office', 'at the Lyon Office'),
    ('people', 438, 'biography', 'worked with Valetine & Sons', 'worked with Valentine & Sons'),
    ('people', 450, 'intro', 'the first women on the body', 'the first woman on the body'),
    ('people', 450, 'biography', 'where the lived in Gibblestone', 'where they lived in Gibblestone'),
    ('people', 459, 'biography', 'has spend his career', 'has spent his career'),
    ('people', 460, 'intro', 'Eupehemia Anderson', 'Euphemia Anderson'),
    ('people', 461, 'biography', "Anderson's first worked with", 'Anderson first worked with'),
    ('people', 461, 'biography', 'a great interest in agricultural', 'a great interest in agriculture'),
    ('people', 461, 'biography', 'the Tingwill Agricultural', 'the Tingwall Agricultural'),
    ('people', 465, 'biography', 'younger bother', 'younger brother'),
    ('people', 475, 'intro', '[person:john-sandison:John] was were both', '[person:john-sandison:John] were both'),
    ('people', 478, 'biography', 'purchased the the Old Manse', 'purchased the Old Manse'),
    ('people', 480, 'biography', 'East AFrica', 'East Africa'),
    ('people', 489, 'biography', 'to work in insurancem,', 'to work in insurance,'),
    ('people', 491, 'biography', 'William wat a member', 'William was a member'),
    ('people', 498, 'biography', 'Church in Kirkauldy', 'Church in Kirkcaldy'),
    ('people', 503, 'intro', 'and brother of [person:charles-ogilvy-ii:Charles]',
     'and sister of [person:charles-ogilvy-ii:Charles]'),
    ('people', 503, 'intro', 'were both. Lerwick Town Councillors', 'were both Lerwick Town Councillors'),
    ('people', 511, 'biography', 'to be the factory of the Sumburgh estate', 'to be the factor of the Sumburgh estate'),
    ('people', 513, 'biography', 'a rare occurence', 'a rare occurrence'),
    ('people', 521, 'death_place', 'Gisbourne', 'Gisborne'),
    ('people', 525, 'biography', 'being dischanged in', 'being discharged in'),
    ('people', 525, 'biography', 'AFter the water,', 'After the war,'),
    ('people', 526, 'biography', 'called up to the miliary', 'called up to the military'),
    ('people', 526, 'biography', 'Burma, Lybia, Germany', 'Burma, Libya, Germany'),
    ('people', 530, 'biography', '(now demolished}', '(now demolished)'),
    ('people', 533, 'intro', 'zero votes again victor', 'zero votes against victor'),
    # Arrived 1939 as a locum, moved on in 1945: the outbreak was WWII.
    ('people', 243, 'biography', 'Mid Yell at the outbreak of WWI.', 'Mid Yell at the outbreak of WWII.'),
    # Missing word; James confirmed "before".
    ('people', 347, 'biography', 'from Sandhurst just WWI,', 'from Sandhurst just before WWI,'),
    # Garbled company names. Merrylees is listed as Lerwick agent of the "Aberdeen, Leith and
    # Clyde Shipping Company" in Cornwall's New Aberdeen Directory 1853-54 (NLS). The New Zealand
    # Shipping Company ran 1873-1973, within Garriock's lifetime; "New ... New" was a doubled word.
    # "North Sea Fisher" is a clipped "Fishery"; the exact company names aren't confirmed.
    ('people', 512, 'intro', 'Aberdeen Leith and Lerwick Shipping Company', 'Aberdeen, Leith and Clyde Shipping Company'),
    ('people', 386, 'biography', 'the New Zealand New Shipping Company', 'the New Zealand Shipping Company'),
    ('people', 314, 'biography', 'the North Sea Fisher and', 'the North Sea Fishery and'),
    # Robert Douglas died at Fontenoy, 30 April 1745 (History of Parliament 1715-54; people.died_date
    # already 1745). His successor at the 1747 general election was James Halyburton.
    ('people', 414, 'intro', 'until his death in 747.', 'until his death in 1745.'),
    ('people', 414, 'intro', '[person:james-halyburton:John Halyburton]', '[person:james-halyburton:James Halyburton]'),
    # The wiki's copy of Fiona Sinclair's Essenquoy page (fionamsinclair.co.uk/genealogy/isles/
    # Essenquoy.htm) stops before the son's name. The page: "and was succeeded by his son III GILBERT
    # SINCLAIR, FIAR OF ESSENQUOY". fix_parse_errors.py then drops the Category trailer after it.
    ('people', 126, 'biography', 'was succeeded by his son ',
     'was succeeded by his son, Gilbert Sinclair, fiar of Essenquoy.'),
    ('elections', 21, 'notes', 'interpretation of the the procedures', 'interpretation of the procedures'),
    ('elections', 22, 'notes', 'interpretation of the the procedures', 'interpretation of the procedures'),
    ('elections', 22, 'notes', '2st Council Group', '2nd Council Group'),
    ('elections', 171, 'notes', 'filling the vanancy', 'filling the vacancy'),
]

# (table, column, old, new) applied to every row: the same slip repeated across many pages.
EVERYWHERE = [
    ('people', 'intro', 'neé', 'née'),
    ('people', 'biography', 'neé', 'née'),
]


def main():
    db = sqlite3.connect(DB_PATH)
    c = db.cursor()
    applied = already = 0
    for table, rid, col, old, new in FIXES:
        row = c.execute(f"SELECT {col} FROM {table} WHERE id = ?", (rid,)).fetchone()
        if row is None:
            raise SystemExit(f"{table}#{rid} not found")
        text = row[0] or ''
        n = text.count(old)
        if n == 1:
            c.execute(f"UPDATE {table} SET {col} = ? WHERE id = ?", (text.replace(old, new), rid))
            applied += 1
        elif n == 0 and new in text:
            already += 1
        else:
            raise SystemExit(f"{table}#{rid}.{col}: '{old}' found {n} times, expected 1")

    for table, col, old, new in EVERYWHERE:
        cur = c.execute(f"UPDATE {table} SET {col} = replace({col}, ?, ?) WHERE instr({col}, ?) > 0",
                        (old, new, old))
        applied += cur.rowcount

    db.commit()
    db.close()
    print(f"Spelling: {applied} applied, {already} already in place.")


if __name__ == '__main__':
    main()
