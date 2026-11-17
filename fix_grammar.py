#!/usr/bin/env python3
"""
Grammar and spelling sweep of every intro and biography, 2026-11-17 (prompted by George
Morrison's "His abiding interest was agriculture, who bred sheep, ...").

Same rules as fix_spelling.py: only slips whose fix is certain from the sentence. Run-on and
broken sentences, missing words and articles, "between X until Y", missing full stops and
unbalanced brackets or quotation marks, typos. Facts, names, dates, quotations, dialect and
variant spellings are left alone.

Runs last, after fix_parse_errors.py, because each `old` string is taken from the text as the
other correction scripts leave it. Idempotent: each fix is guarded on the old text.
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

# (table, id, column, old, new). `old` must occur exactly once in the field.
FIXES = [
    ('people', 1, 'biography', 'had Capt. Halcrow not had already died',
     'had Capt. Halcrow not already died'),  # doubled 'had'
    ('people', 2, 'intro', 'between 1949 to 1955',
     'between 1949 and 1955'),  # between...and
    ('people', 4, 'biography', "he went the St Andrew's",
     "he went to the St Andrew's"),  # missing 'to'
    ('people', 6, 'intro', 'between 1970 until 1978',
     'from 1970 until 1978'),  # between...until
    ('people', 9, 'biography', 'at 111 Commercial Street',
     'at 111 Commercial Street.'),  # missing full stop
    ('people', 9, 'intro', "'Sandy' Bennet, was",
     "'Sandy' Bennet was"),  # stray comma
    ('people', 12, 'biography', 'again between 1705 until',
     'again from 1705 until'),  # between...until
    ('people', 13, 'biography', 'Congressional church',
     'Congregational church'),  # wrong word
    ('people', 14, 'biography', 'His began his career',
     'He began his career'),  # typo
    ('people', 14, 'biography', 'to the UK again 1875',
     'to the UK again in 1875'),  # missing 'in'
    ('people', 15, 'intro', 'between 1826 until his death',
     'from 1826 until his death'),  # between...until
    ('people', 19, 'intro', 'between the years from 1945 until',
     'from 1945 until'),  # broken phrase
    ('people', 22, 'intro', 'was general merchant',
     'was a general merchant'),  # missing article
    ('people', 23, 'biography', 'He was invalidated out of the war',
     'He was invalided out of the war'),  # wrong word (invalided)
    ('people', 23, 'biography', 'in Lerwick He is said',
     'in Lerwick. He is said'),  # missing full stop
    ('people', 23, 'intro', 'When, in 1974 the County',
     'When, in 1974, the County'),  # unbalanced comma
    ('people', 24, 'biography', 'his fathers wood',
     "his father's wood"),  # missing apostrophe
    ('people', 24, 'biography', '[person:laurence-stove:Laurence], returned',
     '[person:laurence-stove:Laurence] returned'),  # stray comma
    ('people', 25, 'intro', 'from 1922 and 1929',
     'from 1922 to 1929'),  # from...to
    ('people', 27, 'biography', "twice removed to Andrew's wife)",
     "twice removed to Andrew's wife)."),  # missing full stop
    ('people', 27, 'intro', 'is a Shetland Islands Council for',
     'is a Shetland Islands Councillor for'),  # wrong word
    ('people', 28, 'biography', 'Parliament Election She appeared',
     'Parliament Election. She appeared'),  # missing full stop
    ('people', 32, 'biography', 'his uncle, James Clark in the',
     'his uncle, James Clark, in the'),  # unbalanced comma
    ('people', 36, 'biography', 'Lochend estate While',
     'Lochend estate. While'),  # missing full stop
    ('people', 42, 'intro', 'between 1974 until 1978',
     'from 1974 until 1978'),  # between...until
    ('people', 47, 'biography', 'Was ordained to Nesting',
     'He was ordained to Nesting'),  # missing subject
    ('people', 50, 'biography', 'as a teacher as the Moray House',
     'as a teacher at the Moray House'),  # wrong word
    ('people', 50, 'biography', 'Jim worked for with Shetland',
     'Jim worked with Shetland'),  # doubled preposition
    ('people', 54, 'biography', 'son of shepherd',
     'son of a shepherd'),  # missing article
    ('people', 59, 'intro', 'the Lairds He served',
     'the Lairds. He served'),  # missing full stop
    ('people', 59, 'intro', 'for the Orkney and Shetland from 1847',
     'for Orkney and Shetland from 1847'),  # stray article
    ('people', 62, 'biography', 'generous friend. This claim',
     'generous friend." This claim'),  # unclosed quotation
    ('people', 62, 'intro', 'a County Councillor His father',
     'a County Councillor. His father'),  # missing full stop
    ('people', 65, 'intro', '9 May 1952 Initially',
     '9 May 1952. Initially'),  # missing full stop
    ('people', 67, 'biography', 'Hoseason Gray, (b.',
     'Hoseason Gray (b.'),  # stray comma
    ('people', 67, 'biography', "was one Jakobsen's sources",
     "was one of Jakobsen's sources"),  # missing 'of'
    ('people', 69, 'biography', '10th. Baronet',
     '10th Baronet'),  # stray full stop
    ('people', 70, 'biography', 'his own shop a chemist',
     'his own shop as a chemist'),  # missing 'as'
    ('people', 70, 'intro', 'from 1887 to 1910',
     'from 1887 to 1910.'),  # missing full stop
    ('people', 73, 'biography', 'and brother of [person:charles-ogilvy-ii',
     'and sister of [person:charles-ogilvy-ii'),  # Charlotte is a sister, not brother
    ('people', 74, 'biography', 'Barbara was founding member',
     'Barbara was a founding member'),  # missing article
    ('people', 74, 'biography', 'War Savings Group She',
     'War Savings Group. She'),  # missing full stop
    ('people', 80, 'biography', 'around 1938, she was one',
     'around 1938 and was one'),  # comma splice
    ('people', 81, 'intro', 'Burgess Eunson, was',
     'Burgess Eunson was'),  # stray comma
    ('people', 84, 'intro', 'was a R. N. R.',
     'was an R. N. R.'),  # a/an
    ('people', 86, 'biography', 'upon death had been',
     'upon his death had been'),  # missing word
    ('people', 88, 'biography', 'farming in general merchandise',
     'farming and general merchandise'),  # wrong word
    ('people', 92, 'biography', 'in 1867, between the completion',
     'in 1867. Between the completion'),  # comma splice
    ('people', 95, 'biography', 'to disembark,but',
     'to disembark, but'),  # missing space after comma
    ('people', 95, 'biography', 'enter and leave all states of the tide',
     'enter and leave at all states of the tide'),  # missing word
    ('people', 96, 'biography', 'settled in Lerwick married Mary',
     'settled in Lerwick and married Mary'),  # missing word
    ('people', 100, 'biography', 'Charles first job',
     "Charles's first job"),  # missing possessive
    ('people', 101, 'intro', 'Mary Mundell .',
     'Mary Mundell.'),  # space before full stop
    ('people', 104, 'biography', 'saw the more than one third',
     'saw more than one third'),  # stray word
    ('people', 105, 'biography', 'began writing series of',
     'began writing a series of'),  # missing article
    ('people', 110, 'intro', 'between 1844 until his death',
     'from 1844 until his death'),  # between/until mismatch
    ('people', 111, 'intro', 'then again from 1893 to 1898',
     'then again from 1893 to 1898.'),  # missing full stop
    ('people', 124, 'intro', 'Council from 1938 to 1944',
     'Council from 1938 to 1944.'),  # missing full stop
    ('people', 127, 'biography', 'group of islanders wanted to fix',
     'group of islanders who wanted to fix'),  # missing word
    ('people', 129, 'intro', 'folklorist merchant',
     'folklorist, merchant'),  # missing comma in list
    ('people', 133, 'biography', 'where he also join the Council',
     'where he also joined the Council'),  # verb tense
    ('people', 134, 'biography', 'apology from him .',
     'apology from him.'),  # space before full stop
    ('people', 139, 'intro', 'between 1970 until his death',
     'from 1970 until his death'),  # between/until mismatch
    ('people', 147, 'biography', 'O.B.E in 1960 connection with',
     'O.B.E in 1960 in connection with'),  # missing word
    ('people', 149, 'intro', '[person:james-moodie:James Moodie] He was',
     '[person:james-moodie:James Moodie]. He was'),  # missing full stop
    ('people', 154, 'intro', 'first 1919 and 1922 then 1929',
     'first between 1919 and 1922 then from 1929'),  # missing words
    ('people', 157, 'intro', 'George Laurence was grocer',
     'George Laurence was a grocer'),  # missing article
    ('people', 158, 'intro', 'George Leslie was farmer',
     'George Leslie was a farmer'),  # missing article
    ('people', 159, 'biography', 'shoals of herring. Then relay',
     'shoals of herring, then relay'),  # sentence fragment
    ('people', 162, 'biography', '(b.11 August 1868, Dunrossness.',
     '(b. 11 August 1868, Dunrossness).'),  # unbalanced bracket, missing space
    ('people', 164, 'biography', 'while on visiting on holiday',
     'while visiting on holiday'),  # stray word
    ('people', 164, 'biography', 'his parents property which they both passed away in between',
     "his parents' property, in which they both passed away between"),  # possessive and word order
    ('people', 165, 'biography', 'left Shetland he went to manage',
     'left Shetland and went to manage'),  # run-on
    ('people', 169, 'biography', 'acquiring the re-opening the premises',
     'acquiring and re-opening the premises'),  # wrong word
    ('people', 170, 'biography', "his father, Laurence's shop",
     "his father Laurence's shop"),  # stray comma
    ('people', 172, 'intro', 'between 1820 to 1829',
     'from 1820 to 1829'),  # between/to mismatch
    ('people', 172, 'intro', 'and brother of [person:charles-ogilvy-ii',
     'and sister of [person:charles-ogilvy-ii'),  # wrong word (Elizabeth is his sister)
    ('people', 177, 'intro', 'between 1835 until his death',
     'from 1835 until his death'),  # between/until mismatch
    ('people', 179, 'intro', 'former Master Mariner Shetland',
     'former Master Mariner and Shetland'),  # missing word
    ('people', 180, 'biography', 'end of the war, she hoped',
     'end of the war, and she hoped'),  # comma splice
    ('people', 180, 'intro', 'Thomson Halcrow, was',
     'Thomson Halcrow was'),  # stray comma
    ('people', 182, 'intro', 'between 1955 and 1964',
     'between 1955 and 1964.'),  # missing full stop
    ('people', 184, 'intro', '1959 to 1975 He was',
     '1959 to 1975. He was'),  # missing full stop
    ('people', 188, 'intro', 'from 1942 and 1949.',
     'from 1942 to 1949.'),  # from...and -> from...to
    ('people', 195, 'biography', 'moved to the Education Officer where',
     'moved to the Education Office where'),  # wrong word: Officer -> Office
    ('people', 203, 'intro', '[person:william-adie:William Adie] had resigned',
     '[person:william-adie:William Adie], had resigned'),  # unbalanced appositive comma
    ('people', 205, 'intro', '[person:thomas-anderson-iii:Thomas] were also',
     '[person:thomas-anderson-iii:Thomas], were also'),  # unbalanced appositive comma
    ('people', 210, 'intro', 'Aithsting was a doctor',
     'Aithsting) was a doctor'),  # unbalanced bracket
    ('people', 213, 'intro', 'ward for from 2007',
     'ward from 2007'),  # stray 'for'
    ('people', 217, 'biography', 'In 1746 was appointed',
     'In 1746 he was appointed'),  # missing subject
    ('people', 217, 'biography', 'In 1757 served',
     'In 1757 he served'),  # missing subject
    ('people', 219, 'biography', "worked with him in Affleck's passing",
     "worked with him until Affleck's passing"),  # wrong word: in -> until
    ('people', 221, 'biography', 'about Shetland was acquired by Lerwick Town Council, and is now',
     'about Shetland, which was acquired by Lerwick Town Council and is now'),  # broken sentence: missing 'which'
    ('people', 221, 'biography', "Shetland Archives James's father",
     "Shetland Archives. James's father"),  # missing full stop
    ('people', 222, 'intro', '[person:archibald-greig:Archibald] was',
     '[person:archibald-greig:Archibald], was'),  # unbalanced appositive comma
    ('people', 223, 'intro', 'Grierson . The',
     'Grierson. The'),  # space before full stop
    ('people', 226, 'intro', 'from 1937 and 1945',
     'from 1937 to 1945'),  # from...and -> from...to
    ('people', 228, 'intro', 'between 1835 until',
     'from 1835 until'),  # between...until -> from...until
    ('people', 234, 'biography', 'messages to business all over',
     'messages to businesses all over'),  # plural
    ('people', 234, 'biography', 'and giving up his Labour',
     'and gave up his Labour'),  # verb form
    ('people', 234, 'intro', 'In 1999 Nesting, Whiteness, Girlsta and Gott from 1999 until',
     'He represented Nesting, Whiteness, Girlsta and Gott from 1999 until'),  # broken sentence: no verb
    ('people', 235, 'biography', 'did not enjoy career',
     'did not enjoy the career'),  # missing word
    ('people', 235, 'biography', '£2,951 worth around',
     '£2,951, worth around'),  # run-together clause
    ('people', 236, 'biography', 'in 1925 as result of',
     'in 1925 as a result of'),  # missing 'a'
    ('people', 236, 'intro', 'resignation in 1950',
     'resignation in 1950.'),  # missing full stop
    ('people', 243, 'biography', 'in Uyeasound, he was transferred',
     'in Uyeasound, and was transferred'),  # comma splice
    ('people', 246, 'biography', 'In politics, he was a Liberal',
     'In politics, he was a Liberal.'),  # missing full stop
    ('people', 250, 'biography', 'in Shetland, he was President',
     'in Shetland: he was President'),  # comma splice
    ('people', 252, 'intro', '[person:john-ogilvy:John] were also councillors',
     '[person:john-ogilvy:John], were also councillors.'),  # appositive comma; missing full stop
    ('people', 258, 'intro', 'between 1919 until he resigned',
     'from 1919 until he resigned'),  # between...until -> from...until
    ('people', 261, 'biography', '[person:magnus-shearer-i:Magnus] was',
     '[person:magnus-shearer-i:Magnus], was'),  # unbalanced appositive comma
    ('people', 262, 'intro', '[person:william-anderson-ii:Bill Anderson]',
     '[person:william-anderson-ii:Bill Anderson].'),  # missing full stop
    ('people', 266, 'biography', 'Norman Bonney the Balerno ward',
     'Norman Bonney in the Balerno ward'),  # missing word
    ('people', 268, 'biography', 'five different proprietors the main two were Foster of Burrastow, Anderton of Vaila.',
     'five different proprietors; the main two were Foster of Burrastow and Anderton of Vaila.'),  # run-on sentence
    ('people', 268, 'biography', 'Vaila estate.’',
     'Vaila estate.'),  # stray quote mark
    ('people', 271, 'intro', 'from 1930 and 1932',
     'from 1930 to 1932'),  # from...and -> from...to
    ('people', 272, 'intro', 'Town Councillor. .',
     'Town Councillor.'),  # stray full stop
    ('people', 277, 'biography', 'with more than 33 as an',
     'with more than 33 years as an'),  # missing word
    ('people', 278, 'biography', 'he elected to the Lerwick North seat',
     'he was elected to the Lerwick North seat'),  # missing word
    ('people', 280, 'biography', 'John Bruce the Younger He was',
     'John Bruce the Younger. He was'),  # missing full stop
    ('people', 281, 'intro', 'between 1971 to 1975',
     'between 1971 and 1975'),  # between...and
    ('people', 288, 'biography', 'M.A.in 1950',
     'M.A. in 1950'),  # missing space
    ('people', 291, 'intro', 'between 1907 to 1913',
     'between 1907 and 1913'),  # between...and
    ('people', 293, 'biography', 'John spend his',
     'John spent his'),  # tense typo
    ('people', 295, 'biography', 'the Principle of Messrs',
     'the Principal of Messrs'),  # wrong word
    ('people', 301, 'biography', 'involved in planning of the New Town',
     'involved in the planning of the New Town'),  # missing word
    ('people', 312, 'biography', 'Soundside, Weisdale the present business',
     'Soundside, Weisdale. The present business'),  # run-on
    ('people', 314, 'biography', 'as the Chief Magistrates of the Burgh',
     'as the Chief Magistrate of the Burgh'),  # number agreement
    ('people', 314, 'biography', 'Justice of the Peace. As well as holding',
     'Justice of the Peace, as well as holding'),  # sentence fragment
    ('people', 314, 'biography', 'United Presbyterian, His nephew',
     'United Presbyterian. His nephew'),  # comma for full stop
    ('people', 315, 'intro', "'John Robertson Jr', this is due",
     "'John Robertson Jr'. This is due"),  # comma splice
    ('people', 317, 'biography', "the North of Scotland's company's monopoly",
     "the North of Scotland company's monopoly"),  # doubled possessive
    ('people', 317, 'intro', 'and a Lerwick Town Councillor',
     'and a Lerwick Town Councillor.'),  # missing full stop
    ('people', 320, 'intro', 'between 1904 to 1906',
     'between 1904 and 1906'),  # between...and
    ('people', 321, 'biography', 'in Lerwick between 1889 until',
     'in Lerwick from 1889 until'),  # between...until
    ('people', 324, 'biography', 'John, and Yell man',
     'John and Yell man'),  # stray comma
    ('people', 325, 'biography', 'Committee tables, and any business',
     'Committee tables", and any business'),  # unbalanced quotation marks
    ('people', 330, 'biography', 'In 1948, then founded',
     'In 1948, he founded'),  # missing subject
    ('people', 333, 'biography', 'in Ulsta, hired John',
     'in Ulsta, he hired John'),  # missing subject
    ('people', 335, 'biography', 'Joseph began working [person:',
     'Joseph began working for [person:'),  # missing word
    ('people', 341, 'biography', 'it was established that as Thomson had failed',
     'it was established that Thomson had failed'),  # stray word
    ('people', 342, 'biography', 'inducted in to North Yell',
     'inducted into North Yell'),  # into
    ('people', 353, 'biography', 'Until the breakout of WWI',
     'Until the outbreak of WWI'),  # wrong word
    ('people', 353, 'intro', 'Guizer Jarl in 1914 a member',
     'Guizer Jarl in 1914 and a member'),  # missing word
    ('people', 355, 'biography', 'in commercial Street',
     'in Commercial Street'),  # proper noun capital
    ('people', 359, 'intro', 'Willie Tait. between 1964',
     'Willie Tait. Between 1964'),  # sentence capital
    ('people', 361, 'intro', 'from 1990 until 1999',
     'from 1990 until 1999.'),  # missing full stop
    ('people', 364, 'intro', '(Neé Stewart)',
     '(née Stewart)'),  # misspelt née
    ('people', 368, 'intro', 'Magnus Laurence Manson, son of',
     'Magnus Laurence Manson was the son of'),  # sentence fragment
    ('people', 371, 'biography', 'Public School,and the',
     'Public School, and the'),  # missing space
    ('people', 376, 'biography', 'Shetland Arts Board,',
     'Shetland Arts Board.'),  # missing full stop at end
    ('people', 379, 'biography', 'Viking Energy windfarm He was',
     'Viking Energy windfarm. He was'),  # run-on, missing full stop
    ('people', 389, 'biography', 'shoemakers apprentice',
     "shoemaker's apprentice"),  # missing apostrophe
    ('people', 391, 'intro', 'resignation in 1960',
     'resignation in 1960.'),  # missing full stop at end
    ('people', 395, 'biography', 'He spent all his working life in Lerwick, getting his first job in 1929 after leaving school aged 14 was as',
     'He spent all his working life in Lerwick. His first job, in 1929 after leaving school aged 14, was as'),  # broken sentence
    ('people', 395, 'biography', 'by the Rev, George Smith',
     'by the Rev. George Smith'),  # comma for full stop in abbreviation
    ('people', 395, 'biography', 'a nurse from Yell, this was in the Congregational Church by Rev, H Woodhouse',
     'a nurse from Yell. This was in the Congregational Church by Rev. H Woodhouse'),  # comma splice; comma for full stop in abbreviation
    ('people', 397, 'biography', 'Gilbert, (10)',
     'Gilbert (10)'),  # stray comma
    ('people', 397, 'biography', 'at his grandparents house',
     "at his grandparents' house"),  # missing apostrophe
    ('people', 397, 'biography', 'more than one job, Peter was no exception',
     'more than one job; Peter was no exception'),  # comma splice
    ('people', 397, 'biography', 'had run into find his mother',
     'had run in to find his mother'),  # run into -> run in to
    ('people', 397, 'intro', 'between 1850 until his death',
     'from 1850 until his death'),  # between...until
    ('people', 398, 'intro', 'Prophet James Laurence Smith, was',
     'Prophet James Laurence Smith was'),  # stray comma
    ('people', 402, 'biography', 'in the 1969 was a teacher',
     'in 1969 was a teacher'),  # stray 'the'
    ('people', 403, 'biography', 'to prove his point. In 1965',
     'to prove his point.&quot; In 1965'),  # unbalanced quotation mark
    ('people', 406, 'biography', "by 11 votes to the 5 won Sir Lawrence's",
     "by 11 votes to the 5 won by Sir Lawrence's"),  # missing 'by'
    ('people', 406, 'biography', 'stood again at the 1784, but',
     'stood again at the 1784 general election, but'),  # missing words
    ('people', 406, 'intro', ', he serving until 1781 when the seat was taken by [person:charles-dundas:Charles Dundas]',
     ', serving until 1781 when the seat was taken by [person:charles-dundas:Charles Dundas].'),  # stray 'he'; missing full stop at end
    ('people', 407, 'intro', 'a merchant seaman County Councillor',
     'a merchant seaman and County Councillor'),  # missing 'and'
    ('people', 411, 'biography', 'Lord Lieutenant for Shetland] from',
     'Lord Lieutenant for Shetland from'),  # stray bracket
    ('people', 411, 'biography', 'passed in to the hands',
     'passed into the hands'),  # in to -> into
    ('people', 414, 'intro', 'Robert Douglas, was',
     'Robert Douglas was'),  # stray comma
    ('people', 416, 'intro', 'first between 1892 - 1895 and 1898 and 1913',
     'first between 1892 and 1895, then between 1898 and 1913'),  # garbled date ranges
    ('people', 428, 'intro', '( b. circa',
     '(b. circa'),  # stray space in bracket
    ('people', 429, 'biography', 'After a few years moved back',
     'After a few years he moved back'),  # missing subject
    ('people', 430, 'intro', 'appointments including; district',
     'appointments including: district'),  # semicolon for colon
    ('people', 431, 'biography', 'Scalloway Congressional Church',
     'Scalloway Congregational Church'),  # wrong word (Congregational)
    ('people', 431, 'intro', 'between 1909 to 1913',
     'between 1909 and 1913'),  # between...to
    ('people', 433, 'biography', 'early adopts of cars sales',
     'early adopters of car sales'),  # wrong word forms
    ('people', 437, 'intro', '(where the The Stage Door',
     '(where The Stage Door'),  # doubled 'the'
    ('people', 437, 'intro', 'jewellers), in later life he was',
     'jewellers). In later life he was'),  # run-on sentence
    ('people', 437, 'intro', 'between 1919 to 1936',
     'from 1919 to 1936'),  # between...to
    ('people', 442, 'biography', 'mail to the rural districts were carried',
     'mail to the rural districts was carried'),  # subject-verb agreement
    ('people', 445, 'biography', 'Aberdeen General Hospital Ronald was',
     'Aberdeen General Hospital. Ronald was'),  # missing full stop
    ('people', 445, 'biography', 'Shetland branch of the British Medical Association',
     'Shetland branch of the British Medical Association.'),  # missing full stop at end
    ('people', 445, 'intro', 'until 1975. and the Lerwick Breiwick seat',
     'until 1975, and won the Lerwick Breiwick seat'),  # broken sentence, missing verb
    ('people', 448, 'intro', 'between 1850 until his death',
     'from 1850 until his death'),  # between...until
    ('people', 449, 'biography', 'was central Zoroastrian thought',
     'was central to Zoroastrian thought'),  # missing 'to'
    ('people', 450, 'biography', 'Mary B. Usher, she was',
     'Mary B. Usher. She was'),  # comma splice
    ('people', 450, 'biography', 'County Buildings on Lerwick were flown',
     'County Buildings in Lerwick was flown'),  # on->in; subject-verb agreement
    ('people', 452, 'biography', '2 February 1912, he transferred',
     '2 February 1912. He transferred'),  # comma splice
    ('people', 455, 'intro', 'between 1945 and 1949',
     'between 1945 and 1949.'),  # missing full stop
    ('people', 457, 'intro', 'until 2011..',
     'until 2011.'),  # double full stop
    ('people', 458, 'biography', 'educated in Shetland and Glasgow University',
     'educated in Shetland and at Glasgow University'),  # missing word
    ('people', 459, 'intro', 'representing the Shetland West',
     'representing Shetland West'),  # stray article
    ('people', 464, 'biography', 'Vice-President i of',
     'Vice-President of'),  # stray letter
    ('people', 464, 'biography', "Directors ' of",
     'Directors of'),  # stray apostrophe
    ('people', 464, 'biography', "Relief' Fund",
     'Relief Fund'),  # stray apostrophe
    ('people', 464, 'intro', 'served as Chief Magistrates of',
     'served as Chief Magistrate of'),  # plural slip
    ('people', 465, 'biography', 'Young Tomas',
     'Young Thomas'),  # misspelling
    ('people', 465, 'intro', '[person:thomas-dundas-ii:Thomas] He was',
     '[person:thomas-dundas-ii:Thomas]. He was'),  # missing full stop
    ('people', 466, 'biography', 'the french commander',
     'the French commander'),  # proper adjective capital
    ('people', 466, 'biography', 'birds of the air" The',
     'birds of the air". The'),  # missing full stop after quotation
    ('people', 466, 'biography', 'St. Pauls Cathedral',
     "St. Paul's Cathedral"),  # missing apostrophe
    ('people', 466, 'intro', '[person:john-balfour:John Balfour]',
     '[person:john-balfour:John Balfour].'),  # missing full stop
    ('people', 470, 'biography', 'after his with Jessie',
     'after his wife Jessie'),  # wrong word
    ('people', 473, 'intro', "His father Thomas Manson, publisher and author, his mother Margaret Elizabeth Lillias (née Crutwell), . 'Mortie'",
     "His father was Thomas Manson, publisher and author, and his mother Margaret Elizabeth Lillias (née Crutwell). 'Mortie'"),  # broken sentence, stray punctuation
    ('people', 474, 'intro', 'Customs Officer as a Lerwick',
     'Customs Officer and a Lerwick'),  # wrong word
    ('people', 475, 'biography', 'estate valued £9,350',
     'estate valued at £9,350'),  # missing word
    ('people', 475, 'intro', 'Thomas Johnson Sandison son of [person:alexander-sandison:Alexander Sandison] was',
     'Thomas Johnson Sandison, son of [person:alexander-sandison:Alexander Sandison], was'),  # missing commas around apposition
    ('people', 476, 'intro', 'between 1936 to 1938',
     'between 1936 and 1938'),  # between...and
    ('people', 478, 'intro', "'Tom' Henderson, was",
     "'Tom' Henderson was"),  # stray comma
    ('people', 481, 'intro', 'Adie was merchant',
     'Adie was a merchant'),  # missing article
    ('people', 482, 'biography', 'He was as Gordon Highlanders',
     'He was with the Gordon Highlanders'),  # garbled words
    ('people', 482, 'biography', 'During the WWII',
     'During WWII'),  # stray article
    ('people', 485, 'intro', 'between 1890 and 1904',
     'between 1890 and 1904.'),  # missing full stop
    ('people', 486, 'intro', 'Town Councilor.',
     'Town Councillor.'),  # misspelling (site uses Councillor)
    ('people', 487, 'biography', 'in 1922 wherein he was',
     'in 1922, whereupon he was'),  # wrong word
    ('people', 493, 'intro', '1686-1691 also farmed',
     '1686-1691, and also farmed'),  # run-on
    ('people', 501, 'biography', 'After leaving work, he began working',
     'After leaving school, he began working'),  # wrong word
    ('people', 507, 'biography', 'teaching role Yell',
     'teaching role in Yell'),  # missing word
    ('people', 507, 'biography', 'life in confined to',
     'life confined to'),  # stray word
    ('people', 508, 'intro', 'Commercial Bank Lerwick Town',
     'Commercial Bank and a Lerwick Town'),  # missing words
    ('people', 517, 'biography', 'on the 25 March 1981',
     'on 25 March 1981'),  # stray article
    ('people', 520, 'intro', 'Sievwright, and his son',
     'Sievwright and his son'),  # stray comma
    ('people', 520, 'intro', 'a Feuror and',
     'a Feuar and'),  # misspelling
    ('people', 524, 'intro', 'between 1907 until',
     'from 1907 until'),  # from...until
    ('people', 525, 'biography', 'employed in South Shields with the Ministry of Labour in South Shields',
     'employed with the Ministry of Labour in South Shields'),  # repeated phrase
    ('people', 527, 'biography', 'a descendent of',
     'a descendant of'),  # misspelling (noun)
    ('people', 530, 'biography', 'went in to business',
     'went into business'),  # into
    ('people', 530, 'biography', 'developed in to goods',
     'developed into goods'),  # into
    ('people', 531, 'intro', 'between 1973 and 1975,',
     'between 1973 and 1975.'),  # comma for full stop at end
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
        if n == 1 and not (new in text and old in new):
            c.execute(f"UPDATE {table} SET {col} = ? WHERE id = ?", (text.replace(old, new), rid))
            applied += 1
        elif new in text:
            already += 1
        else:
            raise SystemExit(f"{table}#{rid}.{col}: '{old}' found {n} times, expected 1")
    db.commit()
    db.close()
    print(f"Grammar: {applied} applied, {already} already in place.")


if __name__ == '__main__':
    main()
