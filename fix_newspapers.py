#!/usr/bin/env python3
"""
Corrections to shetland.db from newspaper reports. Runs after fix_minute_book.py and is
idempotent: re-running does nothing once the corrections are in place.

1. November 1884 Lerwick Town Council election. Shetland Times, 1 Nov 1884: "Tuesday first,
   the 4th instant, is the date for the annual election ... The four gentlemen falling to
   retire this year are Bailies Robertson and Hay and Messrs John Robertson, jr., and William
   Duncan." So:
   - the general election was on 4 November 1884, not 4 December;
   - the William Duncan standing was the sitting councillor elected in 1881, William Duncan (i),
     the Lerwick grocer, not William Duncan (ii) of Scalloway. The same goes for the
     by-election co-option that followed.
   Arthur Hay stays elected: he topped the poll (87) and then declined to take office, which is
   recorded as an election note. The co-option filling his seat was on 22 November 1884
   (newspaper, 22 Nov 1884, per CLAUDE.md), not the 11th.

2. November 1936 Lerwick Town Council co-option of Erling Clausen. Shetland Times preview of
   the 3 Nov 1936 election: "Only four candidates have been nominated for the five vacancies",
   so one seat was left empty at the general and Clausen's co-option on 10 November filled it.
   He did not replace Laurence Cogle: Cogle's seat had gone to T. A. Sinclair, co-opted in July
   1936 and re-elected at the general.

3. August 1938 Lerwick Town Council co-option of John A. Williamson, which the wiki doesn't
   record. Shetland Times, 27 Aug 1938: "A special meeting of Lerwick Town Council was held on
   Thursday of last week" (18 August), where "it was resolved to co-opt Mr John A. Williamson, the
   unsuccessful candidate at the last Council election receiving the highest number of votes", in
   the room of the late Councillor A. S. Manson (died 22 July 1938). He is listed as a retiring
   councillor at the November 1938 election, standing for Labour (Shetland Times, 29 Oct 1938).
   His profile's "Lerwick Town Councillor between 1941 and 1945" gains the 1938 spell.

4. William MacDougall's resignation from Lerwick Town Council. His profile says April 1912. He
   tendered it on 2 April 1912 but "had reconsidered his decision to resign" by that Friday
   (Shetland Times, 6 Apr 1912), attended the 1 October 1912 meeting (Shetland Times, 5 Oct 1912),
   and resigned on Tuesday 8 October 1912 (Shetland Times, 12 Oct 1912). Evidence with BNA links:
   research/bna/ltc-1912-1914.md.

5. The May 1957 Lerwick Town Council election. The wiki lists Grace Halcrow among the four
   returned unopposed. The Shetland Times, 19 Apr 1957, gives the four nominations: Robert B.
   Blance, Alexander Morrison, Andrew J. Nicolson and Robert Ollason. Its preview of the 1964
   election (17 Apr 1964) says Halcrow had been elected ten years before and served only one year
   before resigning to become county councillor. So the 1957 seat was Nicolson's (Labour, like his
   other candidacies), and her profile's "from 1957" becomes 1954-55 and from 1964. She retired by
   rotation in May 1970 and did not re-stand (Shetland Times, 13 Mar and 17 Apr 1970), so "till the
   late 1960s" becomes "to 1970" (research/bna/ltc-1969-1973.md).

6. The "Lerwick Town Council By-Election May 1961" was a co-option in June. The Rev. Kenneth
   Thomson resigned at the statutory meeting on Friday 5 May 1961, being ineligible (Shetland Times,
   12 May 1961). Robert Strachan was co-opted at "Tuesday's Town Council meeting" (Shetland Times,
   16 Jun 1961), i.e. 13 June 1961.

Evidence for 5 and 6 with BNA links: research/bna/ltc-1955-1965.md.

7. Lerwick Town Council polling days 1949-1964. From 1949 the elections were in May, on the
   Tuesday. The baseline dates twelve of them on the Monday before. The Shetland Times gives the
   Tuesday each year: "Tuesday first is polling day" (29 Apr 1949, 1 May 1953, 29 Apr 1955);
   "Tuesday, 2nd May" (21 Apr 1950); "Tuesday 6th May" (2 May 1952); "following Tuesday's
   municipal election" (7 May 1954); "poor weather on Tuesday" (9 May 1958); "5th May" (1 May
   1959); Tuesday 3 May 1960 (15 Apr 1960); "last Tuesday's poll" (12 May 1961); Tuesday 7 May
   1963 (19 Apr 1963); "Tuesday, 5th of May" (1 May 1964). Arthur Johnson's co-option in room of
   James Brownlie (September 1949) was at the monthly meeting "on Tuesday", 13 September 1949
   (Shetland Times, 16 Sep 1949), not Monday the 12th. Evidence: research/bna/ltc-election-dates-1949-1964.md.

8. The May 1954 Lerwick Town Council election was contested. The wiki has the four winners
   unopposed. Shetland Times, 7 May 1954, "Senior Bailie loses his seat", THE RESULT: A. Morrison
   (Soc.) 1031, Miss G. Halcrow (Ind) 1013, R. B. Blance (Soc.) 1010, R. Ollason (Ind) 929;
   unsuccessful J. Inkster (Ind) 813, R. Strachan (Soc.) 719, J. B. A. Sutherland (Ind) 704,
   J. Gair (Soc.) 692. "There are 3950 on the roll, but of these 32 are not eligible to vote until
   the autumn", an effective 3918; 1913 voted, including 16 postal votes. The defeated Senior
   Bailie was "Mr John N. Inkster"; Strachan and Gair "were also unsuccessful last year" (1953).
   Party labels follow the wiki's for the same candidates (Soc. = Labour).
   The "James Inkster" elected in May 1951 was the same man: the nominations (Shetland Times,
   13 Apr 1951) list the four retiring members, among them "JOHN N. INKSTER, Cairnfield", junior
   Bailie. Evidence: research/bna/ltc-1954-result.md.

9. Grace Halcrow on Zetland County Council. Her profile says County Councillor for Cunningsburgh
   "between 1955 and 1961". She was opposed by Mrs Joan McLeod in 1958 (Shetland Times, 25 Apr
   1958, "Six County Council elections"), then "Miss Grace Halcrow has withdrawn from
   Cunningsburgh, leaving Mrs Joan MacLeod returned unopposed" (Shetland Times, 9 May 1958). The
   1964 preview (17 Apr 1964) says she served one three-year term. So 1955-58; the election data
   (McLeod unopposed 1958) was already right. Evidence: research/bna/grace-halcrow-county.md.

10. The October 1941 Lerwick Town Council co-options (John Gear and Charles Inkster, for the
   resigned J. L. Linklater and Thomas Irvine) were made "at Tuesday's meeting of the Council"
   (Shetland Times, Saturday 11 Oct 1941): Tuesday 7 October, not Thursday the 9th.
   Evidence: research/bna/ltc-wartime-1941.md.

11. The "Lerwick Town Council By-Election May 1970" (Robert Adair, for John R. Smith) is dated
   8 August 1970 in the baseline. Smith's letter of resignation was read "on Tuesday night"
   (Shetland Times, 17 Apr 1970), 14 April, hours after nominations closed. Adair was co-opted "at
   Friday's statutory meeting" (Shetland Times, 15 May 1970), 8 May 1970, as the unsuccessful
   candidate with most votes at the 5 May election. Evidence: research/bna/ltc-1969-1973.md.

12. Zetland County Council polling days 1949-1964. The baseline dates five generals on a Monday
   and 1955 on a Thursday. Each was on a Tuesday, a week after the Town Council's except in 1958 and
   1964: "the County Council election on 10th May" (Shetland Times, 29 Apr 1949); "the county will
   have its turn on Tuesday first" (9 May 1952), i.e. 13 May; "Tuesday first, 10th" (6 May 1955);
   "Polling took place ... on Tuesday" (16 May 1958), 13 May; elections "a week from Tuesday"
   (28 Apr 1961), 9 May, with the count on Wednesday 10 May (12 May 1961); "Tuesday first" (8 May
   1964), 12 May. Each general is one row per ward, so every row moves. Evidence:
   research/bna/zcc-election-dates-1949-1964.md.

13. The May 1968 Lerwick Town Council election is dated Thursday 2 May 1968 in the baseline.
   "Polling takes place in Lerwick on 7th May" (Shetland Times, 19 Apr 1968), a Tuesday like every
   other year from 1949. The result (Shetland Times, 10 May 1968) matches the baseline's votes.
   Evidence: research/bna/ltc-1968-polling-day.md.

14. The "Whalsay And Skerries County Council By-Election May 1947" (Magnus Shearer, for Jamieson) is
   dated 1 May 1947 in the baseline, which is month precision only. It was an appointment on petition
   at "Tuesday's meeting of Zetland County Council" (Shetland Times, Friday 23 May 1947), i.e. 20 May:
   "The petition for the election of Lt. Col. Shearer to the vacancy was signed by 207 persons", and
   his appointment was moved "following acceptance of Mr Jamieson's resignation". He is also in that
   meeting's attendance list, as is a "James Jamieson". Evidence: research/bna/ltc-1947-shearer-dalziel.md.

15. The "Yell South County Council By-Election August 1950" (John Williamson (iv)) records George
   Ross as the member replaced. Ross sat for Tingwall: his resignation was accepted at the County
   Council meeting of Tuesday 20 June 1950 (Shetland Times, 23 Jun 1950), and G. K. Spence, "already a
   member of the County Council representing South Yell", was petitioned for Tingwall. "Mr Spence
   recently resigned his Yell seat to contest Tingwall" (Shetland Times, 21 Jul 1950), and both polls
   were on Tuesday 1 August (Shetland Times, 28 Jul and 4 Aug 1950). So the Yell South seat was
   Spence's. The wiki text says the same. Evidence: research/bna/zcc-by-elections.md.

16. Lerwick Town Council co-options 1886-87. Robertson and Anderson were "elected to fill the
   vacancies at the Council Board" at the meeting "on Friday evening" (Shetland Times, Saturday
   20 Nov 1886), i.e. 19 November, not the 24th. Porteous was elected "till the vacancy caused by
   Mr Hunter's resignation" at the fortnightly meeting held "yesterday (Friday) evening" (Shetland
   Times, 19 Mar 1887), i.e. 18 March, not the 24th. The Hunter who resigned (accepted at the
   meeting of Tuesday 4 Jan 1887, on his move to the Union Bank at Portsoy; Shetland Times, 11 Dec
   1886 and 8 Jan 1887) is James Hunter (ii), the bank accountant, not the GP born in 1914.
   Evidence: research/bna/ltc-1885-1889.md.

17. Lerwick Town Council general of November 1889 was polled on Tuesday 5 November, not Saturday
   the 9th (the day the result was published). The Burgh notice gives the "municipal election on
   Tuesday next" and the week's diary has "Voting Tuesday, in the Burgh Court Room" (Shetland
   Times, 2 Nov 1889). Evidence: research/bna/ltc-general-dates-1885-1891.md.

18. Lerwick Town Council co-option of February 1938. Peter Dalziel was co-opted "in place of Mr
   E. J. F. Clausen, resigned" at the monthly meeting held "on Tuesday evening" (Shetland Times,
   Saturday 5 Feb 1938), i.e. 1 February, not Thursday the 3rd. Clausen's resignation letter had
   been read at the monthly meeting of Tuesday 4 January (Shetland Times, 8 Jan 1938).
   Evidence: research/bna/ltc-1936-1945.md.

19. Lerwick Town Council, 1940-45. William Sinclair tendered his resignation in writing at a
   special meeting on Friday 24 May 1940, was asked to reconsider, and adhered to it at the June
   monthly meeting, where the seat was declared vacant (Shetland Times, 25 May and 8 Jun 1940).
   Andrew Mouat, the highest unsuccessful candidate of Nov 1938, was co-opted at the meeting on
   "Tuesday evening of last week" (Shetland Times, 13 Jul 1940), i.e. 2 July, not 6 August.
   T. A. Sinclair was co-opted at a special meeting "on Tuesday, 31st March" 1942 (Shetland Times,
   4 Apr 1942), not 9 April. Alexander Morrison was co-opted for Prophet Smith at the monthly
   meeting "on Tuesday" (Shetland Times, 9 Feb 1945), i.e. 6 February, not Monday the 5th. The
   first post-war general was polled on Tuesday 6 November 1945, not Monday the 5th (Shetland
   Times, 9 Nov 1945). Evidence: research/bna/ltc-1940-1945.md.

20. Lerwick Town Council, June 1921. J. M. Goodlad was co-opted for J. J. Pottinger (who had
   retired) at the monthly meeting "on Tuesday evening" (Shetland Times, 11 Jun 1921), i.e.
   7 June, not the 11th (the paper's publication day). The vote was five each for Goodlad and
   William Bruce, and the Provost gave his casting vote for Goodlad.
   Evidence: research/bna/ltc-1919-1921.md.

21. Lerwick Town Council general of November 1901 was polled on Tuesday 5 November, not Friday
   the 1st. The Burgh notice has the "Municipal Election on TUESDAY NEXT" (Shetland Times, 2 Nov
   1901), and the result was declared that evening (Shetland Times, 9 Nov 1901).
   Evidence: research/bna/ltc-1894-1907.md.

22. Lerwick Town Council co-option of January 1910. James A. Loggie was appointed in place of
   John G. Irvine, resigned, at a special meeting "held on Tuesday night, before the ordinary
   meeting" (Shetland Times, 8 Jan 1910), i.e. 4 January, not the 1st. Irvine's letter of
   resignation had been read at the meeting of Tuesday 7 December 1909 (Shetland Times, 11 Dec
   1909). Evidence: research/bna/ltc-1894-1907.md.

23. Lerwick Town Council general of November 1946 was polled on Tuesday 5 November, not Monday
   the 4th. The preview says Lerwick electors "will find Tuesday's choice a difficult matter"
   (Shetland Times, 1 Nov 1946), and the Harbour Trust election "coincided with the municipal
   election on Tuesday" (Shetland Times, 8 Nov 1946). Evidence: research/bna/ltc-1945-1956.md.

24. Lerwick Town Council, 1967. Grace Halcrow was co-opted in place of Provost A. J. Nicolson "at
   the statutory meeting of the council last Friday night" (Shetland Times, 12 May 1967), i.e.
   Friday 5 May, not the 12th (the paper's publication day). Nicolson's resignation took effect
   on 27 April (Shetland Times, 14 Apr 1967). Ex-Provost Robert A. Anderson (i) died at home "on
   Sunday morning" (Shetland Times, 30 Jun 1967), "on 25th June, 1967" in the death notice
   (Shetland Times, 7 Jul 1967), not the 26th. Evidence: research/bna/ltc-1965-1970.md.

25. Lerwick Town Council general of November 1881 was on Tuesday 1 November, not the 8th. The
   Town Clerk's notice fixes the election of five councillors for "Tuesday the first day of
   November next" (Shetland Times, 22 Oct 1881), and the paper says "Tuesday first is the day
   fixed by the Acts" (Shetland Times, 29 Oct 1881). It was uncontested. Evidence:
   research/bna/ltc-1874-1883.md.

26. Lerwick Town Council general of November 1893 was on Tuesday 7 November, not Thursday the 2nd
   (the nomination day). With nominations equal to the vacancies, "on Tuesday next, the day of
   election, those nominated ... will be declared duly elected" (Shetland Times, 4 Nov 1893).

27. Sinclair Johnson was elected in place of the late A. J. Garriock when the monthly meeting of
   Tuesday 4 April 1899 "sat in committee" (Shetland Times, 8 Apr 1899), not on 2 May: that was
   the meeting at which he took his seat (Shetland Times, 6 May 1899).

28. E. S. Reid Tait was co-opted for James Goodlad at the monthly meeting "on Tuesday evening"
   (Shetland Times, 10 May 1924), i.e. 6 May 1924, not Monday the 5th.
   Evidence for 26-28: research/bna/ltc-1884-1935-citations.md.

29. T. A. Sinclair was elected to the vacancy "caused through the death of Mr David Gray" at the
   Town Council meeting "on Tuesday" (Shetland Times, 5 Apr 1946), i.e. 2 April 1946, not
   Wednesday 22 May. Evidence: research/bna/ltc-1946-1950-citations.md.

30. Zetland County Council by-elections 1890-1899. Most wiki dates were the 1st of the month or
   the Saturday paper's date. The day comes from the Shetland Times: the election day the
   Secretary for Scotland fixed (unopposed returns are dated to it, as for the LTC), the day of
   the Council meeting that appointed the member, or failing both the formal declaration.
   - Fetlar and Walls, March 1890: declared elected "On Saturday last" (ST 5 Apr 1890), 29 Mar.
   - Aithsting and Sandsting, April 1890: re-run polling fixed for 11 April (ST 22 Mar 1890).
   - Lerwick South, January 1891: Goudie appointed at the annual meeting "on Tuesday" (ST 20 Dec
     1890), 16 Dec 1890, for Arthur Laurenson (died 14 Nov 1890).
   - Burra, Fetlar and Walls, February 1893: election fixed for Tue 24 Jan 1893 (ST 31 Dec 1892);
     Fetlar polled "On Tuesday last week" (ST 4 Feb 1893).
   - Aithsting, January 1896: polled "on Tuesday" (ST 18 Jan 1896), 14 Jan.
   - Cunningsburgh, June 1896: appointed at the adjourned meeting "on Thursday" (ST 6 Jun 1896), 4 Jun.
   - Tingwall, March 1897: A. C. Hay appointed at the monthly meeting "on Thursday" (ST 6 Feb 1897), 4 Feb.
   - Northmavine North, December 1897: Robertson appointed at the monthly meeting "on Wednesday"
     (ST 6 Nov 1897), 3 Nov.
   - Dunrossness North, August 1898: left over at the meeting of Thu 4 Aug "till the September
     meeting" (ST 6 Aug 1898); Whyte appointed there (ST 3 Sep 1898), Thursday 1 September.
   - Delting South, February 1899: election fixed for 24 Jan 1899 (ST 31 Dec 1898); Adie the only
     nomination (ST 14 Jan 1899).
   Evidence: research/bna/zcc-1890-1899.md.

31. Whiteness and Weisdale electorate, February 1890: 116, not 126. The Shetland Times (8 Feb
   1890) gives "M. 86, F. 30—Total 116"; the wiki has "126 (86 men, 30 women)", whose own
   breakdown adds up to 116. Turnout 66, so 56.9%, not 52.9%.
   Evidence: research/bna/zcc-1890-1899.md.

32. Zetland County Council by-elections 1900-1919, dated by the rule in #30.
   - Delting South, April 1903: J. A. Adie appointed at the meeting "on Thursday at noon"
     (ST 18 Apr 1903), 16 Apr, for W. J. Adie, resigned (ST 21 Feb 1903).
   - Northmavine North, September 1903: Haldane appointed on a petition at the adjourned general
     meeting "on Thursday at noon" (ST 19 Sep 1903), 17 Sep.
   - Walls, February 1905: new election ordered, nominations 31 Jan, "the election for 14th
     February" (ST 21 Jan 1905); Holborn reported elected (ST 25 Feb 1905). The wiki's 21 Feb is
     unsupported.
   - Yell North, March 1906: T. J. Sandison appointed on the ratepayers' recommendation at the
     monthly meeting "on Thursday at noon" (ST 17 Feb 1906), 15 Feb, for Zachary Hamilton, dead.
   - Dunrossness South, August 1907: Rev. W. Fotheringham appointed on a petition at the monthly
     meeting "on Thursday of last week" (ST 24 Aug 1907), 15 Aug, for John Bruce, dead.
   - Tingwall, March 1909: L. J. Garriock appointed on a petition at the monthly meeting "on
     Thursday forenoon" (ST 20 Mar 1909), 18 Mar, for W. Rae Duncan, resigned.
   - Burra, August 1909: Robert Inkster appointed on two petitions at the monthly meeting "on
     Thursday" (ST 14 Aug 1909), 12 Aug. The DB had 27 Jun, the date Charles Lennie died.
   - Whalsay, July 1910: James Shearer appointed on a Parish Council petition at the monthly
     meeting "on Thursday last week" (ST 30 Jul 1910), 21 Jul, for Rev. C. Stobie, resigned.
   Evidence: research/bna/zcc-1900-1919.md.

33. Delting North, December 1904: the losing candidate was James Inkster, the sitting member, not
   Arthur White. The nominations were "Mr James Inkster, merchant, Brae, and Mr Wm. Pole" (ST 26
   Nov 1904) and the result "DELTING NORTH, Pole J. Inkster" (ST 10 Dec 1904); White was returned
   unopposed for Northmavine South. The wiki table has White 13. The votes are left as they are.
   Evidence: research/bna/zcc-1900-1919.md.

34. Sandwick, December 1907: William Smith was returned unopposed. The wiki's 1907 Sandwick table
   (electorate 276, turnout 161, Smith 91, James Thomson 70) is a copy of 1904's. The 1907
   nominations list Smith alone, and the five contests were Delting North, Delting South, Walls,
   Sandsting and Northmavine North (ST 23 and 30 Nov 1907). Thomson's 1907 candidacy is removed,
   Smith's votes become "Unopposed", and the copied electorate and turnout are cleared.
   Evidence: research/bna/zcc-1900-1919.md.
"""

import os
import sqlite3

DB_PATH = os.environ.get('SHETLAND_DB', '/Users/james/projects/shetland_history/new-site/shetland.db')

GENERAL = 'Lerwick Town Council Election November 1884'
BY_ELECTION = 'Lerwick Town Council By-Election November 1884'
CLAUSEN_BY_ELECTION = 'Lerwick Town Council By-Election November 1936'
UNFILLED = '[unfilled seat]'
WILLIAMSON_BY_ELECTION = 'Lerwick Town Council By-Election August 1938'
WILLIAMSON_NOTE = (
    "Co-option at a special meeting of the Town Council on 18 August 1938 to fill the vacancy left by "
    "the death of Alexander S. Manson. John A. Williamson was chosen as the unsuccessful candidate at "
    "the November 1937 election with the highest number of votes. Not recorded on the wiki; from the "
    "Shetland Times, 27 August 1938."
)
WILLIAMSON_INTRO = ('Lerwick Town Councillor between 1941 and 1945',
                    'Lerwick Town Councillor in 1938 and between 1941 and 1945')
HALCROW_1957 = 'Lerwick Town Council Election May 1957'
HALCROW_INTRO = ('a Lerwick Town Councillor from 1957 till the late 1960s',
                 'a Lerwick Town Councillor in 1954-55 and from 1964 to 1970')
HALCROW_COUNTY = ('a County Councillor for the same area between 1955 and 1961',
                  'a County Councillor for the same area between 1955 and 1958')
STRACHAN_BY_ELECTION = 'Lerwick Town Council By-Election May 1961'
STRACHAN_NOTE = (
    "Co-option at the Town Council meeting of 13 June 1961, to fill the vacancy left when the Rev. "
    "Kenneth Thomson, being ineligible, resigned at the statutory meeting on 5 May 1961. Robert "
    "Strachan had headed the unsuccessful candidates at the May 1961 election. From the Shetland Times, "
    "12 May and 16 June 1961."
)
POLLING_DAYS = [  # (wiki title, baseline Monday, Tuesday from the Shetland Times)
    ('Lerwick Town Council Election May 1949', '1949-05-02', '1949-05-03'),
    ('Lerwick Town Council By-Election September 1949', '1949-09-12', '1949-09-13'),
    ('Lerwick Town Council Election May 1950', '1950-05-01', '1950-05-02'),
    ('Lerwick Town Council Election May 1952', '1952-05-05', '1952-05-06'),
    ('Lerwick Town Council Election May 1953', '1953-05-04', '1953-05-05'),
    ('Lerwick Town Council Election May 1954', '1954-05-03', '1954-05-04'),
    ('Lerwick Town Council Election May 1955', '1955-05-02', '1955-05-03'),
    ('Lerwick Town Council Election May 1958', '1958-05-05', '1958-05-06'),
    ('Lerwick Town Council Election May 1959', '1959-05-04', '1959-05-05'),
    ('Lerwick Town Council Election May 1960', '1960-05-02', '1960-05-03'),
    ('Lerwick Town Council Election May 1961', '1961-05-01', '1961-05-02'),
    ('Lerwick Town Council Election May 1963', '1963-05-06', '1963-05-07'),
    ('Lerwick Town Council Election May 1964', '1964-05-04', '1964-05-05'),
]
RESULT_1954 = 'Lerwick Town Council Election May 1954'
WINNERS_1954 = [('alexander-morrison', 1031), ('grace-halcrow', 1013), ('robert-blance', 1010),
                ('robert-ollason', 929)]
LOSERS_1954 = [  # (person slug or None, candidate_name, party, votes)
    ('john-inkster-ii', 'John N. Inkster', 'Independent', 813),
    ('robert-strachan', 'Robert Strachan', 'Labour', 719),
    (None, '[https://www.bayanne.info/Shetland/getperson.php?personID=I86195&tree=ID1 J. B. A. Sutherland]',
     'Independent', 704),
    (None, '[https://www.bayanne.info/Shetland/getperson.php?personID=I52866&tree=ID1 James R. Gair]',
     'Labour', 692),
]
ELECTORATE_1954 = (3918, '3950 on the roll, 32 not eligible to vote until the autumn', 1913, 48.8)
INKSTER_1951 = 'Lerwick Town Council Election May 1951'
CO_OPTION_1941 = ('Lerwick Town Council By-Election October 1941', '1941-10-09', '1941-10-07')
ZCC_POLLING_DAYS = [  # (wiki title, baseline date, Tuesday from the Shetland Times)
    ('County Council Election May 1949', '1949-05-09', '1949-05-10'),
    ('County Council Election May 1952', '1952-05-05', '1952-05-13'),
    ('County Council Election May 1955', '1955-05-05', '1955-05-10'),
    ('County Council Election May 1958', '1958-05-12', '1958-05-13'),
    ('County Council Election May 1961', '1961-05-08', '1961-05-09'),
    ('County Council Election May 1964', '1964-05-11', '1964-05-12'),
]
POLLING_DAY_1968 = ('Lerwick Town Council Election May 1968', '1968-05-02', '1968-05-07')
SHEARER_ZCC_1947 = ('Whalsay And Skerries County Council By-Election May 1947', '1947-05-01', '1947-05-20')
YELL_SOUTH_1950 = ('Yell South County Council By-Election August 1950', ('George Ross', 164), ('George Spence', 169))
ADAIR_BY_ELECTION = 'Lerwick Town Council By-Election May 1970'
ADAIR_NOTE = (
    "Co-option at the statutory meeting of the Town Council on 8 May 1970, to fill the vacancy left when "
    "John R. Smith resigned on 14 April 1970, just after nominations closed. Robert Adair was the "
    "unsuccessful candidate with most votes at the May 1970 election. From the Shetland Times, "
    "17 April and 15 May 1970."
)
CO_OPTION_1886 = ('Lerwick Town Council By-Election November 1886', '1886-11-24', '1886-11-19')
CO_OPTION_1887 = ('Lerwick Town Council By-Election March 1887', '1887-03-24', '1887-03-18')
HUNTER_1887 = (231, 229)  # replaced_person_id: James Hunter (iv) -> James Hunter (ii)
POLLING_DAY_1889 = ('Lerwick Town Council Election November 1889', '1889-11-09', '1889-11-05')
CO_OPTION_1938 = ('Lerwick Town Council By-Election February 1938', '1938-02-03', '1938-02-01')
WARTIME_DATES = [  # (wiki title, baseline date, date from the Shetland Times)
    ('Lerwick Town Council By-Election August 1940', '1940-08-06', '1940-07-02'),
    ('Lerwick Town Council By-Election April 1942', '1942-04-09', '1942-03-31'),
    ('Lerwick Town Council By-Election February 1945', '1945-02-05', '1945-02-06'),
    ('Lerwick Town Council Election November 1945', '1945-11-05', '1945-11-06'),
]
CO_OPTION_1921 = ('Lerwick Town Council By-Election June 1921', '1921-06-11', '1921-06-07')
POLLING_DAY_1901 = ('Lerwick Town Council Election November 1901', '1901-11-01', '1901-11-05')
CO_OPTION_1910 = ('Lerwick Town Council By-Election January 1910', '1910-01-01', '1910-01-04')
POLLING_DAY_1946 = ('Lerwick Town Council Election November 1946', '1946-11-04', '1946-11-05')
CO_OPTION_1967 = ('Lerwick Town Council By-Election May 1967', '1967-05-12', '1967-05-05')
POLLING_DAY_1881 = ('Lerwick Town Council Election November 1881', '1881-11-08', '1881-11-01')
POLLING_DAY_1893 = ('Lerwick Town Council Election November 1893', '1893-11-02', '1893-11-07')
CO_OPTION_1899 = ('Lerwick Town Council By-Election May 1899', '1899-05-02', '1899-04-04')
CO_OPTION_1924 = ('Lerwick Town Council By-Election May 1924', '1924-05-05', '1924-05-06')
CO_OPTION_1946 = ('Lerwick Town Council By-Election May 1946', '1946-05-22', '1946-04-02')
ANDERSON_DEATH = ('robert-anderson-i', '1967-06-26', '1967-06-25')
ZCC_BY_ELECTIONS_1890S = [  # (wiki title, baseline date, date from the Shetland Times)
    ('Fetlar County Council By-Election March 1890', '1890-03-01', '1890-03-29'),
    ('Walls County Council By-Election March 1890', '1890-03-01', '1890-03-29'),
    ('Aithsting County Council By-Election April 1890', '1890-04-14', '1890-04-11'),
    ('Sandsting County Council By-Election April 1890', '1890-04-14', '1890-04-11'),
    ('Lerwick South County Council By-Election January 1891', '1891-01-01', '1890-12-16'),
    ('Burra County Council By-Election February 1893', '1893-02-01', '1893-01-24'),
    ('Fetlar County Council By-Election February 1893', '1893-02-02', '1893-01-24'),
    ('Walls County Council By-Election February 1893', '1893-02-01', '1893-01-24'),
    ('Aithsting County Council By-Election January 1896', '1896-01-02', '1896-01-14'),
    ('Cunningsburgh County Council By-Election June 1896', '1896-06-06', '1896-06-04'),
    ('Tingwall County Council By-Election March 1897', '1897-03-01', '1897-02-04'),
    ('Northmavine North County Council By-Election December 1897', '1897-12-02', '1897-11-03'),
    ('Dunrossness North County Council By-Election August 1898', '1898-08-01', '1898-09-01'),
    ('Delting South County Council By-Election February 1899', '1899-02-04', '1899-01-24'),
]
ZCC_BY_ELECTIONS_1900S = [  # (wiki title, baseline date, date from the Shetland Times)
    ('Delting South County Council By-Election April 1903', '1903-04-01', '1903-04-16'),
    ('Northmavine North County Council By-Election September 1903', '1903-09-19', '1903-09-17'),
    ('Walls_County_Council_By-Election_February_1905', '1905-02-01', '1905-02-14'),
    ('Yell North County Council By-Election March 1906', '1906-03-01', '1906-02-15'),
    ('Dunrossness South County Council By-Election August 1907', '1907-08-01', '1907-08-15'),
    ('Tingwall County Council By-Election March 1909', '1909-03-01', '1909-03-18'),
    ('Burra County Council By-Election August 1909', '1909-06-27', '1909-08-12'),
    ('Whalsay And Skerries County Council By-Election July 1910', '1910-07-01', '1910-07-21'),
]
DELTING_NORTH_1904 = ('County Council Election December 1904', 'Delting North', 'Arthur White', 'arthur-white',
                      'James Inkster', 'james-inkster')
SANDWICK_1907 = ('County Council Election December 1907', 'Sandwick', (276, 161), ('William Smith', 91), ('James Thomson', 70))
WHITENESS_1890 = ('County Council Election February 1890', 'Whiteness And Weisdale', (126, 52.9), (116, 56.9))
MACDOUGALL_INTRO =('until he resigned in April 1912', 'until he resigned in October 1912')

HAY_NOTE = (
    "Arthur J. Hay topped the poll but declined to take office (letter, 8 November 1884). "
    "The vacancy was filled by co-option: see Lerwick Town Council By-Election November 1884."
)


def one(c, sql, args):
    c.execute(sql, args)
    rows = c.fetchall()
    if len(rows) != 1:
        raise SystemExit(f"expected 1 row, got {len(rows)}: {sql} {args}")
    return rows[0]


def set_date(c, wiki_title, wrong, right):
    row = one(c, "SELECT id, election_date FROM elections WHERE wiki_page_title = ?", (wiki_title,))
    if row['election_date'] == right:
        print(f"  {wiki_title}: already {right}")
    elif row['election_date'] == wrong:
        c.execute("UPDATE elections SET election_date = ? WHERE id = ?", (right, row['id']))
        print(f"  {wiki_title} (id {row['id']}): {wrong} -> {right}")
    else:
        raise SystemExit(f"'{wiki_title}' has unexpected date {row['election_date']}")
    return row['id']


def set_date_all(c, wiki_title, wrong, right):
    """set_date for a general held as one row per ward: every row must move together."""
    c.execute("SELECT DISTINCT election_date FROM elections WHERE wiki_page_title = ?", (wiki_title,))
    dates = {r['election_date'] for r in c.fetchall()}
    if dates == {right}:
        print(f"  {wiki_title}: already {right}")
    elif dates == {wrong}:
        c.execute("UPDATE elections SET election_date = ? WHERE wiki_page_title = ?", (right, wiki_title))
        print(f"  {wiki_title} ({c.rowcount} rows): {wrong} -> {right}")
    else:
        raise SystemExit(f"'{wiki_title}' has unexpected dates {sorted(dates)}")


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    c = db.cursor()

    print("=== 1. November 1884 LTC election ===")
    general_id = set_date(c, GENERAL, '1884-12-04', '1884-11-04')
    by_id = set_date(c, BY_ELECTION, '1884-11-11', '1884-11-22')

    duncan_i = one(c, "SELECT id, name FROM people WHERE slug = 'william-duncan-i'", ())
    duncan_ii = one(c, "SELECT id FROM people WHERE slug = 'william-duncan-ii'", ())
    c.execute("""
        UPDATE candidacies SET person_id = ?
        WHERE person_id = ? AND candidate_name = 'William Duncan' AND election_id IN (?, ?)
    """, (duncan_i['id'], duncan_ii['id'], general_id, by_id))
    print(f"  William Duncan candidacies re-pointed to {duncan_i['name']}: {c.rowcount} row(s)")
    c.execute("""
        SELECT COUNT(*) FROM candidacies
        WHERE person_id = ? AND candidate_name = 'William Duncan' AND election_id IN (?, ?)
    """, (duncan_i['id'], general_id, by_id))
    if c.fetchone()[0] != 2:
        raise SystemExit("expected 2 William Duncan candidacies on the 1884 elections")

    row = one(c, "SELECT notes FROM elections WHERE id = ?", (general_id,))
    if row['notes'] == HAY_NOTE:
        print("  Hay note: already set")
    elif not row['notes']:
        c.execute("UPDATE elections SET notes = ? WHERE id = ?", (HAY_NOTE, general_id))
        print("  Hay note: added")
    else:
        raise SystemExit(f"{GENERAL} already has notes, not overwriting: {row['notes']}")

    print("=== 2. November 1936 LTC co-option ===")
    row = one(c, "SELECT id, replaced_person, replaced_person_id FROM elections WHERE wiki_page_title = ?",
              (CLAUSEN_BY_ELECTION,))
    if row['replaced_person'] == UNFILLED and row['replaced_person_id'] is None:
        print(f"  {CLAUSEN_BY_ELECTION}: already {UNFILLED}")
    elif row['replaced_person'] == 'Laurence Cogle':
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = NULL WHERE id = ?",
                  (UNFILLED, row['id']))
        print(f"  {CLAUSEN_BY_ELECTION} (id {row['id']}): replaced_person Laurence Cogle -> {UNFILLED}")
    else:
        raise SystemExit(f"'{CLAUSEN_BY_ELECTION}' has unexpected replaced_person {row['replaced_person']}")

    print("=== 3. August 1938 LTC co-option ===")
    ltc = one(c, "SELECT id FROM councils WHERE name = 'Lerwick Town Council'", ())
    manson = one(c, "SELECT id, name, died_date FROM people WHERE slug = 'alexander-manson'", ())
    if manson['died_date'] != '1938-07-22':
        raise SystemExit(f"people.alexander-manson died_date is {manson['died_date']}, expected 1938-07-22")
    williamson = one(c, "SELECT id FROM people WHERE slug = 'john-williamson-iii'", ())
    c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (WILLIAMSON_BY_ELECTION,))
    row = c.fetchone()
    if row:
        election_id = row['id']
        print(f"  by-election exists (id {election_id})")
    else:
        c.execute("""
            INSERT INTO elections (council_id, election_date, election_type, wiki_page_title,
                                   replaced_person, replaced_person_id, notes)
            VALUES (?, '1938-08-18', 'by-election', ?, ?, ?, ?)
        """, (ltc['id'], WILLIAMSON_BY_ELECTION, manson['name'], manson['id'], WILLIAMSON_NOTE))
        election_id = c.lastrowid
        print(f"  by-election created (id {election_id})")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = 'John A. Williamson'",
              (election_id,))
    if c.fetchone():
        print("  candidacy exists")
    else:
        c.execute("""
            INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes_text, elected, position)
            VALUES (?, ?, 'John A. Williamson', 'Labour', 'Co-opted', 1, 1)
        """, (election_id, williamson['id']))
        print("  candidacy created")

    old, new = WILLIAMSON_INTRO
    row = one(c, "SELECT intro FROM people WHERE id = ?", (williamson['id'],))
    if new in row['intro']:
        print("  intro: already mentions 1938")
    elif old in row['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (row['intro'].replace(old, new), williamson['id']))
        print("  intro: 1938 added")
    else:
        raise SystemExit(f"john-williamson-iii intro doesn't contain {old!r}")

    print("=== 4. William MacDougall's resignation ===")
    old, new = MACDOUGALL_INTRO
    row = one(c, "SELECT id, intro FROM people WHERE slug = 'william-macdougall'", ())
    if new in row['intro']:
        print("  intro: already October 1912")
    elif old in row['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (row['intro'].replace(old, new), row['id']))
        print("  intro: April 1912 -> October 1912")
    else:
        raise SystemExit(f"william-macdougall intro doesn't contain {old!r}")

    print("=== 5. May 1957 LTC election: Nicolson, not Halcrow ===")
    halcrow = one(c, "SELECT id, intro FROM people WHERE slug = 'grace-halcrow'", ())
    nicolson = one(c, "SELECT id FROM people WHERE slug = 'andrew-nicolson'", ())
    e1957 = one(c, "SELECT id FROM elections WHERE wiki_page_title = ?", (HALCROW_1957,))
    c.execute("SELECT id, person_id FROM candidacies WHERE election_id = ? AND person_id IN (?, ?)",
              (e1957['id'], halcrow['id'], nicolson['id']))
    found = c.fetchall()
    if len(found) != 1:
        raise SystemExit(f"{HALCROW_1957}: expected one Halcrow or Nicolson candidacy, found {len(found)}")
    if found[0]['person_id'] == nicolson['id']:
        print("  candidacy: already Nicolson")
    else:
        c.execute("""UPDATE candidacies SET person_id = ?, candidate_name = 'Andrew J. Nicolson', party = 'Labour'
                     WHERE id = ?""", (nicolson['id'], found[0]['id']))
        print(f"  candidacy {found[0]['id']}: Grace Halcrow -> Andrew J. Nicolson")
    old, new = HALCROW_INTRO
    if new in halcrow['intro']:
        print("  intro: already corrected")
    elif old in halcrow['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (halcrow['intro'].replace(old, new), halcrow['id']))
        print("  intro: from 1957 till the late 1960s -> 1954-55 and from 1964 to 1970")
    else:
        raise SystemExit(f"grace-halcrow intro doesn't contain {old!r}")

    old, new = HALCROW_COUNTY
    halcrow = one(c, "SELECT id, intro FROM people WHERE slug = 'grace-halcrow'", ())
    if new in halcrow['intro']:
        print("  county council: already corrected")
    elif old in halcrow['intro']:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (halcrow['intro'].replace(old, new), halcrow['id']))
        print("  county council: 1955-61 -> 1955-58")
    else:
        raise SystemExit(f"grace-halcrow intro doesn't contain {old!r}")

    print("=== 6. June 1961 LTC co-option of Robert Strachan ===")
    by_id = set_date(c, STRACHAN_BY_ELECTION, '1961-05-05', '1961-06-13')
    row = one(c, "SELECT notes FROM elections WHERE id = ?", (by_id,))
    if row['notes'] == STRACHAN_NOTE:
        print("  note: already set")
    elif not row['notes']:
        c.execute("UPDATE elections SET notes = ? WHERE id = ?", (STRACHAN_NOTE, by_id))
        print("  note: added")
    else:
        raise SystemExit(f"{STRACHAN_BY_ELECTION} already has notes, not overwriting: {row['notes']}")

    print("=== 7. LTC polling days 1949-1964 ===")
    for title, wrong, right in POLLING_DAYS:
        set_date(c, title, wrong, right)

    print("=== 8. May 1954 LTC result, and John N. Inkster in 1951 ===")
    e1954 = one(c, "SELECT id, electorate, electorate_detail, turnout, turnout_pct FROM elections "
                   "WHERE wiki_page_title = ?", (RESULT_1954,))
    electorate, detail, turnout, pct = ELECTORATE_1954
    if (e1954['electorate'], e1954['electorate_detail'], e1954['turnout'], e1954['turnout_pct']) == ELECTORATE_1954:
        print("  electorate/turnout: already set")
    elif e1954['electorate'] is None and e1954['turnout'] is None:
        c.execute("UPDATE elections SET electorate = ?, electorate_detail = ?, turnout = ?, turnout_pct = ? "
                  "WHERE id = ?", (electorate, detail, turnout, pct, e1954['id']))
        print("  electorate/turnout: set")
    else:
        raise SystemExit(f"{RESULT_1954} has unexpected electorate/turnout")
    for slug, votes in WINNERS_1954:
        row = one(c, """SELECT c.id, c.votes, c.votes_text FROM candidacies c JOIN people p ON p.id = c.person_id
                        WHERE c.election_id = ? AND p.slug = ? AND c.elected = 1""", (e1954['id'], slug))
        if row['votes'] == votes and row['votes_text'] is None:
            print(f"  {slug}: already {votes}")
        elif row['votes'] is None and row['votes_text'] == 'Unopposed':
            c.execute("UPDATE candidacies SET votes = ?, votes_text = NULL WHERE id = ?", (votes, row['id']))
            print(f"  {slug}: Unopposed -> {votes}")
        else:
            raise SystemExit(f"{RESULT_1954}: {slug} has unexpected votes {row['votes']} / {row['votes_text']}")
    for position, (slug, name, party, votes) in enumerate(LOSERS_1954, start=len(WINNERS_1954) + 1):
        c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (e1954['id'], name))
        if c.fetchone():
            print(f"  {name}: exists")
            continue
        person_id = one(c, "SELECT id FROM people WHERE slug = ?", (slug,))['id'] if slug else None
        c.execute("""INSERT INTO candidacies (election_id, person_id, candidate_name, party, votes, elected, position)
                     VALUES (?, ?, ?, ?, ?, 0, ?)""", (e1954['id'], person_id, name, party, votes, position))
        print(f"  {name}: added ({votes})")

    inkster = one(c, "SELECT id FROM people WHERE slug = 'john-inkster-ii'", ())
    e1951 = one(c, "SELECT id FROM elections WHERE wiki_page_title = ?", (INKSTER_1951,))
    c.execute("SELECT id, candidate_name, person_id FROM candidacies WHERE election_id = ? AND candidate_name "
              "IN ('James Inkster', 'John N. Inkster')", (e1951['id'],))
    found = c.fetchall()
    if len(found) != 1:
        raise SystemExit(f"{INKSTER_1951}: expected one Inkster candidacy, found {len(found)}")
    if found[0]['candidate_name'] == 'John N. Inkster' and found[0]['person_id'] == inkster['id']:
        print("  1951 Inkster: already John N. Inkster")
    elif found[0]['candidate_name'] == 'James Inkster' and found[0]['person_id'] is None:
        c.execute("UPDATE candidacies SET candidate_name = 'John N. Inkster', person_id = ? WHERE id = ?",
                  (inkster['id'], found[0]['id']))
        print(f"  1951 candidacy {found[0]['id']}: James Inkster -> John N. Inkster (john-inkster-ii)")
    else:
        raise SystemExit(f"{INKSTER_1951}: unexpected Inkster candidacy {dict(found[0])}")

    print("=== 10. October 1941 LTC co-options: Tuesday 7 October ===")
    set_date(c, *CO_OPTION_1941)

    print("=== 11. May 1970 LTC co-option of Robert Adair ===")
    by_id = set_date(c, ADAIR_BY_ELECTION, '1970-08-08', '1970-05-08')
    row = one(c, "SELECT notes FROM elections WHERE id = ?", (by_id,))
    if row['notes'] == ADAIR_NOTE:
        print("  note: already set")
    elif not row['notes']:
        c.execute("UPDATE elections SET notes = ? WHERE id = ?", (ADAIR_NOTE, by_id))
        print("  note: added")
    else:
        raise SystemExit(f"{ADAIR_BY_ELECTION} already has notes, not overwriting: {row['notes']}")

    print("=== 12. ZCC polling days 1949-1964 ===")
    for title, wrong, right in ZCC_POLLING_DAYS:
        set_date_all(c, title, wrong, right)

    print("=== 13. LTC polling day May 1968 ===")
    set_date(c, *POLLING_DAY_1968)

    print("=== 14. ZCC appointment of Magnus Shearer, May 1947 ===")
    set_date(c, *SHEARER_ZCC_1947)

    print("=== 15. Yell South by-election Aug 1950: the seat was George Spence's ===")
    title, wrong, right = YELL_SOUTH_1950
    row = one(c, "SELECT id, replaced_person, replaced_person_id FROM elections WHERE wiki_page_title = ?", (title,))
    if (row['replaced_person'], row['replaced_person_id']) == right:
        print("  already George Spence")
    elif (row['replaced_person'], row['replaced_person_id']) == wrong:
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = ? WHERE id = ?", (*right, row['id']))
        print(f"  election {row['id']}: replaced George Ross -> George Spence")
    else:
        raise SystemExit(f"{title}: unexpected replaced member {row['replaced_person']!r}/{row['replaced_person_id']}")

    print("=== 16. LTC co-options Nov 1886 and Mar 1887 ===")
    set_date(c, *CO_OPTION_1886)
    by_id = set_date(c, *CO_OPTION_1887)
    wrong, right = HUNTER_1887
    row = one(c, "SELECT replaced_person_id FROM elections WHERE id = ?", (by_id,))
    if row['replaced_person_id'] == right:
        print("  replaced: already James Hunter (ii)")
    elif row['replaced_person_id'] == wrong:
        c.execute("UPDATE elections SET replaced_person_id = ? WHERE id = ?", (right, by_id))
        print(f"  election {by_id}: replaced_person_id {wrong} -> {right}")
    else:
        raise SystemExit(f"election {by_id}: unexpected replaced_person_id {row['replaced_person_id']}")

    print("=== 17. LTC polling day Nov 1889 ===")
    set_date(c, *POLLING_DAY_1889)

    print("=== 18. LTC co-option Feb 1938 ===")
    set_date(c, *CO_OPTION_1938)

    print("=== 19. LTC co-options 1940-45 and the Nov 1945 polling day ===")
    for title, wrong, right in WARTIME_DATES:
        set_date(c, title, wrong, right)

    print("=== 20. LTC co-option Jun 1921 ===")
    set_date(c, *CO_OPTION_1921)

    print("=== 21. LTC polling day Nov 1901 ===")
    set_date(c, *POLLING_DAY_1901)

    print("=== 22. LTC co-option Jan 1910 ===")
    set_date(c, *CO_OPTION_1910)

    print("=== 23. LTC polling day Nov 1946 ===")
    set_date(c, *POLLING_DAY_1946)

    print("=== 24. LTC co-option May 1967 and R. A. Anderson's death ===")
    set_date(c, *CO_OPTION_1967)
    slug, wrong, right = ANDERSON_DEATH
    row = one(c, "SELECT id, died_date FROM people WHERE slug = ?", (slug,))
    if row['died_date'] == right:
        print(f"  {slug} died_date: already {right}")
    elif row['died_date'] == wrong:
        c.execute("UPDATE people SET died_date = ? WHERE id = ?", (right, row['id']))
        print(f"  {slug} died_date: {wrong} -> {right}")
    else:
        raise SystemExit(f"people.{slug} died_date is {row['died_date']}, expected {wrong}")

    print("=== 25. LTC polling day Nov 1881 ===")
    set_date(c, *POLLING_DAY_1881)

    print("=== 26-28. LTC polling day Nov 1893, co-options Apr 1899 and May 1924 ===")
    set_date(c, *POLLING_DAY_1893)
    set_date(c, *CO_OPTION_1899)
    set_date(c, *CO_OPTION_1924)

    print("=== 29. LTC co-option Apr 1946 ===")
    set_date(c, *CO_OPTION_1946)

    print("=== 30. ZCC by-elections 1890-1899 ===")
    for args in ZCC_BY_ELECTIONS_1890S:
        set_date(c, *args)

    print("=== 31. Whiteness and Weisdale electorate 1890 ===")
    title, ward, wrong, right = WHITENESS_1890
    row = one(c, """SELECT e.id, e.electorate, e.turnout_pct FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    if (row['electorate'], row['turnout_pct']) == right:
        print(f"  election {row['id']}: already {right}")
    elif (row['electorate'], row['turnout_pct']) == wrong:
        c.execute("UPDATE elections SET electorate = ?, turnout_pct = ? WHERE id = ?", (*right, row['id']))
        print(f"  election {row['id']}: electorate {wrong} -> {right}")
    else:
        raise SystemExit(f"election {row['id']}: unexpected electorate {row['electorate']}/{row['turnout_pct']}")

    print("=== 32. ZCC by-elections 1900-1919 ===")
    for args in ZCC_BY_ELECTIONS_1900S:
        set_date(c, *args)

    print("=== 33. Delting North 1904: the loser was James Inkster ===")
    title, ward, wrong_name, wrong_slug, right_name, right_slug = DELTING_NORTH_1904
    right_id = one(c, "SELECT id FROM people WHERE slug = ?", (right_slug,))['id']
    wrong_id = one(c, "SELECT id FROM people WHERE slug = ?", (wrong_slug,))['id']
    row = one(c, """SELECT c.id, c.candidate_name, c.person_id FROM candidacies c
                   JOIN elections e ON e.id = c.election_id JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ? AND c.elected = 0""", (title, ward))
    if (row['candidate_name'], row['person_id']) == (right_name, right_id):
        print("  already James Inkster")
    elif (row['candidate_name'], row['person_id']) == (wrong_name, wrong_id):
        c.execute("UPDATE candidacies SET candidate_name = ?, person_id = ? WHERE id = ?", (right_name, right_id, row['id']))
        print(f"  candidacy {row['id']}: {wrong_name} -> {right_name}")
    else:
        raise SystemExit(f"candidacy {row['id']}: unexpected {row['candidate_name']!r}/{row['person_id']}")

    print("=== 34. Sandwick 1907: Smith unopposed (the wiki copied 1904) ===")
    title, ward, (electorate, turnout), (winner, winner_votes), (loser, loser_votes) = SANDWICK_1907
    row = one(c, """SELECT e.id, e.electorate, e.turnout FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    eid = row['id']
    if (row['electorate'], row['turnout']) == (electorate, turnout):
        c.execute("UPDATE elections SET electorate = NULL, turnout = NULL WHERE id = ?", (eid,))
        print(f"  election {eid}: copied electorate/turnout cleared")
    elif (row['electorate'], row['turnout']) != (None, None):
        raise SystemExit(f"election {eid}: unexpected electorate/turnout {row['electorate']}/{row['turnout']}")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ? AND votes = ? AND elected = 0",
              (eid, loser, loser_votes))
    ids = [r['id'] for r in c.fetchall()]
    if len(ids) == 1:
        c.execute("DELETE FROM candidacies WHERE id = ?", ids)
        print(f"  candidacy {ids[0]} ({loser}, copied from 1904) removed")
    elif ids:
        raise SystemExit(f"election {eid}: several {loser} rows")
    w = one(c, "SELECT id, votes, votes_text FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, winner))
    if (w['votes'], w['votes_text']) == (winner_votes, None):
        c.execute("UPDATE candidacies SET votes = NULL, votes_text = 'Unopposed' WHERE id = ?", (w['id'],))
        print(f"  candidacy {w['id']} ({winner}): {winner_votes} votes -> Unopposed")
    elif (w['votes'], w['votes_text']) != (None, 'Unopposed'):
        raise SystemExit(f"candidacy {w['id']}: unexpected votes {w['votes']!r}/{w['votes_text']!r}")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
