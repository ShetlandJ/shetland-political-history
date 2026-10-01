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
   - Whiteness and Weisdale, June 1914: the vacancy (Peter Anderson, dead) was filled at the
     monthly meeting "on Thursday at noon", whose report was held over (ST 20 and 27 Jun 1914):
     18 Jun, as the wiki says.
   - Nesting, December 1914: Pearson elected by ballot, 16 to 3, at the monthly meeting "on
     Thursday last" (ST 26 Dec 1914), 24 Dec; the 19 Dec issue has no Council report. The wiki's
     "Thursday 17 December" is a week early.
   - Aithsting, August 1915: T. A. Anderson elected by ballot, 12 to 4, at the monthly meeting
     "on Thursday, of last week" (ST 28 Aug 1915), 19 Aug. The wiki's "Thursday 15 August" was a
     Sunday.
   - Fetlar, July 1917: Sir Arthur J. F. W. Nicolson appointed on a petition of 29 ratepayers at
     the monthly meeting "on Thursday (yesterday)" (ST 21 Jul 1917), 19 Jul.
   - Cunningsburgh, February 1919: Laurence Anderson elected 5 to 3 over James Laing at the
     monthly meeting "on Thursday of last week" (ST 1 Mar 1919; the meeting also ST 22 Feb 1919),
     20 Feb. The wiki's "Thursday 23 February" was a Sunday.
   - Dunrossness South and Tingwall, June 1919: J. R. Irvine and J. P. Mouat appointed at the
     monthly meeting "on Thursday of this week" (ST 21 Jun 1919), 19 Jun.
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

35. ZCC polling day, December 1910: Tuesday 6 December, not Saturday 3rd. "Polling ... takes
   place on Tuesday, [6th] December" (ST 26 Nov 1910); the contested divisions polled "on
   Tuesday in disagreeable weather" (ST 10 Dec 1910).
   Evidence: research/bna/zcc-1900-1919.md.

36. Lerwick South, December 1919: James Laing was returned unopposed, not Charles Stout. Both were
   nominated (ST 15 Nov 1919), and "Mr C. B. Stout from Lerwick South" withdrew, "leaving Mr Jas.
   Laing unopposed" (ST 22 Nov 1919). "J. Laing" sat at the County Council's January 1920 meeting;
   Stout did not (ST 24 Jan 1920). The wiki has Stout unopposed, and Laing's page dates his seat
   from 1922. The candidacy is re-pointed to Laing, and his intro now says 1919.
   Evidence: research/bna/zcc-1900-1919.md.

37. Dunrossness North, December 1922: a contest, not an unopposed return. "Mr J. R. White, 99
   Commercial Street, Lerwick; and Mr A. Irvine, jun." were nominated (Shetland Times, 25 Nov
   1922), and White "topped the poll with 60 votes, Mr A. Irvine, jun., Boddam, Dunrossness,
   receiving 36", two spoiled (ST 16 Dec 1922; the OCR heads it "Dunrossness (South)", but William
   Leslie was the only South nomination). The wiki has White unopposed.
   Evidence: research/bna/zcc-1920-1939.md.

38. ZCC polling day, December 1925: Tuesday 1 December, not Saturday 5th (the paper's date). The
   nominations are for the election "which takes place on Tuesday, 1st December" (Shetland Times,
   14 Nov 1925), and candidates' adverts ask for votes at the poll "on TUESDAY FIRST, 1st
   December" (ST 28 Nov 1925). The results report says polling "took place on Monday" (ST 5 Dec
   1925), taken as a slip.
   Evidence: research/bna/zcc-1920-1939.md.

39. ZCC polling day, December 1928: Tuesday 4 December, not Wednesday 5th. "The polls in these
   constituencies take place on Tuesday, 4th December" (Shetland Times, 24 Nov 1928); they "took
   place on Tuesday" and the count on Wednesday (ST 8 Dec 1928). The wiki's "Tuesday 5 December"
   has the wrong day of the month.
   Evidence: research/bna/zcc-1920-1939.md.

40. Walls electorate, December 1928: 389, not 192. "In Walls, with an electorate of 389, 99
   electors voted" (Shetland Times, 8 Dec 1928). The wiki gives Walls the same 192 as Sandness,
   whose figure the paper confirms. Turnout 99, so 25.4%, not 51.6%.
   Evidence: research/bna/zcc-1920-1939.md.

41. Two ZCC by-elections of February 1933 that the wiki doesn't have. At the December 1932 general
   "Sandwick from which no nomination was lodged" (Shetland Times, 19 Nov 1932), and James A.
   Jamieson, returned unopposed for Sandness, "was not eligible to sit owing to his holding a minor
   appointment under the Council" (ST 28 Jan 1933). The Scottish Office ordered new elections in
   both divisions, nominations by Tue 24 Jan, "The elections take place on Tuesday, 14th February"
   (ST 14 Jan 1933). One nomination each, "there will be no poll": William Jamieson, the former
   member, for Sandwick, and James A. Jamieson, who had resigned the appointment, for Sandness
   (ST 28 Jan 1933). Both are dated to the fixed day, as in #30. His 1932 Sandness win is in
   data/not_seated.csv.
   Evidence: research/bna/zcc-1920-1939.md.

42. Walls, December 1935: Andrew Halcrow had 4 votes, not 85. "John G. Williamson ... 89; *Andrew
   Halcrow ... 4—majority 85, Total electorate 328, total votes cast 93, or 28.3 per cent"
   (Shetland Times, 7 Dec 1935). The wiki took the majority for Halcrow's vote; 89 + 4 = 93, which
   is 28.3% of 328. The electorate and turnout are added.
   Evidence: research/bna/zcc-1920-1939.md.

43. William Jamieson sat for Sandwick until 1935, not 1932. He was re-elected unopposed in February
   1933 (#41), and in November 1935 "the sitting member, Mr William Jamieson, Central House,
   Sandwick, has definitely retired from the Council and has not sought nomination" (Shetland
   Times, 16 Nov 1935). His intro's "between 1919 and 1932" becomes "between 1919 and 1935".
   Evidence: research/bna/zcc-1920-1939.md.

44. Whalsay and Skerries, December 1938: James J. Hay had 32 votes, not 121. "Robert Ollason ...
   153; *James J. Hay ... 82 [32]—majority 121. Total electorate 410, total votes cast 185"
   (Shetland Times, 10 Dec 1938). As with Walls in 1935 (#42), the wiki took the majority for the
   loser's vote: 153 + 32 = 185.
   Evidence: research/bna/zcc-1920-1939.md.

45. Yell North and Yell South, December 1938. Yell South was a contest: "*Thomas R. Manson ... 166;
   William Leask, Vatster, Mid Yell, 145—majority 21. Total electorate 471, votes cast 311, or 66
   per cent. 11 spoiled papers"; Yell North was "Total electorate 257, total votes cast 158 or 61
   per cent" (Shetland Times, 26 Nov and 10 Dec 1938). The wiki has Manson unopposed, and gives
   Yell North the Yell South figures (471, with 331 for 311). Leask is added, Manson gets his
   votes, and each ward gets its own electorate and turnout.
   Evidence: research/bna/zcc-1920-1939.md.

46. Zetland County Council by-elections 1920-1938, dated by the rule in #30. All were
   appointments by the Council at a meeting, usually on a petition. The Council met on Thursdays
   until the 1929 Act and on Tuesdays after it (from May 1930).
   - Lerwick North, March 1920: requisitions for Henry Mouat were submitted at the meeting "on
     Thursday" and he was appointed (ST 21 Feb 1920), 19 Feb, for W. J. Greig, dead. He thanked
     the Council at the 18 March meeting (ST 27 Mar 1920), the wiki's date.
   - Sandness and Walls, October 1921: at the monthly meeting "on Thursday" Coutts was appointed
     for Walls and John Harrison for Sandness (ST 17 Sep 1921), 15 Sep. Their acceptance was
     announced on 20 Oct (ST 29 Oct 1921), the wiki's date.
   - Whalsay and Skerries, January 1924: Shearer appointed on a petition at the meeting "on
     Thursday of last week" (ST 26 Jan 1924), 17 Jan.
   - Gulberwick, June 1924: T. J. Anderson elected 8 to 2 at the meeting "on Thursday of last
     week" (ST 28 Jun 1924), 19 Jun.
   - Yell North, August 1925: Mainland appointed at the meeting "on Thursday of last week" (ST 29
     Aug 1925), 20 Aug.
   - Cunningsburgh, February 1927: the petition for William Sinclair "was submitted to the Zetland
     County Council on Thursday" (ST 19 Feb 1927), 17 Feb. The wiki's "Thursday 15 February" was a
     Tuesday.
   - Northmavine South, July 1927: Arthur Irvine co-opted at the meeting "held on Thursday" (ST 23
     Jul 1927), 21 Jul, not Tuesday 19th.
   - Sandsting, September 1930: White appointed 14 votes to 7 at the meeting "on Tuesday" (ST 20
     Sep 1930), 16 Sep. The wiki has "Thursday 18 September".
   - Dunrossness North, June 1931: J. W. Robertson appointed on a petition of 125 at the meeting
     "on Tuesday" (ST 20 Jun 1931), 16 Jun.
   - Yell North, August 1932: Robert Smith appointed at the meeting "on Tuesday" (ST 20 Aug 1932),
     16 Aug.
   - Bressay, September 1933: J. A. Smith elected 19 to 9 by ballot at the monthly meeting "on
     Tuesday" (ST 23 Sep 1933), 19 Sep.
   - Bressay and Dunrossness North, December 1934: Cameron and Goudie co-opted at the monthly
     meeting "held on Tuesday" (ST 22 Dec 1934), 18 Dec.
   - Whalsay and Skerries, February 1937; Fetlar, October 1937; Unst North, December 1937;
     Sandsting, February 1938: reported in the Saturday papers of 20 Feb, 30 Oct and 18 Dec 1937
     and 26 Feb 1938, so the meetings of Tuesday 16 Feb, 26 Oct and 14 Dec 1937 and 22 Feb 1938,
     the days the wiki gives (it calls the first "Thursday 16 February", and the last "Tuesday 22
     December"). The report's first line, with the day, wasn't read for these four.
   Evidence: research/bna/zcc-1920-1939.md.

47. Unst South, February 1936: an election, not an appointment. W. Fordyce Clark and Magnus L.
   Manson were nominated (ST 25 Jan 1936), and "Mr Manson has withdrawn his candidature, and Mr
   Fordyce Clark is therefore returned" (ST 1 Feb 1936). The wiki has "Unanimously appointed" and
   "Rejected". The day is left at 1 Feb (open question).
   Evidence: research/bna/zcc-1920-1939.md.

48. Aithsting, May 1932: petitions for Leslie (82), Clark (102) and Williamson; "A vote by ballot
   was taken resulted as follows:—Clark, 10 votes; Williamson, 9; Leslie, 3. A vote was then
   taken between the two highest ... Clark, 10; Williamson, 9" (Shetland News, 19 May 1932; the
   Shetland Times isn't digitised then). The wiki has the petitions as a "First Council vote" and a
   ballot of 8, 8 and 3 before the 10 to 9; the parser kept the petitions as votes.
   Evidence: research/bna/zcc-1920-1939.md.
49. Zetland County Council general election, December 1945: "The results of the elections held on
   Tuesday" (Shetland Times, Fri 7 Dec 1945), so Tuesday 4 December. The wiki's "Tuesday 3
   December" has the wrong date (the 3rd was a Monday). Every ward row moves.
50. Zetland County Council by-elections 1940-1959, dated to the Council meeting that appointed the
   member, or the poll, from the Shetland Times report (the baseline had the 1st of the month):
   Cunningsburgh Tue 16 Apr 1940; Yell South Tue 10 Dec 1940 (reported Sat 14 Dec, not in the
   7 Dec issue; the wiki's "Tuesday 8 December" was a Sunday); Dunrossness North Tue 17 Dec 1946;
   Nesting and Yell North Tue 18 May 1948; Northmavine North "co-opted on Tuesday", 20 Feb 1951;
   Dunrossness North Tue 19 Feb 1952; Yell South, poll Tue 29 Jul 1952; Delting South and
   Sandwick, polls "on Tuesday", 12 Jul 1955; Gulberwick "on Tuesday co-opted", 21 Oct 1958;
   Aithsting, poll "on Tuesday", 3 Feb 1959 (the wiki's 9 February was a Monday).
51. Gulberwick, May 1951: a poll, not an appointment. "In the bye-election on Tuesday he polled
   twice as many votes as his Socialist opponent, Mr Prophet Smith—98 to 49" (Shetland Times,
   11 May 1951; Nicolson's thanks for "the Poll on Tuesday, 8th instant"). Nicolson's
   "Unanimously appointed" becomes 98 votes and Prophet Smith is added, linked to his page (James:
   there was only one Prophet Smith). The day is set in fix_parse_errors.py (WRONG_NAMESAKE), which runs later.
52. Gulberwick, March 1945: petitions for Keith and Mitchell, "After a vote Mr C. E. Mitchell was
   elected" by the Council (Shetland Times, 23 Mar 1945). The wiki's 15 and 6 are Council votes,
   not a poll.
   Evidence for 49-52: research/bna/zcc-1940-1959.md.
53. Burra and Quarff electorate, May 1964: 500, not 524. "In Quarff, 28 of the 66 people on the roll
   voted but in Burra only 90 of the 434", "a fairly low poll, 23.6 per cent" (Shetland Times,
   15 May 1964). The wiki's 22.5% is 118 of 524.
54. Aithsting, May 1973: a three-cornered contest. Peter F. M. Tulloch had 46 votes (Caldwell 151,
   Garrick 124, two spoiled; Shetland Times, 11 May 1973). The wiki leaves him out, and its turnout
   of 275 is the other two's votes; 323 counts all three and the spoiled papers.
55. Northmavine South, 1970-1972: Robert Balfour, not Hugh Sutherland. Balfour "has resigned as
   member for Northmavine South" after "seventeen years" (Shetland Times, 24 Mar 1972), and
   spoke as "the Northmavine representative" at the Council in 1970 (2 Oct 1970). The May 1970
   seat is re-pointed to Balfour, the 1972 by-election replaced him, and the two intros drop the
   wiki's Northmavine South years for Sutherland (1970-72 were Balfour's).
56. Six ZCC by-elections that were polls, which the wiki shows as unopposed or appointments:
   Bressay, Tue 29 Mar 1960, John R. Smith 86, George A. Kerr 79 (1 Apr 1960); Delting North,
   Tue 24 Sep 1963, A. W. Peterson 57, William Nicolson 11, 34.5 per cent (27 Sep 1963; the wiki's
   "Willie Peterson" is Andrew W. Peterson, "until last year he represented a Yell division");
   Yell South, Tue 22 Feb 1966, Robert S. Gray 140, Alan Rhodes 22, "163 of the 589 electors voted"
   (25 Feb 1966); Bressay, Tue 14 Jun 1966, John H. Scott 46, William Smith 34, 38.6 per cent of
   216 (17 Jun 1966); Gulberwick and Quarff, Tue 29 Sep 1970, Arthur Irvine 55, Mrs C. F. Johnson
   22, 58 per cent of 131 (2 Oct 1970); Yell North, Tue 8 Jan 1974, David J. Johnston 108,
   Andrew J. Williamson 90, 72 per cent, no spoiled papers (11 Jan 1974).
57. Zetland County Council by-elections 1960-1974, dated to the poll, the day fixed for an
   unopposed return, or the Council meeting (the baseline had the 1st of the month, or the wiki's
   day): Walls, poll "on Tuesday", 9 Feb 1960 (the wiki's 10 February was a Wednesday); Yell North,
   the day fixed, 4 Dec 1962 (Sutherland took his seat on the 18th); Dunrossness North, no
   nomination, Bruce "co-opted ... on Tuesday", 22 Oct 1963; Dunrossness South and Whalsay, "due on
   7th July" 1964, one nomination each; Fetlar and Tingwall, "become county councillors on 25th
   July" 1967; Burra and Whiteness and Weisdale, "declared county councillors on 14th July" 1970;
   Dunrossness North, the Tuesday poll of November 1971 (the day is lost in the OCR; the wiki's
   Wed 10 Nov was probably the count), 9 Nov; Northmavine South, John Jamieson the only nominee,
   takes his seat "on 16th May" 1972 (20 June was his first meeting); Burra, "declared councillor
   ... on 8th August, the date fixed for the poll", 1972; and the polls in #56. Several wiki
   titles give the wrong month; the titles are kept.
58. Unopposed returns that the wiki shows as appointments or unanimous elections: Bentley,
   Dunrossness South 1964; A. B. Irvine, Tingwall 1967; Jamieson, Northmavine South 1972.
   Evidence for 53-58: research/bna/zcc-1960-1974.md.
59. Dunrossness North, August 1971: a by-election the wiki doesn't have. For Iain Campbell's seat,
   "when nominations closed last week the name of Mrs Fisher was the only one lodged", and she
   "will be formally elected ... on 24th August" (Shetland Times, 13 Aug 1971). "A legal
   technicality ... debarred her from taking her seat", and "to assist the council to overcome the
   difficulty created by her election, Mrs Fisher has resigned" (10 Sep 1971): she was a council
   employee (24 Sep 1971). The August by-election is added, her win is in data/not_seated.csv, and
   the November poll becomes a '[voided election re-run]' rather than replacing Campbell again.
   Evidence: research/bna/zcc-1960-1974.md.
60. Orkney and Shetland (Westminster) polling days, 1874-1931. The baseline had the year only
   (the 1st of January) or, for unopposed returns, the national polling day. Until 1910 the
   constituency polled over two days, set here to the first: Mon 26 Apr 1880 ("the election on
   the 26th and 27th", ST 24 Apr 1880); Mon 14 Dec 1885 ("The polling took place in Lerwick on
   Monday", 19 Dec 1885); Mon 26 Jul 1886 ("on Monday and Tuesday last", 31 Jul 1886); Mon 25
   Jul 1892 ("Polling commenced ... on Monday morning", 30 Jul 1892); Tue 6 Aug 1895 ("on Tuesday
   and Wednesday", Shetland News 10 Aug 1895); Tue 23 Oct 1900 ("polls on Tuesday and Wednesday
   next", 20 Oct 1900); Tue 6 Feb 1906 (the Returning Officer's notice, 27 Jan 1906); Tue 8 Feb
   1910 ("on Tuesday and Wednesday", 5 Feb 1910). Unopposed returns are dated to the nomination,
   when the member was declared: Fri 20 Feb 1874 (23 Feb 1874), Wed 14 Dec 1910 (17 Dec 1910),
   Wed 4 Dec 1918 (7 Dec 1918), Sat 18 Oct 1924 (25 Oct 1924) and Fri 16 Oct 1931 (24 Oct 1931).
61. 1880: Laing's opponent was George Roy Badenoch, not H. B. Riddell (Riddell stood in 1868), and
   he had 518, not 578: "the total this week was 1414, giving Liberal majority 378" (ST 1 May
   1880), and "G. R. Badenoch (C.) 518" in the table of past contests (17 Aug 1895). The wiki's
   turnout, 1161, was the 1868 total. Electorate 1703.
62. 1886: "Lyell 2353, Hoare 1382, majority for Lyell 971" (ST 31 Jul 1886). The wiki repeated the
   1885 figures (3352, 1940).
63. 1895: Lyell 2361, Ralph Wardlaw McLeod Fullarton, Q.C. (C.) 1580, majority 781 (ST 17 Aug
   1895, 3 Nov 1900, 29 Nov 1902). The wiki repeated the 1892 result, W. Younger included.
64. Labour candidates 1950-1966: in 1950 H. R. Leslie had 4198, not 3335 (ST 3 Mar 1950); 3335
   was Magnus Fairnie's vote in 1951 (2 Nov 1951), and the 1955 candidate was Edgar Ramsay (3 Jun
   1955), not Leslie. In 1966 it was Hugh Lynch (8 Apr 1966), not Ian MacInnes, who stood in 1964.
   Also "Hon. C. T. Dundas" (1885), not "Dundass" (19 Dec 1885).
65. Westminster turnouts the wiki copied from another election: 1895, 1900 (both 1892's 4241),
   1959 and 1964 (both 1955's 18427), and 1935 (14226): set to the candidates' total, 1895 3941,
   1900 4074, 1935 14586, 1959 18861, 1964 18540. 1880 is in #61.
66. Westminster electorates from the Shetland Times: 1873 1537, 1874 1618, 1880 1703, 1885 7394
   (ST 17 Aug 1895), 1951 29,603 (2 Nov 1951), 1955 28,298 (3 Jun 1955).
   Evidence for 60-66: research/bna/westminster-1873-1974.md.

67. Henry Mouat (ex-County Convener) died on Saturday 20 May 1944, not 20 March. Shetland News,
   Thursday 25 May 1944: "Mr Henry Mouat died early on Saturday morning at his residence,
   Hamarslea, Lerwick, in his 69th year"; funeral on the Wednesday. The Shetland Times obituary
   is 26 May 1944, and the Town Council's tribute came at its meeting of 6 Jun 1944.
   Evidence: research/bna/ww2-councils.md.

68. James Robertson, the Social-Democrat candidate for Lerwick Town Council in 1901 and 1903 and
   for Lerwick North on the County Council in December 1901, is Dr James Robertson, M.B., Ch.B.
   (Bayanne I28791). The 1901 burgh notice gives his address as Northness, and the County
   nominations call him "Jas. Robertson, auctioneer" (Shetland Times, 2 and 23 Nov 1901). His
   death notice (Shetland Times, 4 Feb 1911) is for "JAMES ROBERTSON, M.B., Ch.B., eldest
   surviving son of Thomas and Grace Robertson, North Ness, Lerwick", and the obituary in the
   same issue gives his career as house painter, then auctioneer and fish salesman, then doctor.
   He has no person page (he never sat), so the three candidacies link to Bayanne.
   Evidence: research/bna/early-socialists-1901-1913.md.

69. Biographies for five County Councillors of the 1970s who had none: Fraser Peterson (obituary
   and death notice, Shetland Times, 25 Jun 1982), Arthur Irvine (ii) (nominations, 11 Sep 1970;
   result, 2 Oct 1970; death notice, 12 May 1995), John Laurenson (1973 nominations, 20 Apr 1973;
   death notice, 17 Jan 1992), Robert Balfour (resignation, 24 Mar 1972; advert, 20 Apr 1973;
   death notice, 28 Feb 1975) and Iain Caldwell (Aith Knitwear Factory adverts, Aug and 6 Sep
   1968; his letter as managing director of the Nordport Company, 27 Apr 1973). Irvine and
   Laurenson signed the retiring landward members' statement on oil policy, Peterson and Balfour
   the Democratic Group's (4 May 1973, p2 and p8). Arthur Irvine died on
   26 April 1995, not the 25th: "Peacefully at Fernlea Care Centre, Whalsay, on 26th April".
   Evidence: research/bna/biographies-zcc-1970s.md.

70. Biographies for ten more councillors who had none, from their obituaries and death notices
   in the Shetland Times: John Butler (18 May 1984), James Paton (i) (28 Aug, 4, 11 and 18 Sep
   1981), Harry Gray (11 Apr 1980), Hugh Williamson (31 May 1991; piano-tuning advert 13 Sep 1963,
   nominations 21 Apr 1967, his letter 25 Feb 1972, 20 Apr 1973), Robert Garrick (12 and 19 May
   1961, 22 Oct 1993), James Pottinger (ii) (29 Feb and 7 Mar 1980), Charles Brown (21 Apr 1967,
   12 Aug 1977), William Marshall (16 Sep 1960, 21 Apr 1961, 7 Jun 1963), Peter Henry (16 May
   1952, 23 Oct 1959, 16 Feb 1979) and Robert Ollason (27 Jan 1961). Paton's intro had him on
   the SIC "between 1978 and 1982"; he sat 1974-78 and lost Lerwick Twageos in 1978 (the wiki's
   own succession box, and the Convener's tribute, ST 11 Sep 1981), and the parser had cut the
   start of the sentence listing his defeats. Harry Gray was born in Orkney (ST 11 Apr 1980).
   Paton on the County Council: at its statutory meeting of Fri 8 Nov 1929 the Town Council
   appointed "the twelve good men and true in the Town Council" to represent the burgh on the
   reconstituted County Council (ST 16 Nov 1929, p4, art. 096), and in 1961 the County Council
   was 24 landward members plus the Town Council, 36 in all (ST 12 May 1961, p5, art. 065).
   Evidence: research/bna/biographies-1950s-1980s.md.

71. Biographies for thirteen Town Councillors of 1818-1850 who had none. They died before the
   Shetland Times began, so the sources are the Orkney & Zetland Chronicle (1825), the Shetland
   Journal (1836-37, in the BNA as the Orkney and Shetland Journal), the John o' Groat Journal,
   the Northern Ensign and the Scottish and naval papers' notices: William Copland (Shetland
   Journal, 1836-37), James Yorston (Pilot 26 May 1813; births, Scotsman 20 Aug 1825 and
   Caledonian Mercury 14 Apr 1827; Shetland Journal 1837; death, Hampshire Telegraph 17 Feb and
   Dover Telegraph 24 Feb 1849), Robert Goudie (Northern Ensign and John o' Groat Journal, 2 Dec
   1869), Archibald Greig (Edinburgh Evening Post 27 Oct, Northern Ensign 28 Oct 1852), Charles
   Ogilvy (ii) (Chronicle 31 May 1825; Inverness Courier 12 Jun, John o' Groat Journal 21 Jun
   1844), John Ogilvy (Caledonian Mercury 10 Dec 1840; Inverness Courier 28 May 1845; Aberdeen
   Journal 14 Apr 1847), Gilbert Duncan (Glasgow Courier 9 Mar 1844), Alexander Irvine
   (Chronicle 28 Feb 1825), Gilbert Paterson (Edinburgh Evening Courant 24 Apr, Perthshire
   Courier 1 May 1828), James Pottinger (i) (The News 17 Jul 1836), William Clark (i) (John o'
   Groat Journal 7 Apr 1854), James Hunter (i) (Inverness Courier 17 Aug 1847) and David
   Nicolson (Perthshire Advertiser 14 Jun 1849). Three facts corrected from the notices: James
   Yorston died at Leith, not Lerwick ("At Leith, James Yorston, Esq., Paymaster and Purser,
   R.N.", on the 10th, our date); Gilbert Paterson died on 8 Apr 1828, not the 5th ("on the 8th
   current", Courant; "the 8th ultimo", Courier); Archibald Greig died on 12 Oct 1852, not the 11th
   (Evening Post, Morning Chronicle and Londonderry Standard). Evidence:
   research/bna/biographies-ltc-1818-1850.md.

72. Greig v Edmondston, and Charles Ogilvy (ii)'s mail meeting, from the pre-1872 sweep of the
   Orkney & Zetland Chronicle and the Shetland Journal. James Greig, Procurator Fiscal, sued Dr
   Arthur Edmondston for libel in a letter Edmondston printed in 1823, addressed to the Lord
   Advocate. Tried before the Jury Court on Wednesday 7 June 1826; damages laid at £2,000;
   unanimous verdict for Greig, £300 (Caledonian Mercury 10 Jun, Inverness Courier 14 Jun,
   Chronicle 20 Jun and 20 Sep 1826). Edmondston's page has a full intro (his career, books and
   family, and "notoriously litigious") but an empty biography section, which now holds the case;
   Greig's biography gains a paragraph. Ogilvy chaired the 1836
   Lerwick meeting on the Peterhead mail packet (Shetland Journal, 10 Sep 1836) and led the
   coronation procession of 1838 (Orkney and Shetland Journal, 1 Jun and 1 Aug 1838). Evidence:
   research/bna/ltc-pre-1872-press.md.
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
MOUAT_DEATH = ('henry-mouat', '1944-03-20', '1944-05-20')
IRVINE_DEATH = ('arthur-irvine-ii', '1995-04-25', '1995-04-26')
BIOGRAPHIES = {  # slug -> biography, for people who had none (#69, #70, #71)
    'fraser-peterson': (
        "Fraser Peterson was the elder son of Barron and Maggie Peterson of North House, Burravoe, "
        "Brae. He married Ina Johnson, and they had three daughters. He was a crofter and ran a "
        "transport business, and was a leading member of the Brae community.\n\n"
        "He joined Zetland County Council in 1970, and in March 1973 he was one of the seven "
        "councillors who formed the Shetland Democratic Group. On Shetland Islands Council he sat "
        "for Delting, and from the 1978 boundary changes for Delting North. He chaired the "
        "council's ports and harbours committee and its pilotage committee, and was closely "
        "involved in the developments at the Sullom Voe terminal while still finding time for his "
        "constituents.\n\n"
        "He had been in hospital for some time, but resumed his council duties. He went back into "
        "hospital after the first meeting of the new ports and harbours committee on 7 June 1982, "
        "and died at the Royal Infirmary, Aberdeen, on 18 June 1982, aged 45."
    ),
    'arthur-irvine-ii': (
        "Arthur \"Ertie\" Irvine was a retired schoolteacher living at Crapp, Gulberwick, when "
        "[person:robert-johnson-i:Robert Johnson], the member for Gulberwick and Quarff, died in "
        "1970. Johnson's widow, Christine, also stood at the by-election, and Irvine won on Tuesday "
        "29 September 1970 with 55 votes to her 22.\n\n"
        "Before the 1973 election he was one of the twelve retiring landward members who signed a "
        "statement supporting the County Council's policy on North Sea oil, published the same week "
        "as the Shetland Democratic Group's. He held the seat with 45 votes to Alistair Leask's 43.\n\n"
        "His wife died before him. He died at Fernlea Care Centre, Whalsay, on 26 April 1995, "
        "aged 89."
    ),
    'john-laurenson': (
        "John Morrison \"Jackie\" Laurenson was the elder son of George and Catherine Laurenson, "
        "formerly of Bonniview, Bigton, and married Flora Isbister.\n\n"
        "He was first elected for Fetlar in 1970. At the 1973 election he was a GPO engineer "
        "living at Westing, Ladysmith Road, Scalloway, and was returned for Fetlar unopposed. "
        "Before that election he was one of the twelve retiring landward members who signed a "
        "statement supporting the County Council's policy on North Sea oil.\n\n"
        "He died at Glamis Hospital, Dunedin, New Zealand, on 8 January 1992, aged 67."
    ),
    'robert-balfour': (
        "Robert Balfour, of Lunnister, Sullom, was the last of the family of Thomas and Catherine "
        "Balfour. He married Nessie Hall.\n\n"
        "He won Northmavine South in 1955 and held it for seventeen years. He chaired the Roads "
        "Committee for some years and was its vice-chairman when he retired. He also sat on the "
        "Policy, General Purposes and Education Committees, represented the Council on the "
        "Licensing Court of Appeal, and was a member of the Zetland Executive Council and a "
        "governor of the North of Scotland College of Agriculture. He resigned because of "
        "ill-health in March 1972, when he was perhaps the oldest member of the Council. The "
        "convener, [person:edward-thomason:Edward Thomason], spoke of his deep respect for the "
        "principles of democracy and for the rights of voters. "
        "[person:john-jamieson:John Jamieson] took the seat.\n\n"
        "In 1973 he came back for Northmavine North. In his advert he associated himself with the "
        "Shetland Democratic Group and offered voters a choice between \"a Democratic Council and "
        "what now appears to be a Bureaucratic assembly\". He unseated "
        "[person:andrew-cromarty:Andrew Cromarty] by 46 votes to 37.\n\n"
        "He died suddenly at the Gilbert Bain Hospital on 17 February 1975, and his funeral was at "
        "Aith Kirk."
    ),
    'iain-caldwell': (
        "By August 1968 Iain Caldwell was running the Aith Knitwear Factory at Aith, Bixter, which "
        "he advertised as \"Shetland's newest knitwear firm\". It took on machinists and plain and "
        "Fair Isle finishers across the West Side and Central Mainland, with the work delivered "
        "and collected.\n\n"
        "He first visited Shetland in 1962. By 1973 he was managing director of the Nordport "
        "Company Ltd, which planned an oil complex based mainly at Graven, Sullom Voe, and which "
        "was critical of the County Council's Private Bill.\n\n"
        "He stood for Aithsting in 1970 and lost to [person:robert-garrick:Robert Garrick] by 161 "
        "votes to 124. In 1973 he stood again, \"in broad agreement\" with the aims of the Shetland "
        "Democratic Group, and promised to declare an interest and withdraw from any issue bearing "
        "on his business interests. He won with 151 votes, to Garrick's 124 and Peter F. M. "
        "Tulloch's 46.\n\n"
        "He died at Blairgowrie on 4 July 2007."
    ),
    # 70
    'john-butler': (
        "John Butler, a customs officer, first came to Shetland in 1948, and a year later married "
        "Christina Smith. They lived in London, the south of Scotland and Barnsley before coming "
        "back to Shetland in 1959.\n\n"
        "He stood for Lerwick Town Council as a Labour candidate in 1970 and was not elected, but "
        "was returned unopposed in 1971. On Shetland Islands Council he sat for Lerwick Breiwick, "
        "beating [person:harry-gray:Harry Gray] and [person:ronald-cumming:Ronald Cumming] in 1974 "
        "and Gray again in 1978. He chaired the resources committee, the joint consultative "
        "committees for staff and manual workers, and the investment group, and was on the "
        "fisheries working group and the Licensing Board. He was vice-convener from May 1978 "
        "until 1982, working closely with the convener, "
        "[person:alexander-tulloch:Alexander Tulloch], who praised his courage and his "
        "understanding of finance. He stood down in May 1982 because of ill-health.\n\n"
        "He was involved with the Shetland Council of Social Service in the 1960s, wrote for the "
        "\"Da Wadder Eye\" column in The New Shetlander, and was active in the Althing Debating "
        "Group. He had multiple sclerosis for a number of years, and died in the Gilbert Bain "
        "Hospital on 12 May 1984, aged 58."
    ),
    'james-paton-i': (
        "Jimmy Paton was a lifelong trade unionist and a stalwart of the Labour Party. He first "
        "won a seat on Lerwick Town Council in 1960. From 1930 the Town Council's members also "
        "sat on Zetland County Council as the burgh's representatives, so although he was never "
        "elected to it, he sat on the County Council until local government reorganisation, and "
        "was elected to eight of its committees. He then sat for four years on Shetland Islands "
        "Council. After he left the council he stayed on the "
        "social work committee as the pensioners' representative.\n\n"
        "He helped to found the Shetland branch of the Scottish Old Age Pensioners' Association "
        "and chaired it until shortly before his death, and was the old folks' champion in many "
        "campaigns. He sat on the executive committee of the Shetland Council of Social Service "
        "from 1978. He was known for his heckling at political meetings, and the Convener, "
        "[person:alexander-tulloch:Alexander Tulloch], remembered him as \"a formidable "
        "opponent\" in council.\n\n"
        "He died at the Gilbert Bain Hospital on 26 August 1981, aged 81."
    ),
    'harry-gray': (
        "Harry Gray was born in Orkney, of a Shetland family; his father was at one time connected "
        "with the Bressay lighthouse. He lived and worked in the Edinburgh area until after the "
        "Second World War, in the fish trade, and was active in the Boys' Brigade there. He then "
        "settled in Lerwick, where he worked for Shetland Fish, then Hay & Co., and latterly as "
        "local representative for Richard Irvin's. He was secretary of the Shetland Fish "
        "Merchants' Association.\n\n"
        "On Lerwick Town Council he was Honorary Treasurer and then Bailie, and Provost from 1962 "
        "for three years. He chaired the Northern Burghs Association. He sat on the bench of "
        "Lerwick Burgh Court and was an honorary sheriff. He stood for Lerwick Breiwick at the "
        "first Shetland Islands Council election in 1974 and again in 1978, and lost both times "
        "to [person:john-butler:John Butler].\n\n"
        "He was commandant of the Army Cadet Force in Shetland until 1974, reaching the rank of "
        "lieutenant-colonel, chaired the local committee of the RSPCA, and was a member of the "
        "Shetland-Norwegian Friendship Society. He married Ruth Bethune Scott and lived at 20 "
        "St Olaf Street. He collapsed and died suddenly on 4 April 1980, aged 70, at the "
        "Methodist Schoolroom in Lerwick, where he had come to take part in an \"Any Questions\" "
        "evening. Flags flew at half-mast on public buildings for his funeral at St Columba's."
    ),
    'hugh-williamson': (
        "Hugh Robert Williamson was the son of William and Clara Williamson of West Sandwick, Yell. "
        "He served his time as a piano tuner in Aberdeen and worked in Aberdeenshire and "
        "Kincardineshire, coming to Shetland on visits to tune pianos from 1957; in 1963 he was "
        "still living at Ballogie, Aberdeenshire. He came home to Shetland in 1965, and from "
        "1964 tuned the school pianos twice a year, travelling by motor cycle from Virkie to "
        "Haroldswick, until his health stopped him in 1969.\n\n"
        "In 1967, living at Bayanne House, Sellafirth, he was nominated for Yell North when "
        "[person:hugh-sutherland:Hugh Sutherland] stood down there, and was one of only two "
        "newcomers to the County Council that year. He was returned again in 1970, and did not "
        "stand in 1973.\n\n"
        "He married Ann Jane Jamieson, who died before him. He died at Montfield Hospital, "
        "Lerwick, on 24 May 1991, aged 81."
    ),
    'robert-garrick': (
        "Robbie Garrick, of Bixter, won Aithsting from the sitting member, "
        "[person:catherine-anderson:Catherine Anderson], in 1961 by 206 votes to 99, one of four "
        "retiring members defeated that year; turnout in Aithsting was the highest in the county. "
        "He held the seat unopposed in 1964 and 1967, and beat "
        "[person:iain-caldwell:Iain Caldwell] in 1970. Before the 1973 election he was one of the "
        "twelve retiring landward members who signed a statement supporting the County Council's "
        "North Sea oil policy. Caldwell, standing in broad agreement with the Shetland Democratic "
        "Group, took the seat. In 1974 he stood for Shetland Islands Council and lost to "
        "[person:alexander-tulloch:Alexander Tulloch].\n\n"
        "He married Josephine, and latterly lived at King Erik House, Lerwick. He died in the "
        "Gletness Ward, Montfield, on 15 October 1993, aged 76."
    ),
    'james-pottinger-ii': (
        "James Pottinger, of St Catherine's, Hamnavoe, served in the First World War with the 2nd "
        "Battalion, Gordon Highlanders. He was connected with fishing all his life. In 1939 he "
        "became secretary of the Shetland branch of the Scottish Herring Producers' Association, "
        "and when the Shetland Fishermen's Association was formed in 1943 he became its secretary, "
        "retiring only in 1975. As spokesman for Shetland fishermen he was known in fishing and "
        "government circles throughout Scotland. He was also Shetland's representative of the "
        "Royal Humane Society.\n\n"
        "On the County Council he sat for Burra from 1938 until 1955, when "
        "[person:robert-strachan:Robert Strachan] beat him by 215 votes to 165. Two months later "
        "he won a by-election in Sandwick, which he held until 1958. He came back for Whalsay and "
        "Skerries at a by-election in 1964 and sat until 1973. He was vice-convener of the county "
        "from 1947 to 1955, and became a Justice of the Peace in 1956.\n\n"
        "He died on 27 February 1980, aged 82, and was buried at Papil. The flag on Lerwick Town "
        "Hall flew at half-mast on the day of his funeral."
    ),
    'charles-brown': (
        "Charles Brown, of North Dale, Fetlar, won the Fetlar seat in December 1945 by 64 votes "
        "to 22 against [person:edward-sinclair:Edward Sinclair], and was returned unopposed at "
        "every election from 1949 to 1964. No nomination came from Fetlar in 1967, when the "
        "Shetland Times described him as \"a faithful councillor for many years\" and the oldest member of the "
        "council, and wished \"that seasoned traveller\" well on his retiral.\n\n"
        "He later lived in Edinburgh, at West Pilton Gardens, and died at Leith Hospital on 28 "
        "July 1977."
    ),
    'william-marshall': (
        "William Marshall came to Delting from his first charge, Inch, in the Presbytery of "
        "Stranraer. He preached at Voe, Mossbank and Brae in August 1960 and was unanimously "
        "elected, and in September the Shetland Presbytery sustained the call, although he had "
        "completed only two years at Inch. He lived at the Manse, Brae.\n\n"
        "In May 1961, when he had been minister at Brae for only a few months, he was returned "
        "unopposed to the County Council for Delting North.\n\n"
        "In June 1963 the Presbytery gave him leave to demit his charge from the end of July. He "
        "had been appointed secretary-treasurer of a 600-bed teaching hospital at a mission "
        "station in the Northern Transvaal, and he and his family sailed for South Africa on 4 "
        "July. He planned to keep in touch with the Delting elders by sending them a monthly "
        "tape. His seat was filled at a by-election that September.\n\n"
        "He died at Bathgate, West Lothian, on 18 August 1995."
    ),
    'peter-henry': (
        "Peter John Henry won Walls in 1952, beating "
        "[person:james-anderson-iv:James Anderson] by 93 votes to 45, and was returned unopposed "
        "in 1955 and 1958. He resigned in October 1959, and the County Council accepted his "
        "resignation with regret at its meeting on Tuesday 20 October.\n\n"
        "He married Margaret Ann Reid, who died before him, and latterly lived at 1 Burgh Road, "
        "Lerwick. He died at the Gilbert Bain Hospital on 5 February 1979, aged 78."
    ),
    'robert-ollason': (
        "Robert Ollason was born at Hay's Dock, Lerwick, where his father was a sailmaker. He left "
        "school to be an apprentice in a Lerwick law office, but six years later went into "
        "business. After a spell with Goodlad & Goodlad he became a partner in J. & R. Groat in "
        "1907, and in 1915 he bought the newsagent and stationery business that he ran for the "
        "rest of his life. Until the early 1920s he had financial interests in fishing boats, and "
        "fishermen sought his advice long after. He was the first secretary of the Shetland "
        "branch of the Scottish Herring Producers' Association, formed in 1932.\n\n"
        "He entered the Town Council in 1919. He chaired its housing committee through the "
        "interwar years, when municipal housing changed the face of the town, and was Provost "
        "from 1933 to 1936. Between his spells on the Town Council he sat on the County Council "
        "for Whalsay. He chaired the Health Committee for twenty years, and the Education "
        "Committee for the last seven.\n\n"
        "From 1948 he chaired the Board of Management for Shetland Hospitals, and he was a "
        "member, and for a time vice-chairman, of the North East Regional Hospitals Board. He was "
        "made an OBE in 1957, and had campaigned for the new Gilbert Bain Hospital, whose "
        "foundation stone had been laid but which he did not live to see opened. He was also an "
        "honorary sheriff-substitute and senior member of the bench, chairman of the trustees of "
        "the Feuars and Heritors of Lerwick and of the local Rent Tribunal, founder chairman of "
        "the Shetland Chamber of Commerce, and an elder of St Columba's for over twenty years.\n\n"
        "He was taken ill in his shop at lunch-time on Saturday 21 January 1961 and died in the "
        "night in the Gilbert Bain Hospital, aged 73. He was survived by his wife, Joan White, who had hoped to "
        "celebrate their golden wedding the next month, and by three daughters and two sons."
    ),
    # 1818-1850 (#71)
    'william-copland': (
        "William Copland was a merchant and linen-draper in Lerwick in the 1830s. In October 1836 "
        "he advertised a new stock of woollen and silk goods, bought on a visit to the chief "
        "markets in England and Scotland. He was also the Lerwick agent for the Peninsular Steam "
        "Navigation Company of Willcox and [person:arthur-anderson:Anderson], and agent for "
        "Shetland for several other firms.\n\n"
        "When [person:arthur-anderson:Arthur Anderson] started the Shetland Journal in 1836, "
        "Copland was its man in Lerwick. Letters to \"the originators of the Shetland Journal\" "
        "were left with him, and each number was \"sold by William Copland, Lerwick\". Through "
        "him Anderson offered Shetlandmen a passage south to look for work in the merchant "
        "service. In 1837, when 20,000 people in Shetland were said to be starving, he and "
        "[person:james-yorston:James Yorston] took applications from men wanting a free passage "
        "to a new settlement overseas. Later that year they took applications from parents for "
        "Anderson's scheme to place Shetland boys in the merchant service."
    ),
    'james-yorston': (
        "James Yorston was a purser in the Royal Navy, with seniority from 1813. In May of that "
        "year a J. Yorston, purser of the Pert, was moved to the Leven. By the 1820s he was "
        "living at Sound, near Lerwick, where a daughter was born to him and his wife in July "
        "1825 and a son in February 1827.\n\n"
        "In 1837 he and [person:william-copland:William Copland] were "
        "[person:arthur-anderson:Arthur Anderson]'s contacts in Lerwick. They took applications "
        "from men wanting a passage to a new settlement overseas, and from parents of boys "
        "wanting a place in the merchant service.\n\n"
        "He and his wife died of typhus fever at Leith in February 1849, within two days of each "
        "other: she first, and he on 10 February. The naval obituaries listed him as Paymaster "
        "and Purser."
    ),
    'robert-goudie': (
        "Robert Goudie came to Lerwick around 1810. He served his apprenticeship with his "
        "relatives the Messrs Sinclair, then extensive merchants in the town, and afterwards set "
        "up in business for himself. The son of pious parents, he took a deep interest in "
        "religious and church affairs. From the Disruption of 1843 he was a devoted member of the "
        "Free Church, but he gave to other churches as well.\n\n"
        "He died at Lerwick on the evening of Saturday 20 November 1869, aged 71, after a long "
        "illness, leaving a widow and family. The Northern Ensign called him \"one of our oldest "
        "and most worthy townsmen\"."
    ),
    'archibald-greig': (
        "Archibald Greig, of Sandsound, was Procurator Fiscal of Zetland, the county's public "
        "prosecutor, as his father had been. He had an apoplectic stroke the year before he died "
        "and never fully recovered. He died suddenly at his house in Lerwick on 12 October 1852. "
        "The Northern Ensign's Lerwick correspondent noted that he was the third fiscal to die "
        "within six months."
    ),
    'charles-ogilvy-ii': (
        "Charles Ogilvy, junior, merchant in Lerwick, married Martha Fea, youngest daughter of "
        "Thomas Fea, collector of customs at Lerwick, on 14 May 1825.\n\n"
        "In 1844 he was taken ill on a visit to Edinburgh, and died at 16 Albany Street on 5 June. "
        "His body was brought home to Lerwick on the steamer Sovereign. The John o' Groat "
        "Journal's Zetland correspondent wrote that his death had cast a deep gloom over Zetland. "
        "He was esteemed for his frankness, humility and quiet generosity, and was \"a general "
        "favourite with all classes\". During the week of the funeral the streets of Lerwick "
        "looked as they did on a Sunday, and little business was done."
    ),
    'john-ogilvy': (
        "John Ogilvy of Quarff was a merchant and banker in Lerwick. He died in London on 31 "
        "October 1840. His affairs were not settled at his death. In May 1845 his estate was "
        "sequestrated as that of the \"sometime merchant and banker in Lerwick, now deceased\", "
        "and his creditors were still meeting in Lerwick in 1847."
    ),
    'gilbert-duncan': (
        "Gilbert Duncan was a purser in the Royal Navy as well as a writer (solicitor) in Lerwick. "
        "In February 1825 he was at the Lerwick meeting of Shetland landholders that set out to "
        "make up a valuation roll for Zetland and to claim their right to vote for the county's "
        "Member of Parliament. He acted there under mandates for absent proprietors. He died at "
        "Lerwick on 19 February 1844."
    ),
    'alexander-irvine': (
        "Alexander Cumming Irvine was a merchant in Lerwick. In February 1825 he attended the "
        "Lerwick meeting of Shetland landholders on behalf of his father, Andrew Irvine. The "
        "meeting had been called to make up a valuation roll for Zetland and to claim the "
        "landholders' right to vote for the county's Member of Parliament."
    ),
    'gilbert-paterson': (
        "Gilbert Paterson died at Lerwick on 8 April 1828 after a short illness. The Edinburgh "
        "Evening Courant called him a most active, industrious and enterprising man."
    ),
    'james-pottinger-i': (
        "James Pottinger died at Edinburgh in June 1836. That autumn the quarterly naval "
        "obituary listed him among the pursers who had died."
    ),
    'william-clark-i': (
        "William Clark's premises were at No. 40 Commercial Street, Lerwick, where he died in "
        "March 1854 at the advanced age of 72."
    ),
    'james-hunter-i': (
        "James Hunter's business was the firm of James Hunter and Son, merchants, in Lerwick. He "
        "died at Lerwick on 2 August 1847."
    ),
    'david-nicolson': (
        "David Nicolson lived at Annsbrae in Lerwick, where he died on 4 June 1849 in his "
        "sixty-first year."
    ),
    # Greig v Edmondston (#72): Edmondston's intro covers his life; this fills his empty biography field
    'arthur-edmondston': (
        "Edmondston and [person:james-greig:James Greig], the Procurator Fiscal, both sat on the "
        "first Town Council, elected in 1818. In August 1821 Edmondston wrote officially to the "
        "Lord Advocate about Greig's conduct as fiscal, and in 1823 he printed and published a "
        "letter to the Lord Advocate, Sir William Rae, saying so again. It accused Greig of acting "
        "both for the Crown and for his brother-in-law Francis Heddell in the same cause, over a "
        "pier built below high-water mark at Lerwick, and of taking fees from each. In an earlier "
        "action in the Sheriff Court Edmondston had already been found liable to Greig in "
        "damages.\n\n"
        "Greig sued him for libel, claiming £2,000. The case was tried before a jury in Edinburgh "
        "on Wednesday 7 June 1826, with Francis Jeffrey and Henry Cockburn for Greig. The jury "
        "found unanimously for Greig and awarded him £300. The Orkney & Zetland Chronicle printed "
        "the trial at length."
    ),
}
BIO_ADDITIONS = [  # (slug, text the addition follows, addition) (#72)
    ('james-greig', "James was a baillie of Lerwick.",
     "\n\nIn 1826 he won £300 damages for libel from [person:arthur-edmondston:Dr Arthur "
     "Edmondston], a fellow member of the first Town Council. In a letter printed in 1823 and "
     "addressed to the Lord Advocate, Edmondston had accused him of acting on both sides of a "
     "case while Procurator Fiscal. A jury in Edinburgh found for Greig unanimously on 7 June "
     "1826."),
    ('charles-ogilvy-ii', "Thomas Fea, collector of customs at Lerwick, on 14 May 1825.",
     "\n\nIn 1836 he chaired a public meeting at Lerwick on the mail. It complained that the "
     "Peterhead packet had been kept a week at Peterhead loading cargo for its contractors, and "
     "asked for the contract to be opened to public competition. [person:arthur-anderson:Arthur "
     "Anderson]'s Shetland Journal printed the resolutions while saying it did not entirely "
     "agree: Anderson wanted the Government to pay a steamer to carry the mail. Anderson got his way "
     "in 1838, when the Sovereign was taken up to carry the mail weekly, and the Bailies agreed "
     "to the burgesses' request that the town be illuminated for her first arrival in April. On "
     "Queen Victoria's coronation day, 28 June 1838, Ogilvy as Chief Magistrate and "
     "[person:gilbert-duncan:Gilbert Duncan] as Junior Bailie led the town's procession round "
     "the flagstaff at Fort Charlotte."),
]
YORSTON_DEATH_PLACE = ('james-yorston', 'Lerwick', 'Leith')  # (#71)
PATERSON_DEATH = ('gilbert-paterson', '1828-04-05', '1828-04-08')  # (#71)
GREIG_DEATH = ('archibald-greig', '1852-10-11', '1852-10-12')  # (#71)
PATON_INTRO = (  # (old, new) for james-paton-i (#70)
    "James John Paton was a Lerwick Town Councillor between 1960 and 1975 and a Shetland Islands "
    "Councillor for Lerwick Twageos between 1978 and 1982. He was the grandfather of former "
    "Shetland Islands Councillor for the same area, [person:james-paton-ii:James]. as well as the "
    "Lerwick Twageos seat at the 1978 Shetland Islands Council election.",
    "James John Paton was a Lerwick Town Councillor between 1960 and 1975 and a Shetland Islands "
    "Councillor for Lerwick Twageos between 1974 and 1978. He was the grandfather of former "
    "Shetland Islands Councillor for the same area, [person:james-paton-ii:James]. He "
    "unsuccessfully contested the 1945, 1947, 1958 and 1968 Lerwick Town Council elections, as "
    "well as the Lerwick Twageos seat at the 1978 Shetland Islands Council election.",
)
GRAY_BIRTH_PLACE = ('harry-gray', 'Orkney')  # was empty (#70)
ROBERTSON_BAYANNE = '[https://www.bayanne.info/Shetland/getperson.php?personID=I28791&tree=ID1 James Robertson]'
ROBERTSON_CANDIDACIES = [  # (wiki title, votes): his three unlinked 'James Robertson' candidacies
    ('Lerwick Town Council Election November 1901', 130),
    ('Lerwick Town Council Election November 1903', 163),
    ('County Council Election December 1901', 38),
]
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
    ('Whiteness And Weisdale County Council By-Election June 1914', '1914-06-01', '1914-06-18'),
    ('Nesting County Council By-Election December 1914', '1914-12-01', '1914-12-24'),
    ('Aithsting County Council By-Election August 1915', '1915-08-01', '1915-08-19'),
    ('Fetlar County Council By-Election July 1917', '1917-07-01', '1917-07-19'),
    ('Cunningsburgh_County_Council_By-Election_February_1919', '1919-02-01', '1919-02-20'),
    ('Dunrossness South County Council By-Election June 1919', '1919-06-01', '1919-06-19'),
    ('Tingwall County Council By-Election June 1919', '1919-06-01', '1919-06-19'),
]
DELTING_NORTH_1904 = ('County Council Election December 1904', 'Delting North', 'Arthur White', 'arthur-white',
                      'James Inkster', 'james-inkster')
POLLING_DAY_ZCC_1910 = ('County_Council_Election_December_1910', '1910-12-03', '1910-12-06')
LAING_INTRO = ('james-laing', 'County Councillor for Lerwick South between 1922 and 1929',
               'County Councillor for Lerwick South between 1919 and 1929')
LERWICK_SOUTH_1919 = ('County_Council_Election_December_1919', 'Lerwick South', ('Charles Stout', 'charles-stout'),
                     ('James Laing', 'james-laing'))
SANDWICK_1907 = ('County Council Election December 1907', 'Sandwick', (276, 161), ('William Smith', 91), ('James Thomson', 70))
WHITENESS_1890 = ('County Council Election February 1890', 'Whiteness And Weisdale', (126, 52.9), (116, 56.9))
MACDOUGALL_INTRO =('until he resigned in April 1912', 'until he resigned in October 1912')

HAY_NOTE = (
    "Arthur J. Hay topped the poll but declined to take office (letter, 8 November 1884). "
    "The vacancy was filled by co-option: see Lerwick Town Council By-Election November 1884."
)

DUNROSSNESS_NORTH_1922 = ('County Council Election December 1922', 'Dunrossness North',
                          ('James Robert White', 60), ('A. Irvine, jun.', 36))

POLLING_DAY_ZCC_1925 = ('County Council Election December 1925', '1925-12-05', '1925-12-01')

POLLING_DAY_ZCC_1928 = ('County Council Election December 1928', '1928-12-05', '1928-12-04')
WALLS_1928 = ('County Council Election December 1928', 'Walls', (192, 51.6), (389, 25.4))

ZCC_BY_ELECTIONS_1933 = [
    # (title, ward, replaced_person, (slug, candidate name), note)
    ('Sandwick County Council By-Election February 1933', 'Sandwick', '[unfilled seat]',
     ('william-jamieson', 'William Jamieson'),
     'No nomination was lodged for Sandwick at the December 1932 election. The Scottish Office ordered '
     'a new election for 14 February 1933, and William Jamieson, the former member, was the only nomination.'),
    ('Sandness County Council By-Election February 1933', 'Sandness', '[voided election re-run]',
     ('james-jamieson-ii', 'James A. Jamieson'),
     'James A. Jamieson was returned unopposed for Sandness in December 1932, but was not eligible to sit '
     'because he held a minor appointment under the Council. He resigned it, and was the only nomination '
     'at the new election ordered for 14 February 1933.'),
]
ZCC_1933_DATE = '1933-02-14'

WALLS_1935 = ('County Council Election December 1935', 'Walls', 'Andrew Halcrow', 85, 4, (328, 93, 28.3))
JAMIESON_INTRO = ('william-jamieson', 'Sandwick between 1919 and 1932', 'Sandwick between 1919 and 1935')

WHALSAY_1938 = ('County Council Election December 1938', 'Whalsay And Skerries', 'James Hay', 121, 32)
YELL_1938 = ('County Council Election December 1938',
             ('Yell North', (471, 331, 66.0), (257, 158, 61.5)),
             ('Yell South', (None, None, None), (471, 311, 66.0)),
             ('Thomas R. Manson', 166), ('William Leask', 145))

ZCC_BY_ELECTIONS_1920S_1930S = [
    ('Lerwick North County Council By-Election March 1920', '1920-03-01', '1920-02-19'),
    ('Sandness_County_Council_By-Election_October_1921', '1921-10-01', '1921-09-15'),
    ('Walls_County_Council_By-Election_October_1921', '1921-10-01', '1921-09-15'),
    ('Whalsay_And_Skerries_County_Council_By-Election_January_1924', '1924-01-01', '1924-01-17'),
    ('Gulberwick County Council By-Election June 1924', '1924-06-01', '1924-06-19'),
    ('Yell North County Council By-Election August 1925', '1925-08-01', '1925-08-20'),
    ('Cunningsburgh County Council By-Election February 1927', '1927-02-01', '1927-02-17'),
    ('Northmavine South County Council By-Election July 1927', '1927-07-19', '1927-07-21'),
    ('Sandsting County Council By-Election September 1930', '1930-09-01', '1930-09-16'),
    ('Dunrossness North County Council By-Election June 1931', '1931-06-01', '1931-06-16'),
    ('Yell North County Council By-Election August 1932', '1932-08-01', '1932-08-16'),
    ('Bressay County Council By-Election September 1933', '1933-09-01', '1933-09-19'),
    ('Bressay County Council By-Election December 1934', '1934-12-01', '1934-12-18'),
    ('Dunrossness North County Council By-Election December 1934', '1934-12-01', '1934-12-18'),
    ('Whalsay And Skerries County Council By-Election February 1937', '1937-02-01', '1937-02-16'),
    ('Fetlar County Council By-Election October 1937', '1937-10-01', '1937-10-26'),
    ('Unst North County Council By-Election December 1937', '1937-12-01', '1937-12-14'),
    ('Sandsting County Council By-Election February 1938', '1938-02-01', '1938-02-22'),
]
ZCC_GENERAL_1945 = ('County Council Election December 1945', '1945-12-03', '1945-12-04')
ZCC_BY_ELECTIONS_1940S_1950S = [
    ('Cunningsburgh County Council By-Election April 1940', '1940-04-01', '1940-04-16'),
    ('Yell South County Council By-Election December 1940', '1940-12-01', '1940-12-10'),
    ('Dunrossness North County Council By-Election December 1946', '1946-12-01', '1946-12-17'),
    ('Nesting County Council By-Election May 1948', '1948-05-01', '1948-05-18'),
    ('Yell North County Council By-Election May 1948', '1948-05-01', '1948-05-18'),
    ('Northmavine South County Council By-Election February 1951', '1951-02-01', '1951-02-20'),
    ('Dunrossness North County Council By-Election February 1952', '1952-02-01', '1952-02-19'),
    ('Yell South County Council By-Election July 1952', '1952-07-01', '1952-07-29'),
    ('Delting South County Council By-Election July 1955', '1955-07-01', '1955-07-12'),
    ('Sandwick County Council By-Election July 1955', '1955-07-01', '1955-07-12'),
    ('Gulberwick County Council By-Election October 1958', '1958-10-01', '1958-10-21'),
    ('Aithsting County Council By-Election February 1959', '1959-02-01', '1959-02-03'),
]
GULBERWICK_1951 = ('Gulberwick County Council By-Election May 1951', ('James J. Nicolson', 'Unanimously appointed', 98),
                   ('Prophet Smith', 49, 'prophet-smith'))
GULBERWICK_1945 = ('Gulberwick County Council By-Election March 1945', [
    ('Charles E. Mitchell', 15, '15 Council votes'),
    ('Magnus Keith', 6, '6 Council votes'),
])
UNST_SOUTH_1936 = ('Unst South County Council By-Election February 1936',
                   [('William Fordyce Clark', 'Unanimously appointed', 'Unopposed'), ('Magnus Manson', 'Rejected', 'Withdrew')])
BURRA_1964 = ('County Council Election May 1964', 'Burra', (524, 118, 22.5), (500, 118, 23.6))
AITHSTING_1973 = ('County Council Election May 1973', 'Aithsting', ('Peter F. M. Tulloch', 46), (275, 323))
NORTHMAVINE_SOUTH_1970 = ('County Council Election May 1970', 'Northmavine South', 'Hugh Sutherland', 'hugh-sutherland',
                          'Robert Balfour', 'robert-balfour')
NORTHMAVINE_SOUTH_1972 = ('Northmavine South County Council By-Election June 1972', 'Hugh Sutherland', 'Robert Balfour')
ZCC_INTROS_1970 = [
    ('robert-balfour', 'Northmavine South between 1955 and 1967', 'Northmavine South between 1955 and 1972'),
    ('hugh-sutherland', 'then for Delting South, and finally for Northmavine South.', 'then for Delting South.'),
]
# (title, winner as in the DB, winner as in the paper or None, winner votes, loser, loser votes,
#  (electorate, turnout, turnout_pct) or None)
ZCC_BY_ELECTION_POLLS_1960S_1970S = [
    ('Bressay County Council By-Election March 1960', 'John R. Smith', None, 86, 'George A. Kerr', 79, None),
    ('Delting North County Council By-Election September 1963', 'Willie Peterson', 'Andrew W. Peterson', 57,
     'William Nicolson', 11, (None, None, 34.5)),
    ('Yell South County Council By-Election March 1966', 'Robert S. Gray', None, 140, 'Alan Rhodes', 22, (589, 163, 27.7)),
    ('Bressay County Council By-Election June 1966', 'John Scott', 'John H. Scott', 46, 'William Smith', 34, (216, None, 38.6)),
    ('Gulberwick County Council By-Election October 1970', 'Arthur Irvine', None, 55, 'Mrs C. F. Johnson', 22, (131, None, 58.0)),
    ('Yell North County Council By-Election February 1974', 'David Johnson', 'David J. Johnston', 108,
     'Andrew J. Williamson', 90, (None, 198, 72.0)),
]
ZCC_BY_ELECTIONS_1960S_1970S = [
    ('Walls County Council By-Election February 1960', '1960-02-01', '1960-02-09'),
    ('Bressay County Council By-Election March 1960', '1960-03-01', '1960-03-29'),
    ('Yell North County Council By-Election December 1962', '1962-12-01', '1962-12-04'),
    ('Delting North County Council By-Election September 1963', '1963-09-01', '1963-09-24'),
    ('Dunrossness North County Council By-Election September 1963', '1963-09-01', '1963-10-22'),
    ('Dunrossness South County Council By-Election July 1964', '1964-07-01', '1964-07-07'),
    ('Whalsay And Skerries County Council By-Election July 1964', '1964-07-01', '1964-07-07'),
    ('Yell South County Council By-Election March 1966', '1966-03-01', '1966-02-22'),
    ('Bressay County Council By-Election June 1966', '1966-06-01', '1966-06-14'),
    ('Fetlar County Council By-Election September 1967', '1967-09-01', '1967-07-25'),
    ('Tingwall County Council By-Election September 1967', '1967-09-01', '1967-07-25'),
    ('Burra County Council By-Election August 1970', '1970-08-01', '1970-07-14'),
    ('Whiteness And Weisdale County Council By-Election August 1970', '1970-08-01', '1970-07-14'),
    ('Gulberwick County Council By-Election October 1970', '1970-10-01', '1970-09-29'),
    ('Dunrossness North County Council By-Election October 1971', '1971-10-01', '1971-11-09'),
    ('Northmavine South County Council By-Election June 1972', '1972-06-20', '1972-05-16'),
    ('Burra County Council By-Election September 1972', '1972-09-01', '1972-08-08'),
    ('Yell North County Council By-Election February 1974', '1974-02-01', '1974-01-08'),
]
ZCC_UNOPPOSED_1960S_1970S = [
    ('Dunrossness South County Council By-Election July 1964', 'Raymond Bentley', 'Unanimously appointed'),
    ('Tingwall County Council By-Election September 1967', 'Andrew B. Irvine', 'Unanimously elected'),
    ('Northmavine South County Council By-Election June 1972', 'John Jamieson', 'Unanimously elected'),
]
FISHER_1971 = ('Dunrossness North County Council By-Election August 1971', 'Dunrossness North', '1971-08-24',
               'Iain Campbell', 'iain-campbell', 'Mary T. Fisher',
               "Mrs Mary T. Fisher, the only nominee, was formally elected on 24 August, the day fixed for the "
               "by-election. As a council employee she could not take her seat, and resigned; a second "
               "by-election followed in November. From the Shetland Times, 13 August and 10 and 24 September 1971.")
FISHER_RERUN_1971 = ('Dunrossness North County Council By-Election October 1971', 'Iain Campbell', '[voided election re-run]')
WESTMINSTER_POLLING_DAYS = [
    ('1874 UK General Election, Orkney and Shetland Result', '1874-01-01', '1874-02-20'),
    ('1880 UK General Election, Orkney and Shetland Result', '1880-01-01', '1880-04-26'),
    ('1885 UK General Election, Orkney and Shetland Result', '1885-01-01', '1885-12-14'),
    ('1886 UK General Election, Orkney and Shetland Result', '1886-01-01', '1886-07-26'),
    ('1892 UK General Election, Orkney and Shetland Result', '1892-01-01', '1892-07-25'),
    ('1895 UK General Election, Orkney and Shetland Result', '1895-01-01', '1895-08-06'),
    ('1900 UK General Election, Orkney and Shetland Result', '1900-01-01', '1900-10-23'),
    ('1906 UK General Election, Orkney and Shetland Result', '1906-01-01', '1906-02-06'),
    ('1910 January UK General Election, Orkney and Shetland Result', '1910-01-01', '1910-02-08'),
    ('1910 December UK General Election, Orkney and Shetland Result', '1910-01-01', '1910-12-14'),
    ('1918 UK General Election, Orkney and Shetland Result', '1918-12-14', '1918-12-04'),
    ('1924 UK General Election, Orkney and Shetland Result', '1924-10-29', '1924-10-18'),
    ('1931 UK General Election, Orkney and Shetland Result', '1931-10-27', '1931-10-16'),
]
# (title, (name, votes, party) in the baseline, (name, votes, party) from the paper)
WESTMINSTER_CANDIDACIES = [
    ('1880 UK General Election, Orkney and Shetland Result', ('H. B. Riddell', 578, 'Conservative'),
     ('George Roy Badenoch', 518, 'Conservative')),
    ('1885 UK General Election, Orkney and Shetland Result', ('Hon. C T Dundass', 1940, 'Conservative'),
     ('Hon. C. T. Dundas', 1940, 'Conservative')),
    ('1886 UK General Election, Orkney and Shetland Result', ('Leonard Lyell', 3352, 'Liberal'),
     ('Leonard Lyell', 2353, 'Liberal')),
    ('1886 UK General Election, Orkney and Shetland Result', ('H. Hoare', 1940, 'Liberal Unionist'),
     ('H. Hoare', 1382, 'Liberal Unionist')),
    ('1895 UK General Election, Orkney and Shetland Result', ('Leonard Lyell', 2624, 'Liberal'),
     ('Leonard Lyell', 2361, 'Liberal')),
    ('1895 UK General Election, Orkney and Shetland Result', ('W. Younger', 1617, 'Liberal Unionist'),
     ('Ralph Wardlaw McLeod Fullarton', 1580, 'Conservative')),
    ('1950 UK General Election, Orkney and Shetland Result', ('Harald Robert Leslie', 3335, 'Labour'),
     ('Harald Robert Leslie', 4198, 'Labour')),
    ('1951 UK General Election, Orkney and Shetland Result', ('Harald Robert Leslie', 3335, 'Labour'),
     ('Magnus Fairnie', 3335, 'Labour')),
    ('1955 UK General Election, Orkney and Shetland Result', ('Harald Robert Leslie', 2914, 'Labour'),
     ('Edgar Ramsay', 2914, 'Labour')),
    ('1966 UK General Election, Orkney and Shetland Result', ('Ian MacInnes', 3021, 'Labour'),
     ('Hugh Lynch', 3021, 'Labour')),
]
# (title, column, baseline value, value from the paper)
WESTMINSTER_FIGURES = [
    ('1880 UK General Election, Orkney and Shetland Result', 'turnout', 1161, 1414),
    ('1895 UK General Election, Orkney and Shetland Result', 'turnout', 4241, 3941),
    ('1900 UK General Election, Orkney and Shetland Result', 'turnout', 4241, 4074),
    ('1935 UK General Election, Orkney and Shetland Result', 'turnout', 14226, 14586),
    ('1959 UK General Election, Orkney and Shetland Result', 'turnout', 18427, 18861),
    ('1964 UK General Election, Orkney and Shetland Result', 'turnout', 18427, 18540),
    ('Orkney and Shetland by-election, 1873', 'electorate', None, 1537),
    ('1874 UK General Election, Orkney and Shetland Result', 'electorate', None, 1618),
    ('1880 UK General Election, Orkney and Shetland Result', 'electorate', None, 1703),
    ('1885 UK General Election, Orkney and Shetland Result', 'electorate', None, 7394),
    ('1951 UK General Election, Orkney and Shetland Result', 'electorate', None, 29603),
    ('1955 UK General Election, Orkney and Shetland Result', 'electorate', None, 28298),
]
AITHSTING_1932 = ('Aithsting County Council By-Election May 1932', [
    ('Andrew D. Clark', 102, 'Petition of 102; 10 Council votes, then 10'),
    ('[https://www.bayanne.info/Shetland/getperson.php?personID=I56157&tree=ID1 Creighton G. Williamson]', 99,
     'Petition of 99; 9 Council votes, then 9'),
    ('John Leslie', 82, 'Petition of 82; 3 Council votes'),
])


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

    print("=== 35. ZCC polling day Dec 1910 ===")
    set_date_all(c, *POLLING_DAY_ZCC_1910)

    print("=== 36. Lerwick South 1919: James Laing, not Charles Stout ===")
    title, ward, (wrong_name, wrong_slug), (right_name, right_slug) = LERWICK_SOUTH_1919
    wrong_id = one(c, "SELECT id FROM people WHERE slug = ?", (wrong_slug,))['id']
    right_id = one(c, "SELECT id FROM people WHERE slug = ?", (right_slug,))['id']
    row = one(c, """SELECT c.id, c.candidate_name, c.person_id FROM candidacies c
                   JOIN elections e ON e.id = c.election_id JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    if (row['candidate_name'], row['person_id']) == (right_name, right_id):
        print("  already James Laing")
    elif (row['candidate_name'], row['person_id']) == (wrong_name, wrong_id):
        c.execute("UPDATE candidacies SET candidate_name = ?, person_id = ? WHERE id = ?", (right_name, right_id, row['id']))
        print(f"  candidacy {row['id']}: {wrong_name} -> {right_name}")
    else:
        raise SystemExit(f"candidacy {row['id']}: unexpected {row['candidate_name']!r}/{row['person_id']}")
    slug, wrong, right = LAING_INTRO
    intro = one(c, "SELECT intro FROM people WHERE slug = ?", (slug,))['intro']
    if right in intro:
        print("  Laing intro: already 1919")
    elif wrong in intro:
        c.execute("UPDATE people SET intro = ? WHERE slug = ?", (intro.replace(wrong, right), slug))
        print("  Laing intro: 1922 -> 1919")
    else:
        raise SystemExit("james-laing intro: expected text not found")

    print("=== 37. Dunrossness North 1922: a contest ===")
    title, ward, (winner, winner_votes), (loser, loser_votes) = DUNROSSNESS_NORTH_1922
    eid = one(c, """SELECT e.id FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))['id']
    w = one(c, "SELECT id, votes, votes_text FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, winner))
    if (w['votes'], w['votes_text']) == (None, 'Unopposed'):
        c.execute("UPDATE candidacies SET votes = ?, votes_text = NULL WHERE id = ?", (winner_votes, w['id']))
        print(f"  candidacy {w['id']} ({winner}): Unopposed -> {winner_votes}")
    elif (w['votes'], w['votes_text']) != (winner_votes, None):
        raise SystemExit(f"candidacy {w['id']}: unexpected votes {w['votes']!r}/{w['votes_text']!r}")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, loser))
    if c.fetchall():
        print(f"  {loser}: already added")
    else:
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, votes, elected, position)
                     VALUES (?, ?, ?, 0, 2)""", (eid, loser, loser_votes))
        print(f"  election {eid}: {loser} added, {loser_votes} votes")

    print("=== 38. ZCC polling day Dec 1925 ===")
    set_date_all(c, *POLLING_DAY_ZCC_1925)

    print("=== 39. ZCC polling day Dec 1928 ===")
    set_date_all(c, *POLLING_DAY_ZCC_1928)

    print("=== 40. Walls electorate 1928 ===")
    title, ward, wrong, right = WALLS_1928
    row = one(c, """SELECT e.id, e.electorate, e.turnout_pct FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    if (row['electorate'], row['turnout_pct']) == right:
        print(f"  election {row['id']}: already {right}")
    elif (row['electorate'], row['turnout_pct']) == wrong:
        c.execute("UPDATE elections SET electorate = ?, turnout_pct = ? WHERE id = ?", (*right, row['id']))
        print(f"  election {row['id']}: electorate {wrong} -> {right}")
    else:
        raise SystemExit(f"election {row['id']}: unexpected electorate {row['electorate']}/{row['turnout_pct']}")

    print("=== 41. ZCC by-elections February 1933 (Sandwick, Sandness) ===")
    zcc = one(c, "SELECT id FROM councils WHERE name = 'Zetland County Council'", ())['id']
    for title, ward, replaced, (slug, name), note in ZCC_BY_ELECTIONS_1933:
        ward_id = one(c, """SELECT DISTINCT k.id FROM constituencies k JOIN elections e ON e.constituency_id = k.id
                           WHERE k.name = ? AND e.council_id = ?""", (ward, zcc))['id']
        pid = one(c, "SELECT id FROM people WHERE slug = ?", (slug,))['id']
        c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (title,))
        row = c.fetchone()
        if row:
            eid = row['id']
            print(f"  {title}: exists (id {eid})")
        else:
            c.execute("""INSERT INTO elections (council_id, constituency_id, election_date, election_type,
                                               wiki_page_title, replaced_person, notes)
                         VALUES (?, ?, ?, 'by-election', ?, ?, ?)""", (zcc, ward_id, ZCC_1933_DATE, title, replaced, note))
            eid = c.lastrowid
            print(f"  {title}: created (id {eid})")
        c.execute("SELECT id FROM candidacies WHERE election_id = ?", (eid,))
        if c.fetchall():
            print("    candidacy exists")
        else:
            c.execute("""INSERT INTO candidacies (election_id, person_id, candidate_name, votes_text, elected, position)
                         VALUES (?, ?, ?, 'Unopposed', 1, 1)""", (eid, pid, name))
            print(f"    {name} unopposed")

    print("=== 42. Walls 1935: Halcrow 4 votes ===")
    title, ward, name, wrong, right, (electorate, turnout, pct) = WALLS_1935
    row = one(c, """SELECT e.id, e.electorate, e.turnout, e.turnout_pct FROM elections e
                   JOIN constituencies k ON k.id = e.constituency_id WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    cand = one(c, "SELECT id, votes FROM candidacies WHERE election_id = ? AND candidate_name = ?", (row['id'], name))
    if cand['votes'] == wrong:
        c.execute("UPDATE candidacies SET votes = ? WHERE id = ?", (right, cand['id']))
        print(f"  candidacy {cand['id']} ({name}): {wrong} -> {right}")
    elif cand['votes'] != right:
        raise SystemExit(f"candidacy {cand['id']}: unexpected votes {cand['votes']!r}")
    if (row['electorate'], row['turnout'], row['turnout_pct']) == (None, None, None):
        c.execute("UPDATE elections SET electorate = ?, turnout = ?, turnout_pct = ? WHERE id = ?",
                  (electorate, turnout, pct, row['id']))
        print(f"  election {row['id']}: electorate {electorate}, turnout {turnout}")
    elif (row['electorate'], row['turnout'], row['turnout_pct']) != (electorate, turnout, pct):
        raise SystemExit(f"election {row['id']}: unexpected electorate/turnout")

    print("=== 43. William Jamieson sat to 1935 ===")
    slug, wrong, right = JAMIESON_INTRO
    intro = one(c, "SELECT intro FROM people WHERE slug = ?", (slug,))['intro']
    if right in intro:
        print("  intro: already 1935")
    elif wrong in intro:
        c.execute("UPDATE people SET intro = ? WHERE slug = ?", (intro.replace(wrong, right), slug))
        print("  intro: 1932 -> 1935")
    else:
        raise SystemExit(f"{slug} intro: expected text not found")

    print("=== 44. Whalsay 1938: Hay 32 votes ===")
    title, ward, name, wrong, right = WHALSAY_1938
    eid = one(c, """SELECT e.id FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))['id']
    cand = one(c, "SELECT id, votes FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, name))
    if cand['votes'] == wrong:
        c.execute("UPDATE candidacies SET votes = ? WHERE id = ?", (right, cand['id']))
        print(f"  candidacy {cand['id']} ({name}): {wrong} -> {right}")
    elif cand['votes'] != right:
        raise SystemExit(f"candidacy {cand['id']}: unexpected votes {cand['votes']!r}")

    print("=== 45. Yell North and Yell South 1938 ===")
    title, *wards, (winner, winner_votes), (loser, loser_votes) = YELL_1938
    ids = {}
    for ward, wrong, right in wards:
        row = one(c, """SELECT e.id, e.electorate, e.turnout, e.turnout_pct FROM elections e
                       JOIN constituencies k ON k.id = e.constituency_id WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
        ids[ward] = row['id']
        got = (row['electorate'], row['turnout'], row['turnout_pct'])
        if got == wrong:
            c.execute("UPDATE elections SET electorate = ?, turnout = ?, turnout_pct = ? WHERE id = ?", (*right, row['id']))
            print(f"  election {row['id']} ({ward}): {wrong} -> {right}")
        elif got != right:
            raise SystemExit(f"election {row['id']}: unexpected electorate/turnout {got}")
    eid = ids['Yell South']
    w = one(c, "SELECT id, votes, votes_text FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, winner))
    if (w['votes'], w['votes_text']) == (None, 'Unopposed'):
        c.execute("UPDATE candidacies SET votes = ?, votes_text = NULL WHERE id = ?", (winner_votes, w['id']))
        print(f"  candidacy {w['id']} ({winner}): Unopposed -> {winner_votes}")
    elif (w['votes'], w['votes_text']) != (winner_votes, None):
        raise SystemExit(f"candidacy {w['id']}: unexpected votes {w['votes']!r}/{w['votes_text']!r}")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, loser))
    if c.fetchall():
        print(f"  {loser}: already added")
    else:
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, votes, elected, position)
                     VALUES (?, ?, ?, 0, 2)""", (eid, loser, loser_votes))
        print(f"  election {eid}: {loser} added, {loser_votes} votes")

    print("=== 46. ZCC by-elections 1920-1938 ===")
    for args in ZCC_BY_ELECTIONS_1920S_1930S:
        set_date(c, *args)

    print("=== 47. Unst South 1936: Clark unopposed ===")
    title, rows = UNST_SOUTH_1936
    for name, wrong, right in rows:
        cand = one(c, """SELECT ca.id, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name))
        if cand['votes_text'] == wrong:
            c.execute("UPDATE candidacies SET votes_text = ? WHERE id = ?", (right, cand['id']))
            print(f"  candidacy {cand['id']} ({name}): {wrong!r} -> {right!r}")
        elif cand['votes_text'] != right:
            raise SystemExit(f"candidacy {cand['id']}: unexpected votes_text {cand['votes_text']!r}")

    print("=== 48. Aithsting 1932: petitions and ballots ===")
    title, rows = AITHSTING_1932
    for name, wrong, text in rows:
        cand = one(c, """SELECT ca.id, ca.votes, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name))
        if (cand['votes'], cand['votes_text']) == (wrong, None):
            c.execute("UPDATE candidacies SET votes = NULL, votes_text = ? WHERE id = ?", (text, cand['id']))
            print(f"  candidacy {cand['id']}: {wrong} votes -> {text!r}")
        elif (cand['votes'], cand['votes_text']) != (None, text):
            raise SystemExit(f"candidacy {cand['id']}: unexpected votes {cand['votes']!r}/{cand['votes_text']!r}")

    print("=== 49. ZCC general December 1945: Tuesday 4 December ===")
    set_date_all(c, *ZCC_GENERAL_1945)

    print("=== 50. ZCC by-elections 1940-1959 ===")
    for args in ZCC_BY_ELECTIONS_1940S_1950S:
        set_date(c, *args)

    print("=== 51. Gulberwick 1951: a poll, Nicolson 98, Prophet Smith 49 ===")
    title, (winner, wrong_text, winner_votes), (loser, loser_votes, loser_slug) = GULBERWICK_1951
    eid = one(c, "SELECT id FROM elections WHERE wiki_page_title = ?", (title,))['id']
    w = one(c, "SELECT id, votes, votes_text FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, winner))
    if (w['votes'], w['votes_text']) == (None, wrong_text):
        c.execute("UPDATE candidacies SET votes = ?, votes_text = NULL WHERE id = ?", (winner_votes, w['id']))
        print(f"  candidacy {w['id']} ({winner}): {wrong_text!r} -> {winner_votes}")
    elif (w['votes'], w['votes_text']) != (winner_votes, None):
        raise SystemExit(f"candidacy {w['id']}: unexpected votes {w['votes']!r}/{w['votes_text']!r}")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, loser))
    if c.fetchall():
        print(f"  {loser}: already added")
    else:
        pid = one(c, "SELECT id FROM people WHERE slug = ?", (loser_slug,))['id']
        c.execute("""INSERT INTO candidacies (election_id, person_id, candidate_name, votes, elected, position)
                     VALUES (?, ?, ?, ?, 0, 2)""", (eid, pid, loser, loser_votes))
        print(f"  election {eid}: {loser} added, {loser_votes} votes")

    print("=== 52. Gulberwick 1945: Council votes ===")
    title, rows = GULBERWICK_1945
    for name, wrong, text in rows:
        cand = one(c, """SELECT ca.id, ca.votes, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name))
        if (cand['votes'], cand['votes_text']) == (wrong, None):
            c.execute("UPDATE candidacies SET votes = NULL, votes_text = ? WHERE id = ?", (text, cand['id']))
            print(f"  candidacy {cand['id']}: {wrong} votes -> {text!r}")
        elif (cand['votes'], cand['votes_text']) != (None, text):
            raise SystemExit(f"candidacy {cand['id']}: unexpected votes {cand['votes']!r}/{cand['votes_text']!r}")

    print("=== 53. Burra 1964: electorate 500 ===")
    title, ward, wrong, right = BURRA_1964
    row = one(c, """SELECT e.id, e.electorate, e.turnout, e.turnout_pct FROM elections e
                   JOIN constituencies k ON k.id = e.constituency_id WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    got = (row['electorate'], row['turnout'], row['turnout_pct'])
    if got == wrong:
        c.execute("UPDATE elections SET electorate = ?, turnout = ?, turnout_pct = ? WHERE id = ?", (*right, row['id']))
        print(f"  election {row['id']}: {wrong} -> {right}")
    elif got != right:
        raise SystemExit(f"election {row['id']}: unexpected electorate/turnout {got}")

    print("=== 54. Aithsting 1973: Tulloch 46 ===")
    title, ward, (loser, loser_votes), (wrong, right) = AITHSTING_1973
    row = one(c, """SELECT e.id, e.turnout FROM elections e
                   JOIN constituencies k ON k.id = e.constituency_id WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))
    if row['turnout'] == wrong:
        c.execute("UPDATE elections SET turnout = ? WHERE id = ?", (right, row['id']))
        print(f"  election {row['id']}: turnout {wrong} -> {right}")
    elif row['turnout'] != right:
        raise SystemExit(f"election {row['id']}: unexpected turnout {row['turnout']!r}")
    c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (row['id'], loser))
    if c.fetchall():
        print(f"  {loser}: already added")
    else:
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, votes, elected, position)
                     VALUES (?, ?, ?, 0, 3)""", (row['id'], loser, loser_votes))
        print(f"  election {row['id']}: {loser} added, {loser_votes} votes")

    print("=== 55. Northmavine South 1970-72: Balfour, not Sutherland ===")
    title, ward, wrong_name, wrong_slug, right_name, right_slug = NORTHMAVINE_SOUTH_1970
    eid = one(c, """SELECT e.id FROM elections e JOIN constituencies k ON k.id = e.constituency_id
                   WHERE e.wiki_page_title = ? AND k.name = ?""", (title, ward))['id']
    wrong_pid = one(c, "SELECT id FROM people WHERE slug = ?", (wrong_slug,))['id']
    right_pid = one(c, "SELECT id FROM people WHERE slug = ?", (right_slug,))['id']
    cand = one(c, "SELECT id, person_id, candidate_name FROM candidacies WHERE election_id = ?", (eid,))
    if (cand['person_id'], cand['candidate_name']) == (wrong_pid, wrong_name):
        c.execute("UPDATE candidacies SET person_id = ?, candidate_name = ? WHERE id = ?", (right_pid, right_name, cand['id']))
        print(f"  candidacy {cand['id']}: {wrong_name} -> {right_name}")
    elif (cand['person_id'], cand['candidate_name']) != (right_pid, right_name):
        raise SystemExit(f"candidacy {cand['id']}: unexpected {cand['candidate_name']!r}")
    title, wrong, right = NORTHMAVINE_SOUTH_1972
    row = one(c, "SELECT id, replaced_person, replaced_person_id FROM elections WHERE wiki_page_title = ?", (title,))
    if (row['replaced_person'], row['replaced_person_id']) == (wrong, wrong_pid):
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = ? WHERE id = ?", (right, right_pid, row['id']))
        print(f"  election {row['id']}: replaced {wrong} -> {right}")
    elif (row['replaced_person'], row['replaced_person_id']) != (right, right_pid):
        raise SystemExit(f"election {row['id']}: unexpected replaced_person {row['replaced_person']!r}")
    for slug, wrong, right in ZCC_INTROS_1970:
        intro = one(c, "SELECT intro FROM people WHERE slug = ?", (slug,))['intro']
        if right in intro:
            print(f"  {slug} intro: already fixed")
        elif wrong in intro:
            c.execute("UPDATE people SET intro = ? WHERE slug = ?", (intro.replace(wrong, right), slug))
            print(f"  {slug} intro: fixed")
        else:
            raise SystemExit(f"{slug} intro: expected text not found")

    print("=== 56. ZCC by-elections 1960-1974 that were polls ===")
    for title, winner, new_name, winner_votes, loser, loser_votes, figures in ZCC_BY_ELECTION_POLLS_1960S_1970S:
        row = one(c, "SELECT id, electorate, turnout, turnout_pct FROM elections WHERE wiki_page_title = ?", (title,))
        eid = row['id']
        name = new_name or winner
        c.execute("SELECT id, candidate_name, votes, votes_text FROM candidacies WHERE election_id = ? AND candidate_name IN (?, ?)",
                  (eid, winner, name))
        w = c.fetchall()
        if len(w) != 1:
            raise SystemExit(f"election {eid}: expected one winner row, got {len(w)}")
        w = w[0]
        if (w['candidate_name'], w['votes']) == (name, winner_votes):
            print(f"  election {eid}: {name} already {winner_votes}")
        elif w['votes'] is None and w['candidate_name'] == winner:
            c.execute("UPDATE candidacies SET candidate_name = ?, votes = ?, votes_text = NULL WHERE id = ?", (name, winner_votes, w['id']))
            print(f"  candidacy {w['id']} ({winner}): {w['votes_text']!r} -> {name}, {winner_votes}")
        else:
            raise SystemExit(f"candidacy {w['id']}: unexpected {w['candidate_name']!r}/{w['votes']!r}")
        c.execute("SELECT id FROM candidacies WHERE election_id = ? AND candidate_name = ?", (eid, loser))
        if c.fetchall():
            print(f"  {loser}: already added")
        else:
            c.execute("""INSERT INTO candidacies (election_id, candidate_name, votes, elected, position)
                         VALUES (?, ?, ?, 0, 2)""", (eid, loser, loser_votes))
            print(f"  election {eid}: {loser} added, {loser_votes} votes")
        if figures:
            got = (row['electorate'], row['turnout'], row['turnout_pct'])
            if got == (None, None, None):
                c.execute("UPDATE elections SET electorate = ?, turnout = ?, turnout_pct = ? WHERE id = ?", (*figures, eid))
                print(f"  election {eid}: electorate/turnout {figures}")
            elif got != figures:
                raise SystemExit(f"election {eid}: unexpected electorate/turnout {got}")

    print("=== 57. ZCC by-elections 1960-1974 ===")
    for args in ZCC_BY_ELECTIONS_1960S_1970S:
        set_date(c, *args)

    print("=== 58. ZCC unopposed returns 1964-1972 ===")
    for title, name, wrong in ZCC_UNOPPOSED_1960S_1970S:
        cand = one(c, """SELECT ca.id, ca.votes_text FROM candidacies ca JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.candidate_name = ?""", (title, name))
        if cand['votes_text'] == wrong:
            c.execute("UPDATE candidacies SET votes_text = 'Unopposed' WHERE id = ?", (cand['id'],))
            print(f"  candidacy {cand['id']} ({name}): {wrong!r} -> 'Unopposed'")
        elif cand['votes_text'] != 'Unopposed':
            raise SystemExit(f"candidacy {cand['id']}: unexpected votes_text {cand['votes_text']!r}")

    print("=== 59. Dunrossness North, August 1971: Mrs Fisher elected, not seated ===")
    title, ward, date, replaced, replaced_slug, name, note = FISHER_1971
    zcc = one(c, "SELECT id FROM councils WHERE slug = 'zetland-county-council'", ())['id']
    ward_id = one(c, """SELECT DISTINCT k.id FROM constituencies k JOIN elections e ON e.constituency_id = k.id
                       WHERE k.name = ? AND e.council_id = ?""", (ward, zcc))['id']
    replaced_id = one(c, "SELECT id FROM people WHERE slug = ?", (replaced_slug,))['id']
    c.execute("SELECT id FROM elections WHERE wiki_page_title = ?", (title,))
    row = c.fetchone()
    if row:
        eid = row['id']
        print(f"  {title}: exists (id {eid})")
    else:
        c.execute("""INSERT INTO elections (council_id, constituency_id, election_date, election_type,
                                           wiki_page_title, replaced_person, replaced_person_id, notes)
                     VALUES (?, ?, ?, 'by-election', ?, ?, ?, ?)""", (zcc, ward_id, date, title, replaced, replaced_id, note))
        eid = c.lastrowid
        print(f"  {title}: created (id {eid})")
    c.execute("SELECT id FROM candidacies WHERE election_id = ?", (eid,))
    if c.fetchall():
        print("    candidacy exists")
    else:
        c.execute("""INSERT INTO candidacies (election_id, candidate_name, votes_text, elected, position)
                     VALUES (?, ?, 'Unopposed', 1, 1)""", (eid, name))
        print(f"    {name} unopposed")
    title, wrong, right = FISHER_RERUN_1971
    row = one(c, "SELECT id, replaced_person, replaced_person_id FROM elections WHERE wiki_page_title = ?", (title,))
    if (row['replaced_person'], row['replaced_person_id']) == (wrong, replaced_id):
        c.execute("UPDATE elections SET replaced_person = ?, replaced_person_id = NULL WHERE id = ?", (right, row['id']))
        print(f"  election {row['id']}: replaced {wrong} -> {right}")
    elif (row['replaced_person'], row['replaced_person_id']) != (right, None):
        raise SystemExit(f"election {row['id']}: unexpected replaced_person {row['replaced_person']!r}")

    print("=== 60. Orkney and Shetland polling days 1874-1931 ===")
    for args in WESTMINSTER_POLLING_DAYS:
        set_date(c, *args)

    print("=== 61-64. Orkney and Shetland results ===")
    for title, wrong, right in WESTMINSTER_CANDIDACIES:
        cand = one(c, """SELECT ca.id, ca.candidate_name, ca.votes, ca.party FROM candidacies ca
                        JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.candidate_name IN (?, ?)""", (title, wrong[0], right[0]))
        got = (cand['candidate_name'], cand['votes'], cand['party'])
        if got == right:
            print(f"  candidacy {cand['id']}: already {right}")
        elif got == wrong:
            c.execute("UPDATE candidacies SET candidate_name = ?, votes = ?, party = ? WHERE id = ?", (*right, cand['id']))
            print(f"  candidacy {cand['id']}: {wrong} -> {right}")
        else:
            raise SystemExit(f"candidacy {cand['id']}: unexpected {got}")

    print("=== 65-66. Orkney and Shetland turnouts and electorates ===")
    for title, column, wrong, right in WESTMINSTER_FIGURES:
        row = one(c, f"SELECT id, {column} AS v FROM elections WHERE wiki_page_title = ?", (title,))
        if row['v'] == right:
            print(f"  election {row['id']}: {column} already {right}")
        elif row['v'] == wrong:
            c.execute(f"UPDATE elections SET {column} = ? WHERE id = ?", (right, row['id']))
            print(f"  election {row['id']}: {column} {wrong} -> {right}")
        else:
            raise SystemExit(f"election {row['id']}: unexpected {column} {row['v']!r}")

    print("=== 67. Henry Mouat's death ===")
    slug, wrong, right = MOUAT_DEATH
    row = one(c, "SELECT id, died_date FROM people WHERE slug = ?", (slug,))
    if row['died_date'] == right:
        print(f"  {slug} died_date: already {right}")
    elif row['died_date'] == wrong:
        c.execute("UPDATE people SET died_date = ? WHERE id = ?", (right, row['id']))
        print(f"  {slug} died_date: {wrong} -> {right}")
    else:
        raise SystemExit(f"people.{slug} died_date is {row['died_date']}, expected {wrong}")

    print("=== 68. James Robertson (Social-Democrat) links to Bayanne ===")
    for title, votes in ROBERTSON_CANDIDACIES:
        cand = one(c, """SELECT ca.id, ca.candidate_name, ca.person_id FROM candidacies ca
                        JOIN elections e ON e.id = ca.election_id
                        WHERE e.wiki_page_title = ? AND ca.votes = ?
                          AND ca.candidate_name IN ('James Robertson', ?)""", (title, votes, ROBERTSON_BAYANNE))
        if cand['person_id'] is not None:
            raise SystemExit(f"candidacy {cand['id']}: linked to person {cand['person_id']}")
        if cand['candidate_name'] == ROBERTSON_BAYANNE:
            print(f"  candidacy {cand['id']}: already linked")
        else:
            c.execute("UPDATE candidacies SET candidate_name = ? WHERE id = ?", (ROBERTSON_BAYANNE, cand['id']))
            print(f"  candidacy {cand['id']}: James Robertson -> Bayanne I28791")

    print("=== 69-71. Biographies for people who had none; Arthur Irvine's death ===")
    slug, wrong, right = IRVINE_DEATH
    row = one(c, "SELECT id, died_date FROM people WHERE slug = ?", (slug,))
    if row['died_date'] == right:
        print(f"  {slug} died_date: already {right}")
    elif row['died_date'] == wrong:
        c.execute("UPDATE people SET died_date = ? WHERE id = ?", (right, row['id']))
        print(f"  {slug} died_date: {wrong} -> {right}")
    else:
        raise SystemExit(f"people.{slug} died_date is {row['died_date']}, expected {wrong}")
    for slug, bio in BIOGRAPHIES.items():
        row = one(c, "SELECT id, biography FROM people WHERE slug = ?", (slug,))
        if row['biography'] == bio:
            print(f"  {slug}: biography already set")
        elif not (row['biography'] or '').strip():
            c.execute("UPDATE people SET biography = ? WHERE id = ?", (bio, row['id']))
            print(f"  {slug}: biography added")
        else:
            raise SystemExit(f"people.{slug} already has a different biography")

    print("=== 70. James Paton (i)'s intro; Harry Gray's birth place ===")
    old, new = PATON_INTRO
    row = one(c, "SELECT id, intro FROM people WHERE slug = ?", ('james-paton-i',))
    if row['intro'] == new:
        print("  james-paton-i intro: already fixed")
    elif row['intro'] == old:
        c.execute("UPDATE people SET intro = ? WHERE id = ?", (new, row['id']))
        print("  james-paton-i intro: SIC 1978-82 -> 1974-78, defeats sentence restored")
    else:
        raise SystemExit("people.james-paton-i intro is not the expected text")
    slug, place = GRAY_BIRTH_PLACE
    row = one(c, "SELECT id, birth_place FROM people WHERE slug = ?", (slug,))
    if row['birth_place'] == place:
        print(f"  {slug} birth_place: already {place}")
    elif not row['birth_place']:
        c.execute("UPDATE people SET birth_place = ? WHERE id = ?", (place, row['id']))
        print(f"  {slug} birth_place: -> {place}")
    else:
        raise SystemExit(f"people.{slug} birth_place is {row['birth_place']}")

    print("=== 71. Yorston's death place; Paterson's and Greig's death dates ===")
    for field, (slug, wrong, right) in (('death_place', YORSTON_DEATH_PLACE),
                                        ('died_date', PATERSON_DEATH), ('died_date', GREIG_DEATH)):
        row = one(c, f"SELECT id, {field} FROM people WHERE slug = ?", (slug,))
        if row[field] == right:
            print(f"  {slug} {field}: already {right}")
        elif row[field] == wrong:
            c.execute(f"UPDATE people SET {field} = ? WHERE id = ?", (right, row['id']))
            print(f"  {slug} {field}: {wrong} -> {right}")
        else:
            raise SystemExit(f"people.{slug} {field} is {row[field]}, expected {wrong}")

    print("=== 72. Greig v Edmondston; Ogilvy's mail meeting ===")
    for slug, before, addition in BIO_ADDITIONS:
        row = one(c, "SELECT id, biography FROM people WHERE slug = ?", (slug,))
        bio = row['biography'] or ''
        if before + addition in bio:
            print(f"  {slug}: addition already in")
        elif bio.count(before) == 1:
            c.execute("UPDATE people SET biography = ? WHERE id = ?",
                      (bio.replace(before, before + addition), row['id']))
            print(f"  {slug}: biography extended")
        else:
            raise SystemExit(f"people.{slug} biography doesn't contain the expected text once")

    db.commit()
    db.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
