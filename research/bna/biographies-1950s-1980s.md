# Biographies: ten councillors of the 1950s to 1980s

Researched 2026-10-01 in the British Newspaper Archive (Shetland Times). Every link opens the BNA
viewer on the article; click **Articles** for the OCR text.

Question: biographies for the next ten LTC/ZCC members with none, most recent first, skipping the
living and those who died after 2000 (no Shetland Times coverage in the BNA): John Butler, James
Paton (i), Harry Gray, Hugh Williamson, Robert Garrick, James Pottinger (ii), Charles Brown,
William Marshall, Peter Henry and Robert Ollason. Williamina Tait (d. 2014) was skipped. The
biographies are in `fix_newspapers.py` #70.

Findings:
- **James Paton (i) sat on the SIC 1974–78, not 1978–82.** The wiki intro has 1978–82, but its own
  succession box says 1974–78, he lost Lerwick Twageos in 1978, and the Convener's tribute says
  "a member of the Shetland Islands Council for four years" from 1974. The parser had also cut the
  start of the intro's sentence listing his defeats. Intro fixed (#70).
- **Harry Gray was born in Orkney** ("born in Orkney, nearly 71 years ago", ST 11 Apr 1980). The DB
  had no birth place; set (#70).
- **John Butler's death day.** The obituary says he "died on Sunday" (13 May 1984); the family's
  notice in the same issue says 12 May, as the DB has. Kept 12 May.
- **Charles Brown's age.** The 1967 nominations call him "now over eighty years of age"; the DB has
  him born 19 Dec 1888, which makes 78. A reporter's estimate, so not changed; open question.
- **Peter Henry resigned in October 1959**, accepted at the County Council on Tue 20 Oct 1959. His
  ZCC term ends at the Feb 1960 by-election (derived), as with other ZCC resignations.
- **Paton and the County Council.** The Convener says he served "on both Lerwick Town Council and
  the Zetland County Council" from 1960, i.e. as one of the burgh's members on the County Council.
  We don't model those seats; the biography quotes it.
- 1961 Aithsting: the paper's "Mrs Janet C. Anderson" is our Catherine Janet Anderson.

### John Butler
- **ST Fri 18 May 1984, p2 (art. 019)** and **p3 (art. 027)**: "Former vice-convener dies after
  battling with illness": died in the Gilbert Bain aged 58; multiple sclerosis; customs officer,
  came 1948, married Christina Smith 1949, away (London, south of Scotland, Barnsley) until 1959;
  LTC from May 1971, SIC Breiwick to May 1982; Labour; chaired resources, joint consultative and
  investment committees; vice-convener May 1978–82; A. I. Tulloch's tribute; SCSS, New Shetlander,
  Althing.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19840518&page=0002&article=019
- **Same issue, p4 (art. 037)**: death notice, Gilbert Bain Hospital, 12 May 1984.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19840518&page=0004&article=037

### James Paton (i)
- **ST Fri 28 Aug 1981, p1 (art. 004)**: "Pensioners' champion dies at 81"; founder and chairman of
  the Shetland branch of the Scottish OAP Association; "lifelong trade unionist"; Labour.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19810828&page=0001&article=004
- **ST Fri 4 Sep 1981, p4 (art. 039)**: death notice, New Gilbert Bain, 26 Aug 1981.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19810904&page=0004&article=039
- **ST Fri 11 Sep 1981, p2 (art. 014)**: Convener A. I. Tulloch's tribute at the planning meeting:
  councillor from 1960 on Town and County Councils, SIC four years from 1974, eight County
  committees, "a formidable opponent". → intro fixed (#70).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19810911&page=0002&article=014
- **ST Fri 18 Sep 1981, p15 (arts. 131, 132)**: SCSS executive from 1978; his heckling; Labour
  Party tribute.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19810918&page=0015&article=131

### Harry Gray
- **ST Fri 4 Apr 1980, p12 (art. 093)**: still sitting as "Hon. Sheriff Harry Gray" that Monday.
- **ST Fri 11 Apr 1980, p10 (art. 089)**: "Town pays respect to Harry Gray": collapsed and died on
  Friday night at the Methodist Schoolroom before an SWRI "Any Questions"; born in Orkney; Edinburgh
  and the fish trade until after the war; Shetland Fish, Hay & Co., Richard Irvin's; Fish
  Merchants' Association secretary; Treasurer, Bailie, Provost from 1962 (three years); Northern
  Burghs Association; Army Cadet Force commandant to 1974 (Lt-Col); RSPCA; lost the 1974
  three-cornered Lerwick contest.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19800411&page=0010&article=089
- **Same issue, p16 (art. 129)**: death notice, 4 Apr 1980, aged 70, husband of Ruth Bethune Scott.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19800411&page=0016&article=129

### Hugh Williamson
- **ST Fri 13 Sep 1963, p8 (art. 131)**: piano tuning by "Mr Hugh R. Williamson of Ballogie".
- **ST Fri 21 Apr 1967, p5 (arts. 076, 079)**: nominated for Yell North from Bayanne House,
  Sellafirth; replaces Hugh T. Sutherland; one of two newcomers.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19670421&page=0005&article=079
- **ST Fri 25 Feb 1972, p11 (art. 124)**: his letter "PIANO TUNER": trained in Aberdeen, tuning in
  Shetland from 1957, resident from 1965, school pianos 1964–69 by motor cycle.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19720225&page=0011&article=124
- **ST Fri 20 Apr 1973, p1 (art. 005)**: not standing in 1973.
- **ST Fri 31 May 1991, p4 (art. 034)**: death notice, Montfield Hospital, 24 May 1991, aged 81;
  husband of the late Ann Jane Jamieson; son of Clara and William Williamson, West Sandwick.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19910531&page=0004&article=034

### Robert Garrick
- **ST Fri 12 May 1961, p5 (art. 065)** (on file): one of four who defeated sitting members;
  Aithsting the highest turnout. **19 May 1961, p1 (art. 153)**: his thanks, "ROBERT GARRICK, Bixter".
- 1973 statement (ST 4 May 1973 p2 art. 028) and defeat (11 May 1973 p1 art. 011), both on file.
- **ST Fri 22 Oct 1993, p4 (art. 040)**: death notice, Gletness Ward, Montfield, 15 Oct 1993,
  aged 76; husband of Josephine; 11 King Erik House.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19931022&page=0004&article=040

### James Pottinger (ii)
- **ST Fri 29 Feb 1980, p14 (art. 131)**: funeral notice, St Catherine's, Hamnavoe, to Papil.
- **ST Fri 7 Mar 1980, p18 (art. 132)**: obituary: died "last Wednesday" (27 Feb) aged 82; Burra and
  Quarff 1938–55, Sandwick 1955–58, Whalsay and Skerries 1964–73; vice-convener 1947–55; JP 1956;
  Herring Producers' Association branch secretary 1939 (formed 1932 with Robert Ollason as
  secretary); Fishermen's Association secretary 1943–75; 2nd Gordon Highlanders in WWI; Royal
  Humane Society. The right-hand column's OCR is cut off. No death notice found.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19800307&page=0018&article=132

### Charles Brown
- **ST Fri 21 Apr 1967, p5 (art. 076)**: no nomination from Fetlar; "now over eighty"; oldest
  member. **p4 (art. 057)**, leader: "that seasoned traveller"; good wishes on his retiral.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19670421&page=0004&article=057
- **ST Fri 12 Aug 1977, p14 (art. 156)**: death notice, Leith Hospital, 28 Jul 1977; West Pilton
  Gardens, Edinburgh; formerly of North Dale, Fetlar.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19770812&page=0014&article=156

### William Marshall
- **ST Fri 16 Sep 1960, p4 (art. 051)**: Presbytery sustains the call from Inch (Stranraer
  Presbytery); elected after preaching at Voe, Mossbank and Brae in August; two years in his first
  charge.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19600916&page=0004&article=051
- **ST Fri 21 Apr 1961, p5 (art. 062)** (on file): returned unopposed, minister at Brae "for only a
  few" months.
- **ST Fri 7 Jun 1963, p3 (art. 031)**: leave to demit from end of July; secretary-treasurer of a
  600-bed teaching hospital at a mission station in the Northern Transvaal; sailing 4 Jul on the
  Edinburgh Castle; monthly tapes to the Delting elders. No Shetland Times notice of his 1995 death
  was searched for.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19630607&page=0003&article=031

### Peter Henry
- **ST 16 May 1952, p5 (art. 080)** (on file): Walls, Henry 93, Anderson 45.
- **ST Fri 23 Oct 1959, p4 (art. 039)**: "Mr P. J. Henry has resigned as county councillor for the
  Walls district", accepted "on Tuesday" (20 Oct).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19591023&page=0004&article=039
- **ST Fri 16 Feb 1979, p20 (art. 179)**: death notice, New Gilbert Bain, 5 Feb 1979, aged 78;
  husband of the late Margaret Ann Reid; 1 Burgh Road.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19790216&page=0020&article=179

### Robert Ollason
- **ST Fri 27 Jan 1961, p2 (art. 018)**: obituary (OCR interleaved with a Crofters Bill article):
  born Hay's Dock, father a sailmaker; law office, Goodlad & Goodlad, partner in J. & R. Groat
  1907, own shop 1915; fishing interests to the early 1920s; Town Council from 1919, housing
  committee chairman, Provost 1933–36; County Council for Whalsay between; Health Committee
  chairman 20 years, Education 7; Shetland hospitals board from 1948; OBE 1957; honorary
  sheriff-substitute; Feuars and Heritors; Rent Tribunal; Chamber of Commerce founder chairman;
  St Columba's elder; taken ill in his shop on Saturday; aged 73; widow, three daughters, two sons.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19610127&page=0002&article=018
- **Same issue, p4 (art. 040)**: death notice, Gilbert Bain, 22 Jan 1961, husband of Joan White.
