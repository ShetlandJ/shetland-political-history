# Westminster, Orkney and Shetland 1873–1974: results and polling days

Researched 2026-09-29 in the British Newspaper Archive (Shetland Times; Shetland News for 1895).
Every link opens the BNA viewer on the article; click **Articles** in the toolbar for the OCR text.
Page images didn't render in this session, so everything here is from the OCR and search snippets.

Question: cite the 30 Orkney and Shetland elections of 1872–1975 (queue "Citations batch 3"),
checking each result and polling day against the DB. The wiki source (mwfn_) was read first.

## Findings

- **Wrong results in the wiki** (`fix_newspapers.py` #61–64):
  - 1880: the opponent was **George Roy Badenoch** (518), not H. B. Riddell (578). Riddell stood in
    1868; the wiki's turnout 1161 was the 1868 total. 1880 was 1414, majority 378.
  - 1886: **Lyell 2353, Hoare 1382** (majority 971). The wiki repeated 1885's 3352 and 1940.
  - 1895: **Lyell 2361, R. W. McLeod Fullarton, Q.C. (C.) 1580** (majority 781). The wiki repeated
    1892, W. Younger and all.
  - 1950: Leslie (Labour) had **4198**, not 3335.
  - 1951 Labour was **Magnus Fairnie** (3335), 1955 **Edgar Ramsay** (2914), 1966 **Hugh Lynch**
    (3021): the wiki had Harald Leslie and Ian MacInnes carried forward.
- **1902 by-election** was missing from the site: the parser read the redirect page, so the
  baseline had it hidden with no candidates. Shown now with the wiki's results
  (`fix_parse_errors.py` #17); the ST confirms the polling days and Wason's and Wood's votes.
- **Polling days** (`fix_newspapers.py` #60, `fix_parse_errors.py` #18): until 1910 the county
  polled over two days (the first is used). Unopposed returns are dated to the nomination.
- **Turnouts copied** from other elections (1895, 1900, 1935, 1959, 1964) set to the candidates'
  total (#65); electorates added where the paper gives them (#66).
- Agree with the DB: 1873, 1885, 1892 (see below), 1900, 1906, Jan 1910, 1922, 1923, 1929, 1935
  (votes), 1945, 1959, 1964, 1970, both 1974.

## Unresolved

- **1892**: the ST of 30 Jul 1892 has Younger **1616** twice (majority 1008); the ST's 1902 table
  has 1617 (1007), and the Shetland News (1895) and ST 1906 have Lyell 2623, Younger 1617 (1006).
  The DB keeps the wiki's 2624 and 1617. In open-questions.
- **1923**: Boothby 4318 (majority 811, ST 15 Dec 1923) in the DB; the ST's 1935 preview has 4,315
  and 814. Contemporary figure kept; not raised.
- **1951**: Tennant 5,354 on the night (2 Nov 1951); later tables say 5353. Kept 5,354.
- **1902**: Angier's 740 is illegible in the ST; the Shields Daily Gazette (25 Nov 1902, p3) has
  "Wason 2,412 Wood 2,001 Angier 740", which agrees with the wiki.
- **Oct 1974**: H. Firth (SNP) and R. Fraser (Con.) figures are illegible in the OCR; Grimond's
  lead "more than 2000 above all other candidates combined" fits the wiki's 3025 and 2495.
- **1939–40** (election 1221): not an election (the wartime general never happened); nothing to cite.

## Evidence

### Orkney and Shetland by-election, 1873 (election 1201)
- **Shetland Times, Mon 23 Dec 1872, p2 (art. 012)**: Tait to the electors of Zetland: nomination "takes place on Monday first" (30 Dec 1872). Supports: nomination Mon 30 Dec 1872.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18721223&page=0002&article=012
- **Shetland Times, Mon 30 Dec 1872, p3 (art. 032)**: Liberal Electors of Shetland!: the poll is "one day in the first week of January". Supports: poll in the first week of January 1873.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18721230&page=0003&article=032
- **Shetland Times, Mon 13 Jan 1873, p2 (art. 048)**: Local and District News: result declared at Kirkwall; majority for Laing over Sir Peter Tait; eleven voting papers disallowed. Figures lost in the OCR. Supports: Laing elected over Tait.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18730113&page=0002&article=048
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 061)**: Table of past contests: 1873 electorate 1537, Laing 646, Tait 621; 1874 electorate 1618, unopposed; 1880 electorate 1703, "G. R. Badenoch (C.) 518", majority 378; 1885 electorate 7394; 1886 2353, 1382. Supports: electorate 1537; 646 v 621 (OCR 040, 021), majority 25.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=061

### 1874 UK General Election, Orkney and Shetland Result (election 1202)
- **Shetland Times, Mon 9 Feb 1874, p2 (art. 038)**: Mr Laing in Shetland: Samuel Laing "the only candidate for the representation of Orkney" and Shetland. Supports: only candidate.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18740209&page=0002&article=038
- **Shetland Times, Mon 23 Feb 1874, p2 (art. 067)**: Local news, "Mr Laing's Return": nomination "on Friday last in the Sheriff Court, Kirkwall"; proposed by Mr Iverach, no other candidate. Supports: returned at the nomination, Friday last = 20 Feb 1874.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18740223&page=0002&article=067
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 061)**: Table of past contests: 1873 electorate 1537, Laing 646, Tait 621; 1874 electorate 1618, unopposed; 1880 electorate 1703, "G. R. Badenoch (C.) 518", majority 378; 1885 electorate 7394; 1886 2353, 1382. Supports: electorate 1618; unopposed.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=061

### 1880 UK General Election, Orkney and Shetland Result (election 1203)
- **Shetland Times, Sat 24 Apr 1880, p3 (art. 033)**: The Voting by Ballot: electors vote "at the election on the 26th and 27th"; specimen ballot "BADENOCH (George Roy Badenoch)". Supports: poll 26-27 Apr 1880; opponent George Roy Badenoch.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18800424&page=0003&article=033
- **Shetland Times, Sat 1 May 1880, p2 (art. 025)**: Leader: 1868 "total number polled was 1161"; "the total this week was 1414, giving Liberal majority 378". Supports: total 1414 and majority 378 give Laing 896, Badenoch 518.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18800501&page=0002&article=025
- **Shetland Times, Sat 1 May 1880, p2 (art. 027)**: The County Election, triumphant return of Mr Laing: "the substantial majority 378 for Mr Laing". Supports: majority 378.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18800501&page=0002&article=027
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 061)**: Table of past contests: 1873 electorate 1537, Laing 646, Tait 621; 1874 electorate 1618, unopposed; 1880 electorate 1703, "G. R. Badenoch (C.) 518", majority 378; 1885 electorate 7394; 1886 2353, 1382. Supports: electorate 1703; Badenoch 518, majority 378.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=061

### 1885 UK General Election, Orkney and Shetland Result (election 1204)
- **Shetland Times, Sat 19 Dec 1885, p3 (art. 087)**: Grand Liberal Majority: "The polling took place in Lerwick on Monday"; 443 votes by the close of the first day. Supports: poll Mon 14 Dec 1885, two days.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18851219&page=0003&article=087
- **Shetland Times, Sat 19 Dec 1885, p3 (art. 089)**: Declaration of the Poll: Leonard Lyell (L.), Hon. C. T. Dundas (C.) 1940, "LIBERAL MAJORITY, 1412"; declared at Kirkwall on Friday. Supports: Dundas 1940, majority 1412 (Lyell 3352).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18851219&page=0003&article=089
- **Shetland Times, Sat 30 Jul 1892, p2 (art. 053)**: Representation of County, Declaration of Poll: declared at Kirkwall on Friday, "Majority for Lyell, 1008"; "Polling commenced ... on Monday morning"; restates 1885 and 1886. Supports: 1885 Lyell 3352 Dundas 1940.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18920730&page=0002&article=053
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 061)**: Table of past contests: 1873 electorate 1537, Laing 646, Tait 621; 1874 electorate 1618, unopposed; 1880 electorate 1703, "G. R. Badenoch (C.) 518", majority 378; 1885 electorate 7394; 1886 2353, 1382. Supports: electorate 7394.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=061

### 1886 UK General Election, Orkney and Shetland Result (election 1205)
- **Shetland Times, Sat 31 Jul 1886, p2 (art. 053)**: Leader on the Unionist hoax: the authentic telegram "Lyell 2353, Hoare 1382, majority for Lyell 971". Supports: Lyell 2353, Hoare 1382.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18860731&page=0002&article=053
- **Shetland Times, Sat 31 Jul 1886, p3 (art. 060)**: Representation of the County, return of Lyell, majority of 971: "The polling ... took place on Monday and Tuesday last". Supports: poll Mon 26 and Tue 27 Jul 1886.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18860731&page=0003&article=060
- **Shetland Times, Sat 30 Jul 1892, p2 (art. 053)**: Representation of County, Declaration of Poll: declared at Kirkwall on Friday, "Majority for Lyell, 1008"; "Polling commenced ... on Monday morning"; restates 1885 and 1886. Supports: 1886 Lyell 2353 Hoare 1382.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18920730&page=0002&article=053
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 061)**: Table of past contests: 1873 electorate 1537, Laing 646, Tait 621; 1874 electorate 1618, unopposed; 1880 electorate 1703, "G. R. Badenoch (C.) 518", majority 378; 1885 electorate 7394; 1886 2353, 1382. Supports: 2353 v 1382, majority 971.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=061

### 1892 UK General Election, Orkney and Shetland Result (election 1206)
- **Shetland Times, Sat 30 Jul 1892, p2 (art. 053)**: Representation of County, Declaration of Poll: declared at Kirkwall on Friday, "Majority for Lyell, 1008"; "Polling commenced ... on Monday morning"; restates 1885 and 1886. Supports: poll Mon 25 and Tue 26 Jul 1892.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18920730&page=0002&article=053
- **Shetland Times, Sat 30 Jul 1892, p2 (art. 054)**: Polling in the Country: telegram "Lyell, 2624; Younger, 1616". Supports: Lyell 2624; Younger 1616 (the DB has 1617).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18920730&page=0002&article=054
- **Shetland News, Sat 10 Aug 1895, p5 (art. 027)**: The County Election. The Polling (Shetland News): "took place on Tuesday and Wednesday"; 1892 given as Lyell 2623, Younger 1617, majority 1006. Supports: 1892 as 2623 v 1617.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F18950810&page=0005&article=027
- **Shetland Times, Sat 29 Nov 1902, p4 (art. 078)**: Declaration of the Poll, with past contests: 1892 2624 v 1617, majority 1007; 1895 2361 v 1580; 1900 2057 v 2017; 1902 Wason (L.) 2412, T. McKinnon Wood (L.) 2001, Angier (U.) illegible. Supports: 2624 v 1617, majority 1007.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19021129&page=0004&article=078

### 1895 UK General Election, Orkney and Shetland Result (election 1207)
- **Shetland News, Sat 10 Aug 1895, p5 (art. 027)**: The County Election. The Polling (Shetland News): "took place on Tuesday and Wednesday"; 1892 given as Lyell 2623, Younger 1617, majority 1006. Supports: poll Tue 6 and Wed 7 Aug 1895.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F18950810&page=0005&article=027
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 045)**: Claims notices for the 1895 election: Sir Leonard Lyell, Baronet of Kinnordy; Ralph Wardlaw McLeod Fullarton, Q.C. Supports: candidates' full names.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=045
- **Shetland Times, Sat 17 Aug 1895, p2 (art. 046)**: Leader: figures wired on Saturday, "the Liberal majority to be 781"; Fullarton polled fewer than Younger in 1892. Supports: majority 781.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18950817&page=0002&article=046
- **Shetland News, Sat 17 Aug 1895, p5 (art. 024)**: The County Election (Shetland News): declared at Kirkwall 1.45 on Saturday; OCR "Lyell $361, Fullarton 1680, majority 781"; 21 spoilt papers. Supports: majority 781; the vote figures are garbled in the OCR.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F18950817&page=0005&article=024
- **Shetland Times, Sat 10 Apr 1897, p3 (art. 032)**: Letter: "The polling in 1895 showed thus—Sir Leonard Lyell (L) 2361 R. W. M. Fullarton, Q.C. ..." Supports: Lyell 2361.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F18970410&page=0003&article=032
- **Shetland Times, Sat 3 Nov 1900, p4 (art. 087)**: 1895: Lyell "secured 2361 votes, while his opponent, Mr Fullarton, only obtained 1580"; 1900 "majority of 40 votes". Supports: Lyell 2361, Fullarton 1580, majority 781.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19001103&page=0004&article=087
- **Shetland Times, Sat 29 Nov 1902, p4 (art. 078)**: Declaration of the Poll, with past contests: 1892 2624 v 1617, majority 1007; 1895 2361 v 1580; 1900 2057 v 2017; 1902 Wason (L.) 2412, T. McKinnon Wood (L.) 2001, Angier (U.) illegible. Supports: 2361 v 1580.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19021129&page=0004&article=078

### 1900 UK General Election, Orkney and Shetland Result (election 1208)
- **Shetland Times, Sat 20 Oct 1900, p5 (art. 095)**: Vote for Lyell!: "Orkney and Shetland polls on Tuesday and Wednesday next". Supports: poll Tue 23 and Wed 24 Oct 1900.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19001020&page=0005&article=095
- **Shetland Times, Sat 27 Oct 1900, p4 (art. 094)**: The Parliamentary Election: the polling in Lerwick. Supports: polling report.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19001027&page=0004&article=094
- **Shetland Times, Sat 3 Nov 1900, p4 (art. 087)**: 1895: Lyell "secured 2361 votes, while his opponent, Mr Fullarton, only obtained 1580"; 1900 "majority of 40 votes". Supports: majority 40.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19001103&page=0004&article=087
- **Shetland Times, Sat 29 Nov 1902, p4 (art. 078)**: Declaration of the Poll, with past contests: 1892 2624 v 1617, majority 1007; 1895 2361 v 1580; 1900 2057 v 2017; 1902 Wason (L.) 2412, T. McKinnon Wood (L.) 2001, Angier (U.) illegible. Supports: 2057 v 2017, majority 40.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19021129&page=0004&article=078
- **Shetland Times, Sat 3 Nov 1900, p4 (art. 088)**: Wason 2057, Lyell 2017 Supports: Wason 2057, Lyell 2017.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19001103&page=0004&article=088

### 1902 Orkney and Shetland by-election (election 1209)
- **Shetland Times, Sat 22 Nov 1902, p4 (art. 072)**: The Parliamentary Contest: "The polling ... took place on Tuesday and Wednesday"; three candidates. Supports: poll Tue 18 and Wed 19 Nov 1902.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19021122&page=0004&article=072
- **Shetland Times, Sat 29 Nov 1902, p4 (art. 078)**: Declaration of the Poll, with past contests: 1892 2624 v 1617, majority 1007; 1895 2361 v 1580; 1900 2057 v 2017; 1902 Wason (L.) 2412, T. McKinnon Wood (L.) 2001, Angier (U.) illegible. Supports: Wason 2412, McKinnon Wood 2001.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19021129&page=0004&article=078
- **Shetland Times, Sat 17 Feb 1906, p4 (art. 065)**: McKinnon Wood 2001; T. V. S. Angier Supports: McKinnon Wood 2001; T. V. S. Angier.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19060217&page=0004&article=065

### 1906 UK General Election, Orkney and Shetland Result (election 1210)
- **Shetland Times, Sat 27 Jan 1906, p1 (art. 004)**: Returning Officer's notice: "The Poll will take place on TUESDAY and WEDNESDAY, the 6th and 7th days of February, 1906". Supports: poll 6-7 Feb 1906.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19060127&page=0001&article=004
- **Shetland Times, Sat 17 Feb 1906, p4 (art. 065)**: majority 2816 (3837 v 1021) Supports: majority 2816 (3837 v 1021).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19060217&page=0004&article=065

### 1910 January UK General Election, Orkney and Shetland Result (election 1211)
- **Shetland Times, Sat 5 Feb 1910, p3 (art. 047)**: Leader: the end of the contest; the constituency polls "on Tuesday and Wednesday". Supports: poll Tue 8 and Wed 9 Feb 1910.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19100205&page=0003&article=047
- **Shetland Times, Sat 12 Feb 1910, p4 (art. 076)**: The Orkney and Shetland Election: "[Tuesday] and Wednesday were the polling days". Supports: polling report.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19100212&page=0004&article=076
- **Shetland Times, Sat 19 Feb 1910, p4 (art. 099)**: The Parliamentary Contest: triumphant victory, "Liberal Majority, 3123". Supports: majority 3123 (4117 v 994).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19100219&page=0004&article=099

### 1910 December UK General Election, Orkney and Shetland Result (election 1212)
- **Shetland Times, Sat 17 Dec 1910, p4 (art. 066)**: An Unopposed Return: nominations handed in at Kirkwall on Wednesday; no other candidate, so Wason "is therefore elected". Supports: returned at the nomination, Wednesday = 14 Dec 1910.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19101217&page=0004&article=066

### 1918 UK General Election, Orkney and Shetland Result (election 1213)
- **Shetland Times, Sat 7 Dec 1918, p4 (art. 093)**: Return of Mr Cathcart Wason: nominated at Kirkwall on Wednesday; "There was no other nomination". Supports: returned at the nomination, Wednesday = 4 Dec 1918.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19181207&page=0004&article=093

### Orkney and Shetland by-election, 1921 (election 1214)
- **Shetland Times, Sat 21 May 1921, p4 (art. 082)**: The Nominations: on Tuesday at Kirkwall, twelve papers for Sir Malcolm Smith signed by Liberals and Unionists; no other nomination. Supports: returned unopposed at the nomination, Tuesday = 17 May 1921.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19210521&page=0004&article=082

### 1922 UK General Election, Orkney and Shetland Result (election 1215)
- **Shetland Times, Sat 25 Nov 1922, p4 (art. 064)**: Enthusiasm in the County: Sir Robert Hamilton "returned ... by a majority of 625 votes". Supports: majority 625.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19221125&page=0004&article=064
- **Shetland Times, Sat 25 Nov 1922, p4 (art. 065)**: Result: "SIR ROBERT HAMILTON 4814 SIR MALCOLM SMITH [4]189 LIBERAL MAJORITY". Supports: Hamilton 4814, Smith 4189 (OCR 1189).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19221125&page=0004&article=065
- **Shetland Times, Sat 15 Dec 1923, p4 (art. 078)**: Local and District News: Hamilton "increased his majority from 625 to 811". Supports: majority 625.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19231215&page=0004&article=078

### 1923 UK General Election, Orkney and Shetland Result (election 1216)
- **Shetland Times, Sat 15 Dec 1923, p4 (art. 078)**: Local and District News: Hamilton "increased his majority from 625 to 811". Supports: majority 811.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19231215&page=0004&article=078
- **Shetland Times, Sat 15 Dec 1923, p4 (art. 082)**: Result: "Sir ROBERT HAMILTON 5129 Mr Robert Boothby [illegible] LIBERAL MAJORITY". Supports: Hamilton 5129.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19231215&page=0004&article=082
- **Shetland Times, Sat 2 Nov 1935, p4 (art. 077)**: Election preview with past results: 1923 Boothby 4,315, majority 814; 1924 unopposed; 1929 8,256 v 5404. Supports: 1923 given as 4,315 and 814 (Dec 1923 has majority 811).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19351102&page=0004&article=077

### 1924 UK General Election, Orkney and Shetland Result (election 1217)
- **Shetland Times, Sat 25 Oct 1924, p4 (art. 061)**: Sir R. W. Hamilton Returned Unopposed: returned "on Saturday of last week"; nominations by noon on Saturday. Supports: returned at the nomination, Saturday of last week = 18 Oct 1924.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19241025&page=0004&article=061
- **Shetland Times, Sat 2 Nov 1935, p4 (art. 077)**: Election preview with past results: 1923 Boothby 4,315, majority 814; 1924 unopposed; 1929 8,256 v 5404. Supports: unopposed.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19351102&page=0004&article=077

### 1929 UK General Election, Orkney and Shetland Result (election 1218)
- **Shetland Times, Sat 8 Jun 1929, p4 (art. 073)**: Result: "SIR ROBERT W. HAMILTON 8256 Major Basil H. H. Neven-Spence 5404 Majority 2852". Supports: 8256 v 5404.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19290608&page=0004&article=073
- **Shetland Times, Sat 2 Nov 1935, p4 (art. 077)**: Election preview with past results: 1923 Boothby 4,315, majority 814; 1924 unopposed; 1929 8,256 v 5404. Supports: 8,256 v 5404, majority 2852.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19351102&page=0004&article=077

### 1931 UK General Election, Orkney and Shetland Result (election 1219)
- **Shetland Times, Sat 24 Oct 1931, p5 (art. 082)**: Unopposed Return of Sir Robert Hamilton: "On Friday of last week candidates were nominated". Supports: returned at the nomination, Friday of last week = 16 Oct 1931.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19311024&page=0005&article=082

### 1935 UK General Election, Orkney and Shetland Result (election 1220)
- **Shetland Times, Sat 23 Nov 1935, p4 (art. 075)**: Sir Robert Hamilton Defeated: "NEVEN-SPENCE ... 8406 Sir R. W. Hamilton ... 6180 Majority ... 2226". Supports: 8406 v 6180.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19351123&page=0004&article=075
- **Shetland Times, Fri 27 Jul 1945, p2 (art. 037)**: Former Elections: 1935 "Neven Spence 8406 Sir R. W. Hamilton 6190" (OCR); some 40 per cent polled. Supports: 8406 (Hamilton 6180 misread).
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19450727&page=0002&article=037

### 1945 UK General Election, Orkney and Shetland Result (election 1222)
- **Shetland Times, Fri 27 Jul 1945, p2 (art. 036)**: Election Sequel: "Neven-Spence (Con.) 6304 Major J. Grimond (Lib.) 5975 Mr Prophet Smith (Lab.) 5208". Supports: 6304, 5975, 5208.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19450727&page=0002&article=036
- **Shetland Times, Fri 3 Mar 1950, p5 (art. 064)**: Record Poll, with past results: 1935 8406 v 6108 (OCR), majority 2226; 1945 6304, 5975, 5208, majority 329. Supports: majority 329.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500303&page=0005&article=064
- **Shetland Times, Fri 3 Jun 1955, p5 (art. 083)**: Previous Results: 1945 6304, 5975, 5208; 1950 9237, 6281, "H. R. Leslie (Lab.) 4198", majority 2956. Supports: 6304, 5975, 5208.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550603&page=0005&article=083

### 1950 UK General Election, Orkney and Shetland Result (election 1223)
- **Shetland Times, Fri 3 Mar 1950, p5 (art. 067)**: The Result: J. Grimond, Liberal; Sir B. Neven-Spence; H. R. Leslie, Labour (figures in a068). Supports: candidates.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500303&page=0005&article=067
- **Shetland Times, Fri 3 Mar 1950, p5 (art. 068)**: The figures: "9237 6281 4198 MAJORITY, 2956". Supports: Grimond 9237, Neven-Spence 6281, Leslie 4198.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19500303&page=0005&article=068
- **Shetland Times, Fri 2 Nov 1951, p5 (art. 065)**: Figure Facts: 27 spoiled papers, "20,434 voted out of a total electorate of 29,603"; 1950 Leslie 4198, 19,716 voted. Supports: Leslie 4198; 19,716 voted.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19511102&page=0005&article=065
- **Shetland Times, Fri 3 Jun 1955, p5 (art. 083)**: Previous Results: 1945 6304, 5975, 5208; 1950 9237, 6281, "H. R. Leslie (Lab.) 4198", majority 2956. Supports: 9237, 6281, 4198.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550603&page=0005&article=083

### 1951 UK General Election, Orkney and Shetland Result (election 1224)
- **Shetland Times, Fri 2 Nov 1951, p5 (art. 063)**: Grimond Back, Doubled Majority: "Grimond (Liberal) 11,745 Mr Archibald Tennant (Con.) 5,354 Mr Magnus Fairnie (Labour) 3,335". Supports: 11,745, 5,354; Labour candidate Magnus Fairnie 3,335.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19511102&page=0005&article=063
- **Shetland Times, Fri 2 Nov 1951, p5 (art. 065)**: Figure Facts: 27 spoiled papers, "20,434 voted out of a total electorate of 29,603"; 1950 Leslie 4198, 19,716 voted. Supports: 20,434 voted of 29,603.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19511102&page=0005&article=065
- **Shetland Times, Fri 20 May 1955, p2 (art. 023)**: General Election 1951: Grimond 11745, Tennant 5354, Fairnie 3335, Liberal majority 6391. Supports: 11745, 5354, 3335.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550520&page=0002&article=023
- **Shetland Times, Fri 23 Oct 1959, p6 (art. 067)**: Previous Results: 1950 Leslie 4198; 1951 Fairnie (Lab.) 3335, majority 6391; 1955 E. Ramsay (Lab.) 2914, majority 7993. Supports: Fairnie 3335.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19591023&page=0006&article=067

### 1955 UK General Election, Orkney and Shetland Result (election 1225)
- **Shetland Times, Fri 13 May 1955, p1 (art. 147)**: Advertisement: "EDGAR RAMSAY The Labour Candidate". Supports: Labour candidate Edgar Ramsay.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550513&page=0001&article=147
- **Shetland Times, Fri 3 Jun 1955, p5 (art. 078)**: Grimond again, with record majority: "J. Grimond (Lib.) J. W. Eunson (Con.) E. Ramsay (Lab.) 11,753 3760 2914". Supports: 11,753, 3760, 2914.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550603&page=0005&article=078
- **Shetland Times, Fri 3 Jun 1955, p5 (art. 090)**: Figure Facts: "18,479 voted out of a total electorate of 28,298". Supports: 18,479 voted of 28,298.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19550603&page=0005&article=090
- **Shetland Times, Fri 23 Oct 1959, p6 (art. 067)**: Previous Results: 1950 Leslie 4198; 1951 Fairnie (Lab.) 3335, majority 6391; 1955 E. Ramsay (Lab.) 2914, majority 7993. Supports: E. Ramsay 2914.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19591023&page=0006&article=067

### 1959 UK General Election, Orkney and Shetland Result (election 1226)
- **Shetland Times, Fri 16 Oct 1959, p5 (art. 061)**: Grimond Again with Increased Majority: "J. Grimond (Lib.) R. H. W. Bruce (Con.) R. S. McGowan (Lab.) 12,099 3,487 3,275 Maj. 8,612". Supports: 12,099, 3,487, 3,275.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19591016&page=0005&article=061

### 1964 UK General Election, Orkney and Shetland Result (election 1227)
- **Shetland Times, Fri 23 Oct 1964, p5 (art. 065)**: The North Goes All Liberal: "GRIMOND (Liberal) 11,604 FIRTH (Unionist) 3,704 McINNES (Labour) 3,232". Supports: 11,604, 3,704, 3,232.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19641023&page=0005&article=065

### 1966 UK General Election, Orkney and Shetland Result (election 1228)
- **Shetland Times, Fri 11 Mar 1966, p4 (art. 096)**: Labour Candidate: "Mr Hugh Lynch, a young Coatbridge schoolteacher, was formally adopted". Supports: Labour candidate Hugh Lynch.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19660311&page=0004&article=096
- **Shetland Times, Fri 8 Apr 1966, p4 (art. 082)**: Grimond Again After Very Poor Poll: "Grimond (Liberal) Firth (Tory) Lynch (Labour) 9605 3630 3021". Supports: 9605, 3630, 3021.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19660408&page=0004&article=082

### 1970 UK General Election, Orkney and Shetland Result (election 1229)
- **Shetland Times, Fri 26 Jun 1970, p1 (art. 006)**: Grimond's majority slashed: "Grimond (Liberal) 7896, Firth (Tory) 5364; Reid (Labour) 3552". Supports: 7896, 5364, 3552.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19700626&page=0001&article=006
- **Shetland Times, Fri 8 Mar 1974, p1 (art. 006)**: Nearly Trebled: announced at Kirkwall last Friday, "Liberal 11491; Tory 4186; Labour 2865". Supports: 7896, 5364, 3552.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19740308&page=0001&article=006

### 1974 February UK General Election, Orkney and Shetland Result (election 1230)
- **Shetland Times, Fri 8 Mar 1974, p1 (art. 006)**: Nearly Trebled: announced at Kirkwall last Friday, "Liberal 11491; Tory 4186; Labour 2865". Supports: 11491, 4186, 2865.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19740308&page=0001&article=006

### 1974 October UK General Election, Orkney and Shetland Result (election 1231)
- **Shetland Times, Fri 18 Oct 1974, p1 (art. 002)**: Grimond Back Again: "J. Grimond (Lib.) ... 9877 H. Firth (SNP) R. Fraser (Con.) J. W. G. Wills (Lab.) 2175"; SNP and Conservative figures illegible. Supports: Grimond 9877, Wills 2175.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19741018&page=0001&article=002