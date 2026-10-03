# Birth and death gaps: LTC and ZCC councillors (2026-10-02)

The question: which LTC/ZCC councillors have no birth or death date, and can the papers or the web fill the gaps?
Eleven people had a date missing completely. Five of them already have the date in the wiki text (`mwfn_` pages), and the parser dropped it because it was month-only or approximate. Those are parse slips, not research gaps.

Applied so far: Garrick (`fix_parse_errors.py` #28). Nothing else has been applied.

## Parse slips (the wiki already has the date)

| Person | Wiki text | DB now |
|---|---|---|
| herbert-anderton | b. January 1862, Bradford | born NULL |
| james-williamson | d. September 1985, Tyne and Wear | died NULL |
| david-walker | d. June 2006, Lancashire | died NULL; **birth_place = "d. June 2006"** |
| robert-adair | b. September 1922, Darlington | born NULL |
| iain-caldwell | b. ~1943 | born NULL |
| frederick-dainty | b. ~1917 | born NULL |

## Herbert Anderton

- **Shetland News, Thursday 18 Nov 1937, p4 (article 066).** Death notice: "At Vaila, on 16th inst., HERBERT F. ANDERTON, of Vaila and Melby, in his 76th year."
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F19371118&page=0004&article=066
- **Shetland News, Thursday 25 Nov 1937, p4 (article 066).** Obituary. He was born "at Bolton Royd, Manningham, Bradford, on 29th January, 1862", son of Frederick William Anderton. The family firm was in worsted spinning. He first came to Shetland "a youth of 19" and stayed at Walls. He bought Vaila "about 54 years ago" (so about 1883) and Melby in 1890. He died at Vaila House on Tuesday 16 Nov.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F19371125&page=0004&article=066
- **Shetland News, 2 Dec 1937, p4 (article 062).** Correction: he bought both estates from the late R. T. C. Scott. Coutts was his factor, not the seller.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0003210%2F19371202&page=0004&article=062
- Shetland Times, 8 Jan 1938, p5 (article 094): listed among the year's deaths.

Supports: `herbert-anderton:born_date` = 1862-01-29, `birth_place` = Bolton Royd, Manningham, Bradford (read); died 1937-11-16 confirmed (read).
Contradicts the wiki biography: "came to Shetland around 1892, where he bought the estates of Melby and Vaila". The obituary says he first came at 19 (about 1881), bought Vaila about 1883 and Melby in 1890.

## Iain T. Campbell (ZCC Dunrossness North 1967–71)

- **ST 10 Jun 1966, p7 (a108).** Presbytery: a call to "Mr Ian Campbell, another probationer", to Dunrossness, signed by 82 members. He was married.
- **ST 19 Aug 1966, p1 (a008).** Notice: "Ordination and induction of the Rev. Iain T. Campbell, L.Th.", Thursday 25 Aug, Dunrossness Parish Church.
- **ST 2 Sep 1966, p4 (a042).** Ordained and inducted "last Thursday" (25 Aug).
- **ST 17 Mar 1967, p1 (a131).** Church notice: "I. T. Campbell, C.A., L.Th." (he was a chartered accountant).
- **ST 21 Apr 1967, p5 (a079).** Nominations: "Iain T. Campbell, The Manse, Skelberry, Dunrossness".
- **ST 7 Mar 1969, p5 (a047).** "Ness minister demits charge". The Presbytery let him demit to join the Shetland branch of the Highlands and Islands Development Board as an accountant, his profession before the ministry. He asked to demit "as from 31st March".
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19690307&page=0005&article=047

Birth and death not found. Contradicts the wiki's "31 April 1969" (no such date): it was **31 March 1969**.
Next: Fasti Ecclesiae Scoticanae vol. 10 (1955–75) should give his birth date and parents.

## Peter John Garriock (ii) (ZCC Sandness 1973–75)

- **ST 20 Apr 1973, p8 (a080).** Nominations: "SANDNESS: Peter John **Garrick**, crofter, Griesta Cottage", nominated by J. R. P. Jamieson, Melby, and T. W. Georgeson, Huxter.
  https://www.britishnewspaperarchive.com/image-viewer?issue=BL%2F0000666%2F19730420&page=0008&article=080
- ST 4 May 1962, p4 (a082): death of Annie, wife of Robert Garrick, Huxter, Sandness, 24 Apr 1962, with thanks from "her son and daughter-in-law at Griesta, Tingwall". ST 11 May 1962, p4 (a052): son Henry born 28 Apr 1962 to Mr and Mrs Peter Garrick, Griesta, Tingwall. ST 29 Nov 1974, p23 (a268): a Robert Garrick died at the Brevik Hospital on 21 Nov 1974, aged 90. That is not his father, who died in 1978 (see Bayanne below).
- ST 14 Jan 1977, p12 (a133): Peter Garrick appointed chairman of the local Conservatives.
- Still in the paper in 2000 (Rotary club round-up, ST 6 Oct 2000, p21), so if he has died it was after the BNA's coverage ends.

The surname is spelled **Garrick** in the 1973 and 1974 nominations and everywhere else in the paper; the wiki has "Garriock".

**Bayanne I131893** (read 2026-10-02): Peter John GARRICK, farmer, **b. 30 Dec 1926, South Shields; d. 7 Dec 2021, Griesta, Tingwall**; buried Tingwall Cemetery 20 Dec 2021. His parents were Robert Garrick (b. 1890 Rurewater, Sandness, d. 1978) and Ann Peterson (d. 24 Apr 1962, Huxter, Sandness), which matches the 1962 death notice above. He married Robertha Jessie Irvine in 1957.
https://www.bayanne.info/Shetland/getperson.php?personID=I131893&tree=ID1

## Frederick Dainty

- **ST 20 Apr 1973, p8 (a080).** "Frederick L Dainty, retired army officer, Garderhouse, Isbister, Symbister".
- ST 18 Nov 1977, p1 (a007): "Lt. Col. Fred Dainty". The wiki calls him "Colonel" and "a seaman".

Birth not found (wiki ~1917).

## Searches that found nothing

| Question | Keywords / method | Range | Result |
|---|---|---|---|
| James D. Williamson's death notice | `williamson tyne`; `williamson deaths`; OCR of every births/marriages/deaths column, 6 Sep–18 Oct 1985 | ST Sep–Nov 1985 | Nothing for him |
| Robert Adair's birth | `adair darlington` | ST 1950–2000 | Only his wife's in-memoriam (d. 9 Sep 1956, 41 Gilbertson Road) and his 1958 remarriage (ADAIR–YATES, ST 24 Oct 1958) |
| Iain Campbell's background | `campbell accountant` | ST 1966–71 | Nothing relevant |
| Bayanne for I22864, I258210, I35658 | read after James cleared the check | — | Same month-precision dates as the wiki (Williamson d. Sep 1985 "T&W"; Adair b. Sep 1922 Darlington, d. at the Gilbert Bain Hospital). Stove: no death (page modified 2 Sep 2026) |
| Bayanne: Dainty, Caldwell (Iain) | surname search | — | Not in the tree |
| Bayanne: Cluness | I38324 | — | b. 1 May 1941, no death. Bayanne names him **Alexander James**; the wiki and Companies House have Alexander Jamieson |

## Web (no BNA coverage after 2000)

- Sir John Scott (b. 30 Nov 1936: he retired as Lord-Lieutenant "on his 75th birthday, November 30th 2011", from the wiki text). He read the Bressay war dead at the memorial rededication (Shetland News, 9 Nov 2025), so he was alive then.
- Thomas W. Stove: Companies House gives b. July 1935, of Nordaal, Sandwick, with no death found. Sandy Cluness: Companies House gives b. May 1941, with no death found. Peter Garrick: no death found online.
