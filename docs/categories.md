# Categories and Whisparr Mapping

nzbidx exposes the following Newznab category IDs. These IDs are used by Whisparr's indexer settings.

| ID | Name |
| --- | --- |
| 0000 | Reserved |
| 1000 | Console |
| 1010 | Console/NDS |
| 1020 | Console/PSP |
| 1030 | Console/Wii |
| 1040 | Console/XBox |
| 1050 | Console/XBox 360 |
| 1060 | Console/Wiiware |
| 1070 | Console/XBox 360 DLC |
| 2000 | Movies |
| 2010 | Movies/Foreign |
| 2020 | Movies/Other |
| 2030 | Movies/SD |
| 2040 | Movies/HD |
| 2050 | Movies/BluRay |
| 2060 | Movies/3D |
| 3000 | Audio |
| 3010 | Audio/MP3 |
| 3020 | Audio/Video |
| 3030 | Audio/Audiobook |
| 3040 | Audio/Lossless |
| 4000 | PC |
| 4010 | PC/0day |
| 4020 | PC/ISO |
| 4030 | PC/Mac |
| 4040 | PC/Mobile-Other |
| 4050 | PC/Games |
| 4060 | PC/Mobile-iOS |
| 4070 | PC/Mobile-Android |
| 5000 | TV |
| 5020 | TV/Foreign |
| 5030 | TV/SD |
| 5040 | TV/HD |
| 5050 | TV/Other |
| 5060 | TV/Sport |
| 6000 | XXX |
| 6010 | XXX/DVD |
| 6020 | XXX/WMV |
| 6030 | XXX/XviD |
| 6040 | XXX/x264 |
| 6045 | XXX/UHD |
| 6050 | XXX/Pack |
| 6060 | XXX/ImageSet |
| 6070 | XXX/Other |
| 6080 | XXX/SD |
| 6090 | XXX/WEB-DL |
| 7000 | Other |
| 7010 | Misc |
| 7020 | EBook |
| 7030 | Comics |

## Using These IDs in Whisparr

Whisparr uses the same Newznab category IDs when configuring an indexer. Add nzbidx as a Custom Newznab indexer and set the **Categories** field to the IDs you want to search. For example, to search adult movies you might use:

```
6000,6010,6020,6030,6040,6045,6050,6060,6070,6080,6090
```

You can also include non-adult categories such as `2000` (Movies), `5000` (TV), `3000` (Audio), or `7020` (EBook) if you want Whisparr to use those.

