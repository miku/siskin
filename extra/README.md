# Extra

Misc special scripts and datasets.

## DOI and DDC

Refs: ec5a52ec-b32d-5bfe-14e4-26e3efa48c00, Wed, 24 Jun 2020 00:16:31 PDT.

There are no DDC in crossref.

Extract DOI and subjects from DOAJ dump.

```shell
$ taskcat DOAJDownloadDump | \
    jq -rc '{"doi": .bibjson.identifier[] | select(.type == "doi").id, "s": .bibjson.subject}'
```

3582574 lines with DOI and subjects.

Mostly LCC.

```shell
5220989 LCC
   3825 DOAJ
```

476 unique codes used, top:

```
 569725 R
 403368 Q
 162262 QH301-705.5
 118839 QD1-999
 113398 TA1-2040
 111887 R5-920
 106248 T
 100107 RA1-1270
  83663 QC1-999
  75176 GE1-350
  69339 QA1-939
  69254 H
  60717 S1-972
  59660 QR1-502
  57442 BF1-990
  55470 L7-991
  54602 L
  54468 RC254-282
  53446 RC321-571
  52180 H1-99
  48882 RC109-216
  48330 QH426-470
  47208 TP1-1185
  45189 S
  43101 LC8-6691
  40395 RC666-701
  39295 RD1-811
  37477 HF5001-6182
  36179 QL1-991
  35843 QD241-441
```

Sample dataset with 50000 documents:
[doi_subjects_files_50k.ndj.zst](https://github.com/ubleipzig/siskin/raw/master/extra/doaj_doi_subjects_files_50k.ndj.zst).
The linked PDF files would use at least about 90GB, if downloaded. The full
3.5M documents would occupy around 7TB.

```shell
$ zstdcat doaj_doi_subjects_files_50k.ndj.zst | jq -rc .files[].size | \
    paste -sd+ | bc -l | numfmt --to=iec-i --suffix=B
89GiB
```

### Example document

A document contains DOI, files and the original payload, containing subjects.

```json
{
  "doi": "10.1016/S1134-0096(09)70153-6",
  "files": [
    {
      "ident": "tsgdo2t4affnlhgd2upmcb7az4",
      "md5": "5f550b1d79735e8a189496e727c0c1b1",
      "mimetype": "application/pdf",
      "release_ids": [
        "mrtljhtd4jfpzjchec6b6gspb4"
      ],
      "revision": "9b248806-7c13-4073-aa1e-e7bd7eb13060",
      "sha1": "a4aacf49dad14a4c62ea1146a945c551934e05f7",
      "sha256": "bd37c3d863ce8a55883332da3874667620dd5bf25c2a80860e06e4c204c0184c",
      "size": 95959,
      "state": "active",
      "urls": [
        {
          "rel": "webarchive",
          "url": "https://web.archive.org/web/20170929205230/http://publisher-s..."
        },
        {
          "rel": "web",
          "url": "http://publisher-connector.core.ac.uk/resourcesync/data/elsev..."
        },
        {
          "rel": "webarchive",
          "url": "https://web.archive.org/web/2017/http://publisher-connector.c..."
        }
      ]
    }
  ],
  "payload": {
    "id": "10.1016/S1134-0096(09)70153-6",
    "s": [
      {
        "code": "R",
        "scheme": "LCC",
        "term": "Medicine"
      },
      {
        "code": "RD1-811",
        "scheme": "LCC",
        "term": "Surgery"
      }
    ]
  }
}
```
