# Towards name consolidation

Problem: We do use over 15000 different collection names and they may get out
of sync (between source and config).

## Collections in AIIntermediateSchema

```shell
$ taskcat AIIntermediateSchema | \
    jq -rc '[.["finc.id"], .["finc.source_id"], .["finc.mega_collection"][]] | @tsv' > ai.names
$ cut -f 3- ai.names | tr '\t' '\n' | sort -u > ai.collections
$ wc -l ai.collections
17030 ai.collections
```

A total of 17030 collection names in AI across 16 data sources.

```shell
$ cut -f2 ai.names | sort | uniq -c | sort -nr | column -t
103734768  49                                                                                                                                                                                                      │···························
35141676   48                                                                                                                                                                                                      │···························
10854508   55                                                                                                                                                                                                      │···························
9988570    28                                                                                                                                                                                                      │···························
6425466    89                                                                                                                                                                                                      │···························
3448614    105                                                                                                                                                                                                     │···························
3185753    85                                                                                                                                                                                                      │···························
449764     53                                                                                                                                                                                                      │···························
285766     60                                                                                                                                                                                                      │···························
167939     50                                                                                                                                                                                                      │···························
35580      34                                                                                                                                                                                                      │···························
6648       80                                                                                                                                                                                                      │···························
4392       87                                                                                                                                                                                                      │···························
2354       141                                                                                                                                                                                                     │···························
1813       162                                                                                                                                                                                                     │···························
278        179
```

## Collections in AMSL

```shell
$ taskcat AMSLService > amsl.json
$ jq -rc '.[] | [.["megaCollection"], .["technicalCollectionID"]] | @tsv' amsl.json | sort -u > amsl.tsv
$ wc -l amsl.tsv
15943 amsl.tsv
```

## Collections overlaps

```shell
$ cut -f1 amsl.tsv | sort -u > amsl.collections
$ comm -12 amsl.collections ai.collections | wc -l
11341
$ comm -13 amsl.collections ai.collections | wc -l
5689
$ comm -23 amsl.collections ai.collections | wc -l
4584
```

5689 collections in AI, which are not in AMSL, a sample:

```
$ comm -13 amsl.collections ai.collections | shuf -n 10
ebooks_sozi
The USA Journals (CrossRef)
iLearning Journal Center (CrossRef)
Kostroma State University (CrossRef)
Kocaeli Universitesi Sosyal Bilimler Dergisi (CrossRef)
Institute of Public Administration Zagreb (CrossRef)
Beyond the Horizon ISSG (CrossRef)
The Center for Global Teacher Education, Kongju National University (CrossRef)
Ukrainian Scientific Community (CrossRef)
Universitas Kebangsaan (CrossRef)
```

4584 collections in AMSL, which are not in AI, a sample:

```
$ comm -23 amsl.collections ai.collections | shuf -n 10
base_ftsillinoisuedw
base_ftenssib
base_ftvra
base_ftreroch
base_ftunimchampagnat
base_ftunivmagdrev
base_ftsena
base_ftqueensuniv
base_ftnagasakijuncol
base_ftjohnmarshallaw
```