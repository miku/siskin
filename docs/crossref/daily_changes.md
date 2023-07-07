# Crossref Daily Changes

What kind of changes do we see in crossref, on a daily basis?

Rest API [docs](https://api.crossref.org).

> Notes on incremental metadata updates.
>
> When using time filters to retrieve periodic, incremental metadata updates,
> the from-index-date filter should be used over from-update-date,
> from-deposit-date, from-created-date and from-pub-date. The timestamp that
> from-index-date filters on is guaranteed to be updated every time there is a
> change to metadata requiring a reindex. -- [#Works](/https://api.crossref.org/swagger-ui/index.html#/Works/get_works)

We use "index", but what kind of events trigger an index? Are all events
visible in the metadata? How many new records do we get?

## Reindex docs

```
[finc@indexmaker2 crossref]$ for fn in $(fd 'feed-1-index-2023-06-*'); do echo -n "$fn "; zstdcat -T0 $fn | LC_ALL=C wc -l; done
feed-1-index-2023-06-01-2023-06-01.json.zst 1416542
feed-1-index-2023-06-02-2023-06-02.json.zst 1280465
feed-1-index-2023-06-03-2023-06-03.json.zst 933903
feed-1-index-2023-06-04-2023-06-04.json.zst 1063729
feed-1-index-2023-06-05-2023-06-05.json.zst 999256
feed-1-index-2023-06-06-2023-06-06.json.zst 803099
feed-1-index-2023-06-07-2023-06-07.json.zst 1642269
feed-1-index-2023-06-08-2023-06-08.json.zst 1516309
feed-1-index-2023-06-09-2023-06-09.json.zst 1312880
feed-1-index-2023-06-10-2023-06-10.json.zst 786124
feed-1-index-2023-06-11-2023-06-11.json.zst 725763
feed-1-index-2023-06-12-2023-06-12.json.zst 841403
feed-1-index-2023-06-13-2023-06-13.json.zst 1258861
feed-1-index-2023-06-14-2023-06-14.json.zst 682585
feed-1-index-2023-06-15-2023-06-15.json.zst 961767
feed-1-index-2023-06-16-2023-06-16.json.zst 1412103
feed-1-index-2023-06-17-2023-06-17.json.zst 1394667
feed-1-index-2023-06-18-2023-06-18.json.zst 371782
feed-1-index-2023-06-19-2023-06-19.json.zst 930430
feed-1-index-2023-06-20-2023-06-20.json.zst 1173979
feed-1-index-2023-06-21-2023-06-21.json.zst 1235406
feed-1-index-2023-06-22-2023-06-22.json.zst 825640
feed-1-index-2023-06-23-2023-06-23.json.zst 1154709
feed-1-index-2023-06-24-2023-06-24.json.zst 1422129
feed-1-index-2023-06-25-2023-06-25.json.zst 1387918
feed-1-index-2023-06-26-2023-06-26.json.zst 1416321
feed-1-index-2023-06-27-2023-06-27.json.zst 1106496
feed-1-index-2023-06-28-2023-06-28.json.zst 1346647
feed-1-index-2023-06-29-2023-06-29.json.zst 1101275
feed-1-index-2023-06-30-2023-06-30.json.zst 1258279
```

Affected rows, on average 1.1M per day.

```python
In [9]: df.describe()
Out[9]:
                  1
count      30.00000
mean  1125424.53333
std    293334.50785
min    371782.00000
25%    931298.25000
50%   1164344.00000
75%   1377600.25000
max   1642269.00000
```

## New documents

About 550 files, about 1TB compressed.

```sh
$ fd 'feed-1-index-202*' | wc -l
550
```

Create a first tabular view of crossref.

```sh
$ for fn in $(fd 'feed-1-index-202*'); do zstdcat -T0 $fn | span-crossref-table; done | zstd -c -T0 > out
```

At around 150kdocs/s; around 2h to extract. Yields a 14G compressed TSV, with
509,096,757 lines, 47GB uncompressed.

Include md5 of the raw blob to discover "diffless" changes. Re-running over 550 files.

```sh
$ time for fn in $(fd 'feed-1-index-*'); do zstdcat -T0 $fn 2> /dev/null | \
    span-crossref-table ; done | pv -l | \
    zstd -c -T0 > /data/tmp/span-crossref-table-feed-1-with-md5.tsv.zst
```

Three dates, created, deposited, indexed. Over the 550 files (which contain date w/ zero updates), we have:

```sql
D select count(DISTINCT c), count(DISTINCT d), count(DISTINCT i) from c;
┌───────────────────┬───────────────────┬───────────────────┐
│ count(DISTINCT c) │ count(DISTINCT d) │ count(DISTINCT i) │
│       int64       │       int64       │       int64       │
├───────────────────┼───────────────────┼───────────────────┤
│              7594 │              5970 │               485 │
└───────────────────┴───────────────────┴───────────────────┘
Run Time (s): real 0.887 user 42.651173 sys 0.885386
```

If we export the number of updates per DOI, we have mostly 1, but up to 99 updates per DOI.

```python
In [5]: df.describe()
Out[5]:
                 99
count 146148862.000
mean          3.483
std           6.282
min           1.000
25%           1.000
50%           1.000
75%           3.000
max          99.000
```

3532704 dois have equal created and indexed date:

```sql
D select count(*) from c where c.c == c.i;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      3532704 │
└──────────────┘
```

Of the total 509096757, that's about 0.69%. Estimating that on an average day
with 1.1M docs, about 7633 will be new records.

## Finding duplicates

* created another tabular version of all harvested crossref metadata, via `span-crossref-table`
* that conversion took about 3h

```
[finc@indexmaker2 crossref]$ time for fn in $(fd 'feed-1-index-*'); do \
    zstdcat -T0 $fn 2> /dev/null |
    span-crossref-table ; done |
    zstd -c -T0 > /data/tmp/span-crossref-table-feed-1-with-md5.tsv.zst

real    180m6.404s
user    1091m4.984s
sys     284m9.035s
```

The result is a 24G file, 509096757 lines, 64,114,441,955 bytes uncompressed. Then, load file into duckdb.

```sql
$ duckdb /data/tmp/span-crossref-table-feed-1-with-md5.db
v0.8.1 6536a77232
Enter ".help" for usage hints.
D create table c (doi string, c date, d date, i date, member int, h string);
D .timer on
D copy c from '/data/tmp/span-crossref-table-feed-1-with-md5.tsv.zst' (compression "zstd", ignore_errors true, delimiter "\t");
Run Time (s): real 219.931 user 2913.588391 sys 151.253385
```

Loading and indexing 500M+ rows takes less than 4 minutes. The cli process
consume about 20G of RAM after the import. The resulting database file is 18G,
so more compressed than the zstd-compressed data, plus it includes indices.

```sql
D show table c;
┌─────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
│ column_name │ column_type │  null   │   key   │ default │  extra  │
│   varchar   │   varchar   │ varchar │ varchar │ varchar │ varchar │
├─────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
│ doi         │ VARCHAR     │ YES     │         │         │         │
│ c           │ DATE        │ YES     │         │         │         │
│ d           │ DATE        │ YES     │         │         │         │
│ i           │ DATE        │ YES     │         │         │         │
│ member      │ INTEGER     │ YES     │         │         │         │
│ h           │ VARCHAR     │ YES     │         │         │         │
└─────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┘

D select count(*) from c;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│    509096757 │
└──────────────┘
Run Time (s): real 0.017 user 0.699540 sys 0.004857

D select count(distinct c.c), count(distinct c.d), count(distinct c.i) from c;
┌─────────────────────┬─────────────────────┬─────────────────────┐
│ count(DISTINCT c.c) │ count(DISTINCT c.d) │ count(DISTINCT c.i) │
│        int64        │        int64        │        int64        │
├─────────────────────┼─────────────────────┼─────────────────────┤
│                7594 │                5970 │                 485 │
└─────────────────────┴─────────────────────┴─────────────────────┘
Run Time (s): real 0.854 user 43.321564 sys 5.245419
```

Most and least busy days:

```sql
D select count(*) as cnt, i from c group by c.i order by cnt desc limit 30;
┌──────────┬────────────┐
│   cnt    │     i      │
│  int64   │    date    │
├──────────┼────────────┤
│ 14532481 │ 2022-04-03 │
│ 14135845 │ 2022-04-02 │
│ 12637261 │ 2022-04-04 │
│ 12236764 │ 2022-03-29 │
│ 12025589 │ 2022-04-01 │
│ 11879711 │ 2022-03-31 │
│ 10985560 │ 2022-04-05 │
│ 10916658 │ 2022-03-30 │
│  8850467 │ 2022-04-06 │
│  2857081 │ 2022-04-07 │
│  2568194 │ 2023-04-27 │
│  2389387 │ 2023-04-21 │
│  2088900 │ 2023-04-26 │
│  1994787 │ 2022-12-23 │
│  1944725 │ 2023-04-28 │
│  1845329 │ 2023-04-15 │
│  1837646 │ 2022-12-21 │
│  1832150 │ 2023-01-05 │
│  1819884 │ 2023-04-19 │
│  1801017 │ 2022-12-17 │
│  1790904 │ 2023-02-01 │
│  1787188 │ 2023-01-06 │
│  1781414 │ 2023-04-20 │
│  1780371 │ 2023-01-04 │
│  1736304 │ 2023-02-15 │
│  1732111 │ 2023-02-23 │
│  1721219 │ 2023-04-13 │
│  1719288 │ 2023-04-23 │
│  1696348 │ 2023-02-10 │
│  1672353 │ 2023-04-17 │
├──────────┴────────────┤
│ 30 rows     2 columns │
└───────────────────────┘
Run Time (s): real 0.073 user 4.266158 sys 0.008791

D select count(*) as cnt, i from c group by c.i order by cnt limit 30;
┌────────┬────────────┐
│  cnt   │     i      │
│ int64  │    date    │
├────────┼────────────┤
│      1 │ 2022-01-15 │
│      1 │ 2022-01-16 │
│      1 │ 2022-01-18 │
│      1 │ 2022-01-26 │
│      1 │ 2022-01-27 │
│      1 │ 2022-01-31 │
│      1 │ 2022-02-06 │
│      1 │ 2022-02-15 │
│      1 │ 2022-02-17 │
│      1 │ 2022-02-19 │
│      1 │ 2022-02-23 │
│      1 │ 2022-02-26 │
│      1 │ 2022-03-01 │
│      1 │ 2022-03-08 │
│      1 │ 2022-03-16 │
│      1 │ 2022-03-19 │
│      1 │ 2022-03-21 │
│      2 │ 2022-02-02 │
│      2 │ 2022-02-09 │
│      2 │ 2022-03-03 │
│      3 │ 2022-01-21 │
│  57707 │ 2022-06-05 │
│  59309 │ 2022-04-24 │
│  64838 │ 2022-04-10 │
│  90231 │ 2022-05-01 │
│  93866 │ 2022-08-21 │
│ 108312 │ 2022-04-23 │
│ 108976 │ 2022-04-09 │
│ 110928 │ 2022-11-20 │
│ 121117 │ 2022-04-17 │
├────────┴────────────┤
│ 30 rows   2 columns │
└─────────────────────┘

D select avg(cnt) from (select count(*) as cnt from c group by c.i);
┌────────────────────┐
│      avg(cnt)      │
│       double       │
├────────────────────┤
│ 1049684.0350515463 │
└────────────────────┘
Run Time (s): real 0.071 user 4.243305 sys 0.003358
```

Only a small amount is created.

```sql
D select count(*) from c where c.c == c.i;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      3532704 │
└──────────────┘
Run Time (s): real 0.056 user 2.956140 sys 0.231948
```

The average time between created and indexed is 271262232 seconds, or 3139 days, or 8.6 years.

```sql
D select avg(g) from (select extract(epoch from age(c.i, c.c)) as g from c);
┌───────────────────┐
│      avg(g)       │
│      double       │
├───────────────────┤
│ 271262232.0782606 │
└───────────────────┘
Run Time (s): real 1.062 user 65.950425 sys 0.133972

```

A random sample of intervals:

```sql
D select age(c.i, c.c) as g from c USING SAMPLE RESERVOIR(20) order by g desc;
┌───────────────────────────┐
│             g             │
│         interval          │
├───────────────────────────┤
│ 17 years 9 months 29 days │
│ 14 years 9 months 6 days  │
│ 13 years 3 months 21 days │
│ 11 years 3 months 30 days │
│ 11 years 11 days          │
│ 7 years 7 months 12 days  │
│ 6 years 20 days           │
│ 5 years 9 months 19 days  │
│ 5 years 25 days           │
│ 4 years 2 months 17 days  │
│ 2 years 4 months 12 days  │
│ 2 years 2 months 27 days  │
│ 2 years 15 days           │
│ 1 year 8 months 15 days   │
│ 1 year 7 months 2 days    │
│ 1 year 6 months 10 days   │
│ 1 year 5 months           │
│ 1 year 4 months 24 days   │
│ 6 months 4 days           │
│ 4 months 5 days           │
├───────────────────────────┤
│          20 rows          │
└───────────────────────────┘
Run Time (s): real 0.293 user 1.455579 sys 16.462447
```

DOI, all, distinct.

```
D select count(c.doi) from c;
┌──────────────┐
│ count(c.doi) │
│    int64     │
├──────────────┤
│    509096757 │
└──────────────┘

D select count(distinct c.doi) from c;
┌───────────────────────┐
│ count(DISTINCT c.doi) │
│         int64         │
├───────────────────────┤
│             146148863 │
└───────────────────────┘
Run Time (s): real 42.851 user 609.590649 sys 238.632413

D select c.doi, count(*) from c group by c.doi;
100% ▕████████████████████████████████████████████████████████████▏ ^P
┌────────────────────────────────────────┬──────────────┐
│                  doi                   │ count_star() │
│                varchar                 │    int64     │
├────────────────────────────────────────┼──────────────┤
│ 10.1080/21645515.2021.1950506          │           28 │
│ 10.1016/j.asr.2022.03.027              │            2 │
│ 10.1007/bf00345956                     │            4 │
│ 10.3390/s20236982                      │           30 │
│ 10.1186/s13071-020-04206-y             │            3 │
│ 10.1016/j.corsci.2017.08.026           │            6 │
│ 10.4103/2277-8632.117195               │           11 │
│ 10.1039/c7ra07694k                     │           25 │
│ 10.3390/plants11172214                 │           12 │
│ 10.1016/j.chemosphere.2006.02.041      │           10 │
│ 10.1097/mpg.0b013e3182a27463           │            5 │
│ 10.3390/nu13020526                     │           27 │
│ 10.1016/j.enpol.2020.111467            │           36 │
│ 10.1109/apsipa.2016.7820744            │            6 │
│ 10.3171/jns.1960.17.6.0945             │           41 │
│ 10.1016/j.snb.2018.07.023              │           36 │
│ 10.1039/c3sm50582k                     │           15 │
│ 10.3171/jns-07/09/0617                 │           29 │
│ 10.1016/j.cma.2019.07.028              │           34 │
│ 10.1021/a1980011o                      │           10 │
│         ·                              │            · │
│         ·                              │            · │
│         ·                              │            · │
│ 10.1002/9781394163878.ch4              │            1 │
│ 10.1007/978-981-16-8679-5_154          │            1 │
│ 10.1515/9781399503952-003              │            1 │
│ 10.5771/9783800594528                  │            1 │
│ 10.1515/9780691225784-013              │            1 │
│ 10.1039/d2ra02784d                     │            1 │
│ 10.21055/preprints-3112116             │            1 │
│ 10.56369/tsaes.3473                    │            1 │
│ 10.18574/nyu/9781479812547.003.0015    │            1 │
│ 10.1201/9781003358961-37               │            1 │
│ 10.29003/m2956.sudak.ns2022-18/341-342 │            1 │
│ 10.51737/2766-4775.2022.059            │            1 │
│ 10.1007/978-981-16-8679-5_30           │            1 │
│ 10.53771/ijstra.2022.3.1.0077          │            1 │
│ 10.21832/9781800415973-001             │            1 │
│ 10.14361/9783839457009-015             │            1 │
│ 10.11159/htff22.143                    │            1 │
│ 10.3138/9781487512088-fm               │            1 │
│ 10.20378/irb-54906                     │            1 │
│ 10.1525/aft.2005.32.6.2                │            1 │
├────────────────────────────────────────┴──────────────┤
│ 146148863 rows (40 shown)                   2 columns │
└───────────────────────────────────────────────────────┘
Run Time (s): real 24.553 user 435.765935 sys 153.969322

D select c.doi, count(*) as g from c group by c.doi order by g desc limit 20;
100% ▕████████████████████████████████████████████████████████████▏
┌────────────────────────────────────┬───────┐
│                doi                 │   g   │
│              varchar               │ int64 │
├────────────────────────────────────┼───────┤
│ 10.17163/ret.n10.2015.01           │    99 │
│ 10.17163/ret.n10.2015.03           │    99 │
│ 10.17163/ret.n10.2015.05           │    99 │
│ 10.17163/ret.n10.2015.04           │    99 │
│ 10.17163/ret.n10.2015.06           │    98 │
│ 10.17163/ret.n10.2015.02           │    98 │
│ 10.17163/alt.v12n2.2017.09         │    87 │
│ 10.1210/er.2015-1042               │    79 │
│ 10.1016/j.cemconcomp.2014.11.001   │    78 │
│ 10.1016/j.wneu.2017.04.082         │    77 │
│ 10.21873/invivo.11950              │    77 │
│ 10.1046/j.1365-2265.1999.00754.x   │    77 │
│ 10.1016/j.imavis.2020.103910       │    77 │
│ 10.1111/etap.12143                 │    77 │
│ 10.1136/qhc.12.6.458               │    77 │
│ 10.1001/jama.2016.2323             │    77 │
│ 10.1002/art.23108                  │    77 │
│ 10.1016/b978-0-444-53486-6.00032-6 │    76 │
│ 10.1093/jtm/taaa008                │    76 │
│ 10.2307/3001469                    │    76 │
├────────────────────────────────────┴───────┤
│ 20 rows                          2 columns │
└────────────────────────────────────────────┘
Run Time (s): real 23.837 user 404.847040 sys 118.126102
```

Example of one of the most changed doc:

```sql
D select * from c where c.doi = '10.17163/ret.n10.2015.01';
┌──────────────────────────┬────────────┬────────────┬────────────┬────────┬──────────────────────────────────┐
│           doi            │     c      │     d      │     i      │ member │                h                 │
│         varchar          │    date    │    date    │    date    │ int32  │             varchar              │
├──────────────────────────┼────────────┼────────────┼────────────┼────────┼──────────────────────────────────┤
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-05-30 │ 2022-05-30 │   6739 │ 89164a3235a54c33aaa57bbc6f86432f │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-07-20 │ 2022-07-20 │   6739 │ 242ee202a358a5f10d74e6767892f0be │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-07-22 │ 2022-07-22 │   6739 │ a515b654cbc86e7d291a52df66837194 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-07-31 │ 2022-07-31 │   6739 │ 7985857e7d830e2484a2f442c3dc914d │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-03 │ 2022-08-03 │   6739 │ e1a1c82bb941c03862d56d787c557aff │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-08 │ 2022-08-08 │   6739 │ 7f5af81c6d066421c7918e169927c739 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-09 │ 2022-08-09 │   6739 │ ef2b523046aa133632d05a784741846d │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-28 │ 2022-08-28 │   6739 │ fa1e9eebd87b7fdecba3278db3edcb51 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-29 │ 2022-08-29 │   6739 │ f4a96ab65d677808671ae18ef25d9a30 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-30 │ 2022-08-30 │   6739 │ 789dd340c16a5115d6aac71483effdb9 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-08-31 │ 2022-08-31 │   6739 │ 1657283a68ebeb463d3b981e9f661bc4 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-02 │ 2022-09-02 │   6739 │ d3e0b2132171de643ed7d3e7c24ae046 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-03 │ 2022-09-03 │   6739 │ f962a1162e3c858cccc945ca307b39ba │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-04 │ 2022-09-04 │   6739 │ 6d01107c974c8fec5cba1c33c07f4c92 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-06 │ 2022-09-06 │   6739 │ 4fd2c437a8b2190741c7e63a384a1217 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-10 │ 2022-09-10 │   6739 │ e873a8836600410241907d82bae9b1d7 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-11 │ 2022-09-11 │   6739 │ daa3cfb16c72a22501d7e8b5797c838c │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-12 │ 2022-09-12 │   6739 │ 4bbba5047823e19a36113f4f13a73f80 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-13 │ 2022-09-13 │   6739 │ c636f48e104574857299fcfd5ad525c7 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2022-09-19 │ 2022-09-20 │   6739 │ 58b65bb8272bb832e9bf5710dfc0f870 │
│            ·             │     ·      │     ·      │     ·      │     ·  │                ·                 │
│            ·             │     ·      │     ·      │     ·      │     ·  │                ·                 │
│            ·             │     ·      │     ·      │     ·      │     ·  │                ·                 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-15 │ 2023-05-15 │   6739 │ e8f3d85b9a23e710dfe69cd396fc01ec │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-16 │ 2023-05-16 │   6739 │ 302bd7ecacd585ac884bdefeb995ae88 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-22 │ 2023-05-22 │   6739 │ 05df7a37dd50c12e916a849a9b984810 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-23 │ 2023-05-23 │   6739 │ c0e4fd76a6a5580607330f51a9491d22 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-24 │ 2023-05-24 │   6739 │ d7e8da9e9bde55be8d88838a9823b21c │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-25 │ 2023-05-25 │   6739 │ a7e7b7ad4fb61ac9369c7009182dde05 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-26 │ 2023-05-26 │   6739 │ 499b39c16ea9b029f479cc22eb7d1229 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-27 │ 2023-05-27 │   6739 │ 5c90ccb2a427545d69304a16cbc5226f │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-28 │ 2023-05-28 │   6739 │ 30534e80870751db7d62e5e6881c8f97 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-29 │ 2023-05-29 │   6739 │ 8c428e8c048e7478c67ff21623a9dfc7 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-30 │ 2023-05-30 │   6739 │ a68c947c8744d041584d90bba2176300 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-05-31 │ 2023-05-31 │   6739 │ e81f424dd61c9fe60314ba1e15e53007 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-01 │ 2023-06-01 │   6739 │ fec833f9eb521ddfdfe1264270fbc966 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-03 │ 2023-06-03 │   6739 │ e3cca0a3dae723cd2d2455a77a0cc19a │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-04 │ 2023-06-04 │   6739 │ 7ebad69be4b4ae9e19be7a44441136b5 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-06 │ 2023-06-06 │   6739 │ f004682291aa70c2aff1099344e31f58 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-07 │ 2023-06-07 │   6739 │ 4359a40928fc55b2b01d040f655ae697 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-08 │ 2023-06-08 │   6739 │ 0b52031519932ee8ef2c69d7ec697cbb │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-10 │ 2023-06-10 │   6739 │ 86653e54997317d0b4d6a8d1155d04a2 │
│ 10.17163/ret.n10.2015.01 │ 2017-01-17 │ 2023-06-13 │ 2023-06-13 │   6739 │ cee93eb9fdfd6a7fc1457d479f3abdec │
├──────────────────────────┴────────────┴────────────┴────────────┴────────┴──────────────────────────────────┤
│ 99 rows (40 shown)                                                                                6 columns │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

In this case, all the docs are actually different:

```sql
D select count(distinct(c.h)) from c where c.doi = '10.17163/ret.n10.2015.01';
┌─────────────────────┐
│ count(DISTINCT c.h) │
│        int64        │
├─────────────────────┤
│                  99 │
└─────────────────────┘
Run Time (s): real 0.825 user 51.504724 sys 0.119782
```

Most docs actually are unique, so there are almost no repetitions or duplicates. At most, we find 5 hash duplicates.

```sql
D select count(*) as g from c group by c.h order by g desc limit 20;
100% ▕████████████████████████████████████████████████████████████▏
┌───────┐
│   g   │
│ int64 │
├───────┤
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     5 │
│     4 │
│     4 │
│     4 │
│     4 │
│     4 │
│     4 │
│     4 │
└───────┘
Run Time (s): real 64.690 user 534.884158 sys 321.860719
```

Examples:

```sql
D select any_value(doi), count(c.h) as g from c group by c.h order by g desc limit 20;
100% ▕████████████████████████████████████████████████████████████▏
┌─────────────────────────────────┬───────┐
│         any_value(doi)          │   g   │
│             varchar             │ int64 │
├─────────────────────────────────┼───────┤
│ 10.1016/0029-8018(82)90023-3    │     5 │
│ 10.7326/0003-4819-95-1-129_3    │     5 │
│ 10.1016/j.jaac.2016.07.660      │     5 │
│ 10.1163/1574-9347_dnp_e12213830 │     5 │
│ 10.1364/oe.20.010242.m002       │     5 │
│ 10.33349/1999.28.871            │     5 │
│ 10.7591/9781501744952-004       │     5 │
│ 10.1007/s15002-013-0459-8       │     5 │
│ 10.1515/9783486746907-011       │     5 │
│ 10.1051/ro/2020036              │     5 │
│ 10.1109/icassp.1992.226422      │     5 │
│ 10.1093/ejil/chaa052            │     5 │
│ 10.1037/e718982007-001          │     5 │
│ 10.5617/acta.5721               │     4 │
│ 10.1167/16.12.758               │     4 │
│ 10.1192/bjp.160.3.420a          │     4 │
│ 10.1090/conm/280/04622          │     4 │
│ 10.5771/9783845292786-1         │     4 │
│ 10.1353/tfr.2012.0204           │     4 │
│ 10.1007/978-3-642-53862-9_27    │     4 │
├─────────────────────────────────┴───────┤
│ 20 rows                       2 columns │
└─────────────────────────────────────────┘
Run Time (s): real 355.240 user 1618.754531 sys 1098.779503

D select * from c where c.doi = '10.7326/0003-4819-95-1-129_3';
┌──────────────────────────────┬────────────┬────────────┬────────────┬────────┬──────────────────────────────────┐
│             doi              │     c      │     d      │     i      │ member │                h                 │
│           varchar            │    date    │    date    │    date    │ int32  │             varchar              │
├──────────────────────────────┼────────────┼────────────┼────────────┼────────┼──────────────────────────────────┤
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 02805e8ed4ea9fd0ed3db52667be8408 │
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 8abc47b6d940e6d19ee7b927a3741e5e │
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 8abc47b6d940e6d19ee7b927a3741e5e │
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 8abc47b6d940e6d19ee7b927a3741e5e │
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 8abc47b6d940e6d19ee7b927a3741e5e │
│ 10.7326/0003-4819-95-1-129_3 │ 2013-04-12 │ 2013-04-12 │ 2022-03-30 │   4285 │ 8abc47b6d940e6d19ee7b927a3741e5e │
└──────────────────────────────┴────────────┴────────────┴────────────┴────────┴──────────────────────────────────┘
Run Time (s): real 1.626 user 51.296737 sys 18.765963
```

That may be a glitch in the harvesting code, as the records look all the same.

How many DOI have more than one submission?

```sql
D select count(*) from (select count(*) as g from c group by c.doi having g = 1);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│     88576809 │
└──────────────┘
Run Time (s): real 15.921 user 321.351854 sys 56.661512
D select count(*) from (select count(*) as g from c group by c.doi having g > 1);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│     57572054 │
└──────────────┘
Run Time (s): real 11.261 user 325.209091 sys 34.597917
```

88M seens just once, 57M seen more than once. More than 5, 10, 20, 50 changes?

```sql
 select count(*) from (select count(*) as g from c group by c.doi having g > 5);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│     21285365 │
└──────────────┘
Run Time (s): real 21.433 user 393.477629 sys 113.643156
D select count(*) from (select count(*) as g from c group by c.doi having g > 10);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│     11372527 │
└──────────────┘
Run Time (s): real 14.820 user 391.422370 sys 63.453797
D select count(*) from (select count(*) as g from c group by c.doi having g > 20);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│      4352186 │
└──────────────┘
Run Time (s): real 20.170 user 389.359661 sys 102.976141
D select count(*) from (select count(*) as g from c group by c.doi having g > 50);
100% ▕████████████████████████████████████████████████████████████▏
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│       359018 │
└──────────────┘
Run Time (s): real 15.895 user 388.559850 sys 71.093172
```

Are there any members that are associated with many updates?

```sql
D select m, count(*) as mc from (select any_value(c.member) as m, count(*) as g from c group by c.doi having g > 50) group by m order by mc desc;
100% ▕████████████████████████████████████████████████████████████▏
┌───────┬────────┐
│   m   │   mc   │
│ int32 │ int64  │
├───────┼────────┤
│    78 │ 129695 │
│   297 │  50299 │
│   311 │  30030 │
│   316 │  17612 │
│   286 │   9983 │
│   276 │   8802 │
│   292 │   7659 │
│   301 │   7314 │
│   263 │   6950 │
│   221 │   6859 │
│   179 │   5595 │
│  1968 │   5384 │
│   341 │   4129 │
│    10 │   3228 │
│    22 │   3009 │
│  1965 │   2983 │
│   150 │   2933 │
│  1754 │   2858 │
│    15 │   2450 │
│   239 │   2418 │
│    ·  │      · │
│    ·  │      · │
│    ·  │      · │
│ 10492 │      1 │
│ 10009 │      1 │
│  6936 │      1 │
│  1560 │      1 │
│  3846 │      1 │
│ 25959 │      1 │
│ 16488 │      1 │
│ 28214 │      1 │
│  4638 │      1 │
│  8957 │      1 │
│ 18045 │      1 │
│  4487 │      1 │
│  1967 │      1 │
│ 10083 │      1 │
│ 21822 │      1 │
│  6101 │      1 │
│  6336 │      1 │
│  2169 │      1 │
│ 12682 │      1 │
│   310 │      1 │
├───────┴────────┤
│   1042 rows    │
│   (40 shown)   │
└────────────────┘
Run Time (s): real 35.335 user 518.912785 sys 289.015408
```

Member 78 has the most updates.

```sql
D select m, count(*) as mc from (select any_value(c.member) as m, count(*) as g from c group by c.doi having g > 1) group by m order by mc desc;
100% ▕████████████████████████████████████████████████████████████▏
┌───────┬──────────┐
│   m   │    mc    │
│ int32 │  int64   │
├───────┼──────────┤
│    78 │ 11339218 │
│   297 │  6956399 │
│   311 │  4543015 │
│   301 │  2325168 │
│   263 │  2250918 │
│   286 │  1910349 │
│   316 │  1849009 │
│   276 │  1108227 │
│   179 │  1082353 │
│    56 │  1081153 │
│  1968 │   917324 │
│   374 │   897688 │
│   317 │   828201 │
│   292 │   601552 │
│   266 │   577219 │
│   320 │   539735 │
│    16 │   512476 │
│   147 │   457736 │
│    50 │   395056 │
│  1121 │   349640 │
│    ·  │        · │
│    ·  │        · │
│    ·  │        · │
│ 36803 │        1 │
│ 31003 │        1 │
│ 21881 │        1 │
│ 10800 │        1 │
│ 29659 │        1 │
│ 25595 │        1 │
│ 28844 │        1 │
│ 28421 │        1 │
│ 23380 │        1 │
│ 39471 │        1 │
│ 24471 │        1 │
│ 20220 │        1 │
│  6093 │        1 │
│ 25525 │        1 │
│ 36718 │        1 │
│ 35449 │        1 │
│ 33036 │        1 │
│ 31347 │        1 │
│ 23531 │        1 │
│  8695 │        1 │
├───────┴──────────┤
│    20039 rows    │
│    (40 shown)    │
└──────────────────┘
Run Time (s): real 38.220 user 505.847775 sys 279.201109
```

Which is Elsevier: [78](http://api.crossref.org/members/78).

## Import on i7 8550u CPU / 16G RAM

Can we run this on a 2018 laptop, too?

```sql
$ duckdb /data/tmp/span-crossref-table-feed-1-with-md5.db
v0.8.1 6536a77232
Enter ".help" for usage hints.
D create table c (doi string, c date, d date, i date, member int, h string);
D .timer on
D copy c from 'span-crossref-table-feed-1-with-md5.tsv.zst' (compression "zstd", ignore_errors true, delimiter "\t");
Killed
```

By default, 75% of RAM is set as `memory_limit`, but if we reduce that to,
here: `10G` (62.5% RAM), the copy op works (slower, as process need to resort
to tempfiles).

```sql
D PRAGMA memory_limit='10GB';

D drop table c;
D create table c (doi string, c date, d date, i date, member int, h string);
D copy c from 'span-crossref-table-feed-1-with-md5.tsv.zst' (compression "zstd", ignore_errors true, delimiter "\t");

Run Time (s): real 538.201 user 2761.105100 sys 132.009661
```

Difference:

* 256G RAM: 219.931s (1.0x)
*  16G RAM: 538.201s (2.5x)

Some queries seem to fail:

```sql
D select c.doi, count(*) from c group by c.doi;
 50% ▕██████████████████████████████                              ▏ Run Time (s): real 97.069 user 391.312287 sys 125.325974
Error: Out of Memory Error: failed to pin block of size 262KB (9.9GB/10.0GB used)
```

