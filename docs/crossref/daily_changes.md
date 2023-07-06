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

If we export the number of updates per DOI, we have most 1, but up to 99 updates per DOI.

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


