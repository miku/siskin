# An article index, etc

> 2024-04-23 / 2023-05-05, AG INDEX, [UB Leipzig](https://www.ub.uni-leipzig.de/)

![](pipelines.jpg)

## Orientation

* [Building metadata indices](https://github.com/miku/siskin/blob/master/docs/ai-overview/slides.md) (2017)
* [finc](https://finc.info/) (2011-...), [partners](https://finc.info/anwender), [B'TAG 2017](https://github.com/miku/siskin/blob/master/docs/btag-2017/btag-2017.md) presentation
* BYOI: [Build your own index](https://freidok.uni-freiburg.de/fedora/objects/freidok:11346/datastreams/FILE1/content), 5th German Vufind Meetup 2016, 2016–10–11
* ELAG2016 slides: [Build your own discovery index of scholary e-resources](https://www.slideshare.net/slideshow/build-your-own-discovery-index-of-scholary-eresources/62855622)

## Current developments

* mostly maintenance, fixes and data updates
* what could be a next step? full-text search over complete publication
  content, [maybe](https://scholar.archive.org/)? moving to or including
[openalex](https://openalex.org/)?
* ...

## Data landscape

Heterogenous data landscape; metadata - everyone does it in a different way.

[![](LocomotivGT_Zene.jpg)](https://en.wikipedia.org/wiki/Zene_%E2%80%93_Mindenki_m%C3%A1sk%C3%A9pp_csin%C3%A1lja)

### Crossref

> It is run by the Publishers International Linking Association Inc. (PILA)[2]
> and was launched in early 2000 as a cooperative effort among publishers to
> enable persistent cross-publisher citation linking in online academic
> journals. -- [Wikipedia](https://en.wikipedia.org/wiki/Crossref)

* Crossref has an [API](https://www.crossref.org/documentation/retrieve-metadata/rest-api/)
* we sync updates from the API, daily
* currently about ~8TB of raw data (438,925,575 records), we derive monthly snapshots (some [archived](https://archive.org/details/crossref-2024-01-01))
* we use [zstd](https://en.wikipedia.org/wiki/Zstd) for everything we can -
  text files can be typically [compressed to
10-20%](https://lemire.me/blog/2021/06/30/compressing-json-gzip-vs-zstd/) of
their original size; so 14TB of disk spaces can allow to work with 70-140TB of
text data

### Other

* DOAJ, Jstor, IEEE, BASE, OSF, OLC, ...

In total, our raw (not deduplicated) dataset of mostly article metadata
consists of 161953178 (so 89.1% of data comes from Crossref).

### It's files all the way down

Preprocessing over [data at rest](https://en.wikipedia.org/wiki/Data_at_rest);
data in use / serving from [SOLR](https://en.wikipedia.org/wiki/Apache_Solr)
open source [full-text search
engine](https://en.wikipedia.org/wiki/Full-text_search) (and [key-value
store](https://github.com/ubleipzig/microblob), for now). Moving to
[SOLRCLOUD](https://solr.apache.org/guide/6_6/solrcloud.html) · DD+L.

* one way to [minimize state](https://www.worldofbs.com/minimize-state/)

> What this shows is that every programming philosophy is about how to manage
> state, and each philosophy comes with trade-offs. What this means is that
> there is no "one true way" to deal with state, and that each programming
> philosophy is useful and important in the correct domain. It also shows how
> important minimizing state is.

* files are immutable (not modified after they are created)

```shell
$ tree -sh siskin  | tail -1
117 directories, 16899 files
```

### Licensing

* every institution can get an individual view over a single index (cf. [KBART](https://www.niso.org/standards-committees/kbart))

![](view-s.jpg)

> a group of people looking at a single index all seeing a slightly different part of it as a cubist painting

* we combine license information with the raw data to create indexable files, licensing information is just another source
* AMSL, FOLIO for configuration
* a filter program to pass data through

### Scheduling

* using a framework to schedule tasks, [spotify/luigi](https://github.com/spotify/luigi/), mainly [toposort](https://en.wikipedia.org/wiki/Topological_sorting) - [many options in 2022](https://www.reddit.com/r/dataengineering/comments/s78jvx/best_job_scheduler_in_2022_airflow_dagster/)

> In a scheduling problem, there is a set of tasks, along with a set of
> constraints specifying that starting certain tasks depends on other tasks
> being completed beforehand. We can map these sets to a digraph, with the
> tasks as the nodes and the direct prerequisite constraints as the edges. [6.042](https://openlearninglibrary.mit.edu/assets/courseware/v1/ec6730f747b31e019f98b20842f6f064/asset-v1:OCW+6.042J+2T2019+type@asset+block/MIT6_042JS15_Session17.pdf)



## Deployment

* live free ..., [Linux](https://en.wikipedia.org/wiki/Linux)
* redundant setup with manual failover: 2x SOLR (dual [Xeon](https://ark.intel.com/content/www/de/de/ark/products/83361/intel-xeon-processor-e52667-v3-20m-cache-3-20-ghz.html)), 2x aux blob server (8 core [Xeon](https://www.intel.com/content/www/us/en/products/sku/64597/intel-xeon-processor-e52665-20m-cache-2-40-ghz-8-00-gts-intel-qpi/specifications.html)), all proxied
* one offline machine for data processing, [RAID6](https://en.wikipedia.org/wiki/Standard_RAID_levels#RAID_6), dual [Xeon](https://ark.intel.com/content/www/de/de/ark/products/215274/intel-xeon-gold-6326-processor-24m-cache-2-90-ghz.html)
* [ansible](https://en.wikipedia.org/wiki/Ansible_(software)) for [infrastructure as code](https://en.wikipedia.org/wiki/Infrastructure_as_code)

> Infrastructure as code can be a key attribute of enabling best practices in
> [DevOps](https://en.wikipedia.org/wiki/DevOps). Developers become more
> involved in defining configuration and Ops teams get involved earlier in the
> development process.

Also, kind of: you build it, you run it (ACM Queue [4/4 2006](https://queue.acm.org/detail.cfm?id=1142065)).

![](RAID_6.svg)

> Any form of RAID that can continue to execute read and write requests to all of a RAID array's virtual disks in the **presence of any two concurrent disk failures**.

```
# lvdisplay
  --- Logical volume ---
  LV Path                /dev/data-0/data-1
  LV Name                data-1
  VG Name                data-0
  LV UUID                TcbfLY-xxxxx-dZqx-xxxxx-he2s-xxxx-xxxxxx
  LV Write Access        read/write
  LV Creation host, time xxxxxxxx.uni-leipzig.de, 2022-03-04 07:08:15 +0100
  LV Status              available
  # open                 1
  LV Size                13.97 TiB
  Current LE             3662759
  Segments               1
  Allocation             inherit
  Read ahead sectors     auto
  - currently set to     4096
  Block device           253:0

# mdadm --query /dev/md127
/dev/md127: 13.97TiB raid6 4 devices, 0 spares. Use mdadm --detail for more detail.

# mdadm --detail /dev/md127
/dev/md127:
           Version : 1.2
     Creation Time : Fri Mar  4 07:08:09 2022
        Raid Level : raid6
        Array Size : 15002664960 (13.97 TiB 15.36 TB)
     Used Dev Size : 7501332480 (6.99 TiB 7.68 TB)
      Raid Devices : 4
     Total Devices : 4
       Persistence : Superblock is persistent

     Intent Bitmap : Internal

       Update Time : Thu May  4 16:09:51 2023
             State : clean
    Active Devices : 4
   Working Devices : 4
    Failed Devices : 0
     Spare Devices : 0

            Layout : left-symmetric
        Chunk Size : 512K

Consistency Policy : bitmap

              Name : data
              UUID : 7d8cb9d6:abcdef12:d5644956:1b612f83
            Events : 24678

    Number   Major   Minor   RaidDevice State
       0     259        6        0      active sync   /dev/nvme0n1p1
       1     259        2        1      active sync   /dev/nvme1n1p1
       2     259        5        2      active sync   /dev/nvme2n1p1
       3     259        7        3      active sync   /dev/nvme3n1p1

# nvme list -o json | jq -r '.Devices[].ModelNumber'
Dell Ent NVMe CM6 RI 7.68TB
Dell Ent NVMe CM6 RI 7.68TB
Dell Ent NVMe CM6 RI 7.68TB
Dell Ent NVMe CM6 RI 7.68TB
```

* blobserver serves up to 30 rps, solr probably between 3-10 rps (back-of-the-envelope: Google Scholar may be in the 300-1K rps range)

Redundancy wrapup:

* had RAM failures in the past
* no automatic failover means failover not instant (but easy to switch, via [nginx](https://en.wikipedia.org/wiki/Nginx) [reverse proxy](https://en.wikipedia.org/wiki/Reverse_proxy))
* pro of setup: simple, few moving parts

## Data curation and usability question

* [ ] what should be included
* [ ] where do people actually search for scholarly material

## Data access and quality

* one of the more time consuming aspects
* regular feedback, *upstream* (ex: [CEEOL](https://gist.github.com/miku/f4d97b61121f43efef5550557bc111f5)), [more](https://github.com/ubleipzig/siskin/blob/master/docs/notes/Quality.md)
* balance between comprehensiveness and correctness

## Data questions

* [ ] books, chapters in crossref
* [ ] which dataset is contained in which other

## Operational questions

* [ ] moving from batch indexing to real-time indexing and updates

