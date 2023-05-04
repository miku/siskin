# An article index, etc

## Orientation

* [Building metadata indices](https://github.com/miku/siskin/blob/master/docs/ai-overview/slides.md)

## Data landscape

### Crossref

> It is run by the Publishers International Linking Association Inc. (PILA)[2]
> and was launched in early 2000 as a cooperative effort among publishers to
> enable persistent cross-publisher citation linking in online academic
> journals. -- [Wikipedia](https://en.wikipedia.org/wiki/Crossref)

* Crossref has an API
* we sync updates from the API, daily
* currently about 4TB of raw data, we create monthly snapshots

### Other

* DOAJ, Jstor, IEEE, BASE, ...

### Licensing

* every institution can get an individual view over a single index
* we combine license information with the raw data to create indexable files
* AMSL, FOLIO for configuration

## Deployment

* redundant setup with manual failover: 2x SOLR (dual [Xeon](https://ark.intel.com/content/www/de/de/ark/products/83361/intel-xeon-processor-e52667-v3-20m-cache-3-20-ghz.html)), 2x aux blob server (8 core [Xeon](https://www.intel.com/content/www/us/en/products/sku/64597/intel-xeon-processor-e52665-20m-cache-2-40-ghz-8-00-gts-intel-qpi/specifications.html)), all proxied
* one offline machine for data processing, [RAID6](https://en.wikipedia.org/wiki/Standard_RAID_levels#RAID_6), dual [Xeon](https://ark.intel.com/content/www/de/de/ark/products/215274/intel-xeon-gold-6326-processor-24m-cache-2-90-ghz.html)

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

