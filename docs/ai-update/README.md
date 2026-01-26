# AI Update

## TL;DR

indexhost is the preprocessing hostname

```shell
$ ssh indexhost
$ taskdo AIUpdate --workers 4 --style default --AIExport-with-fullrecord
```

That process will build all data dependencies, up until the AIExport file.
Failure modes: unreachable network, rate limiting, incomplete zip files. Also,
any upstream schema change needs to be addressed in code.

Once that above process exists successfully, update the index; solrhost is
hostname of the solr server.

First, create a new collection in the solr web interface, e.g. aiYYMM. Then
run (in screen or tmux):

```
$ time zstdcat -T0 $(taskoutput AIExport --with-fullrecord) | solrbulk -w 8 -server http://solrhost:8983/solr/aiYYMM -commit 20000000
```

## Additional information

* [AI Overview](../ai-overview/slides.md) (2017), a higher level overview, with all preprocessing steps illustrated
* [Quick Look at the AI](../ai-etc/README.md) (2024), a quick intro with software, hardware and data aspects
