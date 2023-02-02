#!/bin/bash

# Parse and pretty print IP of live/nonlive servers.
#
#   $ siskin-whatislive.sh solr_live
#   1.2.3.4:5678
#
set -eu
set -o pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename $0) NAME"
    echo
    echo "  NAME    hostport to lookup: solr_live, solr_nonlive, blob_live, blob_nonlive"
    echo
    exit 2
fi

U=ai.ub.uni-leipzig.de/whatislive

case $1 in
    solr_live|solr_nonlive|blob_live|blob_nonlive)
        curl --fail -s "$U" | grep -v "[\w]*#" | grep "$1" | grep -oE '[0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}[.][0-9]{1,3}:[0-9]{4,5}'
        ;;
    *)
        exit 2
esac
