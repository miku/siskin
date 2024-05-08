#!/usr/bin/env python
# coding: utf-8

"""
Excursion: Metadata enrichment
==============================

Enrich our metadata with data from Microsoft Academic Graph (MAG).

Goals:

* requirements can be a dictionary, too
* a more complex tasks, where a programming language might be more suitable than a DSL

"""

import collections
import json

import luigi
from luigi.format import Gzip
from s06 import IntermediateSchema


class ReferenceList(luigi.ExternalTask):
    """
    This task stands for an external data source, here MAG.
    """

    def output(self):
        return luigi.LocalTarget("inputs/mag.tsv.gz", format=Gzip)


class CombinedWithRefs(luigi.Task):
    """
    This task lookups up a DOI in the ReferenceList and enriches
    the metadata record with a list of referenced DOI.
    """

    def requires(self):
        return {
            "records": IntermediateSchema(),
            "refs": ReferenceList(),
        }

    def run(self):
        """
        First, load lookup table into a dictionary, then iterate over records
        and add information to them, if possible.
        """
        refmap = collections.defaultdict(list)

        with self.input().get("refs").open() as handle:
            for line in handle:
                parts = line.strip().split("\t")
                if len(parts) > 0:
                    doi, refs = parts[0], parts[1:]
                    refmap[doi] = refs

        with self.input().get("records").open() as handle:
            with self.output().open("w") as output:
                for line in handle:
                    doc = json.loads(line)
                    try:
                        doc["refs"] = refmap[doc.get("doi")]
                    except KeyError:
                        pass
                    output.write(json.dumps(doc) + "\n")

    def output(self):
        return luigi.LocalTarget("outputs/combined-with-refs.is.ldj")


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
