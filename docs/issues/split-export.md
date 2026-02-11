## split up AIExport task for different sources

currently in @siskin/workflows/ai.py the AIExport task is a combined tasks for
all the data sources that are part of the index, cf. @siskin/workflows/ai.py
AIIntermediateSchema. We already broke apart the JstorExport, in
@siskin/sources/jstor.py cf JstorExport task. In the same style, split out all
the remaining data sources that make up the index, e.g. crossref, doaj, etc.

We need to apply the licensing to the intermediate schema and then export to the solr format.

### priority

1

### type

feature
