Collections
===========

A collection is a group of records. A data source can have 1 or more
collections. Collections may overlap.


Where do collections play a role?
---------------------------------

In siskin:

```shell
$ tasknames | grep -i collection
AICollectionsAndSerialNumbers
AMSLCollections
AMSLCollectionsISIL
AMSLCollectionsISILList
AMSLCollectionsShardFilter
CrossrefCollectionStats
CrossrefCollections
CrossrefCollectionsCount
CrossrefCollectionsDifference
JstorCollectionMapping
JstorIntermediateSchemaGenericCollection
```

In span as filter:

* [filter/collection.go](https://github.com/miku/span/blob/2f582de074ece435548ae577febdbb57395ad4aa/filter/collection.go)

Hard-Coded in fast converters:

* [doaj](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/doaj/document.go#L42)
* [ceeol](https://github.com/miku/span/blob/94878b24fa18cddc00af56ce075aae904c5821e2/formats/ceeol/article.go#L20)
* [zvdd](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/zvdd/record.go#L18)
* [genios](https://github.com/miku/span/blob/3c88c49920507c6eee1711aead83ccf9b5481ec8/formats/genios/document.go#L45)
* [degruyter](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/degruyter/article.go#L39)
* [thieme](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/thieme/document.go#L18)
* [elsevier](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/elsevier/dataset.go#L25)
* [ieee](https://github.com/miku/span/blob/43a44f1d62356f12e1486b76b894b68da6949c8a/formats/ieee/publication.go#L20)
* [jstor](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/jstor/article.go#L45)
* [olms](https://github.com/miku/span/blob/ebc6e9f007a666816e822ed0013808fb86d91d99/formats/olms/record.go#L83)
* [hhbd](https://github.com/miku/span/blob/63b4d389e28e6ac73069d032ad989d4523e2b999/formats/hhbd/record.go#L113)
* [highwire](https://github.com/miku/span/blob/a1c1b604f9320fa2c4d5f390c560f69f65e04f8a/formats/highwire/record.go#L62)

Custom package lookups (Because a collection might not be explicitly specified in a record):

* [Genios
packages](https://github.com/miku/span/blob/3c88c49920507c6eee1711aead83ccf9b5481ec8/formats/genios/document.go#L251-L270)
and manually created
[dbmap.json](https://github.com/miku/span/blob/2f582de074ece435548ae577febdbb57395ad4aa/assets/genios/dbmap.json)
from things like #12301.

External collections or packages in holdings data:

* [oclc](https://github.com/miku/span/blob/060bfd0f912655bc7f6e2e8ed0c980ce28341873/licensing/entry.go#L118-L119)
* [own_anchor/package_collection](https://github.com/miku/span/blob/060bfd0f912655bc7f6e2e8ed0c980ce28341873/licensing/entry.go#L106-L107)

Collection blacklists:

* [Crossref artifacts](https://github.com/miku/span/blob/9ee898530614277bd9705f0e42b760955817972d/formats/crossref/document.go#L322-L333)
* [Crossref unknown publisher](https://github.com/miku/span/blob/9ee898530614277bd9705f0e42b760955817972d/formats/crossref/document.go#L336)

