Collections
===========

A collection is a group of records. A data source can have one or more
collections. Collections may overlap. Often, a record has information which
collection it belongs to. Sometimes it does not.


Where do collections play a role?
---------------------------------

### [siskin](https://github.com/miku/siskin)

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

### [span](https://github.com/miku/span) filter

```
Process:
            AMSL Discovery API
                    |
                    v
            AMSLFilterConfig
                    |
                    v
    $ span-tag -c config.json < input.is > output.is
```

* [filter/collection.go](https://github.com/miku/span/blob/2f582de074ece435548ae577febdbb57395ad4aa/filter/collection.go)

A collection filter is used 497 times in the current (2018-03-13)
[AMSLFilterConfig](https://github.com/miku/siskin/blob/732d0c0683ae744bf37dc34b8795301cb76360aa/siskin/sources/amsl.py#L645-L711):

```
$ taskcat AMSLFilterConfig --local-scheduler | jq . | grep -c '"collection":'
497
```

Example from DE-14 config:

```
      ...
      {
        "and": [
          {
            "source": [
              "67"
            ]
          },
          {
            "collection": [
              "SLUB/Deutsche Fotothek"
            ]
          }
        ]
      },
      ...
```

A few month back, we recorded seven cases for attachments, all involving the name of a collection:

* [How to attach and
  ISIL?](https://github.com/miku/siskin/blob/732d0c0683ae744bf37dc34b8795301cb76360aa/siskin/sources/amsl.py#L651-L657)

### Hard-Coded in Go converters

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

### Custom package lookups (Because a collection might not be explicitly specified in a record)

* [Genios
packages](https://github.com/miku/span/blob/3c88c49920507c6eee1711aead83ccf9b5481ec8/formats/genios/document.go#L251-L270)
and manually created
[dbmap.json](https://github.com/miku/span/blob/2f582de074ece435548ae577febdbb57395ad4aa/assets/genios/dbmap.json)
from things like #12301.

### External collections or packages in holdings data

* [oclc](https://github.com/miku/span/blob/060bfd0f912655bc7f6e2e8ed0c980ce28341873/licensing/entry.go#L118-L119)
* [own_anchor/package_collection](https://github.com/miku/span/blob/060bfd0f912655bc7f6e2e8ed0c980ce28341873/licensing/entry.go#L106-L107)

### Collection blacklists

* [Crossref artifacts](https://github.com/miku/span/blob/9ee898530614277bd9705f0e42b760955817972d/formats/crossref/document.go#L322-L333)
* [Crossref unknown publisher](https://github.com/miku/span/blob/9ee898530614277bd9705f0e42b760955817972d/formats/crossref/document.go#L336)

### Hard-coded in [Metafacture](https://github.com/metafacture/metafacture-core/wiki) snippets

* [30.xml](https://github.com/miku/siskin/blob/0209586991cbb501b10b0672ac9d793b53b76925/siskin/assets/30/30_morph.xml#L56-L60)
* [34.xml](https://github.com/miku/siskin/blob/447f48b80423e5e221b3dc1cea5e4c1790a98997/siskin/assets/34/morph.xml#L54-L58)
* [60.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/60/morph.xml#L44-L48),
  [60.flux](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/60/flux.flux#L7)
* [78.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/78/78_morph.xml#L8-L12), 
* [87.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/87/87_morph.xml#L37-L41)
* [101.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/101/101_morph.xml#L12-L16)
* [107.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/107/morph.xml#L48-L52),
  [107.flux](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/107/107.flux#L5)
* [121.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/arxiv/121_morph.xml#L46-L50)
* [124.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/124/124_morph.xml#L50-L54)
* (unused) [datacite.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/datacite/morph.xml#L44-L48)
* (unused) [oai.xml](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/oai/morph.xml#L44-L48)

### Hard-coded in [jq](https://stedolan.github.io/jq/) snippets

* [73.jq](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/73/filter.jq#L5)
* [80.jq](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/assets/80/filter.jq#L5)

### Hard-coded in [luigi](https://github.com/spotify/luigi) tasks

* [55.py](https://github.com/miku/siskin/blob/a87d04d7126f7be721bf37fe4b47048504c16fda/siskin/sources/jstor.py#L425),
  [Jstor and AMSL
  mapping](https://github.com/miku/siskin/blob/a87d04d7126f7be721bf37fe4b47048504c16fda/siskin/sources/jstor.py#L372-L396)
* [123.py](https://github.com/miku/siskin/blob/ea069dbeb189237c6d0b5791a63046846b786075/siskin/sources/jove.py#L123)
* [141.py](https://github.com/miku/siskin/blob/342c55300c72fa97ef047089000fd146727c3a3d/siskin/sources/lynda.py#L121)

### Special cases

* 39, [Persee](https://github.com/miku/siskin/blob/381f754b7758466ae68af751ed1a2231b51c6d9d/siskin/sources/persee.py#L96-L150)
  requires a special step to add MARC 980c field, which in turn is used to
  differentiate a collection for two different client, e.g. here *Persee !=
  Persee* (here, [via ISSN list](https://github.com/miku/siskin/blob/381f754b7758466ae68af751ed1a2231b51c6d9d/siskin/sources/persee.py#L141-L143)).
