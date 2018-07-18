# Data corrections

* 2018-07-18 First notes

Real world data issues. List of links to data corrections in code.

## 49

* publisher, first name, last name, title, subtitle, container title are
  *unescaped* and *trimmed*, via
  [UnescapeTrim](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/common.go#L57-L60)
* a regular expression for
  [authors](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L56-L57)
  applied to [first and last
  name](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L145-L146)
  - this never skips, just replaces parts of the name strings.
* [title
  blocker](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L59-L60),
  a blacklist applied on
  [title](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L266-L270),
  if the title matches *exactly* one of the names, skip the record entirely
* if a title survives, use
  [pattens](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L62-L66)
  to [replace title
  parts](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L272-L274)
* if an document id (ai-xx-yy) [exceed a given
  limit](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L249-L251)
  (here: 250, since this was the maximum memcachedb could handle), the skip the
  record entirely
* if the chosen date of a record lies [too far in the
  future](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L253-L255),
  skip the record entirely, currently [today plus two
  years](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L68-L69)
* a record with the crossref document type *journal-issue* [is skipped
  entirely](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L257-L259)
* the article title is combined from title and subtitle (e.g. if there would be
  no title, just subtitle, use subtitle as title); if [no title is found at
  all](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L261-L264),
  skip the record entirely
* if the article title [exceeds 32000
  chars](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L276-L279),
  skip it
* if [no container
  title](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L298)
  is found, skip the record entirely
* if the [publisher matches a string in
  a blacklist](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L325-L329),
  skip the record entirely
* if no publisher can be determined, use the string "X-U (Crossref)" as
  publisher (X-Unknown)
