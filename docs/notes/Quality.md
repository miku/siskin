# Data corrections

* 2018-07-18 First notes

Real world data issues. List of links to data corrections in code.

## 48

* restrict languages to a minimal subset, via
  [acceptedLanguages](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L82-L83),
  if [no suitable
  language](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L215-L217)
  is detected, leave field blank (which [violates
  schema](https://github.com/ubleipzig/intermediateschema/blob/805ee5a1e9beb39d17cecbdbcecab6ab4ed4ed36/is-0.9.json#L9))
* restrict author length, between [four and 200](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L54-L55)
* limit title length to [4096](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L56)
* if abstract is missing, is a [limited
  number](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L51-L52)
  of chars (2000) from [fulltext for abstract](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L245-L247)
* if a single author field is very long, try to break it down into smaller
  parts using [various
  delimiters](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L155-L160)
* if a lowercases field matches the string "n.n.", [treat it as essentially
  empty](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L134-L138)
* hard-coded, [blacklist of
  strings](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L172-L177),
  that probably are not authors
* slightly [inconsistent
  dates](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L108-L122)
  require some
  [cleanup](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L81)
  and fallback; if no date can be parsed, [skip record
  entirely](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L233-L236)
* remove newlines in [journal
  titles](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L258)
* some opaque [hack](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L282-L283), for package preferences
* at one point in time, at least one document id would have been [longer than
  250
  chars](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L300-L309),
  skip
* at one point, one had to [guess the
  identifier](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/genios/document.go#L124-L127)

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
  publisher ([X-Unknown](https://github.com/miku/span/blob/815d2fe2d623e88f7cee07e33bc0e4bc5ee28a1c/formats/crossref/document.go#L338))

