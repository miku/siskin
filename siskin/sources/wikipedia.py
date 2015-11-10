# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301
#
#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                 by The Finc Authors, http://finc.info
#                 by Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>
#

"""
Wikipedia related.
"""

from elasticsearch import helpers as eshelpers
from gluish.common import Executable
from gluish.format import TSV
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import ElasticsearchMixin
import collections
import datetime
import elasticsearch
import json
import luigi
import urlparse

class WikipediaTask(DefaultTask):
    """
    Generic wikipedia related task.
    """
    TAG = 'wikipedia'

    def closest(self):
        return monthly(self.date)

class WikipediaArticleDump(WikipediaTask):
    """
    Download wiki-latest-pages-articles.xml.bz2 for a given language. Unarchive on the fly.
    """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    @timed
    def run(self):
        host = 'http://dumps.wikimedia.org'
        path = '/{lc}wiki/latest/{lc}wiki-latest-pages-articles.xml.bz2'.format(
                lc=self.language)
        url = urlparse.urljoin(host, path)
        output = shellout("curl {url} | bunzip2 -c > {output}", url=url)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='xml'))

class WikipediaOutlinks(WikipediaTask):
    """ Collect links to the interwebs (best effort). """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaArticleDump(language=self.language, date=self.date)

    @timed
    def run(self):
        """
        Wrong Assumption: There is no space or | in the URL.
        """
        output = shellout("""LANG=C grep -o -E "http://[^| ]*" {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaTitles(WikipediaTask):
    """
    Get a list of wikipedia article titles.

    James Joyce
    Judo
    JapaneseLanguage
    James Bond
    Japanese language
    Johnny Got His Gun
    """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout("""LANG=C grep -E "<title>[^<]*</title>" {input} |
                             LANG=C sed -e 's@<title>@@g' |
                             LANG=C sed -e 's@</title>@@g' | sed -e 's/^ *//g' > {output}""",
                             input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'), format=TSV)

class WikipediaTitlesInBSZ(WikipediaTask):
    """ Run a query with the exact wikipedia title in elasticsearch/bsz
    and report back the number of hits. """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaTitles(date=self.date, language=self.language)

    @timed
    def run(self):
        es = elasticsearch.Elasticsearch()
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('title',)):
                    title = row.title.strip()
                    try:
                        result = es.search(index='bsz', q='"%s"' % title.replace('"','\\"'))
                        output.write_tsv(title, result['hits']['total'])
                    except Exception as exc:
                        self.logger.warn(exc)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'), format=TSV)

class WikipediaIsbnList(WikipediaTask):
    """
    A list ISBN (candidates) found in the dump. Contains dups.

    9780716703440
    9780471925675
    9780486653839
    9780486653839
    9780674033634
    9780201151428
    9780805386622
    9780521316774
    9780201149425
    9780767915014
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikipediaArticleDump(date=self.date, language=self.language),
                'app': Executable(name='isbngrep')}

    @timed
    def run(self):
        output = shellout("cat {input} | isbngrep -n > {output}",
                          input=self.input().get('dump').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'), format=TSV)

class WikipediaCategoryList(WikipediaTask):
    """
    A list of wikipedia categories. Using `grep`.

    ...
    Category:1358 establishments by country
    Category:1358 establishments in England
    Category:1358 in England
    Category:1358 in Europe
    Category:1358 in France
    Category:1358 in Norway
    Category:1358 in international relations
    Category:1358 in law
    Category:1358 in politics
    Category:1358 works
    ...
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaTitles(date=self.date, language=self.language)

    @timed
    def run(self):
        prefixes = {'en': 'Category', 'de': 'Kategorie', 'fr': u'Catégorie'}
        if self.language not in prefixes:
            raise RuntimeError('Categorie prefix not added yet')
        output = shellout("""LANG=C grep -iF "{prefix}:" {input} > {output}""",
                          input=self.input().path,
                          prefix=prefixes.get(self.language))
        output = shellout("LANG=C sort {input} | LANG=C uniq > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'), format=TSV)

class WikipediaJson(WikipediaTask):
    """
    A Json representation of a wikipedia dump. Line delimited JSON.
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikipediaArticleDump(date=self.date, language=self.language),
                'app': Executable(name='wikitojson', message='https://github.com/miku/wikitools')}

    @timed
    def run(self):
        output = shellout("wikitojson {input} > {output}", input=self.input().get('dump').path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class WikipediaCategoryTable(WikipediaTask):
    """
    A list of wikipedia categories. Using wikitools
    (https://github.com/miku/wikitools).

    Similar to `WikipediaCategoryList`.

    Example output:

        ...
        Elia Kazan  Autobiografie
        Elia Kazan  Drehbuchautor
        Elia Kazan  Oscarpreisträger
        ...

    Approximate sizes:

        English: 21759052
        German:   6927106
        French:   5682380
        Spanish:  3365340
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikipediaArticleDump(date=self.date, language=self.language),
                'app': Executable(name='wikicats', message='https://github.com/miku/wikitools')}

    @timed
    def run(self):
        prefixes = {
            'de': 'Kategorie',
            'en': 'Category',
            'fr': u'Catégorie',
            'eo': 'Kategorio',
            'es': u'Categoría',
            'it': 'Categoria',
            'he': u'קטגוריות', # won't work
        }
        if self.language not in prefixes:
            raise RuntimeError('Category prefix not added yet')

        output = shellout("""wikicats -pattern "{prefix}" {input} > {output}""",
                          input=self.input().get('dump').path, prefix=prefixes.get(self.language),
                          encoding='utf-8')
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaCategoryExtension(WikipediaTask):
    """
    For each category, collect the pages it contains. Similar to
    WikipediaCategoryExtensionJson, except the output here is tabular.

    Badeanlage in Niedersachsen Moskaubad|Maschsee|Hardausee|Waldbad Birkerteich
    Filmpreis als Thema Kategorie:Goldene Kamera|Kategorie:Golden Globe Award
    Kanute (Schweiz)    Daniela Baumer|Corrado Filipponi|Ron Fischer (Kanute)|Michael Kurt
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaCategoryTable(date=self.date, language=self.language)

    @timed
    def run(self):
        reverse = collections.defaultdict(set)
        with self.input().open() as handle:
            for row in handle.iter_tsv():
                if not len(row) == 2:
                    continue
                page, category = row
                reverse[category].add(page)
        with self.output().open('w') as output:
            for category, pages in reverse.iteritems():
                output.write_tsv(category, '|'.join(pages))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaCategoryExtensionJson(WikipediaTask):
    """
    For each category, collect the pages it contains.
    Example:

    {
       "category" : "Jazz-Oboist",
       "pages" : [
          "Bob Cooper",
          "Charles Owens (Saxophonist, 1939)",
          "Karl Jenkins",
          "Marshall Allen",
          "Matthias Schubert",
          "Maud Sauer",
          "Paul McCandless",
          "Phil Bodner",
          "Roger Janotta",
          "Sonny Simmons",
          "Werner Baumgart",
          "Yusef Lateef"
       ]
    }
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaCategoryTable(date=self.date, language=self.language)

    @timed
    def run(self):
        reverse = collections.defaultdict(set)
        with self.input().open() as handle:
            for row in handle.iter_tsv():
                if not len(row) == 2:
                    continue
                page, category = row
                reverse[category].add(page)
        with self.output().open('w') as output:
            for category, pages in reverse.iteritems():
                output.write(json.dumps(dict(category=category, pages=sorted(pages))))
                output.write('\n')

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class WikipediaCategoryExtensionIndex(WikipediaTask, CopyToIndex):
    """
    Index the page title authority id docs.
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    @property
    def index(self):
        return '%swikicats' % self.language

    doc_type = 'default'

    mapping = {'default': {'date_detection': False,
                           '_all': {'enabled': True,
                                    'term_vector': 'with_positions_offsets',
                                    'store': True}}}

    def requires(self):
        return WikipediaCategoryExtensionJson(language=self.language, date=self.date)

class WikipediaCategoryDistribution(WikipediaTask):
    """
    Something like this: https://gist.github.com/miku/78d84756d08d7dace204

    Another example from the english wikipedia:

         664354 Living people
          88726 Archived files for deletion discussions
          51015 Year of birth missing (living people)
          37800 Pending DYK nominations
          28772 English-language films
          20531 American films
          19743 Local COIBot Reports
          19606 Year of birth unknown
          18093 Year of birth missing
          17793 The Football League players
          17192 Year of death missing
        ...
    """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaCategoryTable(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout(r"""cat {input} | cut -f2- | LANG=C sort | LANG=C uniq -c |
                              LANG=C sort -nr > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaPagesWithCategories(WikipediaTask):
    """
    Contains dirty rows. But good row look like this:

    ...
    1932 Maccabiah Games
    1932 Madras and Southern Mahratta Railway Strike
    1932 Major League Baseball season
    1932 Massachusetts State Aggies football team
    1932 Memorial Cup
    ...
    A Gleam
    A Gleam Handicap
    A Glen Campbell Christmas
    A Glimmer of Hope
    ...
    Abazu-Akabo
    Abaí
    Abaíra
    Abaúj (region)
    ...
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaCategoryTable(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout(r"""cat {input} | cut -f1 | LANG=C sort | LANG=C uniq > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaIndex(WikipediaTask, CopyToIndex):
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    purge_existing_index = True

    @property
    def index(self):
        return '%swiki' % self.language

    doc_type = 'page'

    def requires(self):
        return WikipediaJson(language=self.language, date=self.date)

#
# Only for the German (Authority data)
#

class WikipediaRawAuthorityData(WikipediaTask):
    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'dump': WikipediaArticleDump(date=self.date, language=self.language),
                'app': Executable(name='wikinorm', message='https://github.com/miku/wikitools')}

    @timed
    def run(self):
        prefixes = {'de': 'Normdaten', 'en': 'Authority control'}
        if not self.language in prefixes:
            raise RuntimeError("Languages available %s" % prefixes)
        output = shellout("""wikinorm -pattern '{prefix}' {input} > {output}""",
                          input=self.input().get('dump').path, prefix=prefixes.get(self.language))
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaRawAuthorityJson(WikipediaTask):
    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())
    allowed_errors = luigi.IntParameter(default=100, significant=False)

    def requires(self):
        return WikipediaRawAuthorityData(date=self.date, language=self.language)

    @timed
    def run(self):
        errors = 0
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv():
                    if not len(row) == 2:
                        errors += 1
                        if errors > self.allowed_errors:
                            raise RuntimeError('more than %s errors, adjust allowed_errors')
                        else:
                            self.logger.error('error in row: {0}'.format(str(row)))
                            continue
                    title, ids = row
                    trimmed = ids.strip("{}")
                    authority = {'title': title}
                    for entry in trimmed.split('|'):
                        parts = entry.split("=")
                        if not len(parts) == 2:
                            continue
                        name, value = parts
                        authority[name.lower()] = value
                    output.write(json.dumps(authority))
                    output.write("\n")

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class WikipediaGNDList(WikipediaTask):
    """
    TSV(title, GND)
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaRawAuthorityJson(date=self.date, language=self.language)

    @timed
    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for line in handle:
                    content = json.loads(line)
                    if 'gnd' in content:
                        output.write_tsv(content.get('title', 'NA').encode('utf-8'),
                                         content.get('gnd', 'NA').encode('utf-8'))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaRawAuthorityIndex(WikipediaTask, CopyToIndex):
    """
    Index the page title authority id docs.
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    index = 'dewikinorm'
    doc_type = 'default'

    mapping = {'default': {'date_detection': False,
                           '_all': {'enabled': True,
                                    'term_vector': 'with_positions_offsets',
                                    'store': True}}}

    def requires(self):
        return WikipediaRawAuthorityJson(language=self.language, date=self.date)

class WikipediaCategoryGNDList(WikipediaTask, ElasticsearchMixin):
    """
    Important. For each category, find the associated pages and find the GND
    lookup the GND of that page.
    """

    language = luigi.Parameter(default='de')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return {'norm': WikipediaRawAuthorityIndex(date=self.date,
                                                   language=self.language),
                'cats': WikipediaCategoryExtensionIndex(date=self.date,
                                                        language=self.language)}
    def run(self):
        es = elasticsearch.Elasticsearch(timeout=30)
        hits = eshelpers.scan(es, {'query': {'match_all': {}},
            'fields': ["category", "pages"]}, index='dewikicats', doc_type='default',
            scroll='20m', size=100000)
        with self.output().open('w') as output:
            for hit in hits:
                fields = hit.get('fields')
                output.write_tsv(hit['_id'], fields['category'][0].encode('utf-8'))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaCategoryExample(WikipediaTask, ElasticsearchMixin):
    """
    See whats in Komponisten der Romantik: NqFMwG6rTYK6yqVpQGF_tQ
    """
    category = luigi.Parameter(default='NqFMwG6rTYK6yqVpQGF_tQ')

    def run(self):
        gnds, empty, i = set(), 0, 0
        es = elasticsearch.Elasticsearch()
        result = es.get(index='dewikicats', id=self.category)

        for i, page in enumerate(result.get('_source').get('pages')):
            self.logger.info(page)
            r = es.search(index='dewikinorm', body={'query': {'query_string': {'query': '"%s"' % page}}})
            for hit in r['hits']['hits']:
                self.logger.info(hit.get('_source').get('gnd'))
                if hit.get('_source').get('gnd'):
                    gnds.add(hit.get('_source').get('gnd'))
                else:
                    empty += 1

        self.logger.info('{0} {1} {2}'.format(len(gnds), empty, i))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class WikipediaRawCitations(WikipediaTask):
    """
    Very crudely extract citations from wikipedia markup.

    Example:

    {{cite book | url=http://books.google.de/books?id=GlICpnVArb8C&amp;pg=PA304 | pages =304–305 | chapter =The Antimonials ...
    {{cite book|author = [[Michael Sipser]] | year = 1997 | title = Introduction to the Theory of Computation
    ...
    """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout("""LANG=C egrep -oi "\\{{\\{{cite book[^}}]*?}}}}" {input} > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='txt'))
