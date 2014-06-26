# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
Wikipedia related.
"""

from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed
from gluish.common import ElasticsearchMixin, Executable
from gluish.esindex import CopyToIndex
from gluish.format import TSV
from gluish.intervals import yearly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.task import DefaultTask
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
        return yearly(self.date)

class WikipediaArticleDump(WikipediaTask):
    """
    Download wiki-latest-pages-articles.xml.bz2 for a given language.
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

class WikipediaTitles(WikipediaTask):
    """
    Get a list of wikipedia article titles.
    """
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout("""grep -E "<title>[^<]*</title>" {input} |
                             sed -e 's@<title>@@g' |
                             sed -e 's@</title>@@g' > {output}""",
                             input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'))

class WikipediaIsbnList(WikipediaTask):
    """
    A list ISBN (candidates) found in the dump.
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
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaTitles(date=self.date, language=self.language)

    @timed
    def run(self):
        prefixes = {'en': 'Category', 'de': 'Kategorie', 'fr': u'Catégorie'}
        if not self.language in prefixes:
            raise RuntimeError('Categorie prefix not added yet')
        output = shellout("""grep -iF "{prefix}:" {input} > {output}""",
                          input=self.input().path,
                          prefix=prefixes.get(self.language))
        output = shellout("sort {input} | uniq > {output}", input=output)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tsv'), format=TSV)

class WikipediaJson(WikipediaTask):
    """
    A Json representation of a wikipedia dump.
    """

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout("wikikit {input} > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))

class WikipediaCategoryTable(WikipediaTask):
    """
    A list of wikipedia categories. Using wikikit
    (https://github.com/miku/wikikit).

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
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        prefixes = {'en': 'Category', 'de': 'Kategorie', 'fr': u'Catégorie',
                    'es': u'Categoría'}
        if not self.language in prefixes:
            raise RuntimeError('Categorie prefix not added yet')

        output = shellout("""wikikit -c "{prefix}" {input} > {output}""",
                          input=self.input().path, prefix=prefixes.get(self.language),
                          encoding='utf-8')
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaCategoryExtension(WikipediaTask):
    """
    For each category, collect the pages it contains.
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
        return WikipediaCategoryExtension(language=self.language, date=self.date)

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
        output = shellout(r"""cat {input} | cut -f2- | sort | uniq -c |
                              sort -nr > {output}""", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaPagesWithCategories(WikipediaTask):

    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

    def requires(self):
        return WikipediaCategoryTable(date=self.date, language=self.language)

    @timed
    def run(self):
        output = shellout(r"""cat {input} | cut -f1 | sort | uniq > {output}""",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class WikipediaIndex(WikipediaTask, CopyToIndex):
    language = luigi.Parameter(default='en')
    date = ClosestDateParameter(default=datetime.date.today())

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
        return WikipediaArticleDump(date=self.date, language=self.language)

    @timed
    def run(self):
        prefixes = {'de': 'Normdaten', 'en': 'Authority control'}
        if not self.language in prefixes:
            raise RuntimeError("Languages available %s" % prefixes)
        output = shellout("""wikikit -a {prefix} {input} > {output}""",
                          input=self.input().path, prefix=prefixes.get(self.language))
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
                            logger.error('error in row: {}'.format(str(row)))
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
            logger.info(page)
            r = es.search(index='dewikinorm', body={'query': {'query_string': {'query': '"%s"' % page}}})
            for hit in r['hits']['hits']:
                logger.info(hit.get('_source').get('gnd'))
                if hit.get('_source').get('gnd'):
                    gnds.add(hit.get('_source').get('gnd'))
                else:
                    empty += 1

        logger.info('{} {} {}'.format(len(gnds), empty, i))
    def output(self):
        return luigi.LocalTarget(path=self.path(ext='ldj'))
