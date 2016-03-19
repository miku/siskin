# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
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

"""
Gallica.
"""

from __future__ import print_function
from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from luigi.contrib.esindex import CopyToIndex
from siskin.benchmark import timed
from siskin.task import DefaultTask
from siskin.utils import iterfiles, date_range
import BeautifulSoup
import codecs
import collections
import datetime
import itertools
import json
import luigi
import marcx
import re
import tempfile

class BNFTask(DefaultTask):
    TAG = '020'

    def closest(self):
        return monthly(date=self.date)

class BNFHarvest(BNFTask):
    """
    Harvest BNF. Gallica.
    """
    def requires(self):
        return Executable(name='oaimi', message='https://github.com/miku/oaimi')

    def run(self):
        output = shellout("oaimi -root collection -verbose -set gallica:typedoc:partitions http://oai.bnf.fr/oai2/OAIHandler > {output}")
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

# class BNFHarvestChunk(BNFTask):
#     """ Harvest all files in chunks. """
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     end = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     @timed
#     def run(self):
#         """ Harvest files for a certain timeframe in a temporary
#         directory, then combine all records into a single file. """
#         stopover = tempfile.mkdtemp(prefix='tasktree-')
#         oai_harvest(url="http://oai.bnf.fr/oai2/OAIHandler",
#                     begin=self.begin, end=self.end, prefix=self.prefix,
#                     directory=stopover, collection=self.collection)
#         with self.output().open('w') as output:
#             output.write("""<collection
#                 xmlns="http://www.openarchives.org/OAI/2.0/"
#                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">""")
#             for path in iterfiles(stopover):
#                 with open(path) as handle:
#                     soup = BeautifulSoup.BeautifulStoneSoup(handle.read())
#                     for record in soup.findAll('record'):
#                         output.write(str(record)) # or unicode?
#             output.write('</collection>\n')

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='xml', digest=True))

# class BNFJsonChunk(BNFTask):
#     """ Combine the chunks for a date range into a single file.
#     The filename will carry a name, that will only include year
#     and month for begin and end."""
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     end = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     def requires(self):
#         return BNFHarvestChunk(begin=self.begin, end=self.end,
#                                prefix=self.prefix, collection=self.collection)

#     @timed
#     def run(self):
#         """ TODO: this is much much too long. """
#         fr_en_map = {
#             u'domaine public': u'public domain',
#             u'musique manuscrite': u'manuscript music',
#             u"Notice d'ensemble : ": '',
#             u'Notice du catalogue : ': '',
#             u'Notice de recueil : ': '',
#         }

#         irregulars = {
#             u'Cellarius, Henri (neveu18..-18..? ; chorégraphe). Chorégraphe':
#                 u'Cellarius, Henri (18..-18..? ; chorégraphe). Chorégraphe',
#             u'Sophocle (0496?-0406 av. J.-C.). Auteur adapté':
#                 u'Sophocle (-0496?-0406). Auteur adapté',
#             u'Horace (0065-0008 av. J.-C.). Auteur du texte':
#                 u'Horace (-0065-0008). Auteur du texte',
#             u'Lacour (17..-ap. 1826). Librettiste':
#                 u'Lacour (17..-1826). Librettiste',
#             u'Eschyle (0525?-0456 av. J.-C.). Auteur adapté':
#                 u'Eschyle (-0525?-0456). Auteur adapté',
#             u'Benaut, Josse-François-Joseph (1743-1794). Compositeur. 1100':
#                 u'Benaut, Josse-François-Joseph (1743-1794). Compositeur',
#             u'Capron, Nicolas (1740?-1784). Compositeur. 1100':
#                 u'Capron, Nicolas (1740?-1784). Compositeur'
#         }

#         def parse_people(s):
#             if s in irregulars.keys():
#                 for match, replacement in irregulars.iteritems():
#                     s = s.replace(match, replacement)
#             if s.endswith(')'):
#                 if re.search(r'[0-9]\)$', s):
#                     s += '. TYPE'
#             if s in (u'Agence de publicité', u'Dédicataire', u'Parolier',
#                      u'Compositeur'):
#                 s = u'Anonyme. ' + s

#             # person name: no digits
#             a = r'(?P<a>[^0-9]+)'
#             # (year-year ; foo)
#             # d = r'\((?P<d>[0-9\.\-\? ]+)(?: ; [^0-9]+)?\)'
#             # (foo ; year-year ; bar)
#             # d = r'\(([^0-9]+ ; )?(?P<d>[0-9\.\-\? ]+)( ; [^0-9]+)?\)'
#             # (foo ; year-year ; bar)
#             d = r'\(([^0-9]+ ; )?(?P<d>[0-9\.\-\? ]+)( ; .*?)?\)'
#             # type of work: no digits
#             e = r'(?P<e>[^0-9]+)'
#             # type of work: no digits
#             # e = r'(?P<e>[^0-9]{5,})'

#             # name (years). type
#             test = re.match(r'%s( %s)?(\. %s)$' % (a, d, e), s)

#             if test:
#                 # io.append({ 'l':line, 'm':test.groupdict() })
#                 # k['t'].append(test.groupdict())
#                 sf = test.groupdict()
#             else:
#                 # io.append({ 'l':line })
#                 # k['f'].append(line)
#                 # no match: assume person name alone
#                 sf = {'a': s, 'd': '', 'e' : ''}
#                 # no match: assume person name alone
#                 # sf = {'a': line, 'd': '', 'e':''}
#             return sf

#         docs = []

#         with self.input().open() as handle:
#             soup = BeautifulSoup.BeautifulStoneSoup(handle.read())

#         for record in soup.findAll('record'):
#             doc = collections.defaultdict(list)

#             header = record.find('header')

#             # identifier
#             doc['oai'] = header.find('identifier').text
#             doc['id'] = doc['oai'].split('/')[-1] # too short?

#             if header.get('status') == 'deleted':
#                 self.logger.debug("Skipping deleted: {0}".format(doc['oai']))
#                 continue

#             metadata = record.find('metadata')

#             # title
#             for title in metadata.findAll('dc:title'):
#                 doc['title'].append(title.text)

#             # creator
#             for creator in metadata.findAll('dc:creator'):
#                 doc['creator'].append(parse_people(creator.text))

#             # subject
#             for subject in metadata.findAll('dc:subject'):
#                 doc['subject'].append(subject.text)

#             # publisher
#             for publisher in metadata.findAll('dc:publisher'):
#                 doc['publisher'].append(publisher.text)

#             # pubdate
#             for pubdate in metadata.findAll('dc:date'):
#                 doc['pubdate'].append(pubdate.text)

#             # description
#             for description in metadata.findAll('dc:description'):
#                 doc['description'].append(description.text)

#             # format
#             for format in metadata.findAll('dc:format'):
#                 if format.text.startswith('application/'):
#                     doc['mimetype'].append(format.text)
#                 else:
#                     doc['format'].append(format.text)

#             # relation
#             relations = set()
#             for rel in metadata.findAll('dc:relation'):
#                 translation = fr_en_map.get(rel.text, rel.text)
#                 if translation:
#                     relations.add(translation)
#             doc['relation'] = list(relations)

#             # score and pubtype
#             scores, pubtype = set(), set()
#             for dctype in metadata.findAll('dc:type'):
#                 translated = fr_en_map.get(dctype.text, '')
#                 if translated == 'manuscript music':
#                     scores.add(translated)
#                 else:
#                     pubtype.add(translated)

#             doc['score'] = list(scores)
#             doc['pubtype'] = list(pubtype)

#             # dc identifier
#             for dcid in metadata.findAll('dc:identifier'):
#                 if dcid.text.startswith('http://'):
#                     doc['url'].append(dcid.text)
#                 else:
#                     doc['identifier'].append(dcid.text)

#             for source in metadata.findAll('dc:source'):
#                 doc['source'].append(source.text)

#             rights = set()
#             for right in metadata.findAll('dc:rights'):
#                 rights.add(fr_en_map.get(right.text))

#             docs.append(doc)

#         # dump as json, one document per line
#         with self.output().open('w') as output:
#             for doc in docs:
#                 output.write(json.dumps(doc))
#                 output.write('\n')

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='json', digest=True))

# class BNFMarcChunk(BNFTask):
#     """ Convert a JSON snippet into MARC. """
#     begin = luigi.DateParameter(default=datetime.date(1970, 1, 1))
#     end = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     def requires(self):
#         return BNFJsonChunk(begin=self.begin, end=self.end,
#                             prefix=self.prefix, collection=self.collection)

#     @timed
#     def run(self):
#         _, stopover = tempfile.mkstemp(prefix="tasktree-")
#         with self.input().open() as handle:
#             with codecs.open(stopover, 'w', 'utf-8') as output:
#                 for line in handle:
#                     record = marcx.FatRecord(to_unicode=True, force_utf8=True)
#                     doc = json.loads(line.encode('utf-8'))

#                     record.add('001', data=doc['id'])
#                     record.add('037', n=doc['oai'])
#                     record.add('042', a='dc')

#                     people = itertools.chain(doc.get('creator', []),
#                                              doc.get('contributors', []))
#                     for i, person in enumerate(people):
#                         a = person.get('a') or ''
#                         d = person.get('d') or ''
#                         e = person.get('e') or ''
#                         if i == 0:
#                             # only add the first creator as 100
#                             record.add('100', a=a, d=d, e=e)
#                         else:
#                             # all others need to go to 700
#                             record.add('700', a=a, d=d, e=e)

#                     for i, title in enumerate(doc.get('title', [])):
#                         if i == 0:
#                             record.add('245', a=title)
#                         else:
#                             record.add('242', a=title)

#                     for publisher in doc.get('publisher', []):
#                         record.add('260', b=publisher)

#                     for pubdate in doc.get('pubdate', []):
#                         record.add('260', c=pubdate)

#                     for description in doc.get('description', []):
#                         record.add('520', a=description)

#                     for score in doc.get('score', []):
#                         record.add('254', a=score)

#                     for pubtype in doc.get('pubtype', []):
#                         record.add('655', indicators='7 ', a=pubtype)

#                     if 'url' in doc:
#                         subs = collections.defaultdict(list)
#                         if 'mimetype' in doc:
#                             subs['q'] = doc.get('mimetype')[0]
#                         if 'rights' in doc:
#                             subs['z'] = doc.get('rights')[0]
#                         subs['u'] = doc.get('url')[0]
#                         record.add('856', indicators='40', **subs)

#                     for subject in doc.get('subject', []):
#                         record.add('609', a=subject)

#                     for ident in doc.get('identifier', []):
#                         record.add('609', a=ident)

#                     if len(doc.get('relation', [])) > 0:
#                         record.add('786', i=[r for r in doc.get('relation', [])])

#                     output.write(record.as_marc().decode('utf-8'))

#         luigi.File(stopover).move(self.output().path)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='mrc', digest=True))

# class BNFMarcHarvest(luigi.WrapperTask, BNFTask):
#     """ Harvest BNF, cut all dates to the first of month. Example:
#     2013-05-12 .. 2013-09-11 would be translated to
#     2013-05-01 .. 2013-09-01"""
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     end = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     def requires(self):
#         """ Only require up to the last full month. """
#         begin = datetime.date(self.begin.year, self.begin.month, 1)
#         end = datetime.date(self.end.year, self.end.month, 1)
#         if end < self.begin:
#             raise RuntimeError('Invalid range: %s - %s' % (begin, end))
#         dates = date_range(begin, end, 1, 'months')
#         for i, _ in enumerate(dates[:-1]):
#             yield BNFMarcChunk(begin=dates[i], end=dates[i + 1],
#                                prefix=self.prefix, collection=self.collection)

#     def output(self):
#         return self.input()

# class BNFCombine(BNFTask):
#     """ Combine all MARC files into one. """
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     def requires(self):
#         return BNFMarcHarvest(begin=self.begin, end=self.date,
#                               prefix=self.prefix, collection=self.collection)

#     @timed
#     def run(self):
#         _, combined = tempfile.mkstemp(prefix='tasktree')
#         for target in self.input():
#             shellout("cat {input} >> {output}", input=target.path,
#                      output=combined)
#         output = shellout("marcuniq -o {output} {input}", input=combined)
#         luigi.File(output).move(self.output().path)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='mrc', digest=True))

# class BNFJson(BNFTask):
#     """ Convert to JSON. """
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     def requires(self):
#         return BNFCombine(begin=self.begin, date=self.date, prefix=self.prefix,
#                           collection=self.collection)

#     def run(self):
#         tmp = shellout("marctojson -m date={date} {input} > {output}",
#                        input=self.input().path, date=self.date)
#         luigi.File(tmp).move(self.output().path)

#     def output(self):
#         """ Use monthly files. """
#         return luigi.LocalTarget(path=self.path(ext='ldj', digest=True))

# class BNFIndex(BNFTask, CopyToIndex):
#     begin = luigi.DateParameter(default=datetime.date(2011, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())
#     prefix = luigi.Parameter(default="oai_dc")
#     collection = luigi.Parameter(default="gallica:typedoc:partitions")

#     index = 'bnf'
#     doc_type = 'title'
#     purge_existing_index = True

#     mapping = {'title': {'date_detection': False,
#                           '_id': {'path': 'content.001'},
#                           '_all': {'enabled': True,
#                                    'term_vector': 'with_positions_offsets',
#                                    'store': True}}}

#     def update_id(self):
#         """ This id will be a unique identifier for this indexing task."""
#         return self.effective_task_id()

#     def requires(self):
#         return BNFJson(begin=self.begin, date=self.date, prefix=self.prefix,
#                        collection=self.collection)
