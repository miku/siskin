# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,R0904
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

from gluish.intervals import monthly
from gluish.parameter import ClosestDateParameter
from gluish.utils import shellout
from siskin.benchmark import timed
from siskin.task import DefaultTask
import BeautifulSoup
import collections
import datetime
import hashlib
import json
import luigi
import marcx
import pymarc
import tempfile

class QucosaTask(DefaultTask):
    TAG = '022'

    def closest(self):
        return monthly(date=self.date)

class QucosaDump(QucosaTask):

    def run(self):
        output = shellout("oaimi -verbose -prefix xMetaDissPlus > {output}")
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

# class QucosaHarvestChunk(OAIHarvestChunk, QucosaTask):
#     """ Harvest all files in chunks. """

#     begin = luigi.DateParameter(default=datetime.date.today())
#     end = luigi.DateParameter(default=datetime.date.today())
#     url = luigi.Parameter(default="http://www.qucosa.de/oai", significant=False)
#     prefix = luigi.Parameter(default="xMetaDissPlus")
#     collection = luigi.Parameter(default=None)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='xml', digest=True))

# class QucosaJsonChunk(QucosaTask):
#     """ Convert Qucosa XML into JSON by hand. Any better way?
#     Maybe sth. like https://github.com/gawel/pyquery """
#     begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
#     end = luigi.DateParameter(default=datetime.date.today())

#     def requires(self):
#         return QucosaHarvestChunk(begin=self.begin, end=self.end)

#     @timed
#     def run(self):
#         docs = []

#         with self.input().open() as handle:
#             soup = BeautifulSoup.BeautifulStoneSoup(handle.read())
#             for record in soup.findAll('record'):
#                 doc = collections.defaultdict(list)

#                 # header
#                 header = record.find('header')
#                 if header.get('status') == 'deleted':
#                     continue

#                 # IDs
#                 doc['oai'] = header.find('identifier').text
#                 doc['id'] = doc['oai'].replace('oai:qucosa.de:', '')

#                 # the whole metadata element
#                 metadata = record.find('metadata')
#                 diss = metadata.find('xmetadiss:xmetadiss')
#                 if not diss:
#                     raise ValueError('qucosa without xmetadiss')

#                 # title
#                 if diss.find('dc:title'):
#                     doc['title'] = diss.find('dc:title').text
#                 else:
#                     self.logger.error("Qucosa w/o title: {0}".format(doc.get('id', 'NA')))
#                     # raise ValueError('qucosa without title')

#                 # subtitle
#                 alternative = diss.find('dcterms:alternative')
#                 if alternative and (alternative.get("xsi:type") ==
#                                     'ddb:talternativeISO639-2'):
#                     doc['alt'] = alternative.text

#                 # source
#                 source = diss.find('dc:source')
#                 if source and source.get('xsi:type') == 'ddb:noScheme':
#                     doc['source'] = source.text

#                 # creators
#                 for creator in diss.findAll('dc:creator'):
#                     forename = creator.find('pc:forename', recursive=True)
#                     surname = creator.find('pc:surname', recursive=True)
#                     doc['creator'].append("%s %s" % (forename.text, surname.text))

#                 # subjects
#                 for subject in diss.findAll('dc:subject'):
#                     doc[subject['xsi:type']].append(subject.text)

#                 # abstracts
#                 for abstract in diss.findAll('dc:abstract'):
#                     doc['abstract'].append(abstract.text)

#                 # tocs
#                 for toc in diss.findAll('dcterms:tableOfContents'):
#                     doc['toc'].append(toc.text)

#                 # publisher
#                 for publisher in diss.findAll('dc:publisher'):
#                     name = publisher.find('cc:name', recursive=True)
#                     place = publisher.find('cc:place', recursive=True)
#                     doc['publisher'].append("%s %s" % (name.text, place.text))

#                 # contributor
#                 for contributor in diss.findAll('dc:contributor'):
#                     forename = contributor.find('pc:forename', recursive=True)
#                     surname = contributor.find('pc:surname', recursive=True)
#                     role = contributor.get('thesis:role')
#                     doc['contributor'].append({
#                         'name': '%s %s' % (forename.text, surname.text),
#                         'role': role
#                     })

#                 # publication type
#                 if diss.find('dc:type').get('xsi:type') == 'dini:PublType':
#                     doc['pubtype'] = diss.find('dc:type').text

#                 # date issued
#                 try:
#                     doc['issued'] = diss.find('dcterms:issued').text
#                 except AttributeError:
#                     pass

#                 # language
#                 try:
#                     doc['lang'] = diss.find('dc:language').text
#                 except AttributeError:
#                     pass

#                 # urns
#                 for dcid in diss.findAll('dc:identifier'):
#                     if dcid.get('xsi:type') == 'urn:nbn':
#                         doc['urn'] = dcid.text
#                         doc['id'] = dcid.text # override id

#                 # transfer (broken urls?)
#                 for transfer in diss.findAll('ddb:transfer'):
#                     if transfer.get('ddb:type') == 'dcterms:URI':
#                         doc['transfer'] = transfer.text

#                 # medium
#                 for medium in diss.findAll('dcterms:medium'):
#                     if medium.get('xsi:type') == 'dcterms:IMT':
#                         doc['mimetype'] = medium.text

#                 doc['filenumber'] = int(diss.find('ddb:filenumber').text)

#                 doc['filesize'] = 0 # adding up
#                 for fileprop in diss.findAll('ddb:fileproperties'):
#                     try:
#                         doc['filesize'] += int(fileprop.get('ddb:filesize', 0))
#                         doc['filename'].append(fileprop.get('ddb:filename'))
#                     except ValueError:
#                         self.logger.warn("Empty/wrong filesize in {0}".format(doc['id']))

#                 for ispartof in diss.findAll('dcterms:ispartof'):
#                     if ispartof.get('xsi:type') == 'ddb:noScheme':
#                         doc['ispartof_title'].append(ispartof.text)
#                     if ispartof.get('xsi:type') == 'ddb:ISSN':
#                         doc['ispartof_issn'].append(ispartof.text)
#                     if ispartof.get('xsi:type') == 'ddb:URI':
#                         doc['ispartof_uri'].append(ispartof.text.replace(
#                                                    'http://nbn-resolving.de/', ''))

#                 docs.append(doc)

#         # dump as json, one document per line
#         with self.output().open('w') as output:
#             for doc in docs:
#                 output.write(json.dumps(doc))
#                 output.write('\n')

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='json'))

# class QucosaMarcChunk(QucosaTask):
#     """ Convert a jsonified qucosa XML into MARC.
#     TODO: use external standalone python script as asset. """

#     begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
#     end = ClosestDateParameter(default=datetime.date.today())

#     def requires(self):
#         return QucosaJsonChunk(begin=self.begin, end=self.end)

#     @timed
#     def run(self):
#         records = []
#         with self.input().open() as handle:
#             for line in handle:
#                 record = marcx.FatRecord(to_unicode=True, force_utf8=True)
#                 doc = json.loads(line)

#                 record.add('001', doc['id'])

#                 if 'lang' in doc:
#                     record.add('041', a=doc['lang'])

#                 if 'issued' in doc:
#                     deof = datetime.datetime.strptime(doc['issued'], '%Y-%m-%d').strftime('%y%m%d')
#                     year = datetime.datetime.strptime(doc['issued'], '%Y-%m-%d').strftime('%Y')
#                     record.add('008',
#                                data='{deof}s{year}    xx{skip}{lang}'.format(
#                                deof=deof, year=year, skip=18 * ' ',
#                                lang=doc.get('lang', '   ')))

#                 for i, name in enumerate(doc.get('creator', [])):
#                     if i == 0:
#                         record.add('100', a=name, e='author')
#                         continue
#                     record.add('700', a=name, e='author')

#                 for contributor in doc.get('contributor', []):
#                     record.add('700', a=contributor.get('name'),
#                                e=contributor.get('role'))

#                 if 'title' in doc:
#                     if 'alt' in doc:
#                         record.add('245', a=doc['title'], b=doc['alt'])
#                     else:
#                         record.add('245', a=doc['title'])

#                 for publisher in doc.get('publisher', []):
#                     record.add('260', b=publisher)

#                 if 'issued' in doc:
#                     year = datetime.datetime.strptime(doc['issued'], '%Y-%m-%d').strftime('%Y')
#                     record.add('260', c=year, _9=doc['issued'])

#                 if 'source' in doc:
#                     record.add('490', a=doc['source'])

#                 for toc in doc.get('toc', []):
#                     hashed = hashlib.sha1(toc.encode('utf-8')).hexdigest()
#                     record.add('520', indicators='2 ', _9=hashed)

#                 for abstract in doc.get('abstract', []):
#                     hashed = hashlib.sha1(abstract.encode('utf-8')).hexdigest()
#                     record.add('520', indicators='3 ', _9=hashed)

#                 if 'pubtype' in doc:
#                     record.add('500', a=doc['pubtype'])

#                 if 'url' in doc:
#                     record.add('856', indicators='40', u=doc['url'])

#                 if 'transfer' in doc:
#                     subs = collections.defaultdict(list)
#                     if 'mimetype' in doc:
#                         subs['q'] = doc.get('mimetype')
#                     if 'filesize' in doc and doc.get('filesize') > 0:
#                         subs['s'] = str(doc.get('filesize'))
#                     for fn in doc.get('filename', [])[:9]:
#                         if doc.get('fn'):
#                             subs['f'].append(doc.get(fn))

#                     if doc.get('filenumber') > 1:
#                         filenames = sorted(doc.get('filename'), reverse=True,
#                                            key=lambda fn: fn.endswith('.pdf'))
#                         fnstr = ', '.join(filenames[:3])
#                         if len(filenames) > 0:
#                             fnstr += ', ...'
#                         subs['_3'] = '%s (%s files: %s)' % (doc['transfer'],
#                                      doc['filenumber'], fnstr)

#                     subs['u'] = doc['transfer']
#                     record.add('856', indicators='41', **subs)

#                 for subject in doc.get('xMetaDiss:noScheme', []):
#                     if subject is not None:
#                         record.add('650', indicators=' 4', a=subject)

#                 for subject in doc.get('dcterms:DDC', []):
#                     record.add('082', indicators='0 ', a=subject)

#                 for subject in doc.get('xMetaDiss:RVK', []):
#                     record.add('084', indicators='  ', _2='rvk', a=subject)

#                 if 'ispartof_uri' in doc:
#                     subs = {}
#                     if 'ispartof_title' in doc:
#                         parts = doc.get('ispartof_title')[0].split(';')
#                         if len(parts) > 1:
#                             t = ';'.join(parts[:-1]).strip()
#                             v = parts[-1]
#                             if t and v:
#                                 subs['t'] = t
#                                 subs['v'] = v
#                         else:
#                             subs['a'] = doc.get('ispartof_title')[0]
#                     if 'ispartof_issn':
#                         subs['x'] = doc.get('ispartof_issn')[0]
#                     subs['_6'] = doc.get('ispartof_uri')[0]
#                     record.add('830', indicators=' 0', **subs)

#                 records.append(record)

#         with self.output().open('w') as output:
#             writer = pymarc.MARCWriter(output)
#             for record in records:
#                 writer.write(record)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='mrc'))

# class QucosaCombine(QucosaTask):
#     """ Harvest qucosa, cut all dates to the first of month. Example:
#     2013-05-12 .. 2013-09-11 would be translated to
#     2013-05-01 .. 2013-09-01"""

#     begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())

#     def requires(self):
#         """ Only require up to the last full month. """
#         date = self.closest()
#         if date < self.begin:
#             raise RuntimeError('invalid range: %s - %s' % (self.begin, self.date))
#         dates = date_range(self.begin, date, 1, 'months')
#         for i, _ in enumerate(dates[:-1]):
#             yield QucosaMarcChunk(begin=dates[i], end=dates[i + 1])

#     @timed
#     def run(self):
#         _, combined = tempfile.mkstemp(prefix='tasktree-')
#         for target in self.input():
#             shellout("cat {input} >> {output}", input=target.path,
#                      output=combined)
#         output = shellout("marcuniq -o {output} {input}", input=combined)
#         luigi.File(output).move(self.output().path)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='mrc'))

# class QucosaJson(QucosaTask):
#     """ Convert to JSON. """

#     begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())

#     def requires(self):
#         return QucosaCombine(begin=self.begin, date=self.date)

#     def run(self):
#         output = shellout("marctojson -m date={date} {input} > {output}",
#                           input=self.input().path, date=self.date)
#         luigi.File(output).move(self.output().path)

#     def output(self):
#         return luigi.LocalTarget(path=self.path(ext='json'))

# class QucosaIndex(QucosaTask, CopyToIndex):
#     begin = luigi.DateParameter(default=datetime.date(2010, 1, 1))
#     date = ClosestDateParameter(default=datetime.date.today())

#     index = 'qucosa'
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
#         return QucosaJson(begin=self.begin, date=self.date)
