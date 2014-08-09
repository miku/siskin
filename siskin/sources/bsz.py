# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301

"""
BSZ.

Configuration keys:

[bsz]

# database connection to mddb3
mddb3-url = mysql://username@password@host/db

# database connection to liberocache
liberocache-url = mysql://username@password@host/db

# glob to match Sonderabzug files (repeatable, seperate by comma)
sa-pattern = /path/to/sa/files/SA-MARC-010_finc_*1403*tar.gz

# the date under which the Sonderabzug is filed
sa-date = 2014-03-04

# must contain a {date} placeholder
ta-pattern = /path/to/TA-MARC-010_finc-{date}.tar.gz
ta-datefmt = %y%m%d

# filenames inside the daily shipments
ta-authority-filename = 010_finc-aut.mrc
ta-local-filename = 010_finc-lok.mrc
ta-title-filename = 010_finc-tit.mrc

# list of dates with no daily updates
mute = 2013-10-26, 2013-10-27, 2013-09-27, 2013-09-28, 2013-09-29, 2013-08-30,
       2013-08-31, 2013-09-01, 2013-11-23, 2013-11-30, 2013-12-01, 2013-12-06,
       2013-12-07, 2013-12-28, 2013-12-30, 2013-12-31, 2014-01-01, 2014-01-31,
       2014-03-26

# must contain {date} placeholder
loeppn-pattern = /path/to/LOEPPN-{date}

# the date format
loeppn-datefmt = %y%m%d

# regular expression to match the date (maybe obsolete)
leoppn-datepattern = [0-9]{6}

"""

from __future__ import print_function
from elasticsearch import helpers as eshelpers
from gluish.benchmark import timed, Timer
from gluish.colors import dim, red, green
from gluish.common import Executable
from gluish.database import sqlite3db, mysqldb
from gluish.format import TSV
from gluish.parameter import ILNParameter
from gluish.path import copyregions
from gluish.utils import shellout, memoize, random_string, nwise
from luigi import date_interval
from siskin.task import DefaultTask
from siskin.configuration import Config
import collections
import datetime
import difflib
import elasticsearch
import glob
import luigi
import operator
import os
import pandas as pd
import pymarc
import re
import shelve
import string
import tarfile
import tempfile
import urllib

try:
    import simplejson as json
except ImportError:
    import json

config = Config.instance()
SeekInfo = collections.namedtuple('SeekInfo', ['offset', 'length'])

# ==============================================================================
#
# Enhanced BSZ Task
#
# ==============================================================================

class BSZTask(DefaultTask):
    TAG = '000'

    # date of the last Sonderabzug
    SONDERABZUG = config.getdate('bsz', 'sa-date', Config.NO_DEFAULT)

    def muted(self):
        """
        Return a set of muted dates.
        """
        mute_value = config.get('bsz', 'mute', '')
        try:
            vals = mute_value.split(',')
            return set([datetime.date(*map(int, s.split('-'))) for s in vals])
        except ValueError:
            raise RuntimeError('failed to parse mutes: {0}'.format(mute_value))

    @memoize
    def mappings(self, filename='/etc/siskin/mappings.json'):
        """
        TODO: memoize must handle kwargs as well!
        """
        if not os.path.exists(filename):
            raise RuntimeError('{0} required'.format(filename))
        with open(filename) as handle:
            content = json.loads(handle.read())
        return content

    @memoize
    def finc_ilns(self):
        """
        Return a set of ILNs that are part of FINC.
        """
        ilns = self.mappings().get('isil_iln').values()
        return set([value.zfill(4) for value in ilns if value.isdigit()])

    def url_for_ppn(self, ppn, iln='0010'):
        """
        Return the search URL for a given PPN and ILN.
        """
        iln = iln.zfill(4)
        params = {'lookfor': 'record_id:%s' % ppn}
        return os.path.join(self.mappings().get('iln_live').get(iln), 'Search',
                            'Results?%s' % urllib.urlencode(params))

# ==============================================================================
#
# SONDERABZUG
#
# ==============================================================================

class SAOriginalFileList(BSZTask):
    """
    Sonderabzug (SA) - provided by BSZ/BW. More information:

    * https://wiki.bsz-bw.de/doku.php?id=v-team:daten:datendienste
    * https://wiki.bsz-bw.de/doku.php?id=v-team:daten:datendienste:sonder

    Fix the files of an Sonderabzug.

    A single glob to find Sonderabzug files. Configure this in
    `bsz.sa-pattern`. Example: `/var/swb-mirror/010/SA-MARC-10_finc*`.
    """
    patterns = luigi.Parameter(default=config.get('bsz', 'sa-pattern', Config.NO_DEFAULT))

    @timed
    def run(self):
        patterns = map(string.strip, self.patterns.split(','))
        with self.output().open('w') as output:
            for pattern in patterns:
                pathlist = sorted(glob.glob(pattern))
                if len(pathlist) == 0:
                    raise RuntimeError('No SA w/ glob: %s' % self.patterns)
                for path in pathlist:
                    output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(digest=True), format=TSV)

class SASync(BSZTask):
    """
    Make the Sonderabzug files available outside probably external fs,
    such as NFS, which has a tendency to stall.
    """
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SAOriginalFileList()

    @timed
    def run(self):
        copied = []
        with self.input().open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('path',))):
                dst = os.path.join(os.path.dirname(self.output().path),
                                   '{0}-{1:02d}.tar.gz'.format(self.date, i))
                luigi.File(path=row.path).copy(dst)
                copied.append(dst)

        with self.output().open('w') as output:
            for path in copied:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SAImport(BSZTask):
    """
    Extract all Sonderabzug tarballs. This will only be considered
    successful, if all files can be extracted in one go!
    Each tarball gets extracted in its own directory (shard).
    """
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SASync(date=self.date)

    @timed
    def run(self):
        extracted = []
        target = os.path.join(os.path.dirname(self.output().path),
                              str(self.date))

        with self.input().open() as handle:
            for i, row in enumerate(handle.iter_tsv(cols=('path',))):
                shard = os.path.join(target, '{0:02d}'.format(i))
                if not os.path.exists(shard):
                    os.makedirs(shard)
                tar = tarfile.open(row.path)
                tar.extractall(path=shard)
                for name in tar.getnames():
                    extracted.append(os.path.join(shard, name))
                tar.close()

        with self.output().open('w') as output:
            for path in extracted:
                output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SATags(BSZTask):
    """
    The tag is just the name of the directory, where the extracted
    marc files reside.
    """
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SAImport(date=self.date)

    @timed
    def run(self):
        tags = set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                tags.add(row.path.split('/')[-2])

        with self.output().open('w') as output:
            for tag in sorted(tags):
                output.write_tsv(tag)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SAFile(BSZTask):
    """
    This task does not create a new file, it only symlinks to an existing
    file from the `SAImport`.
    """
    tag = luigi.IntParameter(default=0)
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.Parameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SAImport(date=self.date)

    @timed
    def run(self):
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('path',)):
                shard, filename = row.path.split('/')[-2:]
                if int(shard) == int(self.tag):
                    if filename.endswith('{0}.mrc'.format(self.kind)):
                        target = os.path.dirname(self.output().path)
                        if not os.path.exists(target):
                            os.makedirs(target)
                        os.symlink(row.path, self.output().path)
                        break
            else:
                raise RuntimeError('no file for {tag} and {kind}'.format(
                                   tag=int(self.tag), kind=self.kind))

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SATransactionTagListRange(BSZTask):
    """
    Report (001, 005, tag) for a kind. Sorted by ID, then time.
    """
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.Parameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        prerequisite = SATags()
        luigi.build([prerequisite])
        tags = prerequisite.output().open().read().split()
        taskmap = {}
        for tag in tags:
            taskmap[tag] = SAFile(tag=int(tag), kind=self.kind, date=self.date)
        return taskmap

    @timed
    def run(self):
        # equivalent, more readable, a bit more robust, but four times slower:
        # `marctotsv {input} 001 005 {tag} > {output}`
        template = """
            yaz-marcdump {input} |
            awk '/^001 / {{printf $2"\t"}};
                 /^005 / {{print $2"\t{tag}"}}' > {output}
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for tag, target in self.input().iteritems():
            tmp = shellout(template, input=target.path, tag=tag,
                           preserve_whitespace=True)
            shellout("cat {input} >> {output}", input=tmp, output=stopover)

        output = shellout("sort -k1,1 -k2,2 {input} > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SATransactionTagSurface(BSZTask):
    """
    Will output a two column TSV with the <id> and the <tag> of the file,
    which contains the last modification of the <id>.
    """
    kind = luigi.Parameter(default='tit', description='one of tit, lok or aut')
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SATransactionTagListRange(kind=self.kind, date=self.date)

    @timed
    def run(self):
        """ c.f. http://unix.stackexchange.com/q/138072/376 """
        output = shellout("tac {input} | uniq -w 9 | cut -f 1,3 > {output}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SATransactionSingleTag(BSZTask):
    """
    Get the deduplicated tag list, that contains only entries for a single
    given tag. This list will only contain the IDs for a given tag,
    that were last modified in that tag.
    """
    tag = luigi.Parameter()
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SATransactionTagSurface(kind=self.kind, date=self.date)

    @timed
    def run(self):
        """ 1 awk < 5 Pandas < 10 python :) """
        output = shellout("awk '$2=={tag} {{print $0}}' {input} > {output}",
                          input=self.input().path, tag=self.tag)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SAIdSeekMapDB(BSZTask):
    """
    Create a sqlite3 db with (id, offset, length) entries for a MARC file.
    """
    tag = luigi.Parameter()
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return SAFile(tag=self.tag, kind=self.kind, date=self.date)

    @timed
    def run(self):
        output = shellout("marcmap -o {output} {input}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SARegionalCopy(BSZTask):
    """
    Copy only regions of a file.

    The inputs are:

        * An sqlite3 `db`, that contains the offsets and lengths of
          the records in the file given by `file`.
        * The `file` is just the raw MARC file.
        * The `idlist` contains the IDs that need to be copied out of this file.

    This weird looking approach is a lot faster than a parsing and filtering,
    because no MARC parsing is needed at all! Just seeks and reads.
    """
    tag = luigi.Parameter()
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        return {
            'db': SAIdSeekMapDB(tag=self.tag, kind=self.kind),
            'file': SAFile(tag=self.tag, kind=self.kind),
            'idlist': SATransactionSingleTag(tag=self.tag, kind=self.kind)
        }

    @timed
    def run(self):
        with sqlite3db(self.input().get('db').fn) as cursor:
            with self.input().get('idlist').open() as fh:
                idset = set(r.id for r in fh.iter_tsv(cols=('id', 'X')))
            # todo: sqli!
            cursor.execute("""
                SELECT offset, length
                FROM seekmap WHERE id IN (%s)
            """ % (','.join([ "'%s'" % id for id in idset])))
            rows = cursor.fetchall()

        with self.input().get('file').open() as handle:
            with self.output().open('w') as output:
                copyregions(handle, output, rows)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class SAMerged(BSZTask):
    """
    Collect the *filtered* files for a kind and cat'em.
    """
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    date = luigi.DateParameter(default=BSZTask.SONDERABZUG)

    def requires(self):
        """ TODO: keep an eye on https://github.com/spotify/luigi/pull/255 """
        prerequisite = SATags()
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('tag',)):
                yield SARegionalCopy(tag=row.tag, kind=self.kind, date=self.date)

    @timed
    def run(self):
        _, merged = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.fn, output=merged)
        luigi.File(merged).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

# ==============================================================================
#
# TAGESABZUG
#
# ==============================================================================

def create_empty_daily_update():
    """ Create an empty daily updated. Used with the muted dates. """
    _, dummy = tempfile.mkstemp(prefix='siskin-')
    handle = tarfile.open(dummy, 'w:gz')
    _, empty = tempfile.mkstemp(prefix='siskin-')
    handle.add(empty, config.get('bsz', 'ta-title-filename'))
    handle.add(empty, config.get('bsz', 'ta-local-filename'))
    handle.add(empty, config.get('bsz', 'ta-authority-filename'))
    handle.close()
    return dummy

class TASync(BSZTask):
    """ Copy from mirror to sync directory.

    If the Tagesupdate is missing, ask the user what to do:

    * mute this error by creating an dummy (empty) file
    * do nothing; all depending tasks will fail!

    or, if date is in `bsz.mute` then 'mute' the file automatically. """
    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        """ TODO: too long. """
        fmt = config.get('bsz', 'ta-datefmt')
        src = config.get('bsz', 'ta-pattern').format(date=self.date.strftime(fmt))
        if not os.path.exists(src):
            if self.date in self.muted():
                self.logger.debug("{0} is muted by config".format(self.date))
                path = create_empty_daily_update()
                luigi.File(path=path).move(self.output().path)
            else:
                raise RuntimeError("No Tagesupdate (yet?) for {0} at {1}".format(self.date, src))
        else:
            self.logger.info("Syncing TA from {0}".format(src))
            luigi.File(path=src).copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='tar.gz'))

class TAImport(BSZTask):
    """ Import the TA for a single kind. """
    date = luigi.DateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit', description='tit, lok, aut')

    def requires(self):
        return TASync(date=self.date)

    @timed
    def run(self):
        filenames = {
            'tit': config.get('bsz', 'ta-title-filename'),
            'lok': config.get('bsz', 'ta-local-filename'),
            'aut': config.get('bsz', 'ta-authority-filename'),
        }
        output = shellout("tar -O -zf {input} -x {filename} > {output}",
                          filename=filenames.get(self.kind),
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='mrc'))

class GenericImport(luigi.WrapperTask):
    """ This one is *special*. You do not need to know anything about SA or TA,
    when you use this task, just the date and the the kind of file and this
    task takes care of the gory details.

    Access raw marc data from BSZ for a date and kind. """
    date = luigi.DateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit')

    def requires(self):
        if self.date == BSZTask.SONDERABZUG:
            return SAMerged(kind=self.kind, date=BSZTask.SONDERABZUG)
        elif self.date > BSZTask.SONDERABZUG:
            return TAImport(kind=self.kind, date=self.date)
        else:
            raise RuntimeError('date lies before the last SONDERABZUG')

    def output(self):
        return self.input()

# ==============================================================================
#
# DELETIONS
#
# ==============================================================================

class DeletionPaths(BSZTask):
    """
    Just return a file with all the deletion file paths.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        """ TODO: star-date is a hack. """
        pattern = config.get('bsz', 'loeppn-pattern').format(date='*')
        with self.output().open('w') as output:
            for path in glob.glob(pattern):
                if not os.path.islink(path):
                    output.write_tsv(path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DeletionDates(BSZTask):
    """
    Dump a file with a list of dates, on which deletions occured.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return DeletionPaths()

    @timed
    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('path',)):
                    pattern = config.get('bsz', 'loeppn-datepattern')
                    match = re.search(pattern, row.path)
                    if match:
                        loeppn_datefmt = config.get('bsz', 'loeppn-datefmt')
                        date = datetime.datetime.strptime(match.group(),
                                                          loeppn_datefmt).date()
                        output.write_tsv(date.strftime('%Y-%m-%d'))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DeletionImport(BSZTask):
    """
    Copy from mirror to sync directory.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    @timed
    def run(self):
        path = config.get('bsz', 'loeppn-pattern').format(
            date=self.date.strftime(config.get('bsz', 'loeppn-datefmt')))
        if not os.path.exists(path):
            raise Exception('No deletions on %s' % (self.date))
        luigi.File(path=path).copy(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class DeletionParsed(BSZTask):
    """
    Generate a list of ID, ILN for a given date when a deletion occured.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    dtype = luigi.Parameter(default='9')

    def requires(self):
        return DeletionImport(date=self.date)

    @timed
    def run(self):
        with self.input().open('r') as handle:
            with self.output().open('w') as output:
                for line in handle:
                    line = line.strip()
                    d_type, xpn, iln = line[11:12], line[12:21], line[21:25]
                    if d_type == self.dtype and iln:
                        output.write_tsv(xpn, iln, self.date)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DeletionRange(BSZTask):
    """
    Merged and sorted and deduplicated. Yes, deduplicated :)

        $ taskcat DeletionImport --date 2013-09-26 | grep 766798690
        1327013450397667986900006
        1327013450397667986900006
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    dtype = luigi.Parameter(default='9')

    def requires(self):
        prerequisite = DeletionDates()
        luigi.build([prerequisite])
        with prerequisite.output().open() as handle:
            for row in handle.iter_tsv(cols=('date',)):
                date = datetime.date(*map(int, row.date.split('-')))
                if self.begin <= date < self.end:
                    yield DeletionParsed(date=date, dtype=self.dtype)

    @timed
    def run(self):
        """
        Concatenate, sort, deduplicate.
        """
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path,
                     output=stopover)
        # TODO: do we need to sort here?
        output = shellout("sort -k1,1 -k2,2 {input} | uniq > {output}",
                          input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class DeletionRangeILN(BSZTask):
    """
    Deletions relevant for an ILN.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    dtype = luigi.Parameter(default='9')
    iln = ILNParameter(default='0010')

    def requires(self):
        return DeletionRange(begin=self.begin, end=self.end, dtype=self.dtype)

    @timed
    def run(self):
        output = shellout("""LANG=C awk -F '\\t' '$2=="{iln}" {{print $0}}' {input} > {output}""", input=self.input().path, iln=self.iln)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)


class DeletionRangeFinc(BSZTask):
    """
    Only keep the deletions, that are relevant to FINC.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    dtype = luigi.Parameter(default='9')

    def requires(self):
        return DeletionRange(begin=self.begin, end=self.end, dtype=self.dtype)

    @timed
    def run(self):
        with self.input().open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('epn', 'iln', 'date')):
                    if row.iln.zfill(4) in self.finc_ilns():
                        output.write_tsv(*row)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

# ==============================================================================
#
# LIBERO/MDDB3 Dumps
#
# ==============================================================================

class BSZDumpTask(BSZTask):
    """ Data transfer tasks. """
    def on_success(self):
        if config.getboolean('core', 'ambience', False):
            self._ambience(kind='complete')

    def on_failure(self, exception):
        import MySQLdb
        if config.getboolean('core', 'ambience', False):
            if isinstance(exception, MySQLdb.OperationalError):
                errno, errmsg = exception.args
                if errno == 2003:
                    self._ambience(kind='unable_to_comply')
                elif errno == 1045:
                    self._ambience(kind='access_denied')
                elif errno == 2013:
                    self._ambience(kind='critical')
                else:
                    self._ambience(kind='deny')
            else:
                self._ambience(kind='deny')
        raise exception

class LiberoCacheDump(BSZDumpTask):
    """
    Dump the liberocache database locally for faster access (per ILN).
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    @timed
    def run(self):
        db_name = self.mappings().get('iln_libero_id').get(self.iln)
        if db_name is None:
            raise RuntimeError("no mapping found from ILN to LIBERO-ID")

        url = config.get('bsz', 'liberocache-url')
        self.logger.debug("Using LiberoCache via {0}".format(url))
        with mysqldb(url, stream=True) as cursor:
            cursor.execute("""SELECT record_id, content from libero_cache WHERE db_name = '%s' """ % (db_name,))
            _, stopover = tempfile.mkstemp(prefix='siskin-')
            with sqlite3db(stopover) as cc:
                cc.execute("""CREATE TABLE IF NOT EXISTS libero_cache (record_id TEXT PRIMARY KEY, content TEXT)""")
                for i, row in enumerate(cursor):
                    if i % 250000 == 0 and i > 0:
                        cc.connection.commit()
                    cc.execute("""INSERT INTO libero_cache (record_id, content) VALUES (?, ?)""", row)
                cc.connection.commit()
            luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class LiberoCacheCopy(BSZDumpTask):
    """ Complete Libero cache db copy. Takes up to an hour. """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=100000, significant=False)

    @timed
    def run(self):
        url = config.get('bsz', 'liberocache-url')
        with mysqldb(url, stream=True) as cursor:
            # http://bit.ly/1fADENH
            cursor.execute("""SET NET_WRITE_TIMEOUT = 600""")
            cursor.execute("""SELECT record_id, db_name, content FROM libero_cache""")
            _, stopover = tempfile.mkstemp(prefix='siskin-')
            with sqlite3db(stopover) as cc:
                cc.execute("""CREATE TABLE IF NOT EXISTS libero_cache
                              (record_id TEXT, db_name TEXT, content TEXT, PRIMARY KEY (record_id, db_name))""")
                for i, row in enumerate(cursor):
                    if i % self.batch == 0 and i > 0:
                        cc.connection.commit()
                        self.logger.debug("Transferred %s rows." % i)
                    cc.execute("""INSERT INTO libero_cache (record_id, db_name, content) VALUES (?, ?, ?)""", row)
                cc.connection.commit()
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_lc_record_id ON libero_cache (record_id)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_lc_record_id_db_name ON libero_cache (record_id, db_name)""")

            luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class FincMappingDump(BSZDumpTask):
    """
    Copies finc_mapping table from MySQL/mddb3 to a local sqlite3 db.
    Takes about 4-5 minutes including index creation. Get generated per day.
    Old artefacts are removed.

    (Finc Ids should probably get their own little web service?)
    """
    date = luigi.DateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=250000, significant=False)

    def pathlist(self):
        """ Previous artefacts. """
        interval = date_interval.Custom(BSZTask.SONDERABZUG, datetime.date.today())
        tasks = (FincMappingDump(date=date) for date in interval)
        return (t.output().fn for t in tasks)

    @timed
    def run(self):
        url = config.get('bsz', 'mddb3-url')
        with mysqldb(url, stream=True) as cursor:
            cursor.execute("""SELECT finc_id, source_id, record_id, created, status FROM finc_mapping""")
            _, stopover = tempfile.mkstemp(prefix='siskin-')
            with sqlite3db(stopover) as cc:
                cc.execute("""CREATE TABLE IF NOT EXISTS finc_mapping
                              (finc_id INTEGER PRIMARY KEY,
                              source_id INTEGER, record_id TEXT,
                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              status INTEGER)""")
                for i, row in enumerate(cursor):
                    if i % self.batch == 0 and i > 0:
                        cc.connection.commit()
                    cc.execute("""INSERT INTO finc_mapping
                                  (finc_id, source_id, record_id, created_at, status) VALUES (?, ?, ?, ?, ?)""", row)
                cc.connection.commit()

                cc.execute("""CREATE INDEX IF NOT EXISTS idx_fm_finc_id ON finc_mapping (finc_id)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_fm_record_id ON finc_mapping (record_id)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_fm_source_id ON finc_mapping (source_id)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_fm_created_at ON finc_mapping (created_at)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_fm_source_id_record_id ON finc_mapping (source_id, record_id)""")

            luigi.File(stopover).move(self.output().path)

        # remove previous artefacts
        for path in self.pathlist():
            if os.path.exists(path):
                try:
                    os.remove(path)
                    self.logger.debug('Removed stale: %s' % path)
                except OSError as err:
                    self.logger.error(err)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class ISBNDump(BSZDumpTask):
    """ Copies isbns table from MySQL/mddb3 to a local sqlite3 db. """
    date = luigi.DateParameter(default=datetime.date.today())
    batch = luigi.IntParameter(default=250000, significant=False)

    def pathlist(self):
        """ Previous artefacts. """
        interval = date_interval.Custom(BSZTask.SONDERABZUG, self.date)
        tasks = (ISBNDump(date=date) for date in interval)
        return (t.output().path for t in tasks)

    @timed
    def run(self):
        url = config.get('bsz', 'mddb3-url')
        with mysqldb(url, stream=True) as cursor:
            cursor.execute("""SELECT finc_id, isbn, updated FROM isbn""")
            _, stopover = tempfile.mkstemp(prefix='siskin-')
            with sqlite3db(stopover) as cc:
                cc.execute("""CREATE TABLE IF NOT EXISTS isbn (finc_id INTEGER, isbn TEXT,
                              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) """)
                for i, row in enumerate(cursor):
                    if i % self.batch == 0 and i > 0:
                        cc.connection.commit()
                    cc.execute("""INSERT INTO isbn (finc_id, isbn, updated_at) VALUES (?, ?, ?)""", row)
                cc.connection.commit()
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_isbn_finc_id ON isbn (finc_id)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_isbn_isbn ON isbn (isbn)""")
                cc.execute("""CREATE INDEX IF NOT EXISTS idx_isbn_updated_at ON isbn (updated_at)""")

            luigi.File(stopover).move(self.output().fn)

        # remove previous artefacts
        for path in self.pathlist():
            if os.path.exists(path):
                try:
                    os.remove(path)
                    self.logger.debug('Removed stale: %s' % path)
                except OSError as err:
                    self.logger.warn(err)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

# ==============================================================================
#
# History
#
# ==============================================================================

class FieldListWithDate(BSZTask):
    """
    Report 001 and the date of the file for a given kind.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    field = luigi.Parameter(default='001')
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return {'data': GenericImport(kind=self.kind, date=self.date),
                'apps': Executable(name='marctotsv')}

    @timed
    def run(self):
        output = shellout("""marctotsv {input} {field} {date} > {output}""",
                          input=self.input().get('data').path, field=self.field,
                          date=self.date)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class FieldListWithDateMerged(BSZTask):
    """
    Contains (ID, DATE) for all IDs and all dates on which an update has been
    observed. Fast enough. Computing this list for *lokal* data with conversions
    from MARC takes less than 3 minutes with a single process, with 12 workers
    about 2 minutes.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    field = luigi.Parameter(default='001')
    kind = luigi.Parameter(default='tit')

    def requires(self):
        interval = date_interval.Custom(self.begin, self.end)
        for date in interval:
            yield FieldListWithDate(date=date, kind=self.kind, field=self.field)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path,
                     output=stopover)
        output = shellout("sort -k1,1 -k2,2 {input} > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class XPNHistory(BSZTask):
    """
    Store XPN history in a file (in custom JSON format).

    If an ID has a lot of records (100+) this might take a while, since
    all records are extracted into single files.

    Examples:

    * https://gist.github.com/miku/6210652
    * https://gist.github.com/miku/6210644
    * https://gist.github.com/miku/6210154
    * https://gist.github.com/miku/6210144

    """
    id = luigi.Parameter(description='a PPN or EPN')
    kind = luigi.Parameter(default='tit', description='tit, lok or aut')
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return FieldListWithDateMerged(begin=self.begin, end=self.end, kind=self.kind)

    @timed
    def run(self):
        dates, history = set(), []

        with self.input().open('r') as handle:
            for row in handle.iter_tsv(cols=('id', 'date')):
                if row.id == self.id:
                    dates.add(row.date)

        for value in sorted(dates):
            date = datetime.date(*map(int, value.split('-')))
            task = Record(kind=self.kind, date=date, id=self.id)
            luigi.build([task])
            with task.output().open() as handle:
                reader = pymarc.MARCReader(handle)
                record = reader.next()
                history.append({'date': value,
                                'id': self.id,
                                'kind': self.kind,
                                'printable': '%s\n' % record})

        with self.output().open('w') as output:
            json.dump(history, output, indent=4)

    def output(self):
        return luigi.LocalTarget(path=self.path())

class XPNDiff(BSZTask):
    """
    Show the changes in PPN as a list of unified diff between dates.
    """
    id = luigi.Parameter(description='A PPN or EPN.')
    kind = luigi.Parameter(default='tit')
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return XPNHistory(id=self.id, kind=self.kind, begin=self.begin,
                          end=self.end)

    @timed
    def run(self):
        with self.input().open() as handle:
            history = json.load(handle)

        if len(history) == 0:
            print('This ID left no trace in our system '
                  'between %s and %s.\n' % (self.begin, self.end))
            return

        first = history[0]

        print(dim('# Original as of %s' % first['date']))
        print(first['printable'])

        for i, _ in enumerate(history[1:], start=1):
            previous, current = history[i - 1], history[i]

            print(dim('# Diff between version %s and %s' % (
                  previous['date'], current['date'])))

            diff = difflib.unified_diff(
                previous['printable'].split('\n'),
                current['printable'].split('\n'), lineterm='')

            difflist = list(diff)
            for line in difflist:
                if line.startswith('-'):
                    line = red(line)
                if line.startswith('+'):
                    line = green(line)
                print(line)

    def complete(self):
        return False

# ==============================================================================
#
# MAIN tasks
#
# ==============================================================================

class ListifyLocal(BSZTask):
    """
    Create a TSV version of local data, for faster, easier access. The columns:

    PPN(004) EPN(001) SIGEL(852.a) Transaction(005) FILEDATE ILN
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return GenericImport(date=self.date, kind='lok')

    @timed
    def run(self):
        output = shellout("""marctotsv {input} 004 001 852.a 005 > {output}""",
                          input=self.input().path)
        target = luigi.LocalTarget(output, format=TSV)
        isil_iln_map = self.mappings().get('isil_iln')

        with target.open() as handle:
            with self.output().open('w') as output:
                for row in handle.iter_tsv(cols=('ppn', 'epn', 'sigel', 'date')):
                    iln = isil_iln_map.get(row.sigel, 'NA')
                    if iln.isdigit():
                        iln = iln.zfill(4)
                    output.write_tsv(row.ppn, row.epn, row.sigel, row.date,
                                     self.date, iln)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class ListifyLocalRange(BSZTask):
    """
    Merge all

        PPN(004) EPN(001) SIGEL(852.a) Transaction(005) FILEDATE ILN

    files from `begin` to `end`.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        interval = date_interval.Custom(self.begin, self.end)
        return [ListifyLocal(date=date) for date in interval]

    def pathlist(self):
        """ Previous artefacts of this task. """
        interval = date_interval.Custom(BSZTask.SONDERABZUG, datetime.date.today())
        tasks = [ListifyLocalRange(begin=BSZTask.SONDERABZUG, end=date)
                 for date in interval]
        return [t.output().path for t in tasks]

    @timed
    def run(self):
        for path in self.pathlist():
            if os.path.exists(path):
                try:
                    os.remove(path)
                    self.logger.debug('Removed stale: %s' % path)
                except OSError as err:
                    self.logger.error(err)

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

# Seekmaps
# ========
#
# Seekmaps are lists of (id, offset, length) tuples. They can be used
# to cut into MARC files and access single records without iterating through
# the file.

class SeekmapDB(BSZTask):
    """
    Create an ID OFFSET LENGTH map of the the marc file.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return GenericImport(kind=self.kind, date=self.date)

    @timed
    def run(self):
        output = shellout("marcmap -o {output} {input}", input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(ext='db'))

class Record(BSZTask):
    """
    Dump the record given a date and a ID. Used by history task.
    TODO: This could be made much faster by using some database or index
    as storage.
    """
    id = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return {'seekmap-db': SeekmapDB(date=self.date, kind=self.kind),
                'file': GenericImport(date=self.date, kind=self.kind)}

    @timed
    def run(self):
        with sqlite3db(self.input().get('seekmap-db').path) as cursor:
            cursor.execute("SELECT offset, length FROM seekmap WHERE id = ?",
                           (self.id,))
            result = cursor.fetchone()
        if result is None:
            raise Exception('ID not found in seekmap db: %s' % self.id)
        seekinfo = SeekInfo(*result)

        with self.input().get('file').open() as handle:
            handle.seek(seekinfo.offset)
            with self.output().open('w') as output:
                output.write(handle.read(seekinfo.length))

    def output(self):
        """ Shard paths, just in case we extract too many of those. """
        return luigi.LocalTarget(path=self.path(shard=True))

class LocalUpdatesILN(BSZTask):
    """
    Get the local additions and updates for a given ILN.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        return ListifyLocalRange(begin=self.begin, end=self.end)

    @timed
    def run(self):
        output = shellout("""LANG=C awk -F '\\t' '$6=="{iln}" {{print $0}}' {input} > {output}""",
                          input=self.input().path, iln=self.iln)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class LocalUpdatesISIL(BSZTask):
    """
    Get the local additions and updates for a given ISIL.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    isil = luigi.Parameter(default='DE-15')

    def requires(self):
        return ListifyLocalRange(begin=self.begin, end=self.end)

    @timed
    def run(self):
        output = shellout("""LANG=C awk -F '\\t' '$3=="{isil}" {{print $0}}' {input} > {output}""", input=self.input().path, isil=self.isil)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class Events(BSZTask):
    """ Testing a new fast clean Event implementation. """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        return {'updates': LocalUpdatesILN(begin=self.begin, end=self.end, iln=self.iln),
                'deletions': DeletionRangeILN(begin=self.begin, end=self.end, iln=self.iln)}

    def run(self):
        Event = collections.namedtuple('Event', ['date', 'type', 'prio', 'ppn', 'isil'])
        epn_events = collections.defaultdict(set)
        PRIO = {'deletion': 0, 'update': 10} # lower means higher

        # Deletions
        with self.input().get('deletions').open() as handle:
            for row in handle.iter_tsv(cols=('epn', 'iln', 'date')):
                epn_events[row.epn].add(Event(*(row.date, 'D', PRIO['deletion'], 'NO_PPN', 'NO_SIGEL')))

        # Additions
        with self.input().get('updates').open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'epn', 'isil', 0, 'date', 'iln')):
                epn_events[row.epn].add(Event(*(row.date, 'U', PRIO['update'], row.ppn, row.isil)))

        with self.output().open('w') as handle:
            for epn, events in sorted(epn_events.iteritems()):
                # sort events by date and prio
                for e in sorted(events, key=operator.itemgetter(0, 2)):
                    handle.write_tsv(epn, e.date, e.type, e.prio, e.ppn, e.isil)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SnapshotBasic(BSZTask):
    """ Process the event list and find the current "state of the onion".
    The final output is sorted by PPN and Date. It is *basic*, because it does
    not take into account any *Lokalsystem* information. """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        return Events(begin=self.begin, end=self.end, iln=self.iln)

    @timed
    def run(self):
        """
        Use a dict. Overwrite values if keys have newer entries, delete
        keys, if deletions are seen. At the end, the dictionary should only
        contain the current (EPN (PPN DATE)) tuples.
        """
        epn_map, epn_misses = dict(), set()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('epn', 'date', 'type',
                                             'prio', 'ppn', 'sigel')):
                if row.type == 'U':
                    epn_map[row.epn] = (row.ppn, row.date, row.sigel)
                elif row.type == 'D':
                    try:
                        del epn_map[row.epn]
                    except KeyError as err:
                        epn_misses.add(row.epn)
                else:
                    raise ValueError('Only [U]pdate and [D]elete types known.')
        self.logger.debug("The %s deletions that weren't there (ex: %s)" % (
                     len(epn_misses), list(epn_misses)[:3]))

        _, stopover = tempfile.mkstemp(prefix='siskin-')
        with luigi.File(stopover, format=TSV).open('w') as output:
            for epn, (ppn, date, sigel) in epn_map.iteritems():
                output.write_tsv(ppn, epn, date, sigel)
        output = shellout("sort -k1,1 -k3,3 {input} > {output}", input=stopover)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class OpacDisplayFlag(BSZTask):
    """
    Dump the record id and the OPAC DISPLAY FLAG for a single ILN to a file.
    There are two possible values for the ODF:

        * 0: regular item, should be displayed
        * 1: suppressed for whatever reason, should not be displayed

    And three values for the comment in the third column:
        * OK: item is in SWB and Cache
        * CACHE_ONLY: this PPN appears in the CACHE but not in the SWB
        * SNAPSHOT_ONLY: this PPN appears on in SWB but not in the cache
                         (e.g. Ebooks)
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        # there can be only a current LC dump, so just use SONDERABZUG and today
        return {
            'dump': LiberoCacheDump(iln=self.iln),
            'snapshot': SnapshotBasic(begin=self.begin, end=self.end, iln=self.iln)
        }

    @timed
    def run(self):
        ppns = set()
        with self.input().get('snapshot').open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'epn', 'date', 'X')):
                ppns.add(row.ppn)

        cached_ppns = set()
        with self.output().open('w') as output:
            with sqlite3db(self.input().get('dump').path) as cursor:
                cursor.execute('SELECT record_id, content FROM libero_cache')
                rows = cursor.fetchall()
                for row in rows:
                    record_id, blob = row[0], json.loads(row[1], 'latin-1')
                    cached_ppns.add(record_id)
                    if blob.get('errorcode') == 0:
                        odf = blob.get('getTitleInformation', {}).get(
                                       record_id, {}).get(
                                       'opac_display_flag', 0)
                        if record_id in ppns:
                            output.write_tsv(record_id, odf, "OK")
                        else:
                            output.write_tsv(record_id, odf, "CACHE_ONLY")

            # write out those ids, that are in the snapshot,
            # but not in the cache
            for uncached in ppns.difference(cached_ppns):
                output.write_tsv(uncached, 0, 'SNAPSHOT_ONLY')

        # the following is just informative and could be omitted
        self.logger.debug(json.dumps({
            'in snapshot': len(ppns),
            'in cache': len(cached_ppns),
            'in cache, but not in snapshot': {
                'size': len(cached_ppns - ppns),
                'sample': [{'ppn': ppn, 'url': self.url_for_ppn(ppn, iln=self.iln)}
                           for ppn in list(cached_ppns - ppns)[:5]]
            },
            'in snapshot, but not in cache': {
                'size': len(ppns - cached_ppns),
                'sample': [{'ppn': ppn, 'url': self.url_for_ppn(ppn, iln=self.iln)}
                           for ppn in list(ppns - cached_ppns)[:5]]
            },
            'in both': len(ppns & cached_ppns),
        }, indent=4))

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SnapshotWithCache(BSZTask):
    """
    Snapshot with ODF processed.
    Only the visible ones remain (ODF != 1).
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        return {
            'snapshot': SnapshotBasic(begin=self.begin, end=self.end, iln=self.iln),
            'odf': OpacDisplayFlag(begin=self.begin, end=self.end, iln=self.iln)
        }

    @timed
    def run(self):
        with self.input().get('snapshot').open() as handle:
            snapshot = pd.read_csv(handle, sep='\t',
                                   names=('ppn', 'epn', 'date', 'sigel'),
                                   dtype={'ppn': pd.np.str, 'epn': pd.np.str})
        with self.input().get('odf').open() as handle:
            odflist = pd.read_csv(handle, sep='\t',
                                  names=('ppn', 'odf', 'comment'),
                                  dtype={'ppn': pd.np.str, 'odf': pd.np.str})

        merged = pd.merge(snapshot, odflist, how='left')
        active = merged[merged.odf == "0"]
        with self.output().open('w') as output:
            active.to_csv(output, columns=('ppn', 'epn', 'date', 'odf', 'comment'),
                          sep='\t', index=False, header=False)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SnapshotPPNList(BSZTask):
    """
    Just a list of PPNs for the current Snapshot.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    iln = ILNParameter(default='0010')

    def requires(self):
        return SnapshotWithCache(begin=self.begin, end=self.end, iln=self.iln)

    @timed
    def run(self):
        output = shellout("awk '{{print $1}}' {input} | sort -u > {output}",
                          input=self.input().path)
        luigi.File(output).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class Snapshot(BSZTask):
    """
    Snapshot for all finc ILNs.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        for iln in self.mappings().get('iln_live').keys():
            yield SnapshotPPNList(begin=self.begin, end=self.end, iln=iln)

    @timed
    def run(self):
        _, stopover = tempfile.mkstemp(prefix='siskin-')
        for target in self.input():
            shellout("cat {input} >> {output}", input=target.path, output=stopover)
        luigi.File(stopover).move(self.output().path)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class UnifiedSnapshot(BSZTask):
    """
    Combine the snapshot into a single list, that contains
    all record ids across all FINC ILNs.
    TODO: the EPN, DATE fields are arbitrary, do we really need them?
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        for iln in self.finc_ilns():
            yield SnapshotBasic(begin=self.begin, end=self.end, iln=iln)

    @timed
    def run(self):
        seen = set()
        counter = collections.Counter()
        with self.output().open('w') as output:
            for target in self.input():
                with target.open() as handle:
                    for row in handle.iter_tsv(cols=('ppn', 'epn', 'date', 'X')):
                        if row.ppn in seen:
                            counter['skipped'] += 1
                        else:
                            counter['written'] += 1
                            output.write_tsv(*row)
                            seen.add(row.ppn)
        self.logger.debug(counter)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class BSZIndexPatch(BSZTask):
    """
    Adjust the index. This is a long task, but it is reasonably fast
    compared to the alternatives. It can bootstrap an index or just move
    between dates (back and forth).
    Moving the index forward a single day takes about 5-10min
    (asking ES for all ids takes about 3min alone - for about 4M ids).

    This task will take into account: additions, explicit deletions,
    implicit deletions (Umhängungen).

    Note: This task will contain all IDs that are currently in FINC
    (18 ILNs that is). Information about which library has what is not stored
    here. The main purpose of this index is to represent "BSZ" in the fuzzy
    deduplication process. It is also an example of managing an index by deltas.

    DONE: Rework this, consumes too much memory - upto 4G.
    TODO: Find a faster alternative to shelves.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    indicator = luigi.Parameter(default=random_string())

    def requires(self):
        return UnifiedSnapshot(begin=self.begin, end=self.end)

    @timed
    def run(self):
        """
        1) Get the current list of ids in the index
        2) Compare desired and current
        3) Delete the obsolete docs
        4) Create Marc/Json patch files
        5) Index that patch file
        """
        # create the 'bsz' index, if necessary
        es = elasticsearch.Elasticsearch(timeout=30)

        if not es.indices.exists('bsz'):
            self.logger.debug('Creating index...')
            # mapping for MLT, see also: http://bit.ly/1cHuJGA
            settings = {
                "settings": {
                    "number_of_shards": 5,
                    "analysis": {
                        "filter": {
                            "autocomplete_filter": {
                                "type":     "edge_ngram",
                                "min_gram": 1,
                                "max_gram": 20
                            }
                        },
                        "analyzer": {
                            "autocomplete": {
                                "type":      "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "autocomplete_filter"
                                ]
                            }
                        }
                    }
                }
            }

            mapping = {
                'title': {
                    'date_detection': False,
                    '_id': {
                        'path': 'content.001'
                    },
                    '_all': {
                        'enabled': True,
                        'term_vector': 'with_positions_offsets',
                        'store': True
                    },
                    'properties': {
                        'content': {
                            'properties': {
                                '245': {
                                    'properties': {
                                        'a': {
                                            'type': 'string',
                                            'analyzer': 'autocomplete'
                                        }
                                    }
                                },
                                '100': {
                                    'properties': {
                                        'a': {
                                            'type': 'string',
                                            'analyzer': 'autocomplete'
                                        }
                                    }
                                },
                                '700': {
                                    'properties': {
                                        'a': {
                                            'type': 'string',
                                            'analyzer': 'autocomplete'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            es.indices.create(index='bsz', body=settings)
            es.indices.put_mapping(index='bsz', doc_type='title', body=mapping)

        # collect all ids=dates, that we need to have in the index
        desired_target = luigi.File(is_tmp=True)
        desired_map = shelve.open(desired_target.path, "c")

        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'X', 'date', 'X')):
                desired_map[row.ppn] = row.date

        # collect all indexed ids, (this is the slowest part, actually)
        # TODO: should be store the ILNs as well?

        current_target = luigi.File(is_tmp=True)
        current_map = shelve.open(current_target.path, "c")

        batch_size = 10000
        with Timer() as timer:
            hits = eshelpers.scan(es, {'query': {'match_all': {}},
                'fields': ['meta.date']}, index='bsz', doc_type='title',
                scroll='10m', size=batch_size)
            for hit in hits:
                fields = hit.get('fields')
                if not fields['meta.date']:
                    raise RuntimeError("BSZ documents has not meta.date field")
                current_map[hit['_id'].encode('utf-8')] = fields['meta.date'][0]

        # inline benchmark, which size works best?
        self.logger.debug(json.dumps({'size': batch_size,
                                 'elapsed': timer.elapsed_s}))

        # outdated items are those, that are already indexed, but do have
        # a differing date (might be earlier or later)
        outdated = set()
        for id, current_date in current_map.iteritems():
            if id in desired_map:
                if not current_date == desired_map[id]:
                    outdated.add(id)

        desired = set(desired_map.iterkeys())
        current = set(current_map.iterkeys())

        # cleanup temporary stuff
        current_map.close()
        desired_map.close()

        try:
            os.remove(desired_target.path)
            os.remove(current_target.path)
        except OSError as err:
            self.logger.warn(err)

        # dump the comparisons (fyi)
        receipt = json.dumps({
            'range': {
                'begin': self.begin.strftime('%A, %Y-%m-%d'),
                'end': self.end.strftime('%A, %Y-%m-%d'),
                'note': 'begin <= ... < end',
            },
            'desired': len(desired),
            'current': len(current),
            'outdated': {
                'size': len(outdated),
                'sample': list(outdated)[:10]
            },
            'to be indexed': {
                'size': len(desired - current),
                'sample': list(desired - current)[:10],
            },
            'to be deleted': {
                'size': len(current - desired),
                'sample': list(current - desired)[:10],
                'note': 'Usually deletions come on Thursday, but '
                        'Umhaengungen happen all the time.'
            },
            'untouched': {
                'size': len(current & desired - outdated),
                'sample': list(current & desired - outdated)[:10]
            }
        }, indent=4)
        self.logger.info(receipt)

        # delete the obsolete docs
        obsolete = current.difference(desired)
        if len(obsolete) == 0:
            self.logger.debug("Nothing to delete.")
        else:
            self.logger.debug('Deleting %s docs...' % len(obsolete))
            for batch in nwise(obsolete, n=10000):
                es.delete_by_query(index='bsz', doc_type='title',
                                   body={'query': {'ids': {'values': batch}}})

        # start constructing the patch
        patch = desired.difference(current).union(outdated)

        if len(patch) == 0:
            self.logger.debug("Nothing to add.")
        else:
            # if len(patch) > 0, we need to index
            self.logger.debug('Preparing %s docs...' % len(patch))

            # store on which date we'll pick up which id
            shards = collections.defaultdict(set)
            with self.input().open() as handle:
                for row in handle.iter_tsv(cols=('ppn', 'epn', 'date', 'X')):
                    if row.ppn in patch:
                        shards[row.date].add(row.ppn)

            # collect all JSON in this file
            _, combined = tempfile.mkstemp(prefix='siskin-')

            # collect residue paths here
            garbage = set()

            # - for each day, we need the raw file (raw) and its seekmap db
            # - we need to convert each snippet to JSON and the concatenate the
            #   snippets, so that each part carries is't date with it, which
            #   will be used to apply delta-patches
            for date_value, ppnset in sorted(shards.iteritems()):
                date = datetime.date(*map(int, date_value.split('-')))

                raw = GenericImport(date=date, kind='tit')
                sdb = SeekmapDB(date=date, kind='tit')
                self.logger.debug("Scheduling prerequisites %s, %s ..." % (sdb, raw))
                luigi.build([sdb, raw])

                _, stopover = tempfile.mkstemp(prefix='siskin-')
                self.logger.debug("Using seekmap-db at %s" % sdb.output().path)

                with sqlite3db(sdb.output().path) as cursor:
                    with raw.output().open() as handle:
                        with open(stopover, 'w') as so:
                            cursor.execute("""
                                SELECT offset, length
                                FROM seekmap WHERE id IN (%s)
                            """ % (','.join([ "'%s'" % id for id in ppnset])))
                            rows = cursor.fetchall()
                            copyregions(handle, so, rows)

                output = shellout("""marctojson -m date={date} {input} >
                                     {output}""", date=date, input=stopover)

                shellout("cat {input} >> {output}", input=output, output=combined)

                garbage.add(stopover)
                garbage.add(output)

            self.logger.debug("Combined JSON patch at {0}".format(combined))

            # index inline
            def docs():
                """ Function to yield all documents, that need to be indexed. """
                with open(combined) as handle:
                    documents = ({
                        '_index': 'bsz',
                        '_type': 'title',
                        '_id': d.get('content').get('001'),
                        '_source': d} for d in (json.loads(line)
                                      for line in handle))
                    for d in documents:
                        yield d

            self.logger.debug("Bulk indexing %s docs..." % len(patch))

            es.indices.put_settings({"index": {"refresh_interval": "-1"}}, index='bsz')
            eshelpers.bulk_index(es, docs(), chunk_size=2000, raise_on_error=True)
            es.indices.put_settings({"index": {"refresh_interval": "1s"}}, index='bsz')

            garbage.add(combined)

            # get rid of the garbage
            for path in garbage:
                try:
                    os.remove(path)
                except OSError as err:
                    self.logger.warning(err)

        # dump the receipt for later inspection
        with self.output().open('w') as output:
            output.write(receipt)

    def output(self):
        return luigi.LocalTarget(path=self.path())

#
# Info tasks
# ==========
#
class Lookup(BSZTask):
    """
    Which EPN are pointing to this PPN (in a certain date interval)?
    TODO: This takes about 0.5s net, but the whole luigi overhead is about 6s.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())
    id = luigi.Parameter()
    kind = luigi.Parameter(default='tit')

    def requires(self):
        return {
            'local': ListifyLocalRange(begin=self.begin, end=self.end),
            'deletion': DeletionRangeFinc(begin=self.begin, end=self.end)
        }

    @timed
    def run(self):
        self.logger.info('Trace for {kind}: {id} (ppn, epn, sigel, 004, date, iln)'.format(
                    kind=self.kind.upper(), id=self.id))
        self.logger.info('Note: could match both, EPNs and PPNs, e.g. 108834948')
        output = shellout("grep {id} {input} > {output}", id=self.id,
                          ignoremap={1: "ID NOT FOUND"},
                          input=self.input().get('local').path)

        epns = set()
        if self.kind == 'tit':
            with luigi.File(output, format=TSV).open() as handle:
                for row in handle.iter_tsv(cols=(0, 'epn', 0, 0, 0, 0)):
                    epns.add(row.epn)
        elif self.kind == 'lok':
            epns.add(self.id)

        self.logger.debug("\n\n{output}".format(output=open(output).read()))

        if len(epns) == 0:
            self.logger.info('# No EPN to consider between {begin} and {end}'.format(
                  self.begin, self.end))

        for epn in sorted(epns):
            self.logger.info('# trace in deletions for EPN (epn, iln, date): %s' % (epn))
            try:
                output = shellout("grep {epn} {input} > {output}", epn=epn,
                                  input=self.input().get('deletion').path)
                self.logger.info("\n\n{output}".format(output=open(output).read()))
            except RuntimeError as err:
                if err.code == 1:
                    self.logger.info("NO DELETIONS FOR {epn}".format(epn=epn))

    def complete(self):
        return False

# ==============================================================================
#
# Extra tasks
#
# ==============================================================================

class SigelFrequency(BSZTask):
    """
    Compute the sigel frequency for a given date.
    """
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return ListifyLocal(date=self.date)

    @timed
    def run(self):
        counter = collections.Counter()
        with self.input().open() as handle:
            for row in handle.iter_tsv(cols=('ppn', 'epn', 'sigel', 'transaction', 'date', 'iln')):
                counter[row.sigel] += 1

        with self.output().open('w') as output:
            for sigel, frequency in counter.most_common():
                output.write_tsv(sigel, frequency)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class SigelFrequencyTotal(BSZTask):
    """
    The overall occurences of all sigels.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        interval = date_interval.Custom(self.begin, self.end)
        return (SigelFrequency(date=date) for date in interval)

    @timed
    def run(self):
        counter = collections.Counter()
        for target in self.input():
            with target.open() as handle:
                for row in handle.iter_tsv(cols=('sigel', 'frequency')):
                    counter[row.sigel] += int(row.frequency)

        with self.output().open('w') as output:
            for sigel, frequency in counter.most_common():
                output.write_tsv(sigel, frequency)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)

class UniqueSigel(BSZTask):
    """
    Compute all uniq sigel that occur in the data.
    """
    begin = luigi.DateParameter(default=BSZTask.SONDERABZUG)
    end = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        interval = date_interval.Custom(self.begin, self.end)
        return (SigelFrequency(date=date) for date in interval)

    @timed
    def run(self):
        filenames = ' '.join([obj.fn for obj in self.input()])
        t = """cat {files} | awk '{{print $1}}' | sort -u > {output}"""
        temp = shellout(t, files=filenames)
        luigi.File(temp).move(self.output().fn)

    def output(self):
        return luigi.LocalTarget(path=self.path(), format=TSV)
