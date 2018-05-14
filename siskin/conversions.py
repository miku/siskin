# coding: utf-8
# pylint: disable=C0103,W0232,C0301,W0703

# Copyright 2018 by Leipzig University Library, http://ub.uni-leipzig.de
#                   The Finc Authors, http://finc.info
#                   Martin Czygan, <martin.czygan@uni-leipzig.de>
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
A new module for conversions. Complement assets.
"""

import collections
import logging
import tarfile
import tempfile
from xml.sax.saxutils import escape, unescape

import xmltodict

import marcx
import pymarc
from siskin.benchmark import timed

html_escape_table = {
    '"': "&quot;",
    "'": "&apos;"
}
html_unescape_table = {v: k for k, v in html_escape_table.items()}

logger = logging.getLogger('siskin')

def html_escape(text):
    """ Escape HTML, see also: https://wiki.python.org/moin/EscapingHtml"""
    return escape(text, html_escape_table)

def html_unescape(text):
    """ Unescape HTML, see also: https://wiki.python.org/moin/EscapingHtml"""
    return unescape(text, html_unescape_table)

@timed
def imslp_tarball_to_marc(tarball, outputfile=None, legacy_mapping=None):
    """
    Convert an IMSLP tarball to MARC binary without extracting it. Optionally
    write to a outputfile (filename). If outputfile is not given, write to a
    temporary location.

    Returns the location of the resulting MARC file.
    """
    if outputfile is None:
        _, outputfile = tempfile.mkstemp(prefix="siskin-")

    stats = collections.Counter()

    with open(outputfile, "wb") as output:
        writer = pymarc.MARCWriter(output)
        with tarfile.open(tarball) as tar:
            for member in tar.getmembers():
                fobj = tar.extractfile(member)
                try:
                    record = imslp_xml_to_marc(fobj.read(),
                                               legacy_mapping=legacy_mapping)
                    writer.write(record)
                except ValueError as exc:
                    logger.warn("conversion failed: %s", exc)       
                    stats["failed"] += 1
                    continue
                finally:
                    fobj.close()
                    stats["processed"] += 1

        writer.close() 
        logger.debug("%d/%d record failed/processed", stats["failed"], stats["processed"])

    return outputfile

def imslp_xml_to_marc(s, legacy_mapping=None):
    """
    Convert a string containing a single IMSLP record to MARC.

    Optionally take a legacy mapping, mappings identifiers to VIAF identifiers.

    Blueprint: https://git.io/vpQPd
    """
    dd = xmltodict.parse(s, force_list={"subject"})
   
    if legacy_mapping is None:
        legacy_mapping = collections.defaultdict(lambda: collections.defaultdict(str))

    record = marcx.Record(force_utf8=True)  
    record.strict = False

    doc = dd["document"]
    
    try:
        record.add("245", a=doc["title"])
    except KeyError:
        raise ValueError("cannot find title: %s ..." % s[:300])

    record.leader = "     ncs  22        450 "
    
    identifier = doc["identifier"]["#text"]
    record.add("001", data="finc-15-%s".format(identifier))
    record.add("007", data="cr")
    language = doc.get("languages", "")
    record.add("008", data="130227uu20uuuuuuxx uuup%s  c" % language)

    record.add("041", a=language)
    creator = doc["creator"]["mainForm"]
    record.add("100", a=creator, e="cmp",
               _0=legacy_mapping.get(identifier, {}).get("viaf", ""))

    record.add("240", a=legacy_mapping.get(identifier, {}).get("title", ""))
    record.add("245", a=html_unescape(doc.get("title", "")))
    record.add("246", a=html_unescape(doc.get("additionalTitle", "")))

    record.add("260", c=doc.get("date", ""))
    record.add("650", y=doc.get("date", ""))
    record.add("500", a=doc.get("abstract", ""))

    for689 = []

    if "subject" in doc:
        if len(doc["subject"]) == 1:
            for689.append(doc["subject"][0]["mainForm"])
        elif len(doc["subject"]) == 2:
            for689.append(doc["subject"][1]["mainForm"])
        else:
            raise ValueError("cannot handle %d subjects", len(doc["subject"]))

        record.add("590", a=for689[0],
                   b=doc.get("music_arrangement_of", ""))

        for689.append(doc.get("music_arrangement_of", ""))
        
        for subject in set(for689):
            record.add("689", a=subject)

    record.add("700", a=doc.get("contributor", {}).get("mainForm", ""), e="ctb")
    record.add("856", q="text/html", _3="Petrucci Musikbibliothek",
               u=doc["url"]["#text"])
    record.add("970", c="PN")
    record.add("980", a=identifier, b="15", c="Petrucci Musikbibliothek")
    return record
