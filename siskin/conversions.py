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

import base64
import collections
import logging
import tarfile
import tempfile
from xml.sax.saxutils import escape, unescape

import xmltodict

import marcx
import pymarc

html_escape_table = {
    '"': "&quot;",
    "'": "&apos;"
}
html_unescape_table = {v: k for k, v in html_escape_table.items()}

logger = logging.getLogger('siskin')

def html_escape(text):
    """
    Escape HTML, see also: https://wiki.python.org/moin/EscapingHtml
    """
    return escape(text, html_escape_table)

def html_unescape(text):
    """
    Unescape HTML, see also: https://wiki.python.org/moin/EscapingHtml
    """
    return unescape(text, html_unescape_table)

def imslp_tarball_to_marc(tarball, outputfile=None, legacy_mapping=None,
                          max_failures=30):
    """
    Convert an IMSLP tarball to MARC binary output file without extracting it.
    If outputfile is not given, write to a temporary location.

    Returns the location of the resulting MARC file.

    A maximum number of failed conversions can be specified with `max_failures`,
    as of 2018-04-25, there were 30 records w/o title.
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
                    record = imslp_xml_to_marc(fobj.read(), legacy_mapping=legacy_mapping)
                    writer.write(record)
                except ValueError as exc:
                    logger.warn("conversion failed: %s", exc)
                    stats["failed"] += 1
                finally:
                    fobj.close()
                    stats["processed"] += 1

        writer.close()

        if stats["failed"] > max_failures:
            raise RuntimeError("more than %d records failed", max_failures)

        logger.debug("%d/%d records failed/processed", stats["failed"], stats["processed"])

    return outputfile

def imslp_xml_to_marc(s, legacy_mapping=None):
    """
    Convert a string containing a single IMSLP XML record to a pymarc MARC record.

    Optionally take a legacy mapping, associating IMSLP Names with VIAF
    identifiers. Blueprint: https://git.io/vpQPd, with one difference: We
    allow records with no subjects.

    A record w/o title is an error. We check for them, when we add the field.
    """
    dd = xmltodict.parse(s, force_list={"subject", "languages"})

    if legacy_mapping is None:
        legacy_mapping = collections.defaultdict(lambda: collections.defaultdict(str))

    record = marcx.Record(force_utf8=True)
    record.strict = False

    doc = dd["document"]

    record.leader = "     ncs  22        450 "

    identifier = doc["identifier"]["#text"]
    encoded_id = base64.b64encode(identifier.encode("utf-8")).rstrip("=")
    record.add("001", data=u"finc-15-{}".format(encoded_id))

    record.add("007", data="cr")

    if doc.get("languages", []):
        langs = [l for l in doc["languages"] if l != "unbekannt"]
        if langs:
            record.add("008", data="130227uu20uuuuuuxx uuup%s  c" % langs[0])
            for l in langs:
                record.add("041", a=l)

    creator = doc["creator"]["mainForm"]
    record.add("100", a=creator, e="cmp",
               _0=legacy_mapping.get(identifier, {}).get("viaf", ""))

    record.add("240", a=legacy_mapping.get(identifier, {}).get("title", ""))

    try:
        record.add("245", a=html_unescape(doc["title"]))
    except KeyError:
        raise ValueError("cannot find title: %s ..." % s[:300])

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

        record.add("590", a=for689[0].title(),
                   b=doc.get("music_arrangement_of", "").title())

        for689.append(doc.get("music_arrangement_of", ""))

        for subject in set(for689):
            record.add("689", a=subject.title())

    record.add("700", a=doc.get("contributor", {}).get("mainForm", ""), e="ctb")
    record.add("856", q="text/html", _3="Petrucci Musikbibliothek",
               u=doc["url"]["#text"])
    record.add("970", c="PN")
    record.add("980", a=identifier, b="15", c="Petrucci Musikbibliothek")
    return record

