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
A module for conversions. This module exists to separate orchestration code
(luigi task) from data conversion logic (here), cf. this note from the British
Library:

> The code that performs tasks and the code that orchestrates those tasks were
mixed together in the same source files. This made it very hard to work on
improving any individual task on it’s own, and made testing difficult.

* https://blogs.bl.uk/webarchive/2022/01/ukwa-2021-technical-update.html
"""

import base64
import collections
import json
import logging
import os
import tarfile
import tempfile
from xml.sax.saxutils import escape, unescape

import marcx
import pymarc
import requests
import six
import xmltodict

from siskin.utils import URLCache

html_escape_table = {'"': "&quot;", "'": "&apos;"}
html_unescape_table = {v: k for k, v in html_escape_table.items()}

marburg_language_mapping = {
    "de": "ger",
    "deu": "ger",
}

logger = logging.getLogger("siskin")


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


def imslp_tarball_to_marc(tarball, outputfile=None, legacy_mapping=None, max_failures=30):
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
            logger.warn(
                "%d records failed, only %d failures allowed",
                stats["failed"],
                max_failures,
            )
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
    encoded_id = base64.b64encode(six.b(identifier)).rstrip(b"=")
    record.add("001", data="finc-15-{}".format(encoded_id.decode("utf-8")))

    record.add("007", data="cr")

    if doc.get("languages", []):
        langs = [lang for lang in doc["languages"] if lang != "unbekannt"]
        if langs:
            record.add("008", data="130227uu20uuuuuuxx uuup%s  c" % langs[0])
            for lang in langs:
                record.add("041", a=lang)

    creator = doc["creator"]["mainForm"]
    record.add("100", a=creator, e="cmp", _0=legacy_mapping.get(identifier, {}).get("viaf", ""))

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

        record.add("590", a=for689[0].title(), b=doc.get("music_arrangement_of", "").title())

        for689.append(doc.get("music_arrangement_of", ""))

        for subject in set(for689):
            record.add("689", a=subject.title())

    record.add("700", a=doc.get("contributor", {}).get("mainForm", ""), e="ctb")
    record.add("856", q="text/html", _3="Petrucci Musikbibliothek", u=doc["url"]["#text"])
    record.add("970", c="PN")
    record.add("980", a=identifier, b="15", c="Petrucci Musikbibliothek")
    return record


def olc_to_intermediate_schema(doc):
    """
    Convert a single OLC from SLUB solr into intermediate schema JSON, w/o tags.

    TODO: every mapping should live outside of code for collaborative editing.
    """
    # "interne Bezeichnung":"Fachkatalog"
    internal_to_name = {
        # $ curl "https://is.gd/qPEypK" |
        #     pup 'tr json{}' |
        #     jq -rc '.[] | [.["children"][1]["children"][0]["text"], .["children"][0]["children"][0]["text"]]' |
        #     sed -e 's@","@":"@; s@^\[@@; s@\]@,@' |
        #     grep 'SSG-OLC'
        "SSG-OLC-ALT": "Altertumswissenschaften",
        "SSG-OLC-ANG": "Anglistik",
        "SSG-OLC-ARC": "Architektur",
        "SSG-OLC-ASS": "Afrika südlich der Sahara",
        "SSG-OLC-AST": "Astronomie",
        "SSG-OLC-BAL": "Baltische Länder",
        "SSG-OLC-BEL": "Niederlande",
        "SSG-OLC-BIF": "Bildungsforschung",
        "SSG-OLC-BUB": "Informations-, Buch- und Bibliothekswesen",
        "SSG-OLC-CHE": "Chemie",
        "SSG-OLC-ETH": "Ethnologie",
        "SSG-OLC-FOR": "Forstwissenschaften",
        "SSG-OLC-FRK": "Frankreichkunde und Allgemeine Romanistik",
        # "SSG-OLC-FTH": "Film und Theater", # refs. #16279
        "SSG-OLC-FTH": "Film / Theater",
        "SSG-OLC-GEO": "Geowissenschaften",
        "SSG-OLC-GER": "Germanistik",
        "SSG-OLC-GWK": "Kunst und Kunstwissenschaft",
        "SSG-OLC-HIS": "Geschichte",
        "SSG-OLC-HSW": "Hochschulwesen",
        "SSG-OLC-IBA": "Ibero-Amerika",
        "SSG-OLC-IBL": "Internationale Beziehungen und Länderkunde",
        "SSG-OLC-ITF": "Italienforschung",
        "SSG-OLC-JUR": "Recht",
        "SSG-OLC-KPH": "Klassische Philologie",
        "SSG-OLC-MAT": "Mathematik und Informatik",
        "SSG-OLC-MFO": "Asien und Nordafrika",
        # "SSG-OLC-MKW": "Medien- und Kommunikationswissenschaft", # refs. #16279
        "SSG-OLC-MKW": "Medien- / Kommunikationswissenschaft",
        "SSG-OLC-MUS": "Musikwissenschaft",
        "SSG-OLC-NED": "Niederlande",
        "SSG-OLC-OAS": "Ost- und Südostasien",
        "SSG-OLC-OEB": "Auswahl deutschsprachiger Zeitschriften",
        "SSG-OLC-OEU": "Osteuropa",
        "SSG-OLC-PHA": "Pharmazie",
        "SSG-OLC-PHI": "Philosophie",
        "SSG-OLC-PHY": "Physik",
        "SSG-OLC-POL": "Politikwissenschaft und Friedensforschung",
        "SSG-OLC-PSY": "Psychologie",
        "SSG-OLC-ROK": "Romanischer Kulturkreis",
        "SSG-OLC-SAS": "Südasien",
        "SSG-OLC-SCA": "Nordeuropa",
        "SSG-OLC-SLA": "Slavistik",
        "SSG-OLC-SOW": "Sozialwissenschaften",
        "SSG-OLC-SPO": "Sportwissenschaften",
        "SSG-OLC-SPP": "Spanien und Portugal",
        "SSG-OLC-TEC": "Technik",
        "SSG-OLC-TGE": "Technikgeschichte",
        "SSG-OLC-UMW": "Umwelt",
        "SSG-OLC-VET": "Veterinärmedizin",
        "SSG-OLC-VOR": "Vorderer Orient",
        "SSG-OLC-WIW": "Wirtschaftswissenschaften",
        "SSG-OLC-ZGE": "Zeitgeschichte",
        "SSG-OPC-ANG": "Anglo-American Culture",
        "SSG-OPC-AST": "Astronomie",
        "SSG-OPC-BAL": "Baltische Länder",
        "SSG-OPC-BBI": "Informations-, Buch- und Bibliothekswesen",
        "SSG-OPC-FIN": "Finnougristik",
        "SSG-OPC-FOR": "Forstwissenschaften",
        "SSG-OPC-GEO": "Fachkatalog Geophysik",
        "SSG-OPC-GGO": "Geowissenschaften",
        "SSG-OPC-MAT": "Mathematik",
        "SSG-OPC-PHA": "Pharmazie",
        "SSG-OPC-VOR": "Vorderer Orient",
    }
    olc_format_to_finc_format = {
        "Journal": "Journal",
        "eJournal": "Journal",
        "Article": "Article",
        "electronic Article": "Article",
        "Monograph Series": "Serial",
        "Serial Volume": "Book",
    }
    mega_collections_set = set()
    for internal in doc.get("collection_details", []):
        if not internal.startswith("SSG-"):
            continue
        name = internal_to_name.get(internal)
        if not name:
            continue
        # Philosophie => OLC SSG Philosophie
        mc = "OLC SSG {}".format(name)
        mega_collections_set.add(mc)

    result = {
        "abstract": de_listify(doc.get("abstract")),
        "authors": [{
            "rft.au": name
        } for name in doc.get("author2", [])],
        "finc.format": olc_format_to_finc_format.get(de_listify(doc.get("format"), "Article")),
        "finc.id": "ai-68-{}".format(doc["id"]),
        "finc.mega_collection": list(mega_collections_set),
        "finc.source_id": "68",
        "languages": doc.get("lang_code", []),
        "rft.atitle": de_listify(doc.get("title", "")),
        "rft.genre": "article",
        "rft.issn": doc.get("issn", []),
        "rft.issue": doc.get("container_issue", ""),
        "rft.jtitle": doc.get("container_title", ""),
        "rft.place": doc.get("rft.place", []),
        "rft.pub": doc.get("publisher", []),
        "rft.volume": doc.get("container_volume", ""),
        "url": doc.get("url", []),
        "x.subtitle": de_listify(doc.get("title_sub", [])),
    }
    date = doc.get("publishDateSort")
    if date and len(date) == 4:
        result["x.date"] = "{}-01-01T00:00:00Z".format(date)
        result["rft.date"] = date
    return result


def de_listify(v, default=None):
    """
    Take any value and returns a string or None, possibly truncating multiple
    values.
    """
    if v is None:
        if default:
            return default
        return v
    if isinstance(v, str):
        if not v and default:
            return default
        return v
    if isinstance(v, set):
        v = list(v)
    if isinstance(v, list):
        if len(v) > 0:
            return v[0]
        else:
            if default:
                return default
            return None
    raise ValueError("cannot de-listify: {}".format(v))


def osf_to_intermediate(osf, force=False, best_effort=True, max_retries=5):
    """
    Convert a document from https://api.osf.io/v2/preprints/?format=json&page=1
    to intermediate schema; see also kiwi:[191], also: 179; refs #20238.
    """
    if not osf:
        return None
    if "id" not in osf:
        raise ValueError("osf record w/o id: {}".format(osf))
    source_id = "179"

    def get_tcid_for_provider(provider):
        # refs 20238#note-8
        provider_tcid_map = {
            "eartharxiv": "sid-{}-col-earth".format(source_id),
            "ecoevorxiv": "sid-{}-col-eco".format(source_id),
            "lawarxiv": "sid-{}-col-law".format(source_id),
            "lissa": "sid-{}-col-lissa".format(source_id),
            "mediarxiv": "sid-{}-col-medi".format(source_id),
            "psyarxiv": "sid-{}-col-psy".format(source_id),
            "socarxiv": "sid-{}-col-soc".format(source_id),
        }
        try:
            return provider_tcid_map[provider]
        except KeyError:
            return "sid-{}-col-{}".format(source_id, provider)

    attrs = osf.get("attributes")
    rels = osf.get("relationships")
    if not attrs or not rels:
        raise ValueError("osf record w/o attributes or relationships: {}".format(osf))

    def find_osf_language(doc, with_default="eng"):
        """
        If possible, detect language from abstract and return 3-letter ISO 639
        code. Otherwise return a default.
        """
        if not attrs.get("description"):
            return with_default
        try:
            import cld3
            from iso639 import languages

            result = cld3.get_language(attrs["description"])
            if not result.is_reliable:
                return with_default
            return languages.get(alpha2=result.language).part2b
        except (ImportError, KeyError):
            return with_default

    def fetch_authors(doc, force=False, best_effort=False, max_retries=5):
        """
        Fetch and cache author docs. We will need an extra request, which we'll
        cache locally.

        Example: https://api.osf.io/v2/preprints/egcsk/contributors/
        """
        result = []
        cache = URLCache(
            directory=os.path.join(tempfile.gettempdir(), ".urlcache"),
            max_tries=max_retries,
        )
        url = doc["relationships"]["contributors"]["links"]["related"]["href"]
        try:
            content = cache.get(url, force=force)
        except (RuntimeError, requests.exceptions.ConnectionError) as exc:
            if best_effort:
                logger.debug("[best-effort] skipping: {}".format(exc))
                return result
            raise
        doc = json.loads(content)
        try:
            for item in doc["data"]:
                try:
                    attrs = item["embeds"]["users"]["data"]["attributes"]
                except KeyError:
                    attrs = item["embeds"]["users"]["errors"][0]["meta"]
                result.append({
                    "rft.aufirst": attrs["given_name"],
                    "rft.aulast": attrs["family_name"],
                })
        except KeyError as exc:
            logger.debug("failed to find authors for {}: {}".format(exc, json.dumps(doc, indent=4)))
        else:
            logger.debug("fetched {}, found {} author(s)".format(url, len(result)))
        return result

    result = {
        "abstract": attrs.get("description", ""),
        "authors": fetch_authors(osf, force=force, best_effort=best_effort, max_retries=max_retries),
        "finc.format": "Preprint",
        "finc.id": "ai-{}-{}".format(source_id, osf["id"]),
        "finc.mega_collection": [
            get_tcid_for_provider(rels["provider"]["data"]["id"]),
            rels["provider"]["data"]["id"].capitalize(),
        ],
        "finc.source_id": source_id,
        "languages": [find_osf_language(osf)],
        "doi": osf["links"]["preprint_doi"].replace("https://doi.org/", ""),
        "rft.atitle": attrs["title"],
        "rft.genre": "article",
        "rft.jtitle": rels["provider"]["data"]["id"],
        "rft.pub": ["OSF Preprints"],
        "url": [osf["links"]["preprint_doi"]],
        "subjects": attrs["tags"],
    }

    if "preprint_doi_created" in attrs and attrs["preprint_doi_created"]:
        result.update({
            "x.date": attrs.get("preprint_doi_created", "") + "Z",
            "rft.date": attrs.get("preprint_doi_created", "")[:10],
        })

    return result


def eastview_solr_to_intermediate_schema(
    blob,
    source_id="210",
    tcid="sid-210-col-udbedu",
    collection_name="Universal Database of Social Sciences & Humanities (UDB-EDU)",
):
    """
    Given a string containing raw XML. Each document contains a list of
    documents (100s), refs #12586.
    """
    parsed = xmltodict.parse(blob)
    converted = []
    for doc in parsed["add"]["doc"]:
        dd = dict()
        for f in doc["field"]:
            key, value = f.get("@name"), f.get("#text")
            if not key or not value:
                continue
            dd[key] = value
        # keys: 'id', 'title', 'author', 'source', 'src', 'year', 'number',
        # 'place', 'content', 'volume', 'pages', 'language', 'url', 'medium',
        # 'coll', 'cdate'
        if not dd.get("title") or not dd.get("url"):
            continue
        encoded_id = (base64.b64encode(dd["id"].encode("utf-8")).decode("utf-8").rstrip("="))
        authors = [{"rft.au": v} for v in dd.get("author", "").split(",") if v]
        result = {
            "version": "0.9",
            "finc.format": "Article",
            "finc.record_id": dd["id"],
            "finc.id": "ai-{}-{}".format(source_id, encoded_id),
            "finc.source_id": source_id,
            "rft.atitle": dd["title"],
            "rft.jtitle": dd.get("source", ""),
            "url": [dd.get("url", "")],
            "authors": authors,
            "finc.mega_collection": [
                collection_name,
                tcid,
            ],
        }
        if dd.get("content"):
            result["abstract"] = dd["content"][:500] + "..."
        if dd.get("language"):
            result["languages"] = [dd["language"]]
        if dd.get("pages") and dd["pages"] != "-":
            result["rft.pages"] = dd["pages"]
        if dd.get("volume"):
            result["rft.volume"] = dd["volume"]
        if dd.get("number"):
            result["rft.issue"] = dd["number"]
        if dd.get("place"):
            result["rft.place"] = [dd["place"]]
        if dd.get("year"):
            result["x.date"] = "{}-01-01T00:00:00Z".format(dd["year"])
            result["rft.date"] = dd["year"]
        converted.append(result)
    return converted


# Facet fields values from author2_role field across all sources.
# TODO: fill out empty strings, maybe add https://git.io/fNLwY, too.
# TODO: move this out of code into an asset folder or the like.
author_role_mapping = {
    "\xc3\xbcbers": "",
    "\xc3\xbcbersetzer": "",
    "\xc3\xbcbersetzerin": "",
    "arrangeur": "",
    "bearb": "",
    "clb": "",
    "darst": "",
    "donateur": "",
    "ed": "",
    "edtomas": "",
    "fotografin": "",
    "herausgeber": "",
    "herausgeberin": "",
    "hg": "",
    "hrsg": "",
    "illustrator": "",
    "kommentatorin": "",
    "komponist": "",
    "librettist": "",
    "mitarb": "",
    "mitarbeiter": "",
    "mitwirkender": "",
    "photographer": "",
    "pord": "",
    "sonstige": "",
    "t\xc3\xa4nzer": "",
    "textdichter": "",
    "textverf": "",
    "veranstalter": "",
    "verfasser einer einleitung": "",
    "verfasser eines nachworts": "",
    "verfasser eines vorworts": "",
    "verfasser von kommentaren": "",
    "verfasser": "",
    "verfasserin einer einleitung": "",
    "verfasserin eines geleitwortes": "",
    "verfasserin eines nachworts": "",
    "verfasserin eines vorworts": "",
    "verfasserin von erg\xc3\xa4nzendem text": "",
    "verfasserin von zusatztexten": "",
    "verfasserin": "",
    "voc": "",
    "zusammenstellender": "",
    # More, from MAB/151 mostly.
    " [Animation]": "",
    " [Ausstattung]": "",
    " [Darst.]": "",
    " [Drehbuch]": "",
    " [Kamera]": "",
    " [Komp.]": "",
    " [Musik]": "",
    " [Prod.]": "",
    " [Regie]": "",
    " [Schnitt]": "",
    " [Ton]": "",
    ", [Drehbuch]": "",
    "Darst.]": "",
    "Drehbuch]": "",
    "Prod.]": "",
    "[Alt]": "",
    "[Animation]": "",
    "[Assistent]": "",
    "[Assistentin]": "",
    "[Aufnahmeleiter]": "",
    "[Ausstattung]": "",
    "[Autor]": "",
    "[Bariton]": "",
    "[Bass]": "",
    "[Bearb.]": "",
    "[Begr.]": "",
    "[Beleuchtung]": "",
    "[Beratung]": "",
    "[Buch]": "",
    "[Casting]": "",
    "[Co-Produzent]": "",
    "[Computeranimation]": "",
    "[DRehbuch]": "",
    "[Darst.]": "",
    "[Darst.r]": "",
    "[Darst]": "",
    "[Darsteller]": "",
    "[Dehbuch]": "",
    "[Digitale Bildgestaltung]": "",
    "[Dir.]": "",
    "[Donat]": "",
    "[Donateur]": "",
    "[Doz.]": "",
    "[Dozent]": "",
    "[Dozentin]": "",
    "[Dramaturgie]": "",
    "[Drast.]": "",
    "[Drebuch]": "",
    "[Drehbuch]": "",
    "[Flöte]": "",
    "[Fotogr.]": "",
    "[Gitarre]": "",
    "[Herst.-Leitung]": "",
    "[Hrsg.]": "",
    "[Idee]": "",
    "[Ill.]": "",
    "[Inszenierung]": "",
    "[Interpret]": "",
    "[Interviewer]": "",
    "[Kamera]": "",
    "[Klavier]": "",
    "[Komp.]": "",
    "[Kostüm]": "",
    "[Kostüme]": "",
    "[Leitung]": "",
    "[Licht]": "",
    "[Malerei]": "",
    "[Mezzosopran]": "",
    "[Mitarb.]": "",
    "[Mitverf.]": "",
    "[Mitwirkende]": "",
    "[Musik]": "",
    "[Nachr.]": "",
    "[Orgel]": "",
    "[Originalkonzept]": "",
    "[Originalvorl.]": "",
    "[Originalvorlage]": "",
    "[Prod.-Leitung]": "",
    "[Prod.]": "",
    "[Prod]": "",
    "[Produzent]": "",
    "[Projektbetreuer]": "",
    "[Projektbetreuerin]": "",
    "[Recherche]": "",
    "[Red.]": "",
    "[Redaktion SWR]": "",
    "[Redaktion]": "",
    "[Reg.]": "",
    "[Regie]": "",
    "[Saxophon]": "",
    "[Schnitt]": "",
    "[Schöpfer]": "",
    "[Sopran]": "",
    "[Special-Effects]": "",
    "[Spezialeffekte]": "",
    "[Sprecher]": "",
    "[Sprecherin]": "",
    "[Studiengangkoordinator]": "",
    "[Studiengangkoordinatorin]": "",
    "[Studiengangskoordinator]": "",
    "[Studiengangskoordinatorin]": "",
    "[Szenebild]": "",
    "[Szenenbild]": "",
    "[Szenerie]": "",
    "[Szenographie]": "",
    "[Tenor]": "",
    "[Ton]": "",
    "[Tonschnitt]": "",
    "[Violine]": "",
    "[Violoncello]": "",
    "[Visual Effects]": "",
    "[Vorl.]": "",
    "[Vorl]": "",
    "[Vorlage]": "",
    "[Vorr.]": "",
    "[Zeichner]": "",
    "[darst.]": "",
    "[gefeierte Pers.]": "",
    "[guitar]": "",
    "[schnitt]": "",
    "[vocal]": "",
    "[Übers.]": "",
    # Below terms are actually ok, as per:
    # http://www.loc.gov/marc/relators/relaterm.html
    "abr": "abr",
    "acp": "acp",
    "act": "act",
    "adi": "adi",
    "adp": "adp",
    "aft": "aft",
    "anl": "anl",
    "anm": "anm",
    "ann": "ann",
    "ant": "ant",
    "ape": "ape",
    "apl": "apl",
    "app": "app",
    "aqt": "aqt",
    "arc": "arc",
    "ard": "ard",
    "arr": "arr",
    "art": "art",
    "asg": "asg",
    "asn": "asn",
    "ato": "ato",
    "att": "att",
    "auc": "auc",
    "aud": "aud",
    "aui": "aui",
    "aus": "aus",
    "aut": "aut",
    "bdd": "bdd",
    "bjd": "bjd",
    "bkd": "bkd",
    "bkp": "bkp",
    "blw": "blw",
    "bnd": "bnd",
    "bpd": "bpd",
    "brd": "brd",
    "brl": "brl",
    "bsl": "bsl",
    "cas": "cas",
    "ccp": "ccp",
    "chr": "chr",
    "cli": "cli",
    "cll": "cll",
    "clr": "clr",
    "clt": "clt",
    "cmm": "cmm",
    "cmp": "cmp",
    "cmt": "cmt",
    "cnd": "cnd",
    "cng": "cng",
    "cns": "cns",
    "coe": "coe",
    "col": "col",
    "com": "com",
    "con": "con",
    "cor": "cor",
    "cos": "cos",
    "cot": "cot",
    "cou": "cou",
    "cov": "cov",
    "cpc": "cpc",
    "cpe": "cpe",
    "cph": "cph",
    "cpl": "cpl",
    "cpt": "cpt",
    "cre": "cre",
    "crp": "crp",
    "crr": "crr",
    "crt": "crt",
    "csl": "csl",
    "csp": "csp",
    "cst": "cst",
    "ctb": "ctb",
    "cte": "cte",
    "ctg": "ctg",
    "ctr": "ctr",
    "cts": "cts",
    "ctt": "ctt",
    "cur": "cur",
    "cwt": "cwt",
    "dbp": "dbp",
    "dfd": "dfd",
    "dfe": "dfe",
    "dft": "dft",
    "dgg": "dgg",
    "dgs": "dgs",
    "dis": "dis",
    "dln": "dln",
    "dnc": "dnc",
    "dnr": "dnr",
    "dpc": "dpc",
    "dpt": "dpt",
    "drm": "drm",
    "drt": "drt",
    "dsr": "dsr",
    "dst": "dst",
    "dtc": "dtc",
    "dte": "dte",
    "dtm": "dtm",
    "dto": "dto",
    "dub": "dub",
    "edc": "edc",
    "edm": "edm",
    "edt": "edt",
    "egr": "egr",
    "elg": "elg",
    "elt": "elt",
    "eng": "eng",
    "enj": "enj",
    "etr": "etr",
    "evp": "evp",
    "exp": "exp",
    "fac": "fac",
    "fds": "fds",
    "fld": "fld",
    "flm": "flm",
    "fmd": "fmd",
    "fmk": "fmk",
    "fmo": "fmo",
    "fmp": "fmp",
    "fnd": "fnd",
    "fpy": "fpy",
    "frg": "frg",
    "gis": "gis",
    "grt": "grt",
    "his": "his",
    "hnr": "hnr",
    "hst": "hst",
    "ill": "ill",
    "ilu": "ilu",
    "ins": "ins",
    "inv": "inv",
    "isb": "isb",
    "itr": "itr",
    "ive": "ive",
    "ivr": "ivr",
    "jud": "jud",
    "jug": "jug",
    "lbr": "lbr",
    "lbt": "lbt",
    "ldr": "ldr",
    "led": "led",
    "lee": "lee",
    "lel": "lel",
    "len": "len",
    "let": "let",
    "lgd": "lgd",
    "lie": "lie",
    "lil": "lil",
    "lit": "lit",
    "lsa": "lsa",
    "lse": "lse",
    "lso": "lso",
    "ltg": "ltg",
    "lyr": "lyr",
    "mcp": "mcp",
    "mdc": "mdc",
    "med": "med",
    "mfp": "mfp",
    "mfr": "mfr",
    "mod": "mod",
    "mon": "mon",
    "mrb": "mrb",
    "mrk": "mrk",
    "msd": "msd",
    "mte": "mte",
    "mtk": "mtk",
    "mus": "mus",
    "nrt": "nrt",
    "opn": "opn",
    "org": "org",
    "orm": "orm",
    "osp": "osp",
    "oth": "oth",
    "own": "own",
    "pan": "pan",
    "pat": "pat",
    "pbd": "pbd",
    "pbl": "pbl",
    "pdr": "pdr",
    "pfr": "pfr",
    "pht": "pht",
    "plt": "plt",
    "pma": "pma",
    "pmn": "pmn",
    "pop": "pop",
    "ppm": "ppm",
    "ppt": "ppt",
    "pra": "pra",
    "prc": "prc",
    "prd": "prd",
    "pre": "pre",
    "prf": "prf",
    "prg": "prg",
    "prm": "prm",
    "prn": "prn",
    "pro": "pro",
    "prp": "prp",
    "prs": "prs",
    "prt": "prt",
    "prv": "prv",
    "pta": "pta",
    "pte": "pte",
    "ptf": "ptf",
    "pth": "pth",
    "ptt": "ptt",
    "pup": "pup",
    "rbr": "rbr",
    "rcd": "rcd",
    "rce": "rce",
    "rcp": "rcp",
    "rdd": "rdd",
    "red": "red",
    "ren": "ren",
    "res": "res",
    "rev": "rev",
    "rpc": "rpc",
    "rps": "rps",
    "rpt": "rpt",
    "rpy": "rpy",
    "rse": "rse",
    "rsg": "rsg",
    "rsp": "rsp",
    "rsr": "rsr",
    "rst": "rst",
    "rth": "rth",
    "rtm": "rtm",
    "sad": "sad",
    "sce": "sce",
    "scl": "scl",
    "scr": "scr",
    "sds": "sds",
    "sec": "sec",
    "sgd": "sgd",
    "sgn": "sgn",
    "sht": "sht",
    "sll": "sll",
    "sng": "sng",
    "spk": "spk",
    "spn": "spn",
    "spy": "spy",
    "srv": "srv",
    "std": "std",
    "stg": "stg",
    "stl": "stl",
    "stm": "stm",
    "stn": "stn",
    "str": "str",
    "tcd": "tcd",
    "tch": "tch",
    "ths": "ths",
    "tld": "tld",
    "tlp": "tlp",
    "trc": "trc",
    "trl": "trl",
    "tyd": "tyd",
    "tyg": "tyg",
    "uvp": "uvp",
    "vac": "vac",
    "vdg": "vdg",
    "wac": "wac",
    "wal": "wal",
    "wam": "wam",
    "wat": "wat",
    "wdc": "wdc",
    "wde": "wde",
    "win": "win",
    "wit": "wit",
    "wpr": "wpr",
    "wst": "wst",
}
