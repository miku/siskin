# coding: utf-8
# pylint: disable=C0301,E1101,C0103

# Copyright 2021 by Leipzig University Library, http://ub.uni-leipzig.de
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

from __future__ import print_function

import json
import logging
import sys
import tempfile
import time

import requests

import xmltodict

logger = logging.getLogger("siskin")


def pqdt_harvest(sleep=2,
                 user_agent="Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0",
                 endpoint="https://pqdtoai.proquest.com/OAIHandler"):
    """
    Harvest PQDT and return path to tempfile with raw response XML.

    Background: Both, metha and sickle report bad resumption tokens.

    Oh, what an ugly endpoint; the interface meant to be machine readable
    yields a HTTP 500 when requested with a machine user agent. One may even
    see the conversation that led to this feature.

    Also: user agent header parsing seems case sensitive; recommended
    reading: https://datatracker.ietf.org/doc/html/rfc2616#section-4.2.

    Also: a data endpoint that needs session management, nice.
    """
    link = "{}?metadataPrefix=oai_dc&verb=ListRecords".format(endpoint)
    cookies = None

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        while True:
            logger.debug(link)
            resp = requests.get(link, cookies=cookies, headers={"User-Agent": user_agent})
            if resp.status_code >= 400:
                raise RuntimeError('harvest failed: {} {}'.format(link, resp.status_code))
            logger.debug("retrieved {} {}".format(len(resp.text), resp.text[:50], "..."))
            dd = xmltodict.parse(resp.text)
            try:
                tokenTag = dd["OAI-PMH"]["ListRecords"]["resumptionToken"]
                cursor = tokenTag["@cursor"]
                size = tokenTag["@completeListSize"]
                token = tokenTag["#text"]
            except KeyError as exc:
                logger.debug(json.dumps({
                    "text": resp.text,
                    "status": resp.status_code,
                }))
                raise RuntimeError('failed to fetch pqdt, unexpected text: {}'.format(resp.text))
            else:
                if not token or int(cursor) >= int(size):
                    break
            if resp.cookies:
                cookies = resp.cookies
            link = r"{}?resumptionToken={}&verb=ListRecords".format(endpoint, token)
            logger.debug("{} {} {}".format(cursor, size, token))
            time.sleep(sleep)

    return tf.name


if __name__ == '__main__':
    formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)-8s] %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)

    filename = pqdt_harvest()
    print(filename, file=sys.stderr)
