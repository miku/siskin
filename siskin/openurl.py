# coding: utf-8
# pylint: disable=F0401,C0111,W0232,E1101,E1103,C0301,C0103,W0614,W0401,E0202

# Copyright 2019 by Leipzig University Library, http://ub.uni-leipzig.de
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
Helper functions for dealing with OpenURL, refs #5163.
"""

from six.moves.urllib.parse import urlencode

def update_on_value(t, tkey, value, first=True):
    """
    Update dictionary t and set t[tkey] to s[skey] exists and is not None. If
    the value in question is a sequence, first controls, whether only the first
    element should be taken.
    """
    if value is not None:
        if first and isinstance(value, (list, tuple)):
            if len(value) > 0:
                t[tkey] = value[0]
        else:
            t[tkey] = value

def openurl_parameters_from_intermediateschema(doc, rfr_id='www.ub.uni-leipzig.de'):
    """
    Given a intermediate schema dictionary, generate OpenURL parameters to be
    used with a (REDI) link resolver.

    Roughly like vufind13/trunk/web/RecordDrivers/AIRecord.php #L1032-1336.
    """
    if not rfr_id:
        raise ValueError('rfr_id must be set')

    if not doc:
        return {}

    params = {
        'url_ver': 'Z39.88-2004',
        'ctx_ver': 'Z39.88-2004',
        'ctx_enc': 'info:ofi/enc:UTF-8',
        'rfr_id': 'info:sid/{}:generator'.format(rfr_id),
    }
    update_on_value(params, 'rft.title', doc.get('rft.atitle'))
    update_on_value(params, 'rft.date', doc.get('rft.date'))
    update_on_value(params, 'rft.language', doc.get('languages'))

    if doc.get('rft.place') is not None:
        params['rft.place'] = ', '.join(doc.get('rft.place', []))

    genre = doc.get('rft.genre', 'article')
    if genre == 'proceeding':
        genre = 'article'

    if genre == 'book':
        params['rft_val_fmt'] = 'info:ofi/fmt:kev:mtx:book'
        params['rft.genre'] = 'book'
        params['rft.btitle'] = params['rft.title']
        del params['rft.title']

        update_on_value(params, 'rft_id', doc.get('finc.record_id'))
        update_on_value(params, 'rft.btitle', doc.get('rft.btitle'))
        update_on_value(params, 'rft.atitle', doc.get('rft.atitle'))
        update_on_value(params, 'rft.edition', doc.get('rft.edition'))
        update_on_value(params, 'rft.isbn', doc.get('rft.isbn'))
        update_on_value(params, 'rft.issn', doc.get('rft.issn'))
        update_on_value(params, 'rft.eissn', doc.get('rft.eissn'))
        update_on_value(params, 'rft.volume', doc.get('rft.volume'))
        update_on_value(params, 'rft.spage', doc.get('rft.spage'))
        update_on_value(params, 'rft.epage', doc.get('rft.epage'))
        update_on_value(params, 'rft.pages', doc.get('rft.pages'))
        update_on_value(params, 'rft.tpages', doc.get('rft.tpages'))
        update_on_value(params, 'rft.issue', doc.get('rft.issue'))
        update_on_value(params, 'bici', doc.get('bici'))
        update_on_value(params, 'rft.series', doc.get('rft.series'))

        authors = doc.get('authors', [])
        if len(authors) > 0:
            author = authors[0]
            update_on_value(params, 'rft.au', author.get('rft.au'))
            update_on_value(params, 'rft.aucorp', author.get('rft.aucorp'))
            update_on_value(params, 'rft.aufirst', author.get('rft.aufirst'))
            update_on_value(params, 'rft.auinit', author.get('rft.auinit'))
            update_on_value(params, 'rft.auinit1', author.get('rft.auinit1'))
            update_on_value(params, 'rft.aulast', author.get('rft.aulast'))
            update_on_value(params, 'rft.ausuffix', author.get('rft.ausuffix'))

        update_on_value(params, 'rft.genre', doc.get('rft.genre'))
        if doc.get('doi'):
            params['rft_id'] = 'info:doi/{}'.format(doc.get('doi'))

        update_on_value(params, 'rft.pub', doc.get('rft.pub'))

    elif genre == 'article':
        del params['rft.title']

        update_on_value(params, 'rft_id', doc.get('finc.record_id'))
        update_on_value(params, 'rft.atitle', doc.get('rft.atitle'))
        update_on_value(params, 'rft.jtitle', doc.get('rft.jtitle'))
        update_on_value(params, 'rft.stitle', doc.get('rft.stitle'))
        update_on_value(params, 'rft.date', doc.get('rft.date'))
        update_on_value(params, 'rft.issn', doc.get('rft.issn'))
        update_on_value(params, 'rft.eissn', doc.get('rft.eissn'))
        update_on_value(params, 'rft.ssn', doc.get('rft.ssn'))
        update_on_value(params, 'rft.volume', doc.get('rft.volume'))
        update_on_value(params, 'rft.spage', doc.get('rft.spage'))
        update_on_value(params, 'rft.epage', doc.get('rft.epage'))
        update_on_value(params, 'rft.pages', doc.get('rft.pages'))
        update_on_value(params, 'rft.issue', doc.get('rft.issue'))
        update_on_value(params, 'rft.coden', doc.get('rft.coden'))
        update_on_value(params, 'rft.artnum', doc.get('rft.artnum'))
        update_on_value(params, 'sici', doc.get('sici'))
        update_on_value(params, 'rft.chron', doc.get('rft.chron'))
        update_on_value(params, 'rft.quarter', doc.get('rft.quarter'))
        update_on_value(params, 'rft.part', doc.get('rft.part'))

        authors = doc.get('authors', [])
        if len(authors) > 0:
            author = authors[0]
            update_on_value(params, 'rft.au', author.get('rft.au'))
            update_on_value(params, 'rft.aucorp', author.get('rft.aucorp'))
            update_on_value(params, 'rft.aufirst', author.get('rft.aufirst'))
            update_on_value(params, 'rft.auinit', author.get('rft.auinit'))
            update_on_value(params, 'rft.auinit1', author.get('rft.auinit1'))
            update_on_value(params, 'rft.aulast', author.get('rft.aulast'))
            update_on_value(params, 'rft.ausuffix', author.get('rft.ausuffix'))

        update_on_value(params, 'rft.genre', doc.get('rft.genre'))
        if doc.get('doi'):
            params['rft_id'] = 'info:doi/{}'.format(doc.get('doi'))

    elif genre == 'journal':
        update_on_value(params, 'rft.issn', doc.get('rft.issn'))
    else:
        params['rft_val_fmt'] = 'info:ofi/fmt:kev:mtx:book'
        authors = doc.get('authors', [])
        if len(authors) > 0:
            update_on_value(params, 'rft.creator', doc.get('rft.au'))
        update_on_value(params, 'rft.pub', doc.get('rft.pub'))
        update_on_value(params, 'rft.format', doc.get('finc.format'))
        update_on_value(params, 'rft.language', doc.get('languages'))

    return params

def openurl_link_from_intermediateschema(doc, base='http://www.redi-bw.de/links/ubl?rl_site=ubl&',
                                         rfr_id='www.ub.uni-leipzig.de'):
    params = openurl_parameters_from_intermediateschema(doc, rfr_id=rfr_id)
    return '{}{}'.format(base, urlencode(params))
