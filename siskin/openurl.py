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

def update_if_key_exists(t, tkey, s, skey, first=True):
    """
    Update dictionary t and set t[tkey] to s[skey] exists and is not None. If
    the value in question is a sequence, first controls, whether only the first
    element should be taken.
    """
    if s.get(skey) is not None:
        if first and isinstance(s[skey], (list, tuple)):
            if len(s[skey]) > 0:
                t[tkey] = s[skey][0]
        else:
            t[tkey] = s[skey]

def openurl_from_intermediateschema(doc, rfr_id='www.ub.uni-leipzig.de'):
    """
    Given a intermediate schema document, generate an OpenURL URL to be used
    with a link resolver.

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
    update_if_key_exists(params, 'rft.title', doc, 'rft.atitle')
    update_if_key_exists(params, 'rft.date', doc, 'rft.date')
    update_if_key_exists(params, 'rft.language', doc, 'languages')
    if doc.get('rft.place') is not None:
        params['rft.place'] = ', '.join(doc.get('rft.place'))

    genre = doc.get('rft.genre', 'article')
    if genre == 'proceeding':
        genre = 'article'

    # TODO(miku): genre specific handling: book, article, jounral, default
    if genre == 'book':
        params['rft_val_fmt'] = 'info:ofi/fmt:kev:mtx:book'
        params['rft.genre'] = 'book'
        params['rft.btitle'] = params['rft.title']
        del params['rft.title']

        update_if_key_exists(params, 'rft_id', doc, 'finc.record_id')
        update_if_key_exists(params, 'rft.btitle', doc, 'rft.btitle')
        update_if_key_exists(params, 'rft.atitle', doc, 'rft.atitle')
        update_if_key_exists(params, 'rft.edition', doc, 'rft.edition')
        update_if_key_exists(params, 'rft.isbn', doc, 'rft.isbn')
        update_if_key_exists(params, 'rft.issn', doc, 'rft.issn')
        update_if_key_exists(params, 'rft.eissn', doc, 'rft.eissn')
        update_if_key_exists(params, 'rft.volume', doc, 'rft.volume')
        update_if_key_exists(params, 'rft.spage', doc, 'rft.spage')
        update_if_key_exists(params, 'rft.epage', doc, 'rft.epage')
        update_if_key_exists(params, 'rft.pages', doc, 'rft.pages')
        update_if_key_exists(params, 'rft.tpages', doc, 'rft.tpages')
        update_if_key_exists(params, 'rft.issue', doc, 'rft.issue')
        update_if_key_exists(params, 'bici', doc, 'bici')
        update_if_key_exists(params, 'rft.series', doc, 'rft.series')

        authors = doc.get('authors', [])
        if len(authors) > 0:
            author = authors[0]
            # TODO(miku): complete author

    return params

