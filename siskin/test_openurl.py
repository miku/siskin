from siskin.openurl import openurl_parameters_from_intermediateschema


def test_openurl_from_intermediateschema():
    cases = (
        ('empty doc', {}, {}),
        (
            'title only',
            {'rft.atitle': 'empty doc'},
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.atitle': 'empty doc',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'title and date',
            {
                'rft.atitle': 'title and date',
                'rft.date': '2018-10-10',
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.date': '2018-10-10',
                'rft.atitle': 'title and date',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'title and date, language',
            {
                'languages': ['eng', 'fra'],
                'rft.atitle': 'title and date, language',
                'rft.date': '2018-10-10',
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.date': '2018-10-10',
                'rft.language': 'eng',
                'rft.atitle': 'title and date, language',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'title and date, language, book',
            {
                'languages': ['eng', 'fra'],
                'rft.atitle': 'Hello',
                'rft.date': '2018-10-10',
                'rft.genre': 'book',
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.atitle': 'Hello',
                'rft.btitle': 'Hello',
                'rft.date': '2018-10-10',
                'rft.genre': 'book',
                'rft.language': 'eng',
                'rft_val_fmt': 'info:ofi/fmt:kev:mtx:book',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'crossref-1',
            {
              "finc.format": "ElectronicArticle",
              "finc.mega_collection": [
                "Springer Science + Business Media (CrossRef)"
              ],
              "finc.id": "ai-49-aHR0cDovL2R4LmRvaS5vcmcvMTAuMTAxNi9qLm51cnguMjAwNi4wNS4wMjU",
              "finc.source_id": "49",
              "ris.type": "EJOUR",
              "rft.atitle": "An Analysis of Correlations Among 4 Outcome Scales Employed in Clinical Trials of Patients With Major Depressive Disorder",
              "rft.epage": "412",
              "rft.genre": "article",
              "rft.issn": [
                "1545-5343"
              ],
              "rft.issue": "3",
              "rft.jtitle": "NeuroRX",
              "rft.tpages": "2",
              "rft.pages": "411-412",
              "rft.pub": [
                "Springer Science + Business Media"
              ],
              "rft.date": "2006-07-01",
              "x.date": "2006-07-01T00:00:00Z",
              "rft.spage": "411",
              "rft.volume": "3",
              "authors": [
                {
                  "rft.aulast": "JIANG",
                  "rft.aufirst": "Q"
                },
                {
                  "rft.aulast": "AHMED",
                  "rft.aufirst": "S"
                },
                {
                  "rft.aulast": "PEDERSEN",
                  "rft.aufirst": "R"
                },
                {
                  "rft.aulast": "MUSGNUNG",
                  "rft.aufirst": "J"
                },
                {
                  "rft.aulast": "ENTSUAH",
                  "rft.aufirst": "R"
                }
              ],
              "doi": "10.1016/j.nurx.2006.05.025",
              "languages": [
                "eng"
              ],
              "url": [
                "http://dx.doi.org/10.1016/j.nurx.2006.05.025"
              ],
              "version": "0.9",
              "x.subjects": [
                "Pharmacology (medical)"
              ],
              "x.type": "journal-article"
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.atitle': 'An Analysis of Correlations Among 4 Outcome Scales Employed in Clinical Trials of Patients With Major Depressive Disorder',
                'rft.aufirst': 'Q',
                'rft.aulast': 'JIANG',
                'rft.date': '2006-07-01',
                'rft.epage': '412',
                'rft.genre': 'article',
                'rft.issn': '1545-5343',
                'rft.issue': '3',
                'rft.jtitle': 'NeuroRX',
                'rft.language': 'eng',
                'rft.pages': '411-412',
                'rft.spage': '411',
                'rft.volume': '3',
                'rft_id': 'info:doi/10.1016/j.nurx.2006.05.025',
                'url_ver': 'Z39.88-2004',
            },
        ),
    )

    for _, doc, want in cases:
        result = openurl_parameters_from_intermediateschema(doc)
        assert result == want
