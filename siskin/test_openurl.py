from siskin.openurl import openurl_from_intermediateschema

def test_openurl_from_intermediateschema():
    cases = (
        ('empty doc', {}, {}),
        (
            'title only',
            {'rft.atitle': 'Hello'},
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.title': 'Hello',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'title and date',
            {
                'rft.atitle': 'Hello',
                'rft.date': '2018-10-10',
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.date': '2018-10-10',
                'rft.title': 'Hello',
                'url_ver': 'Z39.88-2004',
            },
        ),
        (
            'title and date, language',
            {
                'languages': ['eng', 'fra'],
                'rft.atitle': 'Hello',
                'rft.date': '2018-10-10',
            },
            {
                'ctx_enc': 'info:ofi/enc:UTF-8',
                'ctx_ver': 'Z39.88-2004',
                'rfr_id': 'info:sid/www.ub.uni-leipzig.de:generator',
                'rft.date': '2018-10-10',
                'rft.language': 'eng',
                'rft.title': 'Hello',
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
    )

    for _, doc, want in cases:
        result = openurl_from_intermediateschema(doc)
        assert result == want

