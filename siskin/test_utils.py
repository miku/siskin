import io
import json
import os
import tempfile

import requests
import responses

import marcx
import pymarc
from siskin.utils import (SetEncoder, URLCache, dictcheck,
                          get_task_import_cache, load_set, marc_build_imprint,
                          marc_clean_record, marc_clean_subfields, nwise,
                          random_string, scrape_html_listing, xmlstream)


def test_set_encoder_dumps():
	assert json.dumps({'x': {0, 1, 2}}, cls=SetEncoder) == '{"x": [0, 1, 2]}'

def test_dictcheck():
    assert dictcheck({"name": "x"}, contains=["name"]) is True
    assert dictcheck({"name": "x"}, contains=["name"], absent=["somekey"]) is True
    assert dictcheck({"name": "x", "somekey": 123},
                     contains=["name"], absent=["somekey"]) is False
    assert dictcheck({"somekey": None}, absent=["somekey"]) is True
    assert dictcheck({}, absent=["somekey"]) is True


def test_nwise():
    assert list(nwise(range(4))) == [(0, 1), (2, 3)]
    assert list(nwise(range(4), n=2)) == [(0, 1), (2, 3)]
    assert list(nwise(range(4), n=3)) == [(0, 1, 2), (3,)]
    assert list(nwise(range(4), n=10)) == [(0, 1, 2, 3)]
    assert list(nwise([], n=10)) == []

def test_random_string():
    assert len(random_string()) == 16
    assert len(random_string(length=1000)) == 1000
    assert len(random_string(length=-1)) == 0
    assert random_string(length=-100) == ''
    for char in ' \t\n.:,;#~+-?=[]()/&%$"!':
        assert char not in random_string()

def test_get_task_import_cache():
    mapping, path = get_task_import_cache()
    assert os.path.exists(path)
    assert isinstance(mapping, dict)

def test_load_set():
    assert load_set(io.StringIO(u"")) == set()
    assert load_set(io.StringIO(u"1\n1\n1\n")) == {"1"}
    assert load_set(io.StringIO(u"1\n    \n2\n")) == {"1", "2"}
    assert load_set(io.StringIO(u"1\n2\n3\n")) == {"1", "2", "3"}
    assert load_set(io.StringIO(u"1\n2\n3\n"), func=int) == {1, 2, 3}

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write("1\n2\n")
    with open(tf.name) as handle:
        assert load_set(handle) == {"1", "2"}
    assert load_set(tf.name) == {"1", "2"}
    os.remove(tf.name)

def test_get_cache_file(tmpdir):
    cache = URLCache(directory=str(tmpdir))
    fn = cache.get_cache_file("http://x.com")
    assert fn.startswith(str(tmpdir))

@responses.activate
def test_scrape_html_listing():
    responses.add(responses.GET, 'http://fake.com/1',
                  body='<html></html>', status=200)
    resp = requests.get('http://fake.com/1')
    assert scrape_html_listing('http://fake.com/1') == []

    body = """
    <!doctype html><title>Index of /archlinux/iso/2018.12.01</title><h1>Index
    of /archlinux/iso/2018.12.01</h1><table><tr><th valign=top><img
    src=/icons/blank.gif alt=[ICO]><th><a href="?C=N;O=D">Name</a><th><a
    href="?C=M;O=A">Last modified</a><th><a href="?C=S;O=A">Size</a><th><a
    href="?C=D;O=A">Description</a><tr><th colspan=5><hr><tr><td valign=top><img
    src=/icons/back.gif alt=[PARENTDIR]><td><a href=/archlinux/iso/>Parent
    Directory</a><td>&nbsp;<td align=right>-<td>&nbsp;<tr><td valign=top><img
    src=/icons/folder.gif alt=[DIR]><td><a href=arch/>arch/</a><td
    align=right>2018-12-01 11:16<td align=right>-<td>&nbsp;<tr><td valign=top><img
    src=/icons/unknown.gif alt="[   ]"><td><a
    href=archlinux-2018.12.01-x86_64.iso>archlinux-2018.12.01-x86_64.iso</a><td
    align=right>2018-12-01 11:17<td align=right>588M<td>&nbsp;<tr><td
    valign=top><img src=/icons/unknown.gif alt="[   ]"><td><a
    href=archlinux-2018.12.01-x86_64.iso.sig>archlinux-2018.12.01-x86_64.iso.sig</a><td
    align=right>2018-12-01 11:18<td align=right>310<td>&nbsp;<tr><td
    valign=top><img src=/icons/unknown.gif alt="[   ]"><td><a
    href=archlinux-2018.12.01-x86_64.iso.torrent>archlinux-2018.12.01-x86_64.iso.torrent</a><td
    align=right>2018-12-01 11:18<td align=right>38K<td>&nbsp;<tr><td
    valign=top><img src=/icons/compressed.gif alt="[   ]"><td><a
    href=archlinux-bootstrap-2018.12.01-x86_64.tar.gz>archlinux-bootstrap-2018.12.01-x86_64.tar.gz</a><td
    align=right>2018-12-01 11:18<td align=right>141M<td>&nbsp;<tr><td
    valign=top><img src=/icons/unknown.gif alt="[   ]"><td><a
    href=archlinux-bootstrap-2018.12.01-x86_64.tar.gz.sig>archlinux-bootstrap-2018.12.01-x86_64.tar.gz.sig</a><td
    align=right>2018-12-01 11:18<td align=right>310<td>&nbsp;<tr><td
    valign=top><img src=/icons/text.gif alt=[TXT]><td><a
    href=md5sums.txt>md5sums.txt</a><td align=right>2018-12-01 11:18<td
    align=right>145<td>&nbsp;<tr><td valign=top><img src=/icons/text.gif
    alt=[TXT]><td><a href=sha1sums.txt>sha1sums.txt</a><td align=right>2018-12-01
    11:18<td align=right>161<td>&nbsp;<tr><th
    colspan=5><hr></table><address>Apache/2.4.25 (Debian) Server at
    ftp.halifax.rwth-aachen.de Port 80</address>
    """

    expected = [
        'http://fake.com/archlinux-2018.12.01-x86_64.iso',
        'http://fake.com/archlinux-2018.12.01-x86_64.iso.sig',
        'http://fake.com/archlinux-2018.12.01-x86_64.iso.torr',
        'http://fake.com/archlinux-bootstrap-2018.12.01-x86_64.tar.gz',
        'http://fake.com/archlinux-bootstrap-2018.12.01-x86_64.tar.gz.sig',
        'http://fake.com/md5sums.txt',
        'http://fake.com/sha1sums.txt',
    ]

    responses.add(responses.GET, 'http://fake.com/1',
                  body=body, status=200)
    resp = requests.get('http://fake.com/1')
    assert scrape_html_listing('http://fake.com/1') == expected

def test_xmlstream():
    with tempfile.NamedTemporaryFile('w', delete=False) as handle:
        handle.write("""<a><b>C</b><b>C</b></a>""")

    filename = handle.name
    assert [v for v in xmlstream(filename, "b")] == [b'<b>C</b>', b'<b>C</b>']
    os.remove(filename)

def test_marc_clean_subfields():
    record = marcx.Record()
    record.add("001", data="1234")
    record.add("245", a="", b="ok")

    assert record.strict == True

    # Behind the scenes, marcx will not add empty subfield values
    # (https://git.io/fjIWU).
    assert marc_clean_subfields(record["245"], inplace=False) == ['b', 'ok']
    assert record["245"].subfields == ['b', 'ok']

    assert marc_clean_subfields(record["245"], inplace=True) is None
    assert record["245"].subfields == ['b', 'ok']

    # Test pymarc record.
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag='001', data='1234'))
    record.add_field(pymarc.Field(tag='245', indicators=['0', '1'],
                                  subfields=['a', '', 'b', 'ok']))

    assert len(record.get_fields()) == 2

    assert marc_clean_subfields(record["245"], inplace=False) == ['b', 'ok']
    assert record["245"].subfields == ['a', '', 'b', 'ok']
    assert marc_clean_subfields(record["245"], inplace=True) is None
    assert record["245"].subfields == ['b', 'ok']

def test_marc_clean_record():
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag='001', data='1234'))
    record.add_field(pymarc.Field(tag='245', indicators=['0', '1'],
                                  subfields=['a', '', 'b', 'ok']))

    assert len(record.get_fields()) == 2

    assert record["245"].subfields == ['a', '', 'b', 'ok']
    marc_clean_record(record)
    assert record["245"].subfields == ['b', 'ok']

def test_marc_build_imprint():
    assert marc_build_imprint() == ['a', '', 'b', '', 'c', '']
    assert marc_build_imprint(place="A") == ['a', 'A', 'b', '', 'c', '']
    assert marc_build_imprint(publisher="B") == ['a', '', 'b', 'B', 'c', '']
    assert marc_build_imprint(year="C") == ['a', '', 'b', '', 'c', 'C']
    assert marc_build_imprint(place="A", publisher="B") == ['a', 'A : ', 'b', 'B', 'c', '']
    assert marc_build_imprint(place="A", year="C") == ['a', 'A', 'b', ', ', 'c', 'C']
    assert marc_build_imprint(publisher="B", year="C") == ['a', '', 'b', 'B, ', 'c', 'C']
    assert marc_build_imprint(place="A", publisher="B", year="C") == ['a', 'A : ', 'b', 'B, ', 'c', 'C']
