# coding: utf-8

"""
A few tests for the mab module, refs #8392.
"""

import os
import tempfile

import pytest

from siskin.mab import MabXMLFile

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# Example MAB file with one record.
sample_file_one = """<?xml version="1.0" encoding="UTF-8"?>
<datei xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd http://www.d-nb.de/standardisierung/formate/mabxml-1.xsd">
<datensatz typ="u" status="n" mabVersion="M2.0" xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd"><feld nr="001" ind=" ">10598985</feld><feld nr="002" ind="a">20070926</feld><feld nr="003" ind=" ">20160311</feld><feld nr="004" ind=" ">20190110</feld><feld nr="010" ind=" ">10390691</feld><feld nr="020" ind="b">ITKGDK500164319</feld><feld nr="030" ind=" ">e|5|xr|zc||||</feld><feld nr="050" ind=" ">a|a|||||||||||</feld><feld nr="051" ind=" ">s|||||||</feld><feld nr="070" ind="a">GDK</feld><feld nr="071" ind=" ">80</feld><feld nr="081" ind=" ">81875804</feld><feld nr="082" ind=" ">I. 9 I 61</feld><feld nr="083" ind=" ">Verfügbar</feld><feld nr="084" ind=" ">Magazin</feld><feld nr="089" ind=" ">2006</feld><feld nr="090" ind=" ">42006</feld><feld nr="335" ind=" ">FIAF 2007 Tokyo</feld><feld nr="425" ind=" ">2007</feld><feld nr="425" ind="a">2007</feld><feld nr="433" ind=" ">[ca. 300  Seiten ]</feld><feld nr="501" ind=" ">Enthält die Berichte über Aktivitäten im Jahr 2006</feld></datensatz>
</datei>
"""

# Example MAB file with two records.
sample_file_two = """<?xml version="1.0" encoding="UTF-8"?>
<datei xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd http://www.d-nb.de/standardisierung/formate/mabxml-1.xsd">
  <datensatz xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" typ="h" status="n" mabVersion="M2.0">
    <feld nr="001" ind=" ">000062947</feld>
    <feld nr="002" ind="a">20170704</feld>
    <feld nr="003" ind=" ">20170906</feld>
    <feld nr="030" ind=" ">z|zuar|z||||7</feld>
    <feld nr="036" ind="a">CN</feld>
    <feld nr="037" ind="b">eng</feld>
    <feld nr="050" ind=" ">a|a|||||||||||</feld>
    <feld nr="060" ind=" ">
      <uf code="b">txt</uf>
    </feld>
    <feld nr="100" ind=" ">Zhang, Yisheng</feld>
    <feld nr="102" ind=" ">000021597</feld>
    <feld nr="104" ind="b">Ma, Mingtu</feld>
    <feld nr="106" ind=" ">000021596</feld>
    <feld nr="331" ind=" ">Advanced High Strength Steel and Press Hardening [ICHSU2015]</feld>
    <feld nr="335" ind=" ">proceedings of the2nd International Conference on Advanced High Strength Steel and Press Hardening (ICHSU2015)</feld>
    <feld nr="359" ind=" ">Editors Yisheng Zhang (Huazhong University of Science and Technology, China), Mingtu Ma (China Automotive Engineering Research Institute, China).</feld>
    <feld nr="419" ind=" ">
      <uf code="a">Singapur</uf>
      <uf code="b">World Scientific Publishing</uf>
      <uf code="c">2016</uf>
    </feld>
    <feld nr="433" ind=" ">XV, 736 Seiten</feld>
    <feld nr="434" ind=" ">Illustrationen, Tabellen</feld>
    <feld nr="435" ind=" ">24 cm</feld>
    <feld nr="540" ind="a">ISBN 978-981-3140-61-5</feld>
    <feld nr="655" ind="e">
      <uf code="u">http://www2.stahl-online.de/Bibl-Dokumente.nsf/af8dcd050f6931fdc1257798003b85a6/227fdc3019e0cf3cc12581530032b592/$FILE/4.6745.pdf</uf>
    </feld>
    <feld nr="675" ind=" ">ICHSU 2015</feld>
    <feld nr="710" ind=" ">hochfester Stahl</feld>
    <feld nr="710" ind=" ">Blechumformen</feld>
    <feld nr="710" ind=" ">Pressh&#xE4;rten</feld>
  </datensatz>
  <datensatz>
    <!-- record #2 -->
  </datensatz>
</datei>
"""

def test_from_filelike():
    """
    Test, whether we can load from a filelike object.
    """
    filelike = StringIO(sample_file_two)
    mabf = MabXMLFile(filelike)
    assert len([_ for _ in mabf]) == 2

def test_from_file():
    """
    Test, whether we can load from a filename.
    """
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
        f.write(sample_file_two)

    mabf = MabXMLFile(f.name)
    assert len([_ for _ in mabf]) == 2

    os.remove(f.name)


def test_invalid_input():
    """
    A few invalid inputs, there are many more.
    """
    inputs = (
        """<xml></xml>""",
        """<?xml version="1.0" encoding="UTF-8"?>
        <BadRoot></BadRoot>
        """,
        """<a></a>""",
    )

    for s in inputs:
        with pytest.raises(ValueError):
            MabXMLFile(s)

def test_valid_empty_file():
    """
    Ok, but empty.
    """
    inputs = (
        """<?xml version="1.0" encoding="UTF-8"?>
        <datei></datei>
        """,
        """<?xml version="1.0" encoding="UTF-8"?>
        <datei xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd http://www.d-nb.de/standardisierung/formate/mabxml-1.xsd"></datei>
        """,
        """<?xml version="1.0" encoding="UTF-8"?>
        <datei xmlns="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ddb.de/professionell/mabxml/mabxml-1.xsd http://www.d-nb.de/standardisierung/formate/mabxml-1.xsd">
        <a></a>
        </datei>
        """
    )
    for s in inputs:
        assert MabXMLFile(s) is not None

def test_record_iteration():
    """
    Whether we can iterate over the file object.
    """
    inputs = (
        (sample_file_one, 1),
        (sample_file_two, 2),
    )
    for s, count in inputs:
        mabf = MabXMLFile(s)
        assert len([_ for _ in mabf]) == count

def test_record_field():
    """
    Access to first value in fields.
    """
    mabf = MabXMLFile(sample_file_two)
    record = mabf.next()
    assert record.field("36") is None
    assert record.field("036") == "CN"
    assert record.field("060") is None
    assert record.field("710") == "hochfester Stahl"

def test_record_fields():
    """
    Access to multiple values.
    """
    mabf = MabXMLFile(sample_file_two)
    record = mabf.next()
    assert len(record.fields("710")) == 3
    assert len(record.fields("999")) == 0

def test_record_subfield():
    """
    Also known as Unterfeld.
    """
    mabf = MabXMLFile(sample_file_two)
    record = mabf.next()
    assert record.field("419") is None
    assert record.field("419", code="c") == "2016"
    assert record.field("419", "c") == "2016"
