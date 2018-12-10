import pymarc
from siskin.conversions import imslp_xml_to_marc


def test_imslp_xml_to_marc():
    example = """<?xml version="1.0"?>
    <document docID="imslpvalsskramstadhans">
      <localClass localClassName="col">imslp</localClass>
      <localClass localClassName="vifa">vifamusik</localClass>
      <identifier identifierEncodingSchema="originalID">valsskramstadhans</identifier>
      <creator>
        <mainForm>Skramstad, Hans</mainForm>
      </creator>
      <title>Vals for pianoforte</title>
      <subject>
        <mainForm>Romantic</mainForm>
      </subject>
      <music_arrangement_of>Piano</music_arrangement_of>
      <url urlEncodingSchema="originalDetailView">http://imslp.org/wiki/Vals_(Skramstad,_Hans)</url>
      <vifatype>Internetressource</vifatype>
      <fetchDate>2018-04-25T00:00:00.01Z</fetchDate>
      <vifaxml><![CDATA[<document docID="imslpvalsskramstadhans"><localClass
          localClassName="col">imslp</localClass><localClass
          localClassName="vifa">vifamusik</localClass><identifier
          identifierEncodingSchema="originalID">valsskramstadhans</identifier><creator><mainForm>Skramstad,
          Hans</mainForm></creator><title>Vals for
          pianoforte</title><subject><mainForm>Romantic</mainForm></subject><music_arrangement_of>Piano</music_arrangement_of><url
          urlEncodingSchema="originalDetailView">http://imslp.org/wiki/Vals_(Skramstad,_Hans)</url><vifatype>Internetressource</vifatype></document>]]></vifaxml>
    </document>
    """

    result = imslp_xml_to_marc(example)

    assert result is not None
    assert isinstance(result, pymarc.Record)

    assert result["001"].value() == "finc-15-dmFsc3NrcmFtc3RhZGhhbnM"
    assert result["100"]["a"] == "Skramstad, Hans"
    assert result["245"]["a"] == "Vals for pianoforte"
    assert result["856"]["u"] == "http://imslp.org/wiki/Vals_(Skramstad,_Hans)"
