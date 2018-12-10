import json

import pymarc
from siskin.conversions import imslp_xml_to_marc


def test_imslp_xml_to_marc():
    example = """
    <?xml version="1.0"?>
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

    want = """
    {
      "fields": [
        {
          "001": "finc-15-dmFsc3NrcmFtc3RhZGhhbnM"
        },
        {
          "007": "cr"
        },
        {
          "100": {
            "ind1": " ",
            "subfields": [
              {
                "a": "Skramstad, Hans"
              },
              {
                "e": "cmp"
              }
            ],
            "ind2": " "
          }
        },
        {
          "245": {
            "ind1": " ",
            "subfields": [
              {
                "a": "Vals for pianoforte"
              }
            ],
            "ind2": " "
          }
        },
        {
          "590": {
            "ind1": " ",
            "subfields": [
              {
                "a": "Romantic"
              },
              {
                "b": "Piano"
              }
            ],
            "ind2": " "
          }
        },
        {
          "689": {
            "ind1": " ",
            "subfields": [
              {
                "a": "Piano"
              }
            ],
            "ind2": " "
          }
        },
        {
          "689": {
            "ind1": " ",
            "subfields": [
              {
                "a": "Romantic"
              }
            ],
            "ind2": " "
          }
        },
        {
          "700": {
            "ind1": " ",
            "subfields": [
              {
                "e": "ctb"
              }
            ],
            "ind2": " "
          }
        },
        {
          "856": {
            "ind1": " ",
            "subfields": [
              {
                "q": "text/html"
              },
              {
                "u": "http://imslp.org/wiki/Vals_(Skramstad,_Hans)"
              },
              {
                "3": "Petrucci Musikbibliothek"
              }
            ],
            "ind2": " "
          }
        },
        {
          "970": {
            "ind1": " ",
            "subfields": [
              {
                "c": "PN"
              }
            ],
            "ind2": " "
          }
        },
        {
          "980": {
            "ind1": " ",
            "subfields": [
              {
                "a": "valsskramstadhans"
              },
              {
                "c": "Petrucci Musikbibliothek"
              },
              {
                "b": "15"
              }
            ],
            "ind2": " "
          }
        }
      ],
      "leader": "     ncs  22        450 "
    }
    """

    assert result.as_dict() == json.loads(want)
