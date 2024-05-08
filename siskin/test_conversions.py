import json

import pymarc

from siskin.conversions import de_listify, imslp_xml_to_marc, osf_to_intermediate


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


def test_de_listify():
    cases = (
        (None, None),
        ("", ""),
        ([], None),
        ({1, 2, 3}, 1),
        ([1, 2, 3], 1),
    )
    for v, expected in cases:
        assert de_listify(v) == expected


def test_osf_to_intermediate():
    cases = (
        (None, None),
        ({}, None),
        (
            json.loads("""
            {
              "id": "egcsk",
              "type": "preprints",
              "attributes": {
                "date_created": "2021-07-19T07:32:33.252615",
                "date_modified": "2021-07-19T07:42:12.725457",
                "date_published": "2021-07-19T07:41:43.501204",
                "original_publication_date": "2021-02-28T17:00:00",
                "doi": null,
                "title": "Konsep Allah Dalam Teologi Proses",
                "description": "Penulisan karya ilmiah ini dikhususkan untuk membahas mengenai Allah yang dirumuskan dalam teologi proses, yang dicetuskan oleh Alfred Whitehead. Dalam bagian bagian ini penulis menyajikan konsep Allah dalam teologi proses dan bagaimana tanggapan terhadap konsep tersebut secara Alkitabiah Metode penelitian, penulis menggunakan pendekatan metode penelitian kualitatif analisis deskriptif, dengan pendekatan literatur dan tergolong dalam penelitian perpustakaan. Konsep Allah menurut teologi proses adalah Allah yang berproses, tidak berpribadi dan tidak memiliki kedaulatan absolut. Namun pandangan tentang Allah dalam teologi proses adalah suatu kumpulan pengalaman pribadi dan dijadikan sebagai suatu konsep dalam pemikiran manusia. Tanggapan tersebut menunjukan perbandingan dari pola pikir teologi proses mengenai Allah yang menyimpang dan mengarahkan seseorang dalam memahami konsep Allah yang benar sesuai dengan pernyataan Allah m",
                "is_published": true,
                "is_preprint_orphan": false,
                "license_record": {
                  "copyright_holders": [
                    ""
                  ],
                  "year": "2021"
                },
                "tags": [
                  "Gambar",
                  "Respon",
                  "Teologi Proses",
                  "Tuhan"
                ],
                "preprint_doi_created": "2021-07-19T07:42:12.695116",
                "date_withdrawn": null,
                "current_user_permissions": [],
                "public": true,
                "reviews_state": "accepted",
                "date_last_transitioned": "2021-07-19T07:41:43.501204",
                "has_coi": false,
                "conflict_of_interest_statement": null,
                "has_data_links": "no",
                "why_no_data": null,
                "data_links": [],
                "has_prereg_links": "no",
                "why_no_prereg": null,
                "prereg_links": [],
                "prereg_link_info": "",
                "subjects": [
                  [
                    {
                      "id": "584240da54be81056cecaab4",
                      "text": "Arts and Humanities"
                    },
                    {
                      "id": "584240da54be81056cecaa9c",
                      "text": "Religion"
                    },
                    {
                      "id": "584240da54be81056cecaaf5",
                      "text": "Christianity"
                    }
                  ]
                ]
              },
              "relationships": {
                "contributors": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/contributors/",
                      "meta": {}
                    }
                  }
                },
                "bibliographic_contributors": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/bibliographic_contributors/",
                      "meta": {}
                    }
                  }
                },
                "citation": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/citation/",
                      "meta": {}
                    }
                  },
                  "data": {
                    "id": "egcsk",
                    "type": "preprints"
                  }
                },
                "identifiers": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/identifiers/",
                      "meta": {}
                    }
                  }
                },
                "node": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/nodes/uka4p/",
                      "meta": {}
                    },
                    "self": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/relationships/node/",
                      "meta": {}
                    }
                  },
                  "data": {
                    "id": "uka4p",
                    "type": "nodes"
                  }
                },
                "license": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/licenses/563c1cf88c5e4a3877f9e96a/",
                      "meta": {}
                    }
                  },
                  "data": {
                    "id": "563c1cf88c5e4a3877f9e96a",
                    "type": "licenses"
                  }
                },
                "provider": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/providers/preprints/osf/",
                      "meta": {}
                    }
                  },
                  "data": {
                    "id": "osf",
                    "type": "preprint-providers"
                  }
                },
                "files": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/files/",
                      "meta": {}
                    }
                  }
                },
                "primary_file": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/files/60f52a94f1369301d5793a17/",
                      "meta": {}
                    }
                  },
                  "data": {
                    "id": "60f52a94f1369301d5793a17",
                    "type": "files"
                  }
                },
                "review_actions": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/review_actions/",
                      "meta": {}
                    }
                  }
                },
                "requests": {
                  "links": {
                    "related": {
                      "href": "https://api.osf.io/v2/preprints/egcsk/requests/",
                      "meta": {}
                    }
                  }
                }
              },
              "links": {
                "self": "https://api.osf.io/v2/preprints/egcsk/",
                "html": "https://osf.io/egcsk/",
                "preprint_doi": "https://doi.org/10.31219/osf.io/egcsk"
              }
            }"""),
            {
                "abstract": "Penulisan karya ilmiah ini dikhususkan untuk membahas mengenai "
                "Allah yang dirumuskan dalam teologi proses, yang dicetuskan oleh "
                "Alfred Whitehead. Dalam bagian bagian ini penulis menyajikan "
                "konsep Allah dalam teologi proses dan bagaimana tanggapan "
                "terhadap konsep tersebut secara Alkitabiah Metode penelitian, "
                "penulis menggunakan pendekatan metode penelitian kualitatif "
                "analisis deskriptif, dengan pendekatan literatur dan tergolong "
                "dalam penelitian perpustakaan. Konsep Allah menurut teologi "
                "proses adalah Allah yang berproses, tidak berpribadi dan tidak "
                "memiliki kedaulatan absolut. Namun pandangan tentang Allah dalam "
                "teologi proses adalah suatu kumpulan pengalaman pribadi dan "
                "dijadikan sebagai suatu konsep dalam pemikiran manusia. "
                "Tanggapan tersebut menunjukan perbandingan dari pola pikir "
                "teologi proses mengenai Allah yang menyimpang dan mengarahkan "
                "seseorang dalam memahami konsep Allah yang benar sesuai dengan "
                "pernyataan Allah m",
                "authors": [{"rft.aufirst": "Ceria", "rft.aulast": "Ceria"}],
                "doi": "10.31219/osf.io/egcsk",
                "finc.format": "Article",
                "finc.id": "ai-191-egcsk",
                "finc.mega_collection": ["sid-191-col-osf", "Osf"],
                "finc.source_id": "191",
                "languages": ["eng"],
                "rft.atitle": "Konsep Allah Dalam Teologi Proses",
                "rft.date": "2021-07-19",
                "rft.genre": "article",
                "rft.jtitle": "osf",
                "rft.pub": ["OSF Preprints"],
                "subjects": ["Gambar", "Respon", "Teologi Proses", "Tuhan"],
                "url": ["https://doi.org/10.31219/osf.io/egcsk"],
                "x.date": "2021-07-19T07:42:12.695116Z",
            },
        ),
    )
    for v, expected in cases:
        assert osf_to_intermediate(v) == expected
