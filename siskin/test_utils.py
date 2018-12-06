from siskin.utils import dictcheck, nwise, random_string

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
    for char in ' \t\n.:,;#~+-?=[]()/&%$§"!':
        assert char not in random_string()

