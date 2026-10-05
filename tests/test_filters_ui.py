import pytest
from filters import euro


@pytest.mark.parametrize("value,sign,expected", [
    (-1.01, False, "−€1,01"),
    (-15.12, False, "−€15,12"),
    (1.01, False, "€1,01"),
    (1.01, True, "+€1,01"),
    (1234567.5, False, "€1.234.567,50"),
    (-1234.5, True, "−€1.234,50"),
    (0.125, False, "€0,13"),
])
def test_euro_formaat(value, sign, expected):
    assert str(euro(value, sign)) == expected


@pytest.mark.parametrize("value", [0, 0.0, None, -0.004, 0.004])
def test_euro_nul_is_streepje(value):
    assert "—" in str(euro(value))


def test_nooit_dubbel_minteken():
    assert "€-" not in str(euro(-3)) and "−€-" not in str(euro(-3))


def test_dashboard_rendert(client, basis):
    client.post("/login", data={"password": "geheim"})
    r = client.get("/admin")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "Periode afsluiten" in html and "Bewoners" in html
    assert "Vorige standen" not in html and "Maandrapport" not in html


def test_rapport_rendert_met_euro_en_vlaggen(client, basis):
    html = client.get("/rapport").get_data(as_text=True)
    assert "Lopend, cijfers voorlopig" in html
    assert "Voorraad nog niet geteld" in html
    assert "€-" not in html and "%.2f" not in html
