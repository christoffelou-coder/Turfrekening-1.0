from models import db
import pytest
from filters import euro


@pytest.mark.parametrize("cents,sign,expected", [
    (-101, False, "−€1,01"),
    (-1512, False, "−€15,12"),
    (101, False, "€1,01"),
    (101, True, "+€1,01"),
    (123456750, False, "€1.234.567,50"),
    (-123450, True, "−€1.234,50"),
    (5, False, "€0,05"),
])
def test_euro_formaat(cents, sign, expected):
    assert str(euro(cents, sign)) == expected


@pytest.mark.parametrize("value", [0, None])
def test_euro_nul_is_streepje(value):
    assert "—" in str(euro(value))


def test_nooit_dubbel_minteken():
    assert "€-" not in str(euro(-300)) and "−€-" not in str(euro(-300))


def test_dashboard_rendert(client, basis):
    client.post("/login", data={"password": "geheim"})
    r = client.get("/admin")
    html = r.get_data(as_text=True)
    assert r.status_code == 200 and "Periode afsluiten" in html and "Bewoners" in html
    assert "Vorige standen" not in html and "Maandrapport" not in html


def test_rapport_rendert_met_euro_en_vlaggen(client, basis):
    html = client.get("/rapport").get_data(as_text=True)
    assert "Lopend, cijfers voorlopig" in html
    assert "Voorraad nog niet (helemaal) geteld" in html
    assert "€-" not in html and "%.2f" not in html


def test_rapport_met_ho_post_en_telling(client, basis):
    from datetime import date
    from models import HOEvent, InventorySnapshot
    p = basis["period"]
    db.session.add(HOEvent(period_id=p.id, name="Feest", total_cost_cents=4000, distribution_type="equal_all", date=date(2026, 10, 2)))
    db.session.add(InventorySnapshot(period_id=p.id, product_id=basis["pils"].id, snapshot_type="begin", quantity=10))
    db.session.commit()
    html = client.get("/rapport").get_data(as_text=True)
    assert "€40,00" in html and "niet geteld" in html and "Gelijk verdeeld over 4" in html
