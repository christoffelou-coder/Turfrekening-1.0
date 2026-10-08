from datetime import date

import pytest

from models import (db, Payment, Correction, Product, Tally, HOEvent, HOEventShare,
                    InventoryPurchase, InventorySnapshot)


@pytest.fixture()
def admin(client, basis):
    return client


@pytest.mark.parametrize("path", ["/admin", "/admin/users", "/admin/products", "/admin/periods",
                                  "/admin/inventory", "/admin/payments", "/admin/corrections", "/ho"])
def test_alle_adminpaginas_renderen(admin, path):
    r = admin.get(path)
    assert r.status_code == 200, path


def test_overboeking_toevoegen_en_formaat(admin, basis):
    r = admin.post("/admin/payments", data={"action": "add", "user_id": basis["users"][0].id,
                                            "amount": "12,50", "date": "2026-10-02"})
    assert r.status_code == 302
    assert Payment.query.one().amount_cents == 1250
    assert "+€12,50" in admin.get("/admin/payments").get_data(as_text=True)


@pytest.mark.parametrize("amount", ["0", "abc", ""])
def test_overboeking_ongeldig(admin, basis, amount):
    admin.post("/admin/payments", data={"action": "add", "user_id": basis["users"][0].id,
                                        "amount": amount, "date": "2026-10-02"})
    assert Payment.query.count() == 0


def test_correctie_vereist_omschrijving(admin, basis):
    data = {"action": "add", "user_id": basis["users"][0].id, "amount": "-5", "date": "2026-10-02"}
    admin.post("/admin/corrections", data={**data, "description": ""})
    assert Correction.query.count() == 0
    admin.post("/admin/corrections", data={**data, "description": "Terugbetaling"})
    assert Correction.query.one().amount_cents == -500


def test_product_met_turfjes_niet_te_verwijderen(admin, basis):
    db.session.add(Tally(period_id=basis["period"].id, user_id=basis["users"][0].id,
                         product_id=basis["pils"].id, quantity=1, unit_price_cents=100))
    db.session.commit()
    admin.post("/admin/products", data={"action": "delete", "product_id": basis["pils"].id})
    assert db.session.get(Product, basis["pils"].id) is not None
    admin.post("/admin/products", data={"action": "delete", "product_id": basis["fris"].id})
    assert db.session.get(Product, basis["fris"].id) is None


def test_product_prijs_in_centen(admin, basis):
    admin.post("/admin/products", data={"action": "add", "name": "Wijn", "price": "2,5", "emoji": "🍷", "sort_order": "3"})
    assert Product.query.filter_by(name="Wijn").one().price_cents == 250


def test_ho_post_met_bier_in_centen(admin, basis):
    admin.post("/ho", data={"action": "add_event", "name": "Feest", "total_cost": "10", "date": "2026-10-03",
                            "distribution_type": "equal_all", "beer_product_id": basis["pils"].id, "beer_quantity": "5"})
    ev = HOEvent.query.one()
    assert ev.total_cost_cents == 1500


def test_ho_manual_shares(admin, basis):
    a, b = basis["users"][:2]
    admin.post("/ho", data={"action": "add_event", "name": "Man", "total_cost": "30", "date": "2026-10-03",
                            "distribution_type": "manual", f"share_{a.id}": "10", f"share_{b.id}": "20,50"})
    assert sorted(s.amount_cents for s in HOEventShare.query) == [1000, 2050]


def test_eindtelling_leeg_veld_wordt_overgeslagen(admin, basis):
    admin.post("/admin/inventory", data={"action": "snapshot", "snapshot_type": "end",
                                         f"qty_{basis['pils'].id}": "", f"qty_{basis['fris'].id}": "7"})
    snaps = InventorySnapshot.query.all()
    assert len(snaps) == 1 and snaps[0].quantity == 7


def test_inkoop_met_kosten(admin, basis):
    admin.post("/admin/inventory", data={"action": "purchase", "product_id": basis["pils"].id,
                                         "quantity": "24", "total_cost": "16,80"})
    assert InventoryPurchase.query.one().total_cost_cents == 1680


def test_negatieve_overboeking_mag_wel(admin, basis):
    admin.post("/admin/payments", data={"action": "add", "user_id": basis["users"][0].id,
                                        "amount": "-15", "date": "2026-10-02"})
    assert Payment.query.one().amount_cents == -1500


def test_product_geluid_opslaan_en_valideren(admin, basis):
    base = {"action": "add", "name": "Ei", "price": "0,30", "emoji": "🥚", "sort_order": "5"}
    admin.post("/admin/products", data={**base, "sound_url": "piep", "sound_every": "3"})
    ei = Product.query.filter_by(name="Ei").one()
    assert (ei.sound_url, ei.sound_every) == ("piep", 3)
    admin.post("/admin/products", data={"action": "edit", "product_id": ei.id, "name": "Ei", "price": "0,30",
                                        "emoji": "🥚", "sort_order": "5", "is_active": "on",
                                        "sound_url": "/static/sounds/ei.mp3", "sound_every": "6"})
    db.session.refresh(ei)
    assert (ei.sound_url, ei.sound_every) == ("/static/sounds/ei.mp3", 6)
    # ongeldig: geen javascript:-links, interval buiten bereik
    for bad in ({"sound_url": "javascript:alert(1)", "sound_every": "3"}, {"sound_url": "piep", "sound_every": "0"}):
        admin.post("/admin/products", data={**base, "name": "Slecht", **bad})
    assert Product.query.filter_by(name="Slecht").count() == 0
    # leeg = geen geluid
    admin.post("/admin/products", data={**base, "name": "Stil", "sound_url": "", "sound_every": "3"})
    assert Product.query.filter_by(name="Stil").one().sound_url is None


def test_geluid_op_naam_uit_static_sounds(admin, basis):
    base = {"action": "add", "name": "Kip", "price": "1", "emoji": "🐔", "sort_order": "9", "sound_every": "3"}
    # spaties/hoofdletters worden genormaliseerd naar de bestandsnaam
    admin.post("/admin/products", data={**base, "sound_url": "Kakelende Kip"})
    kip = Product.query.filter_by(name="Kip").one()
    assert kip.sound_url == "kakelende-kip"
    html = admin.get("/").get_data(as_text=True)
    assert 'data-sound="/static/sounds/kakelende-kip.mp3"' in html
    # onbekende naam wordt geweigerd
    admin.post("/admin/products", data={**base, "name": "Fout", "sound_url": "bestaat-niet"})
    assert Product.query.filter_by(name="Fout").count() == 0
    # het bestand wordt echt geserveerd
    r = admin.get("/static/sounds/kakelende-kip.mp3")
    assert r.status_code == 200 and r.mimetype in ("audio/mpeg", "audio/mp3")


def test_persoonlijk_geluid_voor_bewoner(admin, basis):
    from models import User
    luis = basis["users"][0]
    admin.post("/admin/users", data={"action": "edit", "user_id": luis.id, "name": luis.name,
                                     "participates_in_ho": "on", "sound_url": "Kakelende Kip"})
    db.session.refresh(luis)
    assert luis.sound_url == "kakelende-kip"
    html = admin.get("/").get_data(as_text=True)
    assert 'data-name="A" data-sound="/static/sounds/kakelende-kip.mp3"' in html
    # onbekende naam geweigerd, leeg wist het geluid
    admin.post("/admin/users", data={"action": "edit", "user_id": luis.id, "name": luis.name, "sound_url": "nietbestaand"})
    db.session.refresh(luis)
    assert luis.sound_url == "kakelende-kip"
    admin.post("/admin/users", data={"action": "edit", "user_id": luis.id, "name": luis.name, "sound_url": ""})
    db.session.refresh(luis)
    assert luis.sound_url is None
    assert "data-sound" not in admin.get("/").get_data(as_text=True).split('data-name="B"')[0].split('data-name="A"')[1]


def test_geluid_luis_beschikbaar_en_wordt_geserveerd(admin, basis):
    luis = basis["users"][0]
    admin.post("/admin/users", data={"action": "edit", "user_id": luis.id, "name": luis.name, "sound_url": "luis"})
    db.session.refresh(luis)
    assert luis.sound_url == "luis"
    assert 'data-name="A" data-sound="/static/sounds/luis.m4a"' in admin.get("/").get_data(as_text=True)
    r = admin.get("/static/sounds/luis.m4a")
    assert r.status_code == 200 and r.mimetype == "audio/mp4" and len(r.data) > 10000
