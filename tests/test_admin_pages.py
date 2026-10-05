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


@pytest.mark.parametrize("amount", ["0", "-5", "abc", ""])
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
