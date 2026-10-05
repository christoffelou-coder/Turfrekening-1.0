"""Rekenregels van compute_period (alle bedragen in centen)."""
from datetime import date

import pytest

from calculations import compute_period, get_total_tallied_per_product
from models import (db, Period, User, Product, Tally, Payment, Correction, HOEvent, HOEventShare,
                    InventorySnapshot, InventoryPurchase, PeriodStartBalance)
from money import split_even


def rows(period_id):
    ov = compute_period(period_id)
    return {r["user"]["name"]: r for r in ov["user_rows"]}, ov


def tally(basis, user, product, qty, price=None):
    p = basis[product]
    db.session.add(Tally(period_id=basis["period"].id, user_id=user.id, product_id=p.id, quantity=qty,
                         unit_price_cents=p.price_cents if price is None else price))


def count_inventory(basis, begin=0, end=0):
    pid, per = basis["pils"].id, basis["period"].id
    db.session.add_all([
        InventorySnapshot(period_id=per, product_id=pid, snapshot_type="begin", quantity=begin),
        InventorySnapshot(period_id=per, product_id=pid, snapshot_type="end", quantity=end),
    ])


def test_stand_formule(basis):
    p, a = basis["period"], basis["users"][0]
    db.session.add_all([
        PeriodStartBalance(period_id=p.id, user_id=a.id, balance_cents=1000),
        Payment(period_id=p.id, user_id=a.id, amount_cents=500),
        Correction(period_id=p.id, user_id=a.id, amount_cents=-100),
    ])
    tally(basis, a, "pils", 3)
    count_inventory(basis, begin=100, end=97)   # alles geturfd: geen verlies
    db.session.commit()
    r, ov = rows(p.id)
    assert r["A"]["stand"] == 1000 + 500 - 300 - 100
    assert ov["inventory_complete"] and ov["turfverlies_total"] == 0


def test_geen_beginstand_is_nul(basis):
    r, _ = rows(basis["period"].id)
    assert all(row["vorige_stand"] == 0 for row in r.values())


def test_zonder_eindtelling_geen_verdeling(basis):
    """Verbeterplan 3.1: ontbrekende eindtelling = voorlopig, niet als tegoed uitdelen."""
    a = basis["users"][0]
    tally(basis, a, "pils", 3)
    db.session.commit()
    r, ov = rows(basis["period"].id)
    assert not ov["inventory_complete"]
    assert ov["turfverlies_distributed"] == 0
    assert r["A"]["stand"] == -300
    assert any(w["code"] == "inventory_incomplete" for w in ov["warnings"])


def test_turfverlies_met_eindtelling_wordt_verdeeld(basis):
    a = basis["users"][0]
    tally(basis, a, "pils", 30)
    count_inventory(basis, begin=100, end=60)   # gebruikt 40, geturfd 30 -> 10 verlies x €1
    db.session.commit()
    r, ov = rows(basis["period"].id)
    assert ov["turfverlies_total"] == 1000 and ov["turfverlies_distributed"] == 1000
    # 4 personen: 1000 / 4 = 250 p.p.
    assert [r[n]["ho"] for n in "ABCD"] == [250] * 4


def test_ho_equal_all_en_afronding_exact(basis):
    p = basis["period"]
    db.session.add(HOEvent(period_id=p.id, name="Feest", total_cost_cents=1000, distribution_type="equal_all"))
    db.session.commit()
    r, ov = rows(p.id)
    # 1000 / 4 = 250
    assert all(x["ho"] == 250 for x in r.values())
    # 1001 cent over 4 personen: som blijft exact 1001
    ev = HOEvent.query.one(); ev.total_cost_cents = 1001; db.session.commit()
    r, ov = rows(p.id)
    assert sum(x["ho"] for x in r.values()) == 1001
    assert ov["totals"]["ho"] == ov["total_ho"] == 1001
    assert ov["ho_uniform"]  # verschil ≤ 1 cent


def test_ho_niet_uniform_label(basis):
    p, (a, b, c, d) = basis["period"], basis["users"]
    ev = HOEvent(period_id=p.id, name="Sel", total_cost_cents=3000, distribution_type="equal_selected")
    db.session.add(ev); db.session.flush()
    db.session.add_all([HOEventShare(ho_event_id=ev.id, user_id=a.id, amount_cents=0),
                        HOEventShare(ho_event_id=ev.id, user_id=b.id, amount_cents=0)])
    db.session.commit()
    r, ov = rows(p.id)
    assert (r["A"]["ho"], r["B"]["ho"], r["C"]["ho"]) == (1500, 1500, 0)
    assert not ov["ho_uniform"] and ov["ho_per_person"] is None


def test_ho_manual(basis):
    p, (a, b, c, d) = basis["period"], basis["users"]
    ev = HOEvent(period_id=p.id, name="Man", total_cost_cents=2000, distribution_type="manual")
    db.session.add(ev); db.session.flush()
    db.session.add(HOEventShare(ho_event_id=ev.id, user_id=c.id, amount_cents=2000))
    db.session.commit()
    r, _ = rows(p.id)
    assert (r["A"]["ho"], r["C"]["ho"]) == (0, 2000)


def test_prijswijziging_raakt_oude_turfjes_niet(basis):
    a = basis["users"][0]
    tally(basis, a, "pils", 2)
    db.session.commit()
    basis["pils"].price_cents = 150
    db.session.commit()
    r, _ = rows(basis["period"].id)
    assert r["A"]["geturfd"] == 200


def test_parent_child_product(basis):
    a, pils = basis["users"][0], basis["pils"]
    krat = Product(name="Halve krat", price_cents=1100, parent_product_id=pils.id, parent_units=12)
    db.session.add(krat); db.session.flush()
    db.session.add(Tally(period_id=basis["period"].id, user_id=a.id, product_id=krat.id, quantity=2,
                         unit_price_cents=1100))
    db.session.commit()
    assert get_total_tallied_per_product(basis["period"].id)[pils.id] == 24


def test_bier_bij_ho_telt_niet_als_verlies(basis):
    a, p, pils = basis["users"][0], basis["period"], basis["pils"]
    tally(basis, a, "pils", 10)
    count_inventory(basis, begin=100, end=80)  # gebruikt 20: 10 geturfd + 10 op feest
    db.session.add(HOEvent(period_id=p.id, name="Feest", total_cost_cents=1000, distribution_type="equal_all",
                           beer_product_id=pils.id, beer_quantity=10))
    db.session.commit()
    _, ov = rows(p.id)
    assert ov["turfverlies_total"] == 0


def test_gedeactiveerd_product_blijft_meetellen(basis):
    a, p = basis["users"][0], basis["period"]
    tally(basis, a, "pils", 10)
    count_inventory(basis, begin=100, end=70)  # verlies 20
    basis["pils"].is_active = False
    db.session.commit()
    _, ov = rows(p.id)
    assert ov["turfverlies_total"] == 2000
    assert basis["pils"].id in [p["id"] for p in ov["products"]]


def test_nieuwe_bewoner_niet_in_oude_periode(basis):
    oud = Period(name="Oud", start_date=date(2026, 9, 1), end_date=date(2026, 9, 30), is_active=False)
    db.session.add(oud); db.session.flush()
    db.session.add(PeriodStartBalance(period_id=oud.id, user_id=basis["users"][0].id, balance_cents=500))
    nieuw = User(name="Nieuw", sort_order=9)
    db.session.add(nieuw); db.session.commit()
    r_oud, _ = rows(oud.id)
    assert list(r_oud) == ["A"]
    r_nu, _ = rows(basis["period"].id)
    assert r_nu["Nieuw"]["vorige_stand"] == 0


def test_vertrokken_met_saldo_blijft_zichtbaar(basis):
    p, a = basis["period"], basis["users"][0]
    db.session.add(PeriodStartBalance(period_id=p.id, user_id=a.id, balance_cents=-13050))
    a.is_active = False
    db.session.commit()
    r, _ = rows(p.id)
    assert r["A"]["stand"] == -13050


@pytest.mark.parametrize("total,n", [(1000, 4), (1001, 4), (1, 3), (-1001, 4), (0, 5), (7, 1)])
def test_split_even_som_klopt(total, n):
    parts = split_even(total, n)
    assert sum(parts) == total and len(parts) == n and max(parts) - min(parts) <= 1
