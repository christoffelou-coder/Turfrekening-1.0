"""Karakteriseringstests: leggen de huidige rekenregels vast vóór de verbouwing."""
from datetime import date

from models import (db, Tally, Payment, Correction, HOEvent, HOEventShare,
                    InventorySnapshot, InventoryPurchase, PeriodStartBalance)
from calculations import get_period_overview


def _rows(period_id):
    ov = get_period_overview(period_id)
    return {r["user"].name: r for r in ov["user_rows"]}, ov


def test_stand_formule(basis):
    p, (a, b, c, d) = basis["period"], basis["users"]
    db.session.add_all([
        PeriodStartBalance(period_id=p.id, user_id=a.id, balance=10),
        Tally(period_id=p.id, user_id=a.id, product_id=basis["pils"].id, quantity=3),
        Payment(period_id=p.id, user_id=a.id, amount=5),
        Correction(period_id=p.id, user_id=a.id, amount=-1),
    ])
    db.session.commit()
    rows, _ = _rows(p.id)
    # 10 + 5 - 3 - 1 = 11, plus 0,75 'tegoed': zonder eindtelling is turfverlies
    # -3 (geturfd 3, voorraad 0) en dat wordt over 4 personen verdeeld.
    # BEKEND GEDRAG (verbeterplan 3.1) — wordt in stap D bewust veranderd naar 11.
    assert rows["A"]["stand"] == 11.75


def test_ho_equal_all_gelijk_verdeeld(basis):
    p = basis["period"]
    db.session.add(HOEvent(period_id=p.id, name="Feest", total_cost=40, distribution_type="equal_all"))
    db.session.commit()
    rows, ov = _rows(p.id)
    assert all(r["ho"] == 10 for r in rows.values())
    assert ov["ho_events_total"] == 40


def test_ho_equal_selected_en_manual(basis):
    p, (a, b, c, d) = basis["period"], basis["users"]
    ev1 = HOEvent(period_id=p.id, name="Sel", total_cost=30, distribution_type="equal_selected")
    ev2 = HOEvent(period_id=p.id, name="Man", total_cost=20, distribution_type="manual")
    db.session.add_all([ev1, ev2])
    db.session.flush()
    db.session.add_all([
        HOEventShare(ho_event_id=ev1.id, user_id=a.id, amount=0),
        HOEventShare(ho_event_id=ev1.id, user_id=b.id, amount=0),
        HOEventShare(ho_event_id=ev2.id, user_id=c.id, amount=20),
    ])
    db.session.commit()
    rows, _ = _rows(p.id)
    assert (rows["A"]["ho"], rows["B"]["ho"], rows["C"]["ho"], rows["D"]["ho"]) == (15, 15, 20, 0)


def test_turfverlies_met_eindtelling(basis):
    p, a = basis["period"], basis["users"][0]
    pils = basis["pils"]
    db.session.add_all([
        InventorySnapshot(period_id=p.id, product_id=pils.id, snapshot_type="begin", quantity=100),
        InventorySnapshot(period_id=p.id, product_id=pils.id, snapshot_type="end", quantity=60),
        InventoryPurchase(period_id=p.id, product_id=pils.id, quantity=0),
        Tally(period_id=p.id, user_id=a.id, product_id=pils.id, quantity=30),
    ])
    db.session.commit()
    _, ov = _rows(p.id)
    # gebruikt 40, geturfd 30 -> 10 verlies x €1
    assert ov["turfverlies_total"] == 10


def test_parent_child_product(basis):
    from models import Product
    p, a, pils = basis["period"], basis["users"][0], basis["pils"]
    krat = Product(name="Halve krat", price=11.0, parent_product_id=pils.id, parent_units=12)
    db.session.add(krat)
    db.session.flush()
    db.session.add(Tally(period_id=p.id, user_id=a.id, product_id=krat.id, quantity=2))
    db.session.commit()
    from calculations import get_total_tallied_per_product
    assert get_total_tallied_per_product(p.id)[pils.id] == 24
