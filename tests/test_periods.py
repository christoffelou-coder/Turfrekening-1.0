from datetime import date

import pytest

from calculations import compute_period
from models import (db, Period, User, Product, Tally, Payment, Correction, HOEvent, InventorySnapshot,
                    PeriodStartBalance, PeriodReport)
from periods import (PeriodError, close_period, get_period_view, check_period_dates, create_first_period,
                     default_next_name, freeze, revive)


def vul(basis, tellen=True):
    p, (a, b, c, d) = basis["period"], basis["users"]
    pils = basis["pils"]
    db.session.add_all([
        PeriodStartBalance(period_id=p.id, user_id=a.id, balance_cents=1000),
        Tally(period_id=p.id, user_id=a.id, product_id=pils.id, quantity=4, unit_price_cents=100),
        Tally(period_id=p.id, user_id=b.id, product_id=pils.id, quantity=6, unit_price_cents=100),
        Payment(period_id=p.id, user_id=b.id, amount_cents=2000),
        Correction(period_id=p.id, user_id=c.id, amount_cents=-150, description="x"),
        HOEvent(period_id=p.id, name="Feest", total_cost_cents=1001, distribution_type="equal_all", date=date(2026, 10, 2)),
        InventorySnapshot(period_id=p.id, product_id=pils.id, snapshot_type="begin", quantity=50),
    ])
    if tellen:
        db.session.add(InventorySnapshot(period_id=p.id, product_id=pils.id, snapshot_type="end", quantity=38))
    db.session.commit()


def test_afsluiten_zonder_eindtelling_geweigerd(basis):
    vul(basis, tellen=False)
    with pytest.raises(PeriodError, match="Eindtelling"):
        close_period(basis["period"], date(2026, 10, 31), "Nieuw", date(2026, 10, 31))
    p = db.session.get(Period, basis["period"].id)
    assert p.is_active and p.closed_at is None and PeriodReport.query.count() == 0
    assert p.end_date is None  # niets half opgeslagen


def test_beginstanden_nieuwe_periode_gelijk_aan_eindstanden(basis):
    vul(basis)
    eind = {r["user"]["id"]: r["stand"] for r in compute_period(basis["period"].id)["user_rows"]}
    nieuw, _ = close_period(basis["period"], date(2026, 10, 31), "Nov", date(2026, 10, 31))
    begin = {s.user_id: s.balance_cents for s in PeriodStartBalance.query.filter_by(period_id=nieuw.id)}
    assert begin == eind
    rows = {r["user"]["id"]: r for r in compute_period(nieuw.id)["user_rows"]}
    assert all(rows[uid]["vorige_stand"] == eind[uid] for uid in eind)
    # beginvoorraad = eindtelling
    snap = InventorySnapshot.query.filter_by(period_id=nieuw.id, snapshot_type="begin").one()
    assert snap.quantity == 38
    assert db.session.get(Period, basis["period"].id).closed_at is not None and nieuw.is_active


def test_bevroren_rapport_blijft_gelijk(basis):
    """Acceptatiecheck: wijzig daarna bewoners, prijzen en HO; het oude rapport is identiek."""
    vul(basis)
    pid = basis["period"].id
    voor = freeze(compute_period(pid))
    close_period(basis["period"], date(2026, 10, 31), "Nov", date(2026, 10, 31))
    # na afsluiten van alles wijzigen
    basis["users"][0].is_active = False
    basis["users"][1].name = "Hernoemd"
    basis["pils"].price_cents = 999
    basis["pils"].is_active = False
    db.session.add(User(name="Nieuwkomer", sort_order=50))
    db.session.query(HOEvent).update({"distribution_type": "manual"})
    db.session.commit()

    view = get_period_view(pid)
    assert view["state"] == "closed"
    na = view.copy()
    na["period"] = dict(view["period"]); 
    # vergelijk alles behalve de afsluit-metadata
    oud = revive(voor)
    for key in ("user_rows", "inventory", "totals", "ho_events", "products", "turfverlies_total", "total_ho", "users"):
        assert view[key] == oud[key], key
    assert [r["user"]["name"] for r in view["user_rows"]] == ["A", "B", "C", "D"]


def test_vertrokken_met_saldo_gaat_mee_zonder_saldo_niet(basis):
    vul(basis)
    a, b, c, d = basis["users"]
    a.is_active = False   # heeft saldo (beginstand 10,00 etc.)
    d.is_active = False   # geen activiteit, saldo na HO-deel is niet nul -> ook mee
    db.session.commit()
    nieuw, _ = close_period(basis["period"], date(2026, 10, 31), "Nov", date(2026, 10, 31))
    ids = {s.user_id for s in PeriodStartBalance.query.filter_by(period_id=nieuw.id)}
    assert a.id in ids and b.id in ids


def test_vertrokken_zonder_enig_saldo_valt_af(basis):
    # geen HO, geen activiteit, geen beginstand -> saldo 0 en niet actief
    d = basis["users"][3]
    d.is_active = False
    p = basis["period"]
    db.session.add_all([
        Tally(period_id=p.id, user_id=basis["users"][0].id, product_id=basis["pils"].id, quantity=1, unit_price_cents=100),
        InventorySnapshot(period_id=p.id, product_id=basis["pils"].id, snapshot_type="begin", quantity=10),
        InventorySnapshot(period_id=p.id, product_id=basis["pils"].id, snapshot_type="end", quantity=9),
    ])
    db.session.commit()
    nieuw, _ = close_period(p, date(2026, 10, 31), "Nov", date(2026, 10, 31))
    assert d.id not in {s.user_id for s in PeriodStartBalance.query.filter_by(period_id=nieuw.id)}


def test_nieuwe_bewoner_na_afsluiten_start_op_nul(basis):
    vul(basis)
    nieuw, _ = close_period(basis["period"], date(2026, 10, 31), "Nov", date(2026, 10, 31))
    u = User(name="Nieuw", sort_order=60)
    db.session.add(u); db.session.commit()
    rows = {r["user"]["name"]: r for r in compute_period(nieuw.id)["user_rows"]}
    assert rows["Nieuw"]["vorige_stand"] == 0
    assert "Nieuw" not in {r["user"]["name"] for r in get_period_view(basis["period"].id)["user_rows"]}


def test_dubbel_afsluiten_geweigerd(basis):
    vul(basis)
    close_period(basis["period"], date(2026, 10, 31), "Nov", date(2026, 10, 31))
    with pytest.raises(PeriodError):
        close_period(basis["period"], date(2026, 11, 30), "Dec", date(2026, 11, 30))


def test_datumchecks(basis):
    basis["period"].end_date = date(2026, 10, 31)
    db.session.commit()
    w = check_period_dates(date(2026, 10, 20), None, today=date(2026, 11, 1))
    assert any("Overlap" in x for x in w)
    w = check_period_dates(date(2026, 12, 1), None, today=date(2026, 11, 1))
    assert any("toekomst" in x for x in w) and any("gat" in x for x in w)
    assert check_period_dates(date(2026, 10, 31), None, today=date(2026, 11, 1)) == []
    with pytest.raises(PeriodError):
        check_period_dates(date(2026, 11, 5), date(2026, 11, 1))


def test_eerste_periode_alleen_zonder_actieve(basis):
    with pytest.raises(PeriodError):
        create_first_period("X", date(2026, 11, 1))


def test_naam_voorstel():
    assert default_next_name(date(2026, 10, 1)) == "Turfrekening oktober 2026"
