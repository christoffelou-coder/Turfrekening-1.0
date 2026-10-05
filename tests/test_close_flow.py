from datetime import date

import pytest

from models import (db, Period, User, Tally, Payment, Correction, InventorySnapshot, PeriodReport,
                    PeriodStartBalance)
from tests.test_periods import vul


@pytest.fixture()
def admin(client, basis):
    client.post("/login", data={"password": "geheim"})
    return client


def test_afsluitpagina_toont_blokkade_zonder_eindtelling(admin, basis):
    vul(basis, tellen=False)
    html = admin.get("/admin/periods/close").get_data(as_text=True)
    assert "Eindtelling ontbreekt" in html and "disabled" in html


def test_afsluiten_via_formulier(admin, basis):
    vul(basis)
    r = admin.post("/admin/periods/close", data={"end_date": "2026-10-31", "next_name": "Turfrekening november 2026",
                                                 "next_start": "2026-10-31", "confirm": "on"})
    assert r.status_code == 302 and "/rapport/" in r.headers["Location"]
    assert PeriodReport.query.count() == 1
    assert Period.query.filter_by(is_active=True).one().name == "Turfrekening november 2026"
    html = admin.get(f"/rapport/{basis['period'].id}").get_data(as_text=True)
    assert "Afgesloten op" in html and "bevroren" in html


def test_afsluiten_vereist_bevestiging(admin, basis):
    vul(basis)
    admin.post("/admin/periods/close", data={"end_date": "2026-10-31", "next_name": "N", "next_start": "2026-10-31"})
    assert PeriodReport.query.count() == 0


def test_gesloten_periode_is_vergrendeld(admin, basis):
    """Scenario 7: wijzigen van een gesloten periode wordt geweigerd."""
    vul(basis)
    oud_id = basis["period"].id
    admin.post("/admin/periods/close", data={"end_date": "2026-10-31", "next_name": "Nov", "next_start": "2026-10-31", "confirm": "on"})
    betaling = Payment.query.filter_by(period_id=oud_id).first()
    tally = Tally.query.filter_by(period_id=oud_id).first()
    n_pay, n_tal = Payment.query.count(), Tally.query.count()

    # turf-ongedaan, betaling/correctie verwijderen, periode bewerken: alles geweigerd
    assert admin.delete(f"/api/tally/{tally.id}").status_code == 403
    admin.post("/admin/payments", data={"action": "delete", "payment_id": betaling.id})
    admin.post("/admin/periods", data={"action": "edit", "period_id": oud_id, "name": "Gehackt", "start_date": "2020-01-01"})
    admin.post("/admin/periods", data={"action": "delete", "period_id": oud_id})
    assert Payment.query.count() == n_pay and Tally.query.count() == n_tal
    p = db.session.get(Period, oud_id)
    assert p.name == "Test" and p.start_date == date(2026, 10, 1)


def test_nieuwe_turfjes_gaan_naar_nieuwe_periode(admin, basis):
    vul(basis)
    admin.post("/admin/periods/close", data={"end_date": "2026-10-31", "next_name": "Nov", "next_start": "2026-10-31", "confirm": "on"})
    r = admin.post("/api/tally", json={"user_id": basis["users"][0].id, "product_id": basis["pils"].id})
    assert r.status_code == 200
    nieuw = Period.query.filter_by(is_active=True).one()
    assert Tally.query.filter_by(period_id=nieuw.id).count() == 1


def test_vertrokken_zetten_waarschuwt_met_bedrag(admin, basis):
    p, a = basis["period"], basis["users"][0]
    db.session.add(PeriodStartBalance(period_id=p.id, user_id=a.id, balance_cents=-13050))
    db.session.commit()
    r = admin.post("/admin/users", data={"action": "leave", "user_id": a.id}, follow_redirects=True)
    html = r.get_data(as_text=True)
    assert "−€130,50" in html and "vertrokken" in html
    assert not db.session.get(User, a.id).is_active and db.session.get(User, a.id).left_at is not None
    # blijft in de periode staan met zijn schuld
    from calculations import compute_period
    rows = {x["user"]["name"]: x for x in compute_period(p.id)["user_rows"]}
    assert rows["A"]["stand"] == -13050
    # niet meer op turfscherm
    assert 'data-name="A"' not in admin.get("/").get_data(as_text=True)


def test_gebruiker_verwijderen_bestaat_niet_meer(admin, basis):
    a = basis["users"][0]
    admin.post("/admin/users", data={"action": "delete", "user_id": a.id})
    assert db.session.get(User, a.id) is not None


def test_nieuwe_bewoner_start_nul(admin, basis):
    admin.post("/admin/users", data={"action": "add", "name": "Nieuw"})
    from calculations import compute_period
    rows = {x["user"]["name"]: x for x in compute_period(basis["period"].id)["user_rows"]}
    assert rows["Nieuw"]["stand"] == 0 and rows["Nieuw"]["vorige_stand"] == 0


def test_vorige_stand_pagina_bestaat_niet(admin):
    assert admin.get("/admin/vorige-stand").status_code == 404


def test_historische_periode_badge(admin, basis):
    oud = Period(name="Oud", start_date=date(2026, 6, 9), end_date=date(2026, 9, 10), is_active=False)
    db.session.add(oud); db.session.commit()
    html = admin.get(f"/rapport/{oud.id}").get_data(as_text=True)
    assert "Historisch, niet bevroren" in html


def test_eerste_periode_aanmaken_zonder_actieve(admin, basis):
    db.session.query(Period).update({"is_active": False})
    db.session.commit()
    admin.post("/admin/periods", data={"action": "first", "name": "Start", "start_date": "2026-11-01"})
    assert Period.query.filter_by(is_active=True).one().name == "Start"
