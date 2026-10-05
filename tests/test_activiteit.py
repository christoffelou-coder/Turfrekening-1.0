from datetime import datetime, timedelta

from models import db, Tally


def tally(basis, user_idx=0, qty=1, minutes_ago=5):
    db.session.add(Tally(period_id=basis["period"].id, user_id=basis["users"][user_idx].id,
                         product_id=basis["pils"].id, quantity=qty, unit_price_cents=100,
                         created_at=datetime.utcnow() - timedelta(minutes=minutes_ago)))
    db.session.commit()


def test_activiteit_pagina_en_nav(client, basis):
    html = client.get("/activiteit").get_data(as_text=True)
    assert client.get("/activiteit").status_code == 200
    assert 'id="chart"' in html and "Activiteit" in html


def test_api_bevat_geen_namen(client, basis):
    tally(basis, 0)
    tally(basis, 1)
    r = client.get("/api/activity")
    raw = r.get_data(as_text=True)
    ev = r.get_json()["events"]
    assert len(ev) == 2 and set(ev[0]) == {"t", "q", "product"}
    for u in basis["users"]:
        assert f'"{u.name}"' not in raw
    assert "user" not in raw


def test_api_negatieve_en_oude_turfjes_buiten(client, basis):
    tally(basis, qty=-1)
    tally(basis, minutes_ago=60 * 24 * 40)
    tally(basis, minutes_ago=10)
    assert len(client.get("/api/activity?days=7").get_json()["events"]) == 1


def test_api_days_begrensd(client, basis):
    tally(basis, minutes_ago=60 * 24 * 20)
    assert len(client.get("/api/activity?days=9999").get_json()["events"]) == 1
    assert len(client.get("/api/activity?days=0").get_json()["events"]) == 0


def test_api_nieuwste_eerst(client, basis):
    tally(basis, minutes_ago=30)
    tally(basis, minutes_ago=2)
    ev = client.get("/api/activity").get_json()["events"]
    assert ev[0]["t"] > ev[1]["t"]


def test_activiteit_heeft_geen_weg_naar_admin_of_rapport(client, basis):
    html = client.get("/activiteit").get_data(as_text=True)
    assert "/admin" not in html and "/rapport" not in html and 'href="/"' in html


def test_turfscherm_heeft_activiteitknop(client, basis):
    assert 'href="/activiteit"' in client.get("/").get_data(as_text=True)
