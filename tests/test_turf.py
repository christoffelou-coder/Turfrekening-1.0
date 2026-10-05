from datetime import date, datetime, timedelta

from models import db, Period, Tally


def _tally(basis, **kw):
    t = Tally(period_id=kw.pop("period_id", basis["period"].id), user_id=basis["users"][0].id,
              product_id=basis["pils"].id, quantity=1, unit_price_cents=100, **kw)
    db.session.add(t)
    db.session.commit()
    return t


def test_undo_binnen_tijd(client, basis):
    t = _tally(basis)
    assert client.delete(f"/api/tally/{t.id}").status_code == 200
    assert Tally.query.count() == 0


def test_undo_te_oud_geweigerd(client, basis):
    t = _tally(basis, created_at=datetime.utcnow() - timedelta(minutes=11))
    r = client.delete(f"/api/tally/{t.id}")
    assert r.status_code == 403 and "correctie" in r.get_json()["error"].lower()
    assert Tally.query.count() == 1


def test_undo_andere_periode_geweigerd(client, basis):
    oud = Period(name="Oud", start_date=date(2026, 9, 1), is_active=False)
    db.session.add(oud)
    db.session.commit()
    t = _tally(basis, period_id=oud.id)
    assert client.delete(f"/api/tally/{t.id}").status_code == 403


def test_last_tally_undoable_vlag(client, basis):
    _tally(basis, created_at=datetime.utcnow() - timedelta(minutes=30))
    d = client.get("/api/last-tally").get_json()["tally"]
    assert d["undoable"] is False and d["created_at"].endswith("Z")


def test_turfscherm_toont_alleen_actieve_bewoners(client, basis):
    basis["users"][1].is_active = False
    db.session.commit()
    html = client.get("/").get_data(as_text=True)
    assert 'data-name="A"' in html and 'data-name="B"' not in html


def test_turfscherm_toont_productafbeelding(client, basis):
    basis["pils"].image_url = "https://example.com/pils.png"
    db.session.commit()
    html = client.get("/").get_data(as_text=True)
    assert 'src="https://example.com/pils.png"' in html


def test_tally_antwoord_bevat_totaal(client, basis):
    u, p = basis["users"][0], basis["pils"]
    for expected in (1, 2, 3):
        d = client.post("/api/tally", json={"user_id": u.id, "product_id": p.id}).get_json()
        assert d["total"] == expected
    # correctie verlaagt het totaal
    d = client.post("/api/tally", json={"user_id": u.id, "product_id": p.id, "quantity": -1}).get_json()
    assert d["total"] == 2
    # ander persoon telt apart
    d = client.post("/api/tally", json={"user_id": basis["users"][1].id, "product_id": p.id}).get_json()
    assert d["total"] == 1


def test_turfscherm_geeft_geluid_door(client, basis):
    basis["pils"].sound_url = "piep"
    basis["pils"].sound_every = 3
    db.session.commit()
    html = client.get("/").get_data(as_text=True)
    assert 'data-sound="piep"' in html and 'data-every="3"' in html and 'id="soundBtn"' in html


def test_turfscherm_blokkeert_zoomen(client, basis):
    html = client.get("/").get_data(as_text=True)
    assert "gesturestart" in html and "no-zoom" in html
    assert "user-scalable=no" in html and "maximum-scale=1.0" in html
    css = client.get("/static/css/app.css").get_data(as_text=True)
    assert "touch-action: pan-x pan-y" in css
