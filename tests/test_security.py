import pytest
from models import db, Tally, User


def test_turfscherm_rapport_en_admin_zijn_bereikbaar(client, basis):
    for path in ("/", "/rapport", "/admin", "/ho"):
        assert client.get(path).status_code == 200, path


def test_csrf_actief(app, basis):
    app.config["WTF_CSRF_ENABLED"] = True
    c = app.test_client()
    r = c.post("/api/tally", json={"user_id": 1, "product_id": 1})
    assert r.status_code == 400
    app.config["WTF_CSRF_ENABLED"] = False


@pytest.mark.parametrize("payload", [
    {}, {"user_id": 1}, {"user_id": "x", "product_id": 1},
    {"user_id": 1, "product_id": 1, "quantity": 0},
    {"user_id": 1, "product_id": 1, "quantity": 25},
    {"user_id": 999, "product_id": 1}, {"user_id": 1, "product_id": 999},
])
def test_tally_ongeldige_invoer(client, basis, payload):
    r = client.post("/api/tally", json=payload)
    assert r.status_code in (400, 404)
    assert Tally.query.count() == 0


def test_tally_inactief_geweigerd(client, basis):
    u, p = basis["users"][0], basis["pils"]
    u.is_active = False
    db.session.commit()
    assert client.post("/api/tally", json={"user_id": u.id, "product_id": p.id}).status_code == 400


def test_tally_ok(client, basis):
    u, p = basis["users"][0], basis["pils"]
    r = client.post("/api/tally", json={"user_id": u.id, "product_id": p.id, "quantity": 2})
    assert r.status_code == 200 and Tally.query.one().quantity == 2


def test_formulier_met_rommel_geeft_melding_geen_500(client, basis):
    r = client.post("/admin/payments", data={"action": "add", "user_id": "1", "amount": "abc", "date": "2026-10-01"})
    assert r.status_code == 302


def test_secret_key_verplicht():
    import subprocess, sys, os
    env = {k: v for k, v in os.environ.items() if k not in ("SECRET_KEY", "FLASK_DEBUG", "FLASK_ENV")}
    env["DATABASE_URL"] = "sqlite:///:memory:"
    r = subprocess.run([sys.executable, "-c", "import app"], env=env, capture_output=True, text=True,
                       cwd=os.path.dirname(os.path.dirname(__file__)))
    assert r.returncode != 0 and "SECRET_KEY" in r.stderr
