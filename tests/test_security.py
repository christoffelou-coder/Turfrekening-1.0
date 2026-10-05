import pytest
from models import db, Tally, User


@pytest.mark.parametrize("path", ["/admin", "/admin/users", "/admin/payments", "/ho", "/admin/periods"])
def test_admin_vereist_login(client, basis, path):
    r = client.get(path)
    assert r.status_code == 302 and "/login" in r.headers["Location"]


def test_admin_post_zonder_login_wijzigt_niets(client, basis):
    r = client.post("/admin/users", data={"action": "add", "name": "Hacker"})
    assert r.status_code == 302 and "/login" in r.headers["Location"]
    assert User.query.filter_by(name="Hacker").count() == 0


def test_turfscherm_en_rapport_zijn_open(client, basis):
    assert client.get("/").status_code == 200
    assert client.get("/rapport").status_code == 200


def test_login_goed_en_fout(client, basis):
    assert client.post("/login", data={"password": "fout"}).status_code == 200
    assert client.get("/admin").status_code == 302
    r = client.post("/login?next=/admin/users", data={"password": "geheim"})
    assert r.headers["Location"].endswith("/admin/users")
    assert client.get("/admin").status_code == 200


def test_login_open_redirect_geblokkeerd(client):
    r = client.post("/login?next=//evil.com", data={"password": "geheim"})
    assert "evil.com" not in r.headers["Location"]


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
    client.post("/login", data={"password": "geheim"})
    r = client.post("/admin/payments", data={"action": "add", "user_id": "1", "amount": "abc", "date": "2026-10-01"})
    assert r.status_code == 302


def test_secret_key_verplicht():
    import subprocess, sys, os
    env = {k: v for k, v in os.environ.items() if k not in ("SECRET_KEY", "FLASK_DEBUG", "FLASK_ENV")}
    env["DATABASE_URL"] = "sqlite:///:memory:"
    r = subprocess.run([sys.executable, "-c", "import app"], env=env, capture_output=True, text=True,
                       cwd=os.path.dirname(os.path.dirname(__file__)))
    assert r.returncode != 0 and "SECRET_KEY" in r.stderr
