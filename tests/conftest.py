import os
import tempfile

# Moet vóór het importeren van app: zo gebruikt load_dotenv() nooit de echte database.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp.name}"
os.environ["SECRET_KEY"] = "test"

import pytest
from datetime import date

import app as app_module
from models import db, Period, User, Product


@pytest.fixture()
def app():
    flask_app = app_module.app
    flask_app.config.update(TESTING=True, WTF_CSRF_ENABLED=False, SESSION_COOKIE_SECURE=False)
    with flask_app.app_context():
        assert "sqlite" in str(db.engine.url), "Tests mogen alleen op SQLite draaien"
        db.drop_all()
        db.create_all()
        yield flask_app
        db.session.remove()


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def basis(app):
    """Eén actieve periode, vier gebruikers, drie producten."""
    period = Period(name="Test", start_date=date(2026, 10, 1), is_active=True)
    users = [User(name=n, sort_order=i) for i, n in enumerate(["A", "B", "C", "D"])]
    pils = Product(name="Pils", price_cents=100, sort_order=1)
    fris = Product(name="Fris", price_cents=50, sort_order=2)
    db.session.add_all([period, pils, fris, *users])
    db.session.commit()
    return {"period": period, "users": users, "pils": pils, "fris": fris}
