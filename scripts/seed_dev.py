"""Vult een LOKALE database met testdata. Weigert te draaien tegen Postgres."""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app as app_module
from models import (db, Period, User, Product, Tally, Payment, HOEvent, InventorySnapshot,
                    InventoryPurchase, PeriodStartBalance)

NAMES = ["Stos", "Teun", "Godard", "Ruben", "Wessel", "Kaastra", "Stijn", "Moffel",
         "Beukers", "Luis", "De bie", "Romeijn", "Jorge", "Thomas", "Noah"]

with app_module.app.app_context():
    assert "sqlite" in str(db.engine.url), "Alleen voor lokale SQLite"
    db.drop_all()
    db.create_all()
    p = Period(name="Turfrekening oktober 2026", start_date=date(2026, 10, 1), is_active=True)
    pils = Product(name="Pils", price=1.0, emoji="🍺", sort_order=1)
    fris = Product(name="Fris", price=0.5, emoji="🥤", sort_order=2)
    wijn = Product(name="Wijn", price=2.5, emoji="🍷", sort_order=3)
    users = [User(name=n, sort_order=i) for i, n in enumerate(NAMES)]
    db.session.add_all([p, pils, fris, wijn, *users])
    db.session.flush()
    for i, u in enumerate(users):
        db.session.add(PeriodStartBalance(period_id=p.id, user_id=u.id, balance=(i - 5) * 3.5))
        db.session.add(Tally(period_id=p.id, user_id=u.id, product_id=pils.id, quantity=i + 2))
        if i % 3 == 0:
            db.session.add(Tally(period_id=p.id, user_id=u.id, product_id=fris.id, quantity=3))
        if i % 2 == 0:
            db.session.add(Payment(period_id=p.id, user_id=u.id, amount=20, date=date(2026, 10, 3)))
    db.session.add(HOEvent(period_id=p.id, name="Limonade", total_cost=18.4, distribution_type="equal_all", date=date(2026, 10, 2)))
    db.session.add(InventorySnapshot(period_id=p.id, product_id=pils.id, snapshot_type="begin", quantity=120))
    db.session.add(InventoryPurchase(period_id=p.id, product_id=pils.id, quantity=48))
    db.session.commit()
    print("Seed klaar:", len(users), "gebruikers")
