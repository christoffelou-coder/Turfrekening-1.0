from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Period(db.Model):
    __tablename__ = "periods"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    tallies = db.relationship("Tally", backref="period", lazy=True)
    payments = db.relationship("Payment", backref="period", lazy=True)
    corrections = db.relationship("Correction", backref="period", lazy=True)
    ho_events = db.relationship("HOEvent", backref="period", lazy=True)
    inventory_purchases = db.relationship("InventoryPurchase", backref="period", lazy=True)
    inventory_snapshots = db.relationship("InventorySnapshot", backref="period", lazy=True)


class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    # Balance carried over from the previous period
    previous_balance = db.Column(db.Float, default=0.0)

    tallies = db.relationship("Tally", backref="user", lazy=True)
    payments = db.relationship("Payment", backref="user", lazy=True)
    corrections = db.relationship("Correction", backref="user", lazy=True)
    ho_shares = db.relationship("HOEventShare", backref="user", lazy=True)


class Product(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    price = db.Column(db.Float, nullable=False)
    emoji = db.Column(db.String(10), default="🍺")
    is_active = db.Column(db.Boolean, default=True)
    sort_order = db.Column(db.Integer, default=0)

    tallies = db.relationship("Tally", backref="product", lazy=True)
    inventory_purchases = db.relationship("InventoryPurchase", backref="product", lazy=True)
    inventory_snapshots = db.relationship("InventorySnapshot", backref="product", lazy=True)


class Tally(db.Model):
    """Een turfjte: persoon X heeft Y stuks van product Z aangeslagen."""
    __tablename__ = "tallies"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    quantity = db.Column(db.Integer, default=1, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class InventoryPurchase(db.Model):
    """Ingekochte voorraad: hoeveel stuks van product X zijn bijgevuld."""
    __tablename__ = "inventory_purchases"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    total_cost = db.Column(db.Float, nullable=True)   # werkelijke aankoopprijs (optioneel)
    date = db.Column(db.Date, default=datetime.utcnow)
    notes = db.Column(db.String(200), nullable=True)


class InventorySnapshot(db.Model):
    """Beginstand of eindstand van de voorraad voor een periode."""
    __tablename__ = "inventory_snapshots"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    snapshot_type = db.Column(db.String(10), nullable=False)  # 'begin' or 'end'
    quantity = db.Column(db.Integer, nullable=False, default=0)
    date = db.Column(db.Date, default=datetime.utcnow)


class HOEvent(db.Model):
    """
    Gezamenlijke kostenpost die niet individueel aangeslagen wordt.
    Bijv: feestje, limonade, bankkosten, CO2 tank.
    Kan gelijk verdeeld worden of handmatig per persoon.
    """
    __tablename__ = "ho_events"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    name = db.Column(db.String(200), nullable=False)
    total_cost = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, default=datetime.utcnow)
    # 'equal_all'  = gelijk verdeeld over alle actieve leden
    # 'equal_selected' = gelijk verdeeld over geselecteerde leden
    # 'manual'     = handmatig bedrag per persoon
    distribution_type = db.Column(db.String(20), default="equal_all")
    notes = db.Column(db.String(500), nullable=True)

    shares = db.relationship("HOEventShare", backref="event", lazy=True, cascade="all, delete-orphan")


class HOEventShare(db.Model):
    """Aandeel van een persoon in een HO-event (alleen bij manual of equal_selected)."""
    __tablename__ = "ho_event_shares"
    id = db.Column(db.Integer, primary_key=True)
    ho_event_id = db.Column(db.Integer, db.ForeignKey("ho_events.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    amount = db.Column(db.Float, nullable=False)


class Payment(db.Model):
    """Overboeking van een persoon naar de huisrekening."""
    __tablename__ = "payments"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, default=datetime.utcnow)
    notes = db.Column(db.String(200), nullable=True)


class Correction(db.Model):
    """Handmatige correctie op de stand van een persoon."""
    __tablename__ = "corrections"
    id = db.Column(db.Integer, primary_key=True)
    period_id = db.Column(db.Integer, db.ForeignKey("periods.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    amount = db.Column(db.Float, nullable=False)   # positief = credit, negatief = debet
    description = db.Column(db.String(200), nullable=True)
    date = db.Column(db.Date, default=datetime.utcnow)
