"""Periodes: afsluiten (bevriezen), nieuwe periode starten, datumcontroles, rapport ophalen."""
import json
from datetime import date, datetime

from calculations import compute_period
from models import (db, Period, User, PeriodReport, PeriodStartBalance, InventorySnapshot,
                    Tally, Payment, Correction, HOEvent, InventoryPurchase)

SCHEMA_VERSION = 1
MAANDEN = ["januari", "februari", "maart", "april", "mei", "juni", "juli", "augustus",
           "september", "oktober", "november", "december"]


class PeriodError(Exception):
    """Fout met een melding die direct aan de gebruiker getoond mag worden."""


# ─── Bevriezen en teruglezen ────────────────────────────────────────────────

def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Niet serialiseerbaar: {type(value)}")


def freeze(overview):
    return json.dumps(overview, default=_json_default, ensure_ascii=False)


def revive(raw):
    ov = json.loads(raw)
    p = ov["period"]
    p["start_date"] = date.fromisoformat(p["start_date"])
    p["end_date"] = date.fromisoformat(p["end_date"]) if p["end_date"] else None
    p["closed_at"] = datetime.fromisoformat(p["closed_at"]) if p["closed_at"] else None
    for ev in ov["ho_events"]:
        ev["date"] = date.fromisoformat(ev["date"]) if ev["date"] else None
    for row in ov["user_rows"]:
        row["tallies_per_product"] = {int(k): v for k, v in row["tallies_per_product"].items()}
    return ov


def get_period_view(period_id):
    """Rapportgegevens: bevroren uit PeriodReport voor afgesloten periodes, anders live.
    Voegt 'state' toe: 'open' | 'closed' | 'historic' (inactief maar nooit bevroren)."""
    period = db.session.get(Period, period_id)
    if period.closed_at:
        report = PeriodReport.query.filter_by(period_id=period_id).first()
        if report:
            ov = revive(report.data)
            ov["state"] = "closed"
            return ov
    ov = compute_period(period_id)
    ov["state"] = "open" if period.is_active else "historic"
    return ov


# ─── Datumcontroles ─────────────────────────────────────────────────────────

def check_period_dates(start, end, exclude_id=None, today=None):
    """Waarschuwingen (geen blokkades) over toekomst, overlap en gaten."""
    today = today or date.today()
    warnings = []
    if end and end < start:
        raise PeriodError("De einddatum ligt vóór de startdatum.")
    if start > today:
        warnings.append("De startdatum ligt in de toekomst.")
    if end and end > today:
        warnings.append("De einddatum ligt in de toekomst.")
    others = [p for p in Period.query.order_by(Period.start_date) if p.id != exclude_id]
    for o in others:
        o_end = o.end_date or date.max
        if start < o_end and o.start_date < (end or date.max):
            warnings.append(f"Overlap met periode '{o.name}'.")
    before = [o for o in others if o.end_date and o.end_date <= start]
    if before:
        prev = max(before, key=lambda o: o.end_date)
        if prev.end_date < start:
            warnings.append(f"Er zit een gat tussen '{prev.name}' (t/m {prev.end_date:%d-%m-%Y}) en deze periode.")
    return warnings


def default_next_name(start):
    return f"Turfrekening {MAANDEN[start.month - 1]} {start.year}"


# ─── Aanmaken, afsluiten ────────────────────────────────────────────────────

def has_data(period):
    pid = period.id
    return any(m.query.filter_by(period_id=pid).first() for m in
               (Tally, Payment, Correction, HOEvent, InventoryPurchase, InventorySnapshot, PeriodStartBalance))


def create_first_period(name, start):
    """Alleen als er nog geen actieve periode is. Beginstanden zijn 0."""
    if Period.query.filter_by(is_active=True).first():
        raise PeriodError("Er is al een actieve periode. Sluit die af om een nieuwe te starten.")
    if not name.strip():
        raise PeriodError("Geef de periode een naam.")
    period = Period(name=name.strip(), start_date=start, is_active=True)
    db.session.add(period)
    db.session.commit()
    return period


def close_blockers(overview):
    """Redenen waarom afsluiten niet kan."""
    blockers = []
    if not overview["inventory"]:
        blockers.append("Er is nog geen voorraad geregistreerd. Vul begin- en eindtelling in.")
    elif not overview["inventory_complete"]:
        missing = ", ".join(r["product"]["name"] for r in overview["inventory"] if not r["counted"])
        blockers.append(f"Eindtelling ontbreekt voor: {missing}.")
    return blockers


def close_period(period, end_date, next_name, next_start):
    """Sluit de periode af en start de volgende. Alles in één transactie.
    Geeft (nieuwe_periode, waarschuwingen) terug."""
    period = db.session.get(Period, period.id)
    if period.closed_at or not period.is_active:
        raise PeriodError("Deze periode is al afgesloten.")
    if end_date < period.start_date:
        raise PeriodError("De einddatum ligt vóór de startdatum.")
    if not next_name.strip():
        raise PeriodError("Geef de nieuwe periode een naam.")
    if next_start < end_date:
        raise PeriodError("De nieuwe periode kan niet beginnen vóór het einde van de vorige.")

    warnings = []
    if next_start != end_date:
        warnings.append(f"De nieuwe periode begint niet op de einddatum van de vorige ({end_date:%d-%m-%Y}).")

    try:
        period.end_date = end_date
        overview = compute_period(period.id)
        blockers = close_blockers(overview)
        if blockers:
            raise PeriodError(" ".join(blockers))

        overview["state"] = "closed"
        now = datetime.utcnow()
        overview["period"]["closed_at"] = now
        overview["period"]["is_active"] = False
        db.session.add(PeriodReport(period_id=period.id, schema_version=SCHEMA_VERSION, data=freeze(overview)))
        period.closed_at = now
        period.is_active = False

        new_period = Period(name=next_name.strip(), start_date=next_start, is_active=True)
        db.session.add(new_period)
        db.session.flush()

        # Beginstanden: eindstand per persoon. Vertrokken bewoners zonder saldo vallen af.
        for row in overview["user_rows"]:
            user = db.session.get(User, row["user"]["id"])
            if user.is_active or row["stand"] != 0:
                db.session.add(PeriodStartBalance(period_id=new_period.id, user_id=user.id,
                                                  balance_cents=row["stand"]))
        # Beginvoorraad: de eindtelling van de vorige periode
        for snap in InventorySnapshot.query.filter_by(period_id=period.id, snapshot_type="end"):
            db.session.add(InventorySnapshot(period_id=new_period.id, product_id=snap.product_id,
                                             snapshot_type="begin", quantity=snap.quantity, date=next_start))
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return new_period, warnings
