"""
Berekeningslogica voor de turfrekening.
Alle financiële berekeningen zitten hier centraal.
"""
from models import (
    db, Period, User, Product, Tally, InventoryPurchase,
    InventorySnapshot, HOEvent, HOEventShare, Payment, Correction
)
from sqlalchemy import func


def get_active_period():
    return Period.query.filter_by(is_active=True).order_by(Period.id.desc()).first()


# ─── Turfjes per persoon ────────────────────────────────────────────────────

def get_tallied_per_user_product(period_id):
    """Returns dict: {user_id: {product_id: quantity}}"""
    rows = (
        db.session.query(Tally.user_id, Tally.product_id, func.sum(Tally.quantity))
        .filter(Tally.period_id == period_id)
        .group_by(Tally.user_id, Tally.product_id)
        .all()
    )
    result = {}
    for user_id, product_id, qty in rows:
        result.setdefault(user_id, {})[product_id] = qty
    return result


def get_geturfd_cost(period_id, user_id):
    """Totale kosten van turfjes voor een persoon in een periode."""
    rows = (
        db.session.query(func.sum(Tally.quantity * Product.price))
        .join(Product, Tally.product_id == Product.id)
        .filter(Tally.period_id == period_id, Tally.user_id == user_id)
        .scalar()
    )
    return rows or 0.0


def get_total_tallied_per_product(period_id):
    """Returns dict: {product_id: total_quantity_tallied}"""
    rows = (
        db.session.query(Tally.product_id, func.sum(Tally.quantity))
        .filter(Tally.period_id == period_id)
        .group_by(Tally.product_id)
        .all()
    )
    return {pid: qty for pid, qty in rows}


# ─── Voorraad ───────────────────────────────────────────────────────────────

def get_inventory_data(period_id):
    """
    Berekent per product:
      - stock_begin, bijstock, stock_eind
      - gebruikt (= begin + bij - eind)
      - geturfd
      - turfverlies_qty (= gebruikt - geturfd), kan negatief zijn (dan geen verlies)
      - turfverlies_eur
    Returns list of dicts.
    """
    products = Product.query.filter_by(is_active=True).all()
    tallied = get_total_tallied_per_product(period_id)

    result = []
    for p in products:
        begin_snap = InventorySnapshot.query.filter_by(
            period_id=period_id, product_id=p.id, snapshot_type="begin"
        ).first()
        end_snap = InventorySnapshot.query.filter_by(
            period_id=period_id, product_id=p.id, snapshot_type="end"
        ).first()

        bijstock = (
            db.session.query(func.sum(InventoryPurchase.quantity))
            .filter_by(period_id=period_id, product_id=p.id)
            .scalar() or 0
        )

        stock_begin = begin_snap.quantity if begin_snap else 0
        stock_eind = end_snap.quantity if end_snap else 0
        gebruikt = stock_begin + bijstock - stock_eind
        geturfd = tallied.get(p.id, 0)
        verlies_qty = max(0, gebruikt - geturfd)
        verlies_eur = verlies_qty * p.price

        result.append({
            "product": p,
            "stock_begin": stock_begin,
            "bijstock": bijstock,
            "stock_eind": stock_eind,
            "gebruikt": gebruikt,
            "geturfd": geturfd,
            "turfverlies_qty": verlies_qty,
            "turfverlies_eur": verlies_eur,
        })
    return result


def get_total_turfverlies(period_id):
    """Totaal turfverlies in euro's voor een periode."""
    inv = get_inventory_data(period_id)
    return sum(row["turfverlies_eur"] for row in inv)


# ─── HO berekening ──────────────────────────────────────────────────────────

def get_ho_events_total(period_id):
    """Totale kosten van HO-events (excl. turfverlies)."""
    total = (
        db.session.query(func.sum(HOEvent.total_cost))
        .filter_by(period_id=period_id)
        .scalar()
    )
    return total or 0.0


def get_ho_share_for_user(period_id, user_id):
    """
    Berekent het HO-aandeel voor een specifieke gebruiker in een periode.

    Logica:
    - Turfverlies wordt GELIJK verdeeld over alle actieve gebruikers die deelnemen aan HO
    - Voor HOEvents: afhankelijk van distribution_type:
        - equal_all: gelijk over alle actieve users
        - equal_selected: gelijk over geselecteerde users (via HOEventShare records)
        - manual: exact het bedrag uit HOEventShare
    """
    active_users = User.query.filter_by(is_active=True).all()
    active_user_ids = {u.id for u in active_users}
    n_active = len(active_users)

    if n_active == 0:
        return 0.0

    total = 0.0

    # 1. Turfverlies gelijk verdeeld
    turfverlies = get_total_turfverlies(period_id)
    total += turfverlies / n_active

    # 2. HO events
    events = HOEvent.query.filter_by(period_id=period_id).all()
    for event in events:
        if event.distribution_type == "equal_all":
            total += event.total_cost / n_active

        elif event.distribution_type == "equal_selected":
            shares = HOEventShare.query.filter_by(ho_event_id=event.id).all()
            participant_ids = {s.user_id for s in shares}
            if user_id in participant_ids:
                n_participants = len(participant_ids)
                total += event.total_cost / n_participants if n_participants > 0 else 0

        elif event.distribution_type == "manual":
            share = HOEventShare.query.filter_by(
                ho_event_id=event.id, user_id=user_id
            ).first()
            if share:
                total += share.amount

    return total


def get_total_ho_per_person(period_id):
    """Totale HO-kosten gedeeld door actief aantal personen (voor overzicht)."""
    active_count = User.query.filter_by(is_active=True).count()
    if active_count == 0:
        return 0.0
    turfverlies = get_total_turfverlies(period_id)
    events_total = get_ho_events_total(period_id)
    # equal_all events only in this simplified total
    return (turfverlies + events_total) / active_count


# ─── Betalingen & correcties ────────────────────────────────────────────────

def get_payments_total(period_id, user_id):
    total = (
        db.session.query(func.sum(Payment.amount))
        .filter_by(period_id=period_id, user_id=user_id)
        .scalar()
    )
    return total or 0.0


def get_corrections_total(period_id, user_id):
    total = (
        db.session.query(func.sum(Correction.amount))
        .filter_by(period_id=period_id, user_id=user_id)
        .scalar()
    )
    return total or 0.0


# ─── Stand per persoon ──────────────────────────────────────────────────────

def get_stand(user, period_id):
    """
    Berekent de huidige stand voor een gebruiker.
    Stand = Vorige Stand + Overgemaakt − Geturfd − HO + Correctie
    """
    overgemaakt = get_payments_total(period_id, user.id)
    geturfd = get_geturfd_cost(period_id, user.id)
    ho = get_ho_share_for_user(period_id, user.id)
    correctie = get_corrections_total(period_id, user.id)
    return user.previous_balance + overgemaakt - geturfd - ho + correctie


# ─── Volledig overzicht ──────────────────────────────────────────────────────

def get_period_overview(period_id):
    """
    Genereert het volledige maandoverzicht voor een periode.
    Returns dict met alle benodigde data voor het rapport.
    """
    period = Period.query.get(period_id)
    users = User.query.filter_by(is_active=True).order_by(User.name).all()
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()

    # Per-persoon overzicht
    user_rows = []
    for u in users:
        overgemaakt = get_payments_total(period_id, u.id)
        geturfd = get_geturfd_cost(period_id, u.id)
        ho = get_ho_share_for_user(period_id, u.id)
        correctie = get_corrections_total(period_id, u.id)
        stand = u.previous_balance + overgemaakt - geturfd - ho + correctie

        # Turfjes per product
        tally_map = get_tallied_per_user_product(period_id)
        user_tallies = tally_map.get(u.id, {})

        user_rows.append({
            "user": u,
            "vorige_stand": u.previous_balance,
            "overgemaakt": overgemaakt,
            "geturfd": geturfd,
            "ho": ho,
            "correctie": correctie,
            "stand": stand,
            "tallies_per_product": user_tallies,
        })

    # Voorraadoverzicht
    inventory = get_inventory_data(period_id)
    turfverlies_total = sum(r["turfverlies_eur"] for r in inventory)

    # HO events
    ho_events = HOEvent.query.filter_by(period_id=period_id).all()
    ho_events_total = sum(e.total_cost for e in ho_events)

    total_ho = turfverlies_total + ho_events_total
    active_count = len(users)
    ho_per_person = total_ho / active_count if active_count > 0 else 0

    return {
        "period": period,
        "users": users,
        "products": products,
        "user_rows": user_rows,
        "inventory": inventory,
        "turfverlies_total": turfverlies_total,
        "ho_events": ho_events,
        "ho_events_total": ho_events_total,
        "total_ho": total_ho,
        "ho_per_person": ho_per_person,
        "active_count": active_count,
    }
