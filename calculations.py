"""
Berekeningslogica voor de turfrekening.
Alle financiële berekeningen zitten hier centraal.
"""
from models import (
    db, Period, User, Product, Tally, InventoryPurchase,
    InventorySnapshot, HOEvent, HOEventShare, Payment, Correction,
    PeriodStartBalance
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
    """Returns dict: {product_id: total_quantity_tallied}.
    Producten met parent_product_id tellen mee als parent_units × aantal bij het parent product."""
    rows = (
        db.session.query(Tally.product_id, func.sum(Tally.quantity))
        .filter(Tally.period_id == period_id)
        .group_by(Tally.product_id)
        .all()
    )
    result = {pid: qty for pid, qty in rows}

    # Voeg child product tallies toe aan parent product
    child_products = Product.query.filter(Product.parent_product_id.isnot(None)).all()
    for child in child_products:
        child_qty = result.get(child.id, 0)
        if child_qty > 0:
            units = child.parent_units or 1
            result[child.parent_product_id] = result.get(child.parent_product_id, 0) + child_qty * units

    return result


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
    # Alleen standalone producten — child-producten tellen mee bij hun parent
    products = Product.query.filter_by(is_active=True, parent_product_id=None).all()
    tallied = get_total_tallied_per_product(period_id)

    # Bieren gedronken bij HO-events tellen niet als turfverlies
    ho_beers = {}
    for ev in HOEvent.query.filter_by(period_id=period_id).filter(
        HOEvent.beer_product_id.isnot(None), HOEvent.beer_quantity.isnot(None)
    ).all():
        ho_beers[ev.beer_product_id] = ho_beers.get(ev.beer_product_id, 0) + ev.beer_quantity

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
        ho_qty = ho_beers.get(p.id, 0)
        verlies_qty = gebruikt - geturfd - ho_qty  # negatief = te veel geturfd (credit)
        verlies_eur = verlies_qty * p.price

        result.append({
            "product": p,
            "stock_begin": stock_begin,
            "bijstock": bijstock,
            "stock_eind": stock_eind,
            "gebruikt": gebruikt,
            "geturfd": geturfd,
            "ho_qty": ho_qty,
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
    - Turfverlies en equal_all events worden verdeeld over gebruikers met participates_in_ho=True
    - equal_selected: gelijk over de geselecteerde deelnemers (HOEventShare records)
    - manual: exact het bedrag uit HOEventShare
    - Gebruikers met participates_in_ho=False doen niet mee aan turfverlies/equal_all,
      maar kunnen nog wel aan equal_selected/manual events deelnemen
    """
    ho_users = User.query.filter_by(participates_in_ho=True).all()
    ho_user_ids = {u.id for u in ho_users}
    n_ho = len(ho_users)

    if n_ho == 0:
        return 0.0

    total = 0.0

    # 1. Turfverlies gelijk verdeeld over HO-deelnemers
    if user_id in ho_user_ids:
        turfverlies = get_total_turfverlies(period_id)
        total += turfverlies / n_ho

    # 2. HO events
    events = HOEvent.query.filter_by(period_id=period_id).all()
    for event in events:
        if event.distribution_type == "equal_all":
            if user_id in ho_user_ids:
                total += event.total_cost / n_ho

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


def get_stands_bulk(period_id, users):
    """Berekent eindstand voor alle gebruikers in bulk — geen N+1 queries."""
    payment_rows = (
        db.session.query(Payment.user_id, func.sum(Payment.amount))
        .filter_by(period_id=period_id).group_by(Payment.user_id).all()
    )
    payments_by_user = {uid: amt for uid, amt in payment_rows}

    geturfd_rows = (
        db.session.query(Tally.user_id, func.sum(Tally.quantity * Product.price))
        .join(Product, Tally.product_id == Product.id)
        .filter(Tally.period_id == period_id).group_by(Tally.user_id).all()
    )
    geturfd_by_user = {uid: amt for uid, amt in geturfd_rows}

    correction_rows = (
        db.session.query(Correction.user_id, func.sum(Correction.amount))
        .filter_by(period_id=period_id).group_by(Correction.user_id).all()
    )
    corrections_by_user = {uid: amt for uid, amt in correction_rows}

    turfverlies = get_total_turfverlies(period_id)
    ho_shares = get_ho_shares_bulk(period_id, users, turfverlies)

    snapshots = PeriodStartBalance.query.filter_by(period_id=period_id).all()
    start_balance = {s.user_id: s.balance for s in snapshots}
    for u in users:
        if u.id not in start_balance:
            start_balance[u.id] = u.previous_balance

    return {
        u.id: round(
            start_balance[u.id]
            + payments_by_user.get(u.id, 0.0)
            - geturfd_by_user.get(u.id, 0.0)
            - ho_shares.get(u.id, 0.0)
            + corrections_by_user.get(u.id, 0.0),
            2
        )
        for u in users
    }


# ─── Volledig overzicht ──────────────────────────────────────────────────────

def get_ho_shares_bulk(period_id, users, turfverlies_total):
    """Berekent HO-aandeel voor alle gebruikers in één keer (geen N+1)."""
    ho_user_ids = {u.id for u in users if u.participates_in_ho}
    n_ho = len(ho_user_ids)
    shares = {u.id: 0.0 for u in users}

    if n_ho == 0:
        return shares

    # Turfverlies gelijk over HO-deelnemers
    turfverlies_share = turfverlies_total / n_ho
    for uid in ho_user_ids:
        shares[uid] += turfverlies_share

    # HO events — alle shares in één query ophalen
    events = HOEvent.query.filter_by(period_id=period_id).all()
    if not events:
        return shares

    event_ids = [e.id for e in events]
    all_shares = HOEventShare.query.filter(HOEventShare.ho_event_id.in_(event_ids)).all()
    shares_by_event = {}
    for s in all_shares:
        shares_by_event.setdefault(s.ho_event_id, []).append(s)

    for event in events:
        if event.distribution_type == "equal_all":
            per_person = event.total_cost / n_ho
            for uid in ho_user_ids:
                shares[uid] += per_person

        elif event.distribution_type == "equal_selected":
            event_shares = shares_by_event.get(event.id, [])
            participant_ids = {s.user_id for s in event_shares}
            n = len(participant_ids)
            if n > 0:
                per_person = event.total_cost / n
                for uid in participant_ids:
                    if uid in shares:
                        shares[uid] += per_person

        elif event.distribution_type == "manual":
            for s in shares_by_event.get(event.id, []):
                if s.user_id in shares:
                    shares[s.user_id] += s.amount

    return shares


def get_period_overview(period_id):
    """
    Genereert het volledige maandoverzicht voor een periode.
    Alles wordt in bulk opgehaald — geen N+1 queries.
    """
    period = Period.query.get(period_id)
    users = User.query.order_by(User.sort_order, User.name).all()
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()

    # ── Bulk queries ──────────────────────────────────────────────────────────

    # Betalingen per gebruiker
    payment_rows = (
        db.session.query(Payment.user_id, func.sum(Payment.amount))
        .filter_by(period_id=period_id)
        .group_by(Payment.user_id).all()
    )
    payments_by_user = {uid: amt for uid, amt in payment_rows}

    # Geturfd bedrag per gebruiker
    geturfd_rows = (
        db.session.query(Tally.user_id, func.sum(Tally.quantity * Product.price))
        .join(Product, Tally.product_id == Product.id)
        .filter(Tally.period_id == period_id)
        .group_by(Tally.user_id).all()
    )
    geturfd_by_user = {uid: amt for uid, amt in geturfd_rows}

    # Correcties per gebruiker
    correction_rows = (
        db.session.query(Correction.user_id, func.sum(Correction.amount))
        .filter_by(period_id=period_id)
        .group_by(Correction.user_id).all()
    )
    corrections_by_user = {uid: amt for uid, amt in correction_rows}

    # Turfjes per gebruiker per product
    tally_map = get_tallied_per_user_product(period_id)

    # Voorraad + turfverlies (één keer)
    inventory = get_inventory_data(period_id)
    turfverlies_total = sum(r["turfverlies_eur"] for r in inventory)

    # HO shares in bulk
    ho_shares = get_ho_shares_bulk(period_id, users, turfverlies_total)

    # Beginstand uit snapshot (zodat oude rapporten niet veranderen na nieuwe periode)
    snapshots = PeriodStartBalance.query.filter_by(period_id=period_id).all()
    start_balance = {s.user_id: s.balance for s in snapshots}
    # Geen snapshot → gebruik huidige previous_balance (actieve periode of eerste periode)
    for u in users:
        if u.id not in start_balance:
            start_balance[u.id] = u.previous_balance

    # ── Per-persoon rijen samenstellen ────────────────────────────────────────
    user_rows = []
    for u in users:
        overgemaakt = payments_by_user.get(u.id, 0.0)
        geturfd = geturfd_by_user.get(u.id, 0.0)
        ho = ho_shares.get(u.id, 0.0)
        correctie = corrections_by_user.get(u.id, 0.0)
        vorige = start_balance[u.id]
        stand = vorige + overgemaakt - geturfd - ho + correctie

        user_rows.append({
            "user": u,
            "vorige_stand": vorige,
            "overgemaakt": overgemaakt,
            "geturfd": geturfd,
            "ho": ho,
            "correctie": correctie,
            "stand": stand,
            "tallies_per_product": tally_map.get(u.id, {}),
        })

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
