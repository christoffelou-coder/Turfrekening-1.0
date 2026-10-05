"""
Berekeningslogica voor de turfrekening.

Eén rekenpad: compute_period(period_id) levert alles wat rapport, admin en
dashboard nodig hebben. Alle bedragen zijn hele centen (int).

Stand = beginstand + overgemaakt − geturfd − HO + correctie
"""
from datetime import date

from sqlalchemy import func

from models import (
    db, Period, User, Product, Tally, InventoryPurchase,
    InventorySnapshot, HOEvent, HOEventShare, Payment, Correction,
    PeriodStartBalance,
)
from money import split_even


def _user_dict(u):
    return {"id": u.id, "name": u.name, "is_active": bool(u.is_active)}


def _product_dict(p):
    return {"id": p.id, "name": p.name, "emoji": p.emoji, "price_cents": p.price_cents,
            "is_active": bool(p.is_active), "image_url": p.image_url}


def _period_dict(p):
    return {"id": p.id, "name": p.name, "start_date": p.start_date, "end_date": p.end_date,
            "is_active": bool(p.is_active), "closed_at": p.closed_at}


def get_active_period():
    return Period.query.filter_by(is_active=True).order_by(Period.id.desc()).first()


# ─── Turfjes ─────────────────────────────────────────────────────────────────

def get_tallied_per_user_product(period_id):
    """{user_id: {product_id: quantity}}"""
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


def get_total_tallied_per_product(period_id):
    """{product_id: totaal geturfd}. Een child-product (halve krat) telt mee als
    parent_units × aantal bij het parent-product."""
    rows = (
        db.session.query(Tally.product_id, func.sum(Tally.quantity))
        .filter(Tally.period_id == period_id)
        .group_by(Tally.product_id)
        .all()
    )
    result = {pid: qty for pid, qty in rows}
    for child in Product.query.filter(Product.parent_product_id.isnot(None)).all():
        child_qty = result.get(child.id, 0)
        if child_qty:
            result[child.parent_product_id] = (
                result.get(child.parent_product_id, 0) + child_qty * (child.parent_units or 1)
            )
    return result


# ─── Hoofdberekening ─────────────────────────────────────────────────────────

def _sum_by_user(model, column, period_id):
    rows = (
        db.session.query(model.user_id, func.sum(column))
        .filter(model.period_id == period_id)
        .group_by(model.user_id)
        .all()
    )
    return {uid: int(total or 0) for uid, total in rows}


def _inventory(period_id):
    """Voorraadregels per standalone product dat in deze periode iets te maken heeft.
    Zonder eindtelling is een regel 'niet geteld' en telt hij niet mee als turfverlies."""
    begin = {s.product_id: s.quantity for s in
             InventorySnapshot.query.filter_by(period_id=period_id, snapshot_type="begin")}
    end = {s.product_id: s.quantity for s in
           InventorySnapshot.query.filter_by(period_id=period_id, snapshot_type="end")}
    bought = {pid: int(q or 0) for pid, q in
              db.session.query(InventoryPurchase.product_id, func.sum(InventoryPurchase.quantity))
              .filter(InventoryPurchase.period_id == period_id)
              .group_by(InventoryPurchase.product_id)}
    tallied = get_total_tallied_per_product(period_id)

    ho_beers = {}
    for ev in HOEvent.query.filter_by(period_id=period_id).filter(
        HOEvent.beer_product_id.isnot(None), HOEvent.beer_quantity.isnot(None)
    ):
        ho_beers[ev.beer_product_id] = ho_beers.get(ev.beer_product_id, 0) + ev.beer_quantity

    relevant = set(begin) | set(end) | set(bought) | set(tallied)
    products = (
        Product.query.filter(Product.parent_product_id.is_(None), Product.id.in_(relevant))
        .order_by(Product.sort_order, Product.id).all()
        if relevant else []
    )

    rows = []
    for p in products:
        counted = p.id in end
        stock_begin = begin.get(p.id, 0)
        bijstock = bought.get(p.id, 0)
        geturfd = tallied.get(p.id, 0)
        ho_qty = ho_beers.get(p.id, 0)
        if counted:
            gebruikt = stock_begin + bijstock - end[p.id]
            verlies_qty = gebruikt - geturfd - ho_qty  # negatief = meer geturfd dan gebruikt
            verlies_cents = verlies_qty * p.price_cents
        else:
            gebruikt = verlies_qty = None
            verlies_cents = 0
        rows.append({
            "product": _product_dict(p),
            "counted": counted,
            "stock_begin": stock_begin,
            "bijstock": bijstock,
            "stock_eind": end.get(p.id),
            "gebruikt": gebruikt,
            "geturfd": geturfd,
            "ho_qty": ho_qty,
            "turfverlies_qty": verlies_qty,
            "turfverlies_cents": verlies_cents,
        })
    return rows


def compute_period(period_id):
    period = db.session.get(Period, period_id)
    all_users = User.query.order_by(User.sort_order, User.name).all()
    is_open = bool(period.is_active)

    # ── Ruwe gegevens, allemaal in bulk ──
    tally_rows = (
        db.session.query(Tally.user_id, func.sum(Tally.quantity * Tally.unit_price_cents))
        .filter(Tally.period_id == period_id).group_by(Tally.user_id).all()
    )
    geturfd_by_user = {uid: int(c or 0) for uid, c in tally_rows}
    tally_map = get_tallied_per_user_product(period_id)
    paid_by_user = _sum_by_user(Payment, Payment.amount_cents, period_id)
    corr_by_user = _sum_by_user(Correction, Correction.amount_cents, period_id)
    start_by_user = {s.user_id: s.balance_cents for s in
                     PeriodStartBalance.query.filter_by(period_id=period_id)}

    events = HOEvent.query.filter_by(period_id=period_id).order_by(HOEvent.date, HOEvent.id).all()
    shares_by_event = {}
    if events:
        for s in HOEventShare.query.filter(HOEventShare.ho_event_id.in_([e.id for e in events])):
            shares_by_event.setdefault(s.ho_event_id, []).append(s)
    share_user_ids = {s.user_id for lst in shares_by_event.values() for s in lst}

    # ── Wie doet mee in deze periode ──
    activity = set(geturfd_by_user) | set(tally_map) | set(paid_by_user) | set(corr_by_user)
    present = activity | set(start_by_user) | share_user_ids
    users = [u for u in all_users if u.id in present or (is_open and u.is_active)]
    user_ids = [u.id for u in users]

    # ── Voorraad en turfverlies ──
    inventory = _inventory(period_id)
    inventory_complete = bool(inventory) and all(r["counted"] for r in inventory)
    turfverlies_total = sum(r["turfverlies_cents"] for r in inventory)
    # Zonder volledige eindtelling is turfverlies voorlopig en wordt het niet verdeeld.
    turfverlies_distributed = turfverlies_total if inventory_complete else 0

    # ── HO verdelen (exact in centen) ──
    warnings = []
    participants = [u.id for u in users if u.participates_in_ho]
    ho_turf = {uid: 0 for uid in user_ids}
    ho_events_by_user = {uid: 0 for uid in user_ids}
    if participants:
        for uid, c in zip(participants, split_even(turfverlies_distributed, len(participants))):
            ho_turf[uid] += c
    elif turfverlies_distributed:
        warnings.append({"code": "no_ho_participants", "message": "Niemand doet mee aan HO: turfverlies is niet verdeeld."})

    ho_events_total = 0
    for ev in events:
        shares = shares_by_event.get(ev.id, [])
        if ev.distribution_type == "equal_all":
            targets = participants
        elif ev.distribution_type == "equal_selected":
            chosen = {s.user_id for s in shares}
            targets = [uid for uid in user_ids if uid in chosen]
        else:  # manual
            targets = None

        if targets is None:
            for s in shares:
                if s.user_id in ho_events_by_user:
                    ho_events_by_user[s.user_id] += s.amount_cents
            ho_events_total += sum(s.amount_cents for s in shares)
            if sum(s.amount_cents for s in shares) != ev.total_cost_cents:
                warnings.append({"code": "manual_mismatch",
                                 "message": f"HO-post '{ev.name}': handmatige bedragen tellen niet op tot het totaal."})
        elif targets:
            for uid, c in zip(targets, split_even(ev.total_cost_cents, len(targets))):
                ho_events_by_user[uid] += c
            ho_events_total += ev.total_cost_cents
        else:
            warnings.append({"code": "event_no_targets",
                             "message": f"HO-post '{ev.name}' heeft niemand om over te verdelen."})

    # ── Rijen per persoon ──
    user_rows = []
    for u in users:
        begin = start_by_user.get(u.id, 0)           # geen beginstand = 0, nooit een terugval
        paid = paid_by_user.get(u.id, 0)
        geturfd = geturfd_by_user.get(u.id, 0)
        ho = ho_turf[u.id] + ho_events_by_user[u.id]
        corr = corr_by_user.get(u.id, 0)
        user_rows.append({
            "user": _user_dict(u),
            "vorige_stand": begin,
            "overgemaakt": paid,
            "geturfd": geturfd,
            "ho": ho,
            "ho_turfverlies": ho_turf[u.id],
            "ho_events": ho_events_by_user[u.id],
            "correctie": corr,
            "stand": begin + paid - geturfd - ho + corr,
            "tallies_per_product": tally_map.get(u.id, {}),
        })
        if u.id not in activity:
            warnings.append({"code": "no_activity", "message": f"{u.name} heeft geen activiteit in deze periode."})

    ho_values = [r["ho"] for r in user_rows]
    ho_uniform = bool(ho_values) and max(ho_values) - min(ho_values) <= 1  # ≤ 1 cent door afronding

    if inventory and not inventory_complete:
        missing = ", ".join(r["product"]["name"] for r in inventory if not r["counted"])
        warnings.append({"code": "inventory_incomplete", "message": f"Eindtelling ontbreekt voor: {missing}."})
    if inventory_complete and turfverlies_total < 0:
        warnings.append({"code": "negative_turfverlies", "message": "Negatief turfverlies: er is meer geturfd dan gebruikt."})
    if period.end_date and period.end_date > date.today():
        warnings.append({"code": "end_in_future", "message": "De einddatum ligt in de toekomst."})

    # Producten die in het rapport als kolom staan: actief, of met turfjes in deze periode
    tallied_product_ids = {pid for per_user in tally_map.values() for pid in per_user}
    report_products = [_product_dict(p) for p in Product.query.order_by(Product.sort_order, Product.id)
                       if p.is_active or p.id in tallied_product_ids]

    beer_names = {p.id: p for p in Product.query.filter(Product.id.in_([e.beer_product_id for e in events if e.beer_product_id]))} if events else {}
    event_dicts = [{
        "id": e.id, "name": e.name, "date": e.date, "notes": e.notes,
        "total_cost_cents": e.total_cost_cents, "distribution_type": e.distribution_type,
        "beer_quantity": e.beer_quantity,
        "beer_product_name": beer_names[e.beer_product_id].name if e.beer_product_id in beer_names else None,
        "beer_cost_cents": (e.beer_quantity or 0) * beer_names[e.beer_product_id].price_cents if e.beer_product_id in beer_names else 0,
    } for e in events]

    return {
        "period": _period_dict(period),
        "users": [_user_dict(u) for u in users],
        "products": report_products,
        "user_rows": user_rows,
        "inventory": inventory,
        "inventory_complete": inventory_complete,
        "turfverlies_total": turfverlies_total,
        "turfverlies_distributed": turfverlies_distributed,
        "ho_events": event_dicts,
        "ho_events_total": ho_events_total,
        "total_ho": turfverlies_distributed + ho_events_total,
        "ho_uniform": ho_uniform,
        "ho_per_person": user_rows[0]["ho"] if ho_uniform else None,
        "active_count": len(users),
        "warnings": warnings,
        "totals": {
            "geturfd": sum(r["geturfd"] for r in user_rows),
            "overgemaakt": sum(r["overgemaakt"] for r in user_rows),
            "ho": sum(r["ho"] for r in user_rows),
            "correctie": sum(r["correctie"] for r in user_rows),
            "vorige_stand": sum(r["vorige_stand"] for r in user_rows),
            "stand": sum(r["stand"] for r in user_rows),
        },
    }


# ─── Dashboard-status ────────────────────────────────────────────────────────

def get_period_status(period, overview=None):
    ov = overview or compute_period(period.id)
    return {
        "days": max((date.today() - period.start_date).days, 0),
        "geturfd": ov["totals"]["geturfd"],
        "betaald": ov["totals"]["overgemaakt"],
        "ho_events": len(ov["ho_events"]),
        "inventory_done": ov["inventory_complete"],
    }
