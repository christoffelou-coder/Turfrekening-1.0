import os
from flask import Flask, render_template, request, jsonify, redirect, url_for
from datetime import date, datetime
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()
from models import (
    db, Period, User, Product, Tally, InventoryPurchase,
    InventorySnapshot, HOEvent, HOEventShare, Payment, Correction
)
from calculations import (
    get_active_period, get_stand, get_geturfd_cost, get_payments_total,
    get_corrections_total, get_ho_share_for_user, get_period_overview,
    get_inventory_data, get_total_turfverlies, get_tallied_per_user_product
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

# Gebruik DATABASE_URL omgevingsvariabele (Railway/Supabase), anders lokale SQLite
database_url = os.environ.get("DATABASE_URL")
if database_url:
    # Railway/Supabase geeft soms 'postgres://' maar SQLAlchemy wil 'postgresql://'
    database_url = database_url.replace("postgres://", "postgresql://", 1)
else:
    database_url = f"sqlite:///{os.path.join(BASE_DIR, 'turfrekening.db')}"

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.secret_key = os.environ.get("SECRET_KEY", "turfrekening-secret-2024")

db.init_app(app)


# ─── Init DB ─────────────────────────────────────────────────────────────────

@app.cli.command("init-db")
def init_db():
    db.create_all()
    print("Database aangemaakt.")


def create_tables():
    with app.app_context():
        db.create_all()


# ════════════════════════════════════════════════════════════════════════════
# HOOFD SCHERM — iPad turfinterface
# ════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    period = get_active_period()
    users = User.query.filter_by(is_active=True).order_by(User.name).all()
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()

    # Huidige stand per persoon
    user_balances = {}
    if period:
        for u in users:
            user_balances[u.id] = get_stand(u, period.id)

    return render_template(
        "turf.html",
        period=period,
        users=users,
        products=products,
        user_balances=user_balances,
    )


# ─── API: Turf aanslaan ───────────────────────────────────────────────────────

@app.route("/api/tally", methods=["POST"])
def add_tally():
    data = request.json
    period = get_active_period()
    if not period:
        return jsonify({"error": "Geen actieve periode"}), 400

    user = User.query.get(data["user_id"])
    product = Product.query.get(data["product_id"])
    if not user or not product:
        return jsonify({"error": "Gebruiker of product niet gevonden"}), 404

    tally = Tally(
        period_id=period.id,
        user_id=user.id,
        product_id=product.id,
        quantity=data.get("quantity", 1),
    )
    db.session.add(tally)
    db.session.commit()

    new_stand = get_stand(user, period.id)
    return jsonify({
        "ok": True,
        "tally_id": tally.id,
        "user": user.name,
        "product": product.name,
        "quantity": tally.quantity,
        "new_stand": round(new_stand, 2),
    })


@app.route("/api/tally/<int:tally_id>", methods=["DELETE"])
def undo_tally(tally_id):
    tally = Tally.query.get_or_404(tally_id)
    user = tally.user
    period_id = tally.period_id
    db.session.delete(tally)
    db.session.commit()
    new_stand = get_stand(user, period_id)
    return jsonify({"ok": True, "new_stand": round(new_stand, 2)})


@app.route("/api/last-tally")
def last_tally():
    """Geeft het meest recente turfje terug (voor undo-knop)."""
    period = get_active_period()
    if not period:
        return jsonify({"tally": None})
    tally = (
        Tally.query.filter_by(period_id=period.id)
        .order_by(Tally.created_at.desc())
        .first()
    )
    if not tally:
        return jsonify({"tally": None})
    return jsonify({
        "tally": {
            "id": tally.id,
            "user": tally.user.name,
            "product": tally.product.name,
            "quantity": tally.quantity,
            "created_at": tally.created_at.strftime("%H:%M"),
        }
    })


@app.route("/api/product-counts/<int:product_id>")
def product_counts(product_id):
    """Geeft het aantal turfjes per persoon voor een product in de actieve periode."""
    period = get_active_period()
    if not period:
        return jsonify({})
    tally_map = get_tallied_per_user_product(period.id)
    counts = {}
    for user_id, products in tally_map.items():
        counts[str(user_id)] = products.get(product_id, 0)
    return jsonify(counts)


@app.route("/api/balance/<int:user_id>")
def get_balance(user_id):
    user = User.query.get_or_404(user_id)
    period = get_active_period()
    if not period:
        return jsonify({"stand": user.previous_balance})
    stand = get_stand(user, period.id)
    return jsonify({"stand": round(stand, 2)})


# ════════════════════════════════════════════════════════════════════════════
# RAPPORT — maandoverzicht
# ════════════════════════════════════════════════════════════════════════════

@app.route("/rapport")
@app.route("/rapport/<int:period_id>")
def rapport(period_id=None):
    if period_id is None:
        p = get_active_period()
        period_id = p.id if p else None
    if period_id is None:
        return render_template("rapport.html", overview=None, periods=[])

    overview = get_period_overview(period_id)
    periods = Period.query.order_by(Period.start_date.desc()).all()
    return render_template("rapport.html", overview=overview, periods=periods, current_period_id=period_id)


# ════════════════════════════════════════════════════════════════════════════
# ADMIN — gebruikers
# ════════════════════════════════════════════════════════════════════════════

@app.route("/admin")
def admin():
    period = get_active_period()
    users = User.query.order_by(User.name).all()
    products = Product.query.order_by(Product.sort_order).all()
    periods = Period.query.order_by(Period.start_date.desc()).all()
    return render_template("admin/index.html", period=period, users=users, products=products, periods=periods)


# Gebruikers
@app.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            if name:
                user = User(name=name, previous_balance=float(request.form.get("previous_balance", 0)))
                db.session.add(user)
                db.session.commit()
        elif action == "edit":
            user = User.query.get(request.form.get("user_id"))
            if user:
                user.name = request.form.get("name", user.name).strip()
                user.is_active = "is_active" in request.form
                user.previous_balance = float(request.form.get("previous_balance", user.previous_balance))
                db.session.commit()
        elif action == "delete":
            user = User.query.get(request.form.get("user_id"))
            if user:
                db.session.delete(user)
                db.session.commit()
        return redirect(url_for("admin_users"))

    users = User.query.order_by(User.name).all()
    period = get_active_period()
    user_stands = {}
    if period:
        for u in users:
            user_stands[u.id] = round(get_stand(u, period.id), 2)
    return render_template("admin/users.html", users=users, user_stands=user_stands, period=period)


# Producten
@app.route("/admin/products", methods=["GET", "POST"])
def admin_products():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            price = float(request.form.get("price", 0))
            emoji = request.form.get("emoji", "🍺").strip()
            sort_order = int(request.form.get("sort_order", 0))
            if name:
                product = Product(name=name, price=price, emoji=emoji, sort_order=sort_order)
                db.session.add(product)
                db.session.commit()
        elif action == "edit":
            product = Product.query.get(request.form.get("product_id"))
            if product:
                product.name = request.form.get("name", product.name).strip()
                product.price = float(request.form.get("price", product.price))
                product.emoji = request.form.get("emoji", product.emoji).strip()
                product.sort_order = int(request.form.get("sort_order", product.sort_order))
                product.is_active = "is_active" in request.form
                db.session.commit()
        elif action == "delete":
            product = Product.query.get(request.form.get("product_id"))
            if product:
                db.session.delete(product)
                db.session.commit()
        return redirect(url_for("admin_products"))

    products = Product.query.order_by(Product.sort_order).all()
    return render_template("admin/products.html", products=products)


# Periodes
@app.route("/admin/periods", methods=["GET", "POST"])
def admin_periods():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            start_date = datetime.strptime(request.form.get("start_date"), "%Y-%m-%d").date()
            # Deactiveer alle andere periodes
            Period.query.update({"is_active": False})
            period = Period(name=name, start_date=start_date, is_active=True)
            db.session.add(period)
            db.session.commit()
        elif action == "activate":
            Period.query.update({"is_active": False})
            period = Period.query.get(request.form.get("period_id"))
            if period:
                period.is_active = True
                db.session.commit()
        elif action == "close":
            period = Period.query.get(request.form.get("period_id"))
            if period:
                period.end_date = datetime.strptime(request.form.get("end_date"), "%Y-%m-%d").date()
                period.is_active = False
                db.session.commit()
        return redirect(url_for("admin_periods"))

    periods = Period.query.order_by(Period.start_date.desc()).all()
    return render_template("admin/periods.html", periods=periods)


# Voorraad
@app.route("/admin/inventory", methods=["GET", "POST"])
def admin_inventory():
    period = get_active_period()
    if not period:
        return render_template("admin/inventory.html", period=None, inventory=[], products=[])

    if request.method == "POST":
        action = request.form.get("action")

        if action == "snapshot":
            snap_type = request.form.get("snapshot_type")  # begin or end
            for key, val in request.form.items():
                if key.startswith("qty_"):
                    product_id = int(key[4:])
                    qty = int(val or 0)
                    # Update or create snapshot
                    snap = InventorySnapshot.query.filter_by(
                        period_id=period.id, product_id=product_id, snapshot_type=snap_type
                    ).first()
                    if snap:
                        snap.quantity = qty
                    else:
                        snap = InventorySnapshot(
                            period_id=period.id, product_id=product_id,
                            snapshot_type=snap_type, quantity=qty,
                            date=date.today()
                        )
                        db.session.add(snap)
            db.session.commit()

        elif action == "purchase":
            product_id = int(request.form.get("product_id"))
            quantity = int(request.form.get("quantity", 0))
            total_cost = request.form.get("total_cost")
            notes = request.form.get("notes", "").strip()
            purchase = InventoryPurchase(
                period_id=period.id,
                product_id=product_id,
                quantity=quantity,
                total_cost=float(total_cost) if total_cost else None,
                notes=notes,
                date=date.today(),
            )
            db.session.add(purchase)
            db.session.commit()

        elif action == "delete_purchase":
            purchase = InventoryPurchase.query.get(request.form.get("purchase_id"))
            if purchase:
                db.session.delete(purchase)
                db.session.commit()

        return redirect(url_for("admin_inventory"))

    inventory = get_inventory_data(period.id)
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()
    purchases = InventoryPurchase.query.filter_by(period_id=period.id).order_by(InventoryPurchase.date.desc()).all()

    # Huidige snapshots
    begin_snaps = {
        s.product_id: s.quantity
        for s in InventorySnapshot.query.filter_by(period_id=period.id, snapshot_type="begin").all()
    }
    end_snaps = {
        s.product_id: s.quantity
        for s in InventorySnapshot.query.filter_by(period_id=period.id, snapshot_type="end").all()
    }

    return render_template(
        "admin/inventory.html",
        period=period,
        inventory=inventory,
        products=products,
        purchases=purchases,
        begin_snaps=begin_snaps,
        end_snaps=end_snaps,
    )


# Betalingen
@app.route("/admin/payments", methods=["GET", "POST"])
def admin_payments():
    period = get_active_period()
    if not period:
        return render_template("admin/payments.html", period=None, payments=[], users=[])

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            user_id = int(request.form.get("user_id"))
            amount = float(request.form.get("amount", 0))
            pay_date = datetime.strptime(request.form.get("date"), "%Y-%m-%d").date()
            notes = request.form.get("notes", "").strip()
            payment = Payment(
                period_id=period.id, user_id=user_id, amount=amount,
                date=pay_date, notes=notes
            )
            db.session.add(payment)
            db.session.commit()
        elif action == "delete":
            payment = Payment.query.get(request.form.get("payment_id"))
            if payment:
                db.session.delete(payment)
                db.session.commit()
        return redirect(url_for("admin_payments"))

    users = User.query.filter_by(is_active=True).order_by(User.name).all()
    payments = (
        Payment.query.filter_by(period_id=period.id)
        .order_by(Payment.date.desc())
        .all()
    )
    # Totaal per persoon
    totals = {}
    for u in users:
        totals[u.id] = sum(p.amount for p in payments if p.user_id == u.id)

    return render_template(
        "admin/payments.html",
        period=period, payments=payments, users=users, totals=totals,
        today=date.today().isoformat()
    )


# Correcties
@app.route("/admin/corrections", methods=["GET", "POST"])
def admin_corrections():
    period = get_active_period()
    if not period:
        return render_template("admin/corrections.html", period=None, corrections=[], users=[])

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            user_id = int(request.form.get("user_id"))
            amount = float(request.form.get("amount", 0))
            description = request.form.get("description", "").strip()
            corr_date = datetime.strptime(request.form.get("date"), "%Y-%m-%d").date()
            corr = Correction(
                period_id=period.id, user_id=user_id, amount=amount,
                description=description, date=corr_date
            )
            db.session.add(corr)
            db.session.commit()
        elif action == "delete":
            corr = Correction.query.get(request.form.get("correction_id"))
            if corr:
                db.session.delete(corr)
                db.session.commit()
        return redirect(url_for("admin_corrections"))

    users = User.query.filter_by(is_active=True).order_by(User.name).all()
    corrections = (
        Correction.query.filter_by(period_id=period.id)
        .order_by(Correction.date.desc())
        .all()
    )
    return render_template(
        "admin/corrections.html",
        period=period, corrections=corrections, users=users
    )


# ════════════════════════════════════════════════════════════════════════════
# HO BEHEER
# ════════════════════════════════════════════════════════════════════════════

@app.route("/ho", methods=["GET", "POST"])
def ho():
    period = get_active_period()
    if not period:
        return render_template("ho.html", period=None, ho_events=[], users=[])

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add_event":
            name = request.form.get("name", "").strip()
            total_cost = float(request.form.get("total_cost", 0))
            distribution_type = request.form.get("distribution_type", "equal_all")
            notes = request.form.get("notes", "").strip()
            ev_date = datetime.strptime(request.form.get("date"), "%Y-%m-%d").date()

            event = HOEvent(
                period_id=period.id,
                name=name,
                total_cost=total_cost,
                distribution_type=distribution_type,
                notes=notes,
                date=ev_date,
            )
            db.session.add(event)
            db.session.flush()  # get event.id

            # Verdeling instellen
            if distribution_type in ("equal_selected", "manual"):
                users = User.query.filter_by(is_active=True).all()
                for u in users:
                    field = f"share_{u.id}"
                    if distribution_type == "equal_selected" and field in request.form:
                        share = HOEventShare(ho_event_id=event.id, user_id=u.id, amount=0)
                        db.session.add(share)
                    elif distribution_type == "manual":
                        amount_str = request.form.get(field, "").strip()
                        if amount_str:
                            share = HOEventShare(
                                ho_event_id=event.id, user_id=u.id,
                                amount=float(amount_str)
                            )
                            db.session.add(share)
            db.session.commit()

        elif action == "delete_event":
            event = HOEvent.query.get(request.form.get("event_id"))
            if event:
                db.session.delete(event)
                db.session.commit()

        return redirect(url_for("ho"))

    users = User.query.filter_by(is_active=True).order_by(User.name).all()
    ho_events = HOEvent.query.filter_by(period_id=period.id).order_by(HOEvent.date.desc()).all()
    turfverlies = get_total_turfverlies(period.id)
    inventory = get_inventory_data(period.id)

    # HO aandeel per persoon
    ho_per_user = {u.id: round(get_ho_share_for_user(period.id, u.id), 2) for u in users}

    return render_template(
        "ho.html",
        period=period,
        ho_events=ho_events,
        users=users,
        turfverlies=turfverlies,
        inventory=inventory,
        ho_per_user=ho_per_user,
    )


# ─── Manifest voor PWA ───────────────────────────────────────────────────────

@app.route("/manifest.json")
def manifest():
    return jsonify({
        "name": "Turfrekening",
        "short_name": "Turfen",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#1a1a2e",
        "theme_color": "#f59e0b",
        "icons": [
            {"src": "/static/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    })


# ════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS SYNC
# ════════════════════════════════════════════════════════════════════════════

@app.route("/api/sync-sheets", methods=["POST"])
def sync_sheets():
    try:
        from sheets_sync import sync_all
        result = sync_all(app)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _scheduled_sync():
    try:
        from sheets_sync import sync_all
        result = sync_all(app)
        print(f"[Sheets sync] {result}")
    except Exception as e:
        print(f"[Sheets sync] Fout: {e}")


if __name__ == "__main__":
    create_tables()

    scheduler = BackgroundScheduler()
    scheduler.add_job(_scheduled_sync, "cron", hour=2, minute=0)
    scheduler.start()

    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)
