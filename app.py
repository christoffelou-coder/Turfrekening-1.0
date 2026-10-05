import os
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_wtf.csrf import CSRFProtect, CSRFError
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from flask_migrate import Migrate

load_dotenv()
from models import (
    db, Period, User, Product, Tally, InventoryPurchase,
    InventorySnapshot, HOEvent, HOEventShare, Payment, Correction
)
from calculations import (
    get_active_period, compute_period, get_period_status,
)

from filters import euro, euro_cls
from money import parse_cents, cents_input
from forms import FormError, parse_date, parse_int
from periods import (PeriodError, check_period_dates, close_blockers, close_period, create_first_period,
                     default_next_name, get_period_view, has_data)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


SOUNDS_DIR = os.path.join(BASE_DIR, "static", "sounds")
SOUND_EXTENSIONS = (".mp3", ".wav", ".ogg", ".m4a")


def _slug(text):
    return "-".join((text or "").strip().lower().replace("_", " ").split())


def available_sounds():
    """{naam: bestandsnaam} van de geluiden in static/sounds, bv. {'kakelende-kip': 'kakelende-kip.mp3'}."""
    try:
        files = sorted(os.listdir(SOUNDS_DIR))
    except OSError:
        return {}
    return {os.path.splitext(f)[0]: f for f in files if f.lower().endswith(SOUND_EXTENSIONS)}


def sound_src(value):
    """Wat het turfscherm afspeelt: 'piep', een volledige link, of /static/sounds/<bestand>."""
    value = (value or "").strip()
    if not value or value == "piep" or value.startswith(("http://", "https://", "/static/")):
        return value
    filename = available_sounds().get(_slug(value))
    return f"/static/sounds/{filename}" if filename else ""


def parse_sound(raw_url, raw_every):
    """Geluid: 'piep', de naam van een geluid in static/sounds (bv. 'kakelende kip'),
    een https-link of een pad onder /static/."""
    url = (raw_url or "").strip()
    if url and url != "piep" and not url.startswith(("http://", "https://", "/static/")):
        name = _slug(url)
        if name not in available_sounds():
            namen = ", ".join(["piep"] + list(available_sounds())) 
            raise FormError(f"Geluid-link: '{url}' is niet bekend. Kies uit: {namen}, of gebruik een https-link naar een mp3.")
        url = name
    every = parse_int(raw_every, "Geluid elke … stuks", 3)
    if every < 1 or every > 100:
        raise FormError("Geluid elke … stuks: kies een getal tussen 1 en 100.")
    return (url or None), every


app = Flask(__name__)

# Gebruik DATABASE_URL omgevingsvariabele (Railway/Supabase), anders lokale SQLite
database_url = os.environ.get("DATABASE_URL")
if database_url:
    # Railway/Supabase geeft soms 'postgres://' maar SQLAlchemy wil 'postgresql://'
    database_url = database_url.replace("postgres://", "postgresql://", 1)
    # Forceer de psycopg2-driver (anders pakt SQLAlchemy een andere geïnstalleerde driver)
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    database_url = f"sqlite:///{os.path.join(BASE_DIR, 'turfrekening.db')}"

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_recycle": 280}  # Supabase-pooler sluit idle verbindingen
_secret = os.environ.get("SECRET_KEY")
if not _secret:
    if os.environ.get("FLASK_DEBUG") == "1" or os.environ.get("FLASK_ENV") == "development":
        _secret = "alleen-lokaal-ontwikkelen"
    else:
        raise RuntimeError("SECRET_KEY ontbreekt. Zet die als omgevingsvariabele (alleen lokaal mag FLASK_DEBUG=1).")
app.secret_key = _secret
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = bool(os.environ.get("DATABASE_URL"))
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 12

db.init_app(app)
migrate = Migrate(app, db)
csrf = CSRFProtect(app)
app.jinja_env.filters["euro"] = euro
app.jinja_env.filters["euro_cls"] = euro_cls
app.jinja_env.filters["cents_input"] = cents_input
app.jinja_env.filters["sound_src"] = sound_src


@app.context_processor
def _globals():
    css = os.path.join(BASE_DIR, "static", "css", "app.css")
    try:
        version = int(os.path.getmtime(css))
    except OSError:
        version = 0
    return {"css_version": version}


@app.errorhandler(PeriodError)
def _period_error(err):
    flash(str(err), "error")
    return redirect(request.referrer or url_for("admin_periods"))


@app.errorhandler(FormError)
def _form_error(err):
    flash(str(err), "error")
    return redirect(request.referrer or url_for("admin"))


@app.errorhandler(CSRFError)
def _csrf_error(err):
    if request.path.startswith("/api/"):
        return jsonify({"error": "Sessie verlopen, ververs de pagina."}), 400
    flash("Sessie verlopen, probeer het opnieuw.", "error")
    return redirect(request.referrer or url_for("index"))


# ════════════════════════════════════════════════════════════════════════════
# HOOFD SCHERM — iPad turfinterface
# ════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    period = get_active_period()
    users = User.query.filter_by(is_active=True).order_by(User.sort_order, User.name).all()
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()

    # Saldo wordt niet meer getoond op het turfscherm (verwarrend zolang
    # overboekingen niet direct verwerkt zijn) — dus geen stand-berekening meer nodig hier.

    return render_template(
        "turf.html",
        period=period,
        users=users,
        products=products,
    )


# ─── API: Turf aanslaan ───────────────────────────────────────────────────────

@app.route("/api/tally", methods=["POST"])
def add_tally():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Ongeldige aanvraag"}), 400
    period = get_active_period()
    if not period:
        return jsonify({"error": "Geen actieve periode"}), 400

    try:
        user_id = int(data["user_id"])
        product_id = int(data["product_id"])
        quantity = int(data.get("quantity", 1))
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "Ongeldige aanvraag"}), 400
    # Negatief = een turfje terugdraaien (min-knop op het turfscherm)
    if quantity == 0 or abs(quantity) > 24:
        return jsonify({"error": "Aantal moet tussen 1 en 24 liggen"}), 400

    user = db.session.get(User, user_id)
    product = db.session.get(Product, product_id)
    if not user or not product:
        return jsonify({"error": "Gebruiker of product niet gevonden"}), 404
    if not user.is_active or not product.is_active:
        return jsonify({"error": "Gebruiker of product is niet actief"}), 400

    tally = Tally(
        period_id=period.id,
        user_id=user.id,
        product_id=product.id,
        quantity=quantity,
        unit_price_cents=product.price_cents,
    )
    db.session.add(tally)
    db.session.commit()

    total = (
        db.session.query(db.func.coalesce(db.func.sum(Tally.quantity), 0))
        .filter(Tally.period_id == period.id, Tally.user_id == user.id, Tally.product_id == product.id)
        .scalar()
    )
    return jsonify({
        "ok": True,
        "tally_id": tally.id,
        "user": user.name,
        "product": product.name,
        "quantity": tally.quantity,
        "total": int(total),
    })


UNDO_WINDOW = timedelta(minutes=10)


def _is_undoable(tally):
    """Alleen turfjes uit de actieve periode, en niet langer dan UNDO_WINDOW geleden."""
    period = get_active_period()
    return bool(
        period and tally.period_id == period.id
        and datetime.utcnow() - tally.created_at <= UNDO_WINDOW
    )


@app.route("/api/tally/<int:tally_id>", methods=["DELETE"])
def undo_tally(tally_id):
    tally = db.get_or_404(Tally, tally_id)
    if not _is_undoable(tally):
        return jsonify({"error": "Te lang geleden om ongedaan te maken. Gebruik een correctie."}), 403
    db.session.delete(tally)
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/last-tally")
def last_tally():
    """Geeft het meest recente turfje terug (voor de ongedaan-maken-balk)."""
    period = get_active_period()
    if not period:
        return jsonify({"tally": None})
    tally = (
        Tally.query.filter_by(period_id=period.id)
        .order_by(Tally.created_at.desc(), Tally.id.desc())
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
            "created_at": tally.created_at.isoformat() + "Z",
            "undoable": _is_undoable(tally),
        }
    })


# ─── Activiteit (anoniem: wanneer is er geturfd, nooit door wie) ─────────────

@app.route("/activiteit")
def activiteit():
    return render_template("activiteit.html", period=get_active_period())


@app.route("/api/activity")
def api_activity():
    """Tijdstippen van turfjes. Bevat bewust GEEN persoon, alleen tijd, product en aantal."""
    days = max(1, min(request.args.get("days", 7, type=int), 31))
    since = datetime.utcnow() - timedelta(days=days)
    rows = (
        db.session.query(Tally.created_at, Tally.quantity, Product.name)
        .join(Product, Product.id == Tally.product_id)
        .filter(Tally.created_at >= since, Tally.quantity > 0)
        .order_by(Tally.created_at.desc())
        .limit(5000)
        .all()
    )
    return jsonify({
        "now": datetime.utcnow().isoformat() + "Z",
        "events": [{"t": t.isoformat() + "Z", "q": q, "product": name} for t, q, name in rows],
    })


@app.route("/api/product-counts/<int:product_id>")
def product_counts(product_id):
    """Geeft het aantal turfjes per persoon voor een product in de actieve periode."""
    period = get_active_period()
    if not period:
        return jsonify({})
    rows = (
        db.session.query(Tally.user_id, db.func.sum(Tally.quantity))
        .filter(Tally.period_id == period.id, Tally.product_id == product_id)
        .group_by(Tally.user_id)
        .all()
    )
    return jsonify({str(uid): int(total or 0) for uid, total in rows})


@app.route("/rapport")
@app.route("/rapport/<int:period_id>")
def rapport(period_id=None):
    if period_id is None:
        p = get_active_period()
        period_id = p.id if p else None
    if period_id is None or db.session.get(Period, period_id) is None:
        return render_template("rapport.html", overview=None, periods=[])

    overview = get_period_view(period_id)
    periods = Period.query.order_by(Period.start_date.desc()).all()
    return render_template("rapport.html", overview=overview, periods=periods, current_period_id=period_id)


# ════════════════════════════════════════════════════════════════════════════
# ADMIN — gebruikers
# ════════════════════════════════════════════════════════════════════════════

@app.route("/admin")
def admin():
    period = get_active_period()
    users = User.query.order_by(User.sort_order, User.name).all()
    products = Product.query.order_by(Product.sort_order).all()
    periods = Period.query.order_by(Period.start_date.desc()).all()
    status = get_period_status(period) if period else None
    return render_template("admin/index.html", period=period, users=users, products=products,
                           periods=periods, status=status)


# Bewoners
@app.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    period = get_active_period()
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            if not name:
                raise FormError("Naam: vul een naam in.")
            max_order = db.session.query(db.func.max(User.sort_order)).scalar() or 0
            db.session.add(User(name=name, sort_order=max_order + 1))
            db.session.commit()
            flash(f"{name} toegevoegd. Start in de lopende periode op €0,00.", "success")
        elif action == "edit":
            user = db.session.get(User, parse_int(request.form.get("user_id"), "Id"))
            if user:
                name = request.form.get("name", "").strip()
                if name:
                    user.name = name
                user.participates_in_ho = "participates_in_ho" in request.form
                db.session.commit()
        elif action == "leave":
            user = db.session.get(User, parse_int(request.form.get("user_id"), "Id"))
            if user and user.is_active:
                user.is_active = False
                user.left_at = date.today()
                db.session.commit()
                stand = 0
                if period:
                    stand = next((r["stand"] for r in compute_period(period.id)["user_rows"]
                                  if r["user"]["id"] == user.id), 0)
                if stand:
                    flash(f"{user.name} is vertrokken en staat nog op {euro(stand, sign=True)}. "
                          "Die stand blijft in de volgende periodes staan tot hij is vereffend.", "error")
                else:
                    flash(f"{user.name} is op vertrokken gezet.", "success")
        elif action == "return":
            user = db.session.get(User, parse_int(request.form.get("user_id"), "Id"))
            if user and not user.is_active:
                user.is_active = True
                user.left_at = None
                db.session.commit()
        return redirect(url_for("admin_users"))

    users = User.query.order_by(User.sort_order, User.name).all()
    user_stands = {}
    if period:
        user_stands = {r["user"]["id"]: r["stand"] for r in compute_period(period.id)["user_rows"]}
    return render_template("admin/users.html", users=users, user_stands=user_stands, period=period)


# Producten
@app.route("/admin/products", methods=["GET", "POST"])
def admin_products():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("name", "").strip()
            price_cents = parse_cents(request.form.get("price"), "Prijs")
            emoji = request.form.get("emoji", "🍺").strip()
            sort_order = parse_int(request.form.get("sort_order"), "Volgorde", 0)
            image_url = request.form.get("image_url", "").strip() or None
            sound_url, sound_every = parse_sound(request.form.get("sound_url"), request.form.get("sound_every"))
            parent_product_id = request.form.get("parent_product_id") or None
            parent_units = parse_int(request.form.get("parent_units"), "Aantal eenheden", 1)
            if name:
                product = Product(name=name, price_cents=price_cents, emoji=emoji, sort_order=sort_order,
                                  image_url=image_url, sound_url=sound_url, sound_every=sound_every,
                                  parent_product_id=parent_product_id,
                                  parent_units=parent_units)
                db.session.add(product)
                db.session.commit()
        elif action == "edit":
            product = db.session.get(Product, parse_int(request.form.get("product_id"), "Id"))
            if product:
                product.name = request.form.get("name", product.name).strip()
                product.price_cents = parse_cents(request.form.get("price"), "Prijs", product.price_cents)
                product.emoji = request.form.get("emoji", product.emoji).strip()
                product.sort_order = parse_int(request.form.get("sort_order"), "Volgorde", product.sort_order)
                product.is_active = "is_active" in request.form
                product.image_url = request.form.get("image_url", "").strip() or None
                product.sound_url, product.sound_every = parse_sound(request.form.get("sound_url"), request.form.get("sound_every"))
                product.parent_product_id = request.form.get("parent_product_id") or None
                product.parent_units = parse_int(request.form.get("parent_units"), "Aantal eenheden", 1)
                db.session.commit()
        elif action == "delete":
            product = db.session.get(Product, parse_int(request.form.get("product_id"), "Id"))
            if product:
                in_use = (
                    Tally.query.filter_by(product_id=product.id).first()
                    or InventoryPurchase.query.filter_by(product_id=product.id).first()
                    or InventorySnapshot.query.filter_by(product_id=product.id).first()
                    or HOEvent.query.filter_by(beer_product_id=product.id).first()
                    or Product.query.filter_by(parent_product_id=product.id).first()
                )
                if in_use:
                    flash(f"{product.name} heeft turfjes, voorraad of koppelingen en kan niet worden verwijderd. Zet het op inactief.", "error")
                else:
                    db.session.delete(product)
                    db.session.commit()
        return redirect(url_for("admin_products"))

    products = Product.query.order_by(Product.sort_order).all()
    return render_template("admin/products.html", products=products, sounds=list(available_sounds()))


# Periodes
@app.route("/admin/periods", methods=["GET", "POST"])
def admin_periods():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "first":
            start = parse_date(request.form.get("start_date"), "Startdatum")
            warnings = check_period_dates(start, None)
            create_first_period(request.form.get("name", ""), start)
            for w in warnings:
                flash(w, "error")
            flash("Periode aangemaakt.", "success")
        elif action == "edit":
            period = db.session.get(Period, parse_int(request.form.get("period_id"), "Periode"))
            if period and period.is_active and not period.closed_at:
                name = request.form.get("name", "").strip()
                start = parse_date(request.form.get("start_date"), "Startdatum")
                for w in check_period_dates(start, None, exclude_id=period.id):
                    flash(w, "error")
                if name:
                    period.name = name
                period.start_date = start
                db.session.commit()
                flash("Periode bijgewerkt.", "success")
            else:
                flash("Een afgesloten periode kan niet meer worden gewijzigd.", "error")
        elif action == "delete":
            period = db.session.get(Period, parse_int(request.form.get("period_id"), "Periode"))
            if period and not period.is_active and not period.closed_at and not has_data(period):
                db.session.delete(period)
                db.session.commit()
            else:
                flash("Alleen een lege periode kan worden verwijderd.", "error")
        return redirect(url_for("admin_periods"))

    periods = Period.query.order_by(Period.start_date.desc()).all()
    active = get_active_period()
    start_balances = []
    if active:
        start_balances = [r for r in compute_period(active.id)["user_rows"]]
    return render_template("admin/periods.html", periods=periods, active=active, start_balances=start_balances,
                           today=date.today().isoformat())


@app.route("/admin/periods/close", methods=["GET", "POST"])
def admin_close_period():
    period = get_active_period()
    if not period or period.closed_at:
        flash("Er is geen lopende periode om af te sluiten.", "error")
        return redirect(url_for("admin_periods"))

    if request.method == "POST":
        end_date = parse_date(request.form.get("end_date"), "Einddatum")
        next_start = parse_date(request.form.get("next_start"), "Startdatum nieuwe periode")
        if "confirm" not in request.form:
            raise FormError("Vink aan dat het rapport klopt voordat je afsluit.")
        new_period, warnings = close_period(period, end_date, request.form.get("next_name", ""), next_start)
        for w in warnings:
            flash(w, "error")
        flash(f"'{period.name}' is afgesloten en bevroren. '{new_period.name}' is gestart.", "success")
        return redirect(url_for("rapport", period_id=period.id))

    end_default = max(date.today(), period.start_date)
    ov = compute_period(period.id)
    try:
        date_warnings = check_period_dates(period.start_date, end_default, exclude_id=period.id)
    except PeriodError as err:
        date_warnings = [str(err)]
    if period.start_date > date.today():
        date_warnings.append("De startdatum van deze periode ligt in de toekomst. Pas die eerst aan bij Periodes als dat een vergissing is.")
    return render_template(
        "admin/close_period.html", period=period, overview=ov,
        blockers=close_blockers(ov), end_default=end_default.isoformat(),
        next_name=default_next_name(end_default), today=date.today().isoformat(),
        date_warnings=date_warnings,
    )


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
                    if not val.strip():
                        continue  # leeg = niet (opnieuw) geteld
                    qty = parse_int(val, "Aantal")
                    if qty < 0:
                        raise FormError("Aantal mag niet negatief zijn.")
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
            product_id = parse_int(request.form.get("product_id"), "Product")
            quantity = parse_int(request.form.get("quantity"), "Aantal")
            total_cost = request.form.get("total_cost")
            notes = request.form.get("notes", "").strip()
            purchase = InventoryPurchase(
                period_id=period.id,
                product_id=product_id,
                quantity=quantity,
                total_cost_cents=parse_cents(total_cost, "Kosten") if total_cost else None,
                notes=notes,
                date=date.today(),
            )
            db.session.add(purchase)
            db.session.commit()

        elif action == "delete_purchase":
            purchase = db.session.get(InventoryPurchase, parse_int(request.form.get("purchase_id"), "Id"))
            if purchase and purchase.period_id == period.id:
                db.session.delete(purchase)
                db.session.commit()

        return redirect(url_for("admin_inventory"))

    ov = compute_period(period.id)
    inventory = ov["inventory"]
    used_ids = {r["product"]["id"] for r in inventory}
    products = [p for p in Product.query.filter_by(parent_product_id=None).order_by(Product.sort_order, Product.id)
                if p.is_active or p.id in used_ids]
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
        complete=ov["inventory_complete"],
        turfverlies=ov["turfverlies_total"],
    )


# Betalingen en correcties (één pagina, twee tabbladen)
def _money_page(tab, period, users, items, **extra):
    return render_template("admin/money.html", tab=tab, period=period, users=users, items=items,
                           today=date.today().isoformat(), **extra)


@app.route("/admin/payments", methods=["GET", "POST"])
def admin_payments():
    period = get_active_period()
    if not period:
        return _money_page("payments", None, [], [], totals={})

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            user_id = parse_int(request.form.get("user_id"), "Persoon")
            amount = parse_cents(request.form.get("amount"), "Bedrag")
            if amount == 0:
                raise FormError("Bedrag: vul een bedrag in dat niet nul is (negatief mag, bijv. een terugbetaling).")
            pay_date = parse_date(request.form.get("date"), "Datum")
            db.session.add(Payment(period_id=period.id, user_id=user_id, amount_cents=amount,
                                   date=pay_date, notes=request.form.get("notes", "").strip()))
            db.session.commit()
            flash("Overboeking toegevoegd.", "success")
        elif action == "delete":
            payment = db.session.get(Payment, parse_int(request.form.get("payment_id"), "Id"))
            if payment and payment.period_id == period.id:
                db.session.delete(payment)
                db.session.commit()
        return redirect(url_for("admin_payments"))

    users = User.query.order_by(User.is_active.desc(), User.sort_order, User.name).all()  # ook vertrokken bewoners: die kunnen nog betalen
    payments = Payment.query.filter_by(period_id=period.id).order_by(Payment.date.desc(), Payment.id.desc()).all()
    totals = {}
    for p in payments:
        totals[p.user_id] = totals.get(p.user_id, 0) + p.amount_cents
    return _money_page("payments", period, users, payments, totals=totals)


@app.route("/admin/corrections", methods=["GET", "POST"])
def admin_corrections():
    period = get_active_period()
    if not period:
        return _money_page("corrections", None, [], [])

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            user_id = parse_int(request.form.get("user_id"), "Persoon")
            amount = parse_cents(request.form.get("amount"), "Bedrag")
            description = request.form.get("description", "").strip()
            if amount == 0:
                raise FormError("Bedrag: een correctie van nul heeft geen effect.")
            if len(description) < 3:
                raise FormError("Omschrijving: vul in waarom je corrigeert.")
            corr_date = parse_date(request.form.get("date"), "Datum")
            db.session.add(Correction(period_id=period.id, user_id=user_id, amount_cents=amount,
                                      description=description, date=corr_date))
            db.session.commit()
            flash("Correctie toegevoegd.", "success")
        elif action == "delete":
            corr = db.session.get(Correction, parse_int(request.form.get("correction_id"), "Id"))
            if corr and corr.period_id == period.id:
                db.session.delete(corr)
                db.session.commit()
        return redirect(url_for("admin_corrections"))

    users = User.query.order_by(User.is_active.desc(), User.sort_order, User.name).all()
    corrections = Correction.query.filter_by(period_id=period.id).order_by(Correction.date.desc(), Correction.id.desc()).all()
    return _money_page("corrections", period, users, corrections)


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
            base_cost = parse_cents(request.form.get("total_cost"), "Kosten", 0)
            distribution_type = request.form.get("distribution_type", "equal_all")
            notes = request.form.get("notes", "").strip()
            ev_date = parse_date(request.form.get("date"), "Datum")

            beer_product_id = request.form.get("beer_product_id") or None
            beer_quantity = request.form.get("beer_quantity") or None
            beer_cost = 0
            if beer_product_id and beer_quantity:
                beer_product_id = parse_int(beer_product_id, "Bierproduct")
                beer_quantity = parse_int(beer_quantity, "Aantal bier")
                bp = db.session.get(Product, beer_product_id)
                if bp:
                    beer_cost = bp.price_cents * beer_quantity
            else:
                beer_product_id = None
                beer_quantity = None

            event = HOEvent(
                period_id=period.id,
                name=name,
                total_cost_cents=base_cost + beer_cost,
                distribution_type=distribution_type,
                notes=notes,
                date=ev_date,
                beer_product_id=beer_product_id,
                beer_quantity=beer_quantity,
            )
            db.session.add(event)
            db.session.flush()  # get event.id

            # Verdeling instellen
            if distribution_type in ("equal_selected", "manual"):
                users = User.query.all()
                for u in users:
                    field = f"share_{u.id}"
                    if distribution_type == "equal_selected" and f"select_{u.id}" in request.form:
                        share = HOEventShare(ho_event_id=event.id, user_id=u.id, amount_cents=0)
                        db.session.add(share)
                    elif distribution_type == "manual":
                        amount_str = request.form.get(field, "").strip()
                        if amount_str:
                            share = HOEventShare(
                                ho_event_id=event.id, user_id=u.id,
                                amount_cents=parse_cents(amount_str, "Aandeel")
                            )
                            db.session.add(share)
            db.session.commit()

        elif action == "delete_event":
            event = db.session.get(HOEvent, parse_int(request.form.get("event_id"), "Id"))
            if event and event.period_id == period.id:
                db.session.delete(event)
                db.session.commit()

        return redirect(url_for("ho"))

    users = User.query.filter_by(is_active=True).order_by(User.sort_order, User.name).all()
    products = Product.query.filter_by(is_active=True).order_by(Product.sort_order).all()
    ho_events = HOEvent.query.filter_by(period_id=period.id).order_by(HOEvent.date.desc(), HOEvent.id.desc()).all()
    ov = compute_period(period.id)

    return render_template(
        "ho.html",
        period=period,
        ho_events=ho_events,
        users=users,
        products=products,
        overview=ov,
        today=date.today().isoformat(),
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=os.environ.get("FLASK_DEBUG") == "1", use_reloader=False)
