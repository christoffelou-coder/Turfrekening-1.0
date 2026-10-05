"""Admin-login: één gedeeld wachtwoord uit ADMIN_PASSWORD, sessie in een cookie."""
import hmac
import os
from functools import wraps

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

bp = Blueprint("auth", __name__)


def _password_ok(given):
    expected = os.environ.get("ADMIN_PASSWORD", "")
    return bool(expected) and hmac.compare_digest(given.encode(), expected.encode())


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("is_admin"):
            return redirect(url_for("auth.login", next=request.full_path.rstrip("?")))
        return view(*args, **kwargs)
    return wrapped


def _safe_next(target):
    # Alleen relatieve paden binnen de app, geen open redirect.
    if target and target.startswith("/") and not target.startswith("//"):
        return target
    return url_for("admin")


@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if _password_ok(request.form.get("password", "")):
            session.clear()
            session["is_admin"] = True
            session.permanent = True
            return redirect(_safe_next(request.args.get("next")))
        flash("Verkeerd wachtwoord.", "error")
    return render_template("login.html")


@bp.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("index"))
