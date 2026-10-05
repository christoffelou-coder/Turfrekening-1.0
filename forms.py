"""Kleine parse-helpers voor formulierinvoer. Ze gooien FormError met een nette melding."""
from datetime import date, datetime


class FormError(ValueError):
    pass


def parse_float(raw, label, default=None):
    raw = (raw or "").strip().replace(",", ".")
    if not raw:
        if default is not None:
            return default
        raise FormError(f"{label}: vul een getal in.")
    try:
        return float(raw)
    except ValueError:
        raise FormError(f"{label}: '{raw}' is geen geldig getal.")


def parse_int(raw, label, default=None):
    raw = (raw if isinstance(raw, str) else str(raw if raw is not None else "")).strip()
    if not raw:
        if default is not None:
            return default
        raise FormError(f"{label}: vul een geheel getal in.")
    try:
        return int(raw)
    except ValueError:
        raise FormError(f"{label}: '{raw}' is geen geheel getal.")


def parse_date(raw, label, default=None):
    raw = (raw or "").strip()
    if not raw:
        if default is not None:
            return default
        raise FormError(f"{label}: kies een datum.")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        raise FormError(f"{label}: '{raw}' is geen geldige datum.")
