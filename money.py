"""Geld in hele centen (integers). Invoer en weergave in euro's."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from forms import FormError


def to_cents(value):
    """'12,50' / '12.5' / 12.5 -> 1250. Ongeldig -> ValueError."""
    if isinstance(value, int):
        return value * 100
    raw = str(value).strip().replace(",", ".")
    try:
        return int((Decimal(raw) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except InvalidOperation:
        raise ValueError(raw)


def parse_cents(raw, label, default=None):
    raw = (raw or "").strip()
    if not raw:
        if default is not None:
            return default
        raise FormError(f"{label}: vul een bedrag in.")
    try:
        return to_cents(raw)
    except ValueError:
        raise FormError(f"{label}: '{raw}' is geen geldig bedrag.")


def cents_input(cents):
    """Waarde voor <input type=number step=0.01>: 1250 -> '12.50'."""
    if cents is None:
        return ""
    sign = "-" if cents < 0 else ""
    c = abs(int(cents))
    return f"{sign}{c // 100}.{c % 100:02d}"


def split_even(total_cents, count):
    """Verdeel total_cents over count personen; de som is exact total_cents.
    De resterende centen gaan naar de eerste personen. Werkt ook voor negatieve totalen."""
    if count <= 0:
        return []
    sign = -1 if total_cents < 0 else 1
    base, rest = divmod(abs(int(total_cents)), count)
    return [sign * (base + (1 if i < rest else 0)) for i in range(count)]
