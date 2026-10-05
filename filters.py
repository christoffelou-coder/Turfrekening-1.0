"""Jinja-filters. Bedragen komen binnen als hele centen (int)."""
from markupsafe import Markup

MINUS = "−"
_DASH = Markup('<span class="zero">—</span>')


def _fmt(abs_cents):
    whole, frac = divmod(int(abs_cents), 100)
    return f"€{whole:,}".replace(",", ".") + f",{frac:02d}"


def euro(cents, sign=False):
    """−€1,01 · +€1,01 (sign=True) · — voor nul of leeg. Nederlandse notatie."""
    if cents is None or int(cents) == 0:
        return _DASH
    cents = int(cents)
    if cents < 0:
        return MINUS + _fmt(-cents)
    return ("+" if sign else "") + _fmt(cents)


def euro_cls(cents):
    """CSS-klasse voor een stand: pos / neg / zero."""
    if not cents:
        return "zero"
    return "pos" if cents > 0 else "neg"
