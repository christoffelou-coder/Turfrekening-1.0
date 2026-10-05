"""Jinja-filters."""
from decimal import Decimal, ROUND_HALF_UP

from markupsafe import Markup

MINUS = "−"


def _fmt(abs_value):
    q = Decimal(str(abs_value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    whole, frac = f"{q:.2f}".split(".")
    whole = f"{int(whole):,}".replace(",", ".")
    return f"€{whole},{frac}"


def euro(value, sign=False):
    """−€1,01 · +€1,01 (sign=True) · — voor nul of leeg. Nederlandse notatie."""
    if value is None:
        return Markup('<span class="zero">—</span>')
    dec = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if dec == 0:
        return Markup('<span class="zero">—</span>')
    text = _fmt(abs(dec))
    if dec < 0:
        return MINUS + text
    return ("+" if sign else "") + text


def euro_cls(value):
    """CSS-klasse voor een stand: pos / neg / zero."""
    if value is None:
        return "zero"
    dec = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return "zero" if dec == 0 else ("pos" if dec > 0 else "neg")
