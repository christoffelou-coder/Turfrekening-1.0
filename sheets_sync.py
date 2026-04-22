import os
from datetime import datetime
import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = "1FNj7tLlPjbt-R3L_kpuhCUXojWcOKg1Sx6328ovcFIE"
EXISTING_SPREADSHEET_ID = "10MGTFssPTg6GUmMp0sEp-O43wY8_aUd8BqvI190W4FY"
CREDENTIALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_credentials.json")

# Kleuren (RGB 0-1)
DARK_BG      = {"red": 0.13, "green": 0.13, "blue": 0.18}
HEADER_BG    = {"red": 0.20, "green": 0.22, "blue": 0.28}
SECTION_BG   = {"red": 0.27, "green": 0.30, "blue": 0.37}
GREEN_BG     = {"red": 0.18, "green": 0.62, "blue": 0.27}
RED_BG       = {"red": 0.83, "green": 0.18, "blue": 0.18}
WHITE        = {"red": 1.0,  "green": 1.0,  "blue": 1.0}
GOLD         = {"red": 0.96, "green": 0.62, "blue": 0.04}
GRAY_TEXT    = {"red": 0.5,  "green": 0.5,  "blue": 0.5}
LIGHT_BG     = {"red": 0.95, "green": 0.95, "blue": 0.97}
ROW_ALT      = {"red": 0.97, "green": 0.97, "blue": 1.00}
# Kleurcodes voor nieuwe tabs
ORANGE_INPUT = {"red": 1.0,  "green": 0.88, "blue": 0.70}   # 🟠 gebruiker vult in
BLUE_CALC    = {"red": 0.88, "green": 0.94, "blue": 1.0}    # 🔵 formule / app synct
GREEN_RESULT = {"red": 0.84, "green": 0.94, "blue": 0.85}   # 🟢 eindresultaat
WHITE_CELL   = {"red": 1.0,  "green": 1.0,  "blue": 1.0}    # ⬜ app synct (wit)

# Rij-indices (0-gebaseerd) voor vaste layouts
STAND_R_TITLE = 0; STAND_R_LEGEND = 1; STAND_R_HEADERS = 2; STAND_R_DATA = 3
VOOR_R_TITLE  = 0; VOOR_R_LEGEND  = 1; VOOR_R_HEADERS  = 2; VOOR_R_DATA  = 3
HO_R_TITLE = 0; HO_R_LEGEND = 1; HO_R_SEC1 = 2; HO_R_HEADERS1 = 3
HO_R_EVENTS = 4   # eerste event-rij (10 slots: rij 5-14 in sheet)
HO_R_TV     = 14  # turfverlies rij
HO_R_TOTAAL = 15; HO_R_PER_P = 16; HO_R_ACTIEF = 17
HO_R_SEC2 = 19; HO_R_HEADERS2 = 20; HO_R_BET_DATA = 21


def _client():
    # Op Railway: lees credentials uit env var GOOGLE_CREDENTIALS_JSON
    json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if json_str:
        import json
        info = json.loads(json_str)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def _ws(spreadsheet, title, rows=200, cols=20):
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def _euro(val):
    return round(float(val), 2) if val is not None else 0.0


def _fmt(sheet_id, r1, c1, r2, c2, **props):
    """Bouw een formatCells request op (0-gebaseerde indices)."""
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": r1,
                "endRowIndex": r2,
                "startColumnIndex": c1,
                "endColumnIndex": c2,
            },
            "cell": {"userEnteredFormat": props},
            "fields": "userEnteredFormat(" + ",".join(props.keys()) + ")",
        }
    }


def _bold(sheet_id, r1, c1, r2, c2, size=10, color=None):
    tf = {"bold": True, "fontSize": size}
    if color:
        tf["foregroundColor"] = color
    return _fmt(sheet_id, r1, c1, r2, c2, textFormat=tf)


def _bg(sheet_id, r1, c1, r2, c2, bg):
    return _fmt(sheet_id, r1, c1, r2, c2, backgroundColor=bg)


def _align(sheet_id, r1, c1, r2, c2, h="LEFT", v="MIDDLE"):
    return _fmt(sheet_id, r1, c1, r2, c2,
                horizontalAlignment=h, verticalAlignment=v)


def _col_width(sheet_id, col_start, col_end, pixels):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": col_start, "endIndex": col_end},
            "properties": {"pixelSize": pixels},
            "fields": "pixelSize",
        }
    }


def _row_height(sheet_id, row_start, row_end, pixels):
    return {
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": row_start, "endIndex": row_end},
            "properties": {"pixelSize": pixels},
            "fields": "pixelSize",
        }
    }


def _merge(sheet_id, r1, c1, r2, c2):
    return {
        "mergeCells": {
            "range": {"sheetId": sheet_id, "startRowIndex": r1, "endRowIndex": r2,
                      "startColumnIndex": c1, "endColumnIndex": c2},
            "mergeType": "MERGE_ALL",
        }
    }


def _border(sheet_id, r1, c1, r2, c2, style="SOLID"):
    b = {"style": style, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}}
    return _fmt(sheet_id, r1, c1, r2, c2, borders={
        "top": b, "bottom": b, "left": b, "right": b
    })


# Synoniemen: elk item in een groep matcht met de andere namen in die groep
PRODUCT_ALIASES = [
    {"bier", "pils", "biertje", "beer"},
    {"blikje", "blik", "blikjes"},
    {"flesje", "fles", "flesjes"},
    {"ei", "eitje", "eieren"},
    {"pakjes", "pakje", "pak"},
    {"stelz", "stelzje"},
    {"co2 tank", "co2", "co2tank"},
    {"pizza", "pizzaatje"},
    {"overig", "overige", "rest"},
    {"cola", "cola light", "fanta", "frisdrank"},
]


def _canonical(name):
    """Geeft de eerste naam uit de aliasgroep terug, of de naam zelf als hij niet bekend is."""
    n = name.strip().lower()
    for group in PRODUCT_ALIASES:
        if n in group:
            return min(group)  # gebruik altijd het alfabetisch eerste als sleutel
    return n


def _col_letter(idx):
    result = ""
    i = idx + 1
    while i:
        i, rem = divmod(i - 1, 26)
        result = chr(65 + rem) + result
    return result


def _normalize(name):
    return name.strip().lower()


def _sync_existing_overview(spreadsheet, overview):
    """Update C (Vorige Stand), D (Overgemaakt), E (Geturfd), F (HO), G (Correctie), I (Stand) in Overview tab."""
    ws = spreadsheet.worksheet("Overview")
    all_data = ws.get_all_values()

    user_lookup = {_normalize(r["user"].name): r for r in overview["user_rows"]}
    skip = {"", "totaal:", "naam"}

    updates = []
    for i, row in enumerate(all_data):
        if len(row) < 2:
            continue
        name = row[1].strip()
        if not name or _normalize(name) in skip:
            continue
        ur = user_lookup.get(_normalize(name))
        if not ur:
            continue
        r = i + 1
        updates += [
            {"range": f"C{r}", "values": [[_euro(ur["vorige_stand"])]]},
            {"range": f"D{r}", "values": [[_euro(ur["overgemaakt"])]]},
            {"range": f"E{r}", "values": [[_euro(ur["geturfd"])]]},
            {"range": f"F{r}", "values": [[_euro(ur["ho"])]]},
            {"range": f"G{r}", "values": [[_euro(ur["correctie"])]]},
            # Stand als formule zodat alles gelinkt blijft
            {"range": f"I{r}", "values": [[f"=C{r}+D{r}-E{r}-F{r}+G{r}"]]},
        ]

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def _sync_existing_invullen(spreadsheet, overview):
    """Update Invullen tab: turfcounts per persoon, volledige voorraad + formules, HO sectie."""
    ws = spreadsheet.worksheet("Invullen")
    all_data = ws.get_all_values()

    if len(all_data) < 6:
        return

    header_row = all_data[5]  # rij 6 (0-based: 5)

    product_col_map = {}
    for j, cell in enumerate(header_row):
        cell_clean = cell.strip()
        if cell_clean and cell_clean != "-":
            product_col_map[_canonical(cell_clean)] = j

    db_product_map = {_canonical(p.name): p for p in overview["products"]}
    user_lookup = {_normalize(r["user"].name): r for r in overview["user_rows"]}
    skip_cols = {_canonical("gekocht"), _canonical("totaal"), _canonical("hh")}
    inv_map = {_canonical(row["product"].name): row for row in overview["inventory"]}

    updates = []
    ho_header_row = None
    stock_data_rows = []  # (row_num) voor formules in totaalrij

    for i, row in enumerate(all_data):
        r = i + 1

        # ── Personen sectie (col L = idx 11) ────────────────────────────────
        if len(row) > 11:
            name = row[11].strip()
            if name and _normalize(name) not in ("hh", "totaal", "ho", ""):
                ur = user_lookup.get(_normalize(name))
                if ur:
                    tally_map = ur["tallies_per_product"]
                    for prod_canonical, col_idx in product_col_map.items():
                        if prod_canonical in skip_cols:
                            continue
                        db_prod = db_product_map.get(prod_canonical)
                        if db_prod:
                            count = tally_map.get(db_prod.id, 0)
                            updates.append({"range": f"{_col_letter(col_idx)}{r}", "values": [[count]]})
                    totaal_idx = product_col_map.get(_canonical("totaal"))
                    if totaal_idx is not None:
                        updates.append({"range": f"{_col_letter(totaal_idx)}{r}", "values": [[_euro(ur["geturfd"])]]})

        # ── Stock sectie (col A = idx 0) ──────────────────────────────────────
        if len(row) > 0:
            prod_name = row[0].strip()
            norm = _normalize(prod_name)

            if norm == "ho":
                ho_header_row = r
                continue
            if not prod_name or norm in ("totaal", "limo", "limo ", "", "turfverlies"):
                continue

            db_prod = db_product_map.get(_canonical(prod_name))
            inv_row = inv_map.get(_canonical(prod_name))

            if db_prod:
                stock_data_rows.append(r)
                updates.append({"range": f"B{r}", "values": [[_euro(db_prod.price)]]})

                if inv_row:
                    updates += [
                        {"range": f"C{r}", "values": [[inv_row["stock_begin"]]]},
                        {"range": f"D{r}", "values": [[inv_row["bijstock"]]]},
                        {"range": f"E{r}", "values": [[inv_row["stock_eind"]]]},
                        # Formules: alles gelinkt aan de ruwe data
                        {"range": f"F{r}", "values": [[f"=C{r}+D{r}-E{r}"]]},          # Gebruikt
                        {"range": f"G{r}", "values": [[f"=E{r}*B{r}"]]},                # In Stock (€)
                        {"range": f"H{r}", "values": [[inv_row["geturfd"]]]},
                        {"range": f"I{r}", "values": [[f"=F{r}-H{r}"]]},               # Turf tekort
                        {"range": f"J{r}", "values": [[f"=I{r}*B{r}"]]},               # Turfverlies (€)
                    ]
                else:
                    total_qty = sum(ur["tallies_per_product"].get(db_prod.id, 0) for ur in overview["user_rows"])
                    updates.append({"range": f"H{r}", "values": [[total_qty]]})

    # ── Totaalrij stock sectie: formules die optellen ────────────────────────
    for i, row in enumerate(all_data):
        if len(row) > 0 and _normalize(row[0].strip()) == "totaal" and i < 20:
            r = i + 1
            if stock_data_rows:
                first, last = stock_data_rows[0], stock_data_rows[-1]
                updates += [
                    {"range": f"F{r}", "values": [[f"=SUM(F{first}:F{last})"]]},
                    {"range": f"G{r}", "values": [[f"=SUM(G{first}:G{last})"]]},
                    {"range": f"H{r}", "values": [[f"=SUM(H{first}:H{last})"]]},
                    {"range": f"I{r}", "values": [[f"=SUM(I{first}:I{last})"]]},
                    {"range": f"J{r}", "values": [[f"=SUM(J{first}:J{last})"]]},
                ]
            break

    # ── Totaalrij personen sectie ────────────────────────────────────────────
    for i, row in enumerate(all_data):
        if len(row) > 11 and _normalize(row[11].strip()) == "totaal":
            r = i + 1
            totaal_idx = product_col_map.get(_canonical("totaal"))
            if totaal_idx is not None:
                grand_total = sum(_euro(ur["geturfd"]) for ur in overview["user_rows"])
                updates.append({"range": f"{_col_letter(totaal_idx)}{r}", "values": [[grand_total]]})
            break

    # ── HO sectie: events vanuit DB ──────────────────────────────────────────
    if ho_header_row is not None:
        ho_events = overview.get("ho_events", [])
        turfverlies_total = _euro(overview.get("turfverlies_total", 0))

        r = ho_header_row + 1  # rij na "HO" header

        # Kolomkoppen HO sectie
        updates += [
            {"range": f"A{r}", "values": [["Naam"]]},
            {"range": f"C{r}", "values": [["Bedrag"]]},
            {"range": f"E{r}", "values": [["Omschrijving"]]},
            {"range": f"F{r}", "values": [["Verdeling"]]},
        ]

        r += 1
        tv_row = r
        updates += [
            {"range": f"A{r}", "values": [["Turfverlies"]]},
            {"range": f"C{r}", "values": [[turfverlies_total]]},
            {"range": f"E{r}", "values": [["Automatisch berekend uit voorraad"]]},
        ]

        for event in ho_events:
            r += 1
            updates += [
                {"range": f"A{r}", "values": [[event.name]]},
                {"range": f"C{r}", "values": [[_euro(event.total_cost)]]},
                {"range": f"E{r}", "values": [[event.notes or ""]]},
                {"range": f"F{r}", "values": [[event.distribution_type]]},
            ]

        # Totaal HO met formule
        r += 1
        updates += [
            {"range": f"A{r}", "values": [["Totaal HO"]]},
            {"range": f"C{r}", "values": [[f"=SUM(C{tv_row}:C{r-1})"]]},
        ]

        # Lege rijen daarna leegmaken (opruimen oude data)
        for clear_r in range(r + 1, r + 6):
            updates += [
                {"range": f"A{clear_r}", "values": [[""]]},
                {"range": f"C{clear_r}", "values": [[""]]},
                {"range": f"E{clear_r}", "values": [[""]]},
                {"range": f"F{clear_r}", "values": [[""]]},
            ]

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def _sync_betalingen(spreadsheet, overview):
    """Update Totalen kolom (H) in Betalingen tab vanuit DB betalingen."""
    ws = spreadsheet.worksheet("Betalingen")
    all_data = ws.get_all_values()

    user_lookup = {_normalize(r["user"].name): r for r in overview["user_rows"]}
    skip = {"", "totaal:"}

    updates = []
    for i, row in enumerate(all_data):
        if len(row) < 2:
            continue
        name = row[1].strip()
        if not name or _normalize(name) in skip:
            continue
        ur = user_lookup.get(_normalize(name))
        if ur:
            row_num = i + 1
            updates.append({
                "range": f"H{row_num}",
                "values": [[_euro(ur["overgemaakt"])]],
            })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def _sync_maandoverzicht(spreadsheet, overview):
    ws = _ws(spreadsheet, "Maandoverzicht", rows=300, cols=16)
    ws.clear()
    sid = ws.id

    period = overview["period"]
    eind = period.end_date.strftime("%-d %b %Y") if period.end_date else "heden"
    periode_label = f"{period.start_date.strftime('%-d %b')} – {eind}"
    ur = overview["user_rows"]
    n = len(ur)

    # ── Waarden schrijven ──────────────────────────────────────────────────

    # Rij 0: grote titelbalk
    # Rij 1: subkop links | rechts "Op rekening / Wessel"
    # Rij 2: IBAN info
    # Rij 3: kolomkoppen
    # Rij 4..4+n-1: data
    # Rij 4+n: totaalrij
    # Rij 4+n+2: timestamp

    R_TITLE   = 0
    R_SUBKOP  = 1
    R_IBAN    = 2
    R_HEADERS = 3
    R_DATA    = 4
    R_TOTAAL  = R_DATA + n
    R_STAMP   = R_TOTAAL + 2

    # Kolommen
    # A=0 naam | B=1 vorige stand | C=2 overgemaakt | D=3 geturfd | E=4 HO | F=5 correctie | G=6 stand
    # (gap) I=8 | J=9 op rekening box

    all_values = []

    # Rij 0: titelbalk
    row0 = ["Turfrekening", "", "", "", "", "", periode_label, "", "", "Op rekening"]
    all_values.append(row0)

    # Rij 1: subkop
    row1 = ["Overzicht per persoon", "", "", "", "", "", "", "", "", "Overmaken naar: Wessel"]
    all_values.append(row1)

    # Rij 2: IBAN
    row2 = ["Overmaken naar: NL21INGB0109055772 t.n.v. Wessel", "", "", "", "", "", "", "", "", ""]
    all_values.append(row2)

    # Rij 3: headers
    row3 = ["Naam", "Vorige Stand", "Overgemaakt", "Geturfd", "HO", "Correctie", "Stand", "", "", "Debet", "", "Credit"]
    all_values.append(row3)

    # Data rijen
    for r in ur:
        all_values.append([
            r["user"].name,
            _euro(r["vorige_stand"]),
            _euro(r["overgemaakt"]),
            _euro(r["geturfd"]),
            _euro(r["ho"]),
            _euro(r["correctie"]),
            _euro(r["stand"]),
        ])

    # Totaalrij
    all_values.append([
        "Totaal:",
        _euro(sum(r["vorige_stand"] for r in ur)),
        _euro(sum(r["overgemaakt"] for r in ur)),
        _euro(sum(r["geturfd"] for r in ur)),
        _euro(sum(r["ho"] for r in ur)),
        _euro(sum(r["correctie"] for r in ur)),
        _euro(sum(r["stand"] for r in ur)),
    ])

    # Lege rij
    all_values.append([])

    # Timestamp
    all_values.append([f"Bijgewerkt: {datetime.now().strftime('%d-%m-%Y %H:%M')}"])

    ws.update(all_values, "A1", value_input_option="USER_ENTERED")

    # ── Opmaak via batchUpdate ─────────────────────────────────────────────
    requests = []

    # Kolombreedtes
    requests += [
        _col_width(sid, 0, 1, 160),   # A: naam
        _col_width(sid, 1, 7, 115),   # B-G: cijfers
        _col_width(sid, 7, 8, 30),    # H: spacer
        _col_width(sid, 8, 12, 110),  # I-L: op rekening
    ]

    # Rijhoogtes
    requests += [
        _row_height(sid, R_TITLE, R_TITLE + 1, 40),
        _row_height(sid, R_HEADERS, R_HEADERS + 1, 30),
        _row_height(sid, R_DATA, R_TOTAAL + 1, 28),
    ]

    # Titelbalk rij (donker)
    requests += [
        _bg(sid, R_TITLE, 0, R_TITLE + 1, 7, DARK_BG),
        _bg(sid, R_TITLE, 9, R_TITLE + 1, 12, DARK_BG),
        _fmt(sid, R_TITLE, 0, R_TITLE + 1, 1,
             textFormat={"bold": True, "fontSize": 16, "foregroundColor": GOLD},
             verticalAlignment="MIDDLE"),
        _fmt(sid, R_TITLE, 6, R_TITLE + 1, 7,
             textFormat={"bold": True, "fontSize": 12, "foregroundColor": WHITE},
             horizontalAlignment="RIGHT", verticalAlignment="MIDDLE"),
        _fmt(sid, R_TITLE, 9, R_TITLE + 1, 12,
             textFormat={"bold": True, "fontSize": 12, "foregroundColor": GOLD},
             horizontalAlignment="CENTER", verticalAlignment="MIDDLE"),
    ]

    # Subkop rij
    requests += [
        _bg(sid, R_SUBKOP, 0, R_SUBKOP + 1, 7, HEADER_BG),
        _fmt(sid, R_SUBKOP, 0, R_SUBKOP + 1, 1,
             textFormat={"bold": True, "fontSize": 11, "foregroundColor": WHITE},
             verticalAlignment="MIDDLE"),
    ]

    # IBAN rij
    requests += [
        _fmt(sid, R_IBAN, 0, R_IBAN + 1, 7,
             textFormat={"italic": True, "foregroundColor": GRAY_TEXT},
             verticalAlignment="MIDDLE"),
    ]

    # Kolomkoppen rij
    requests += [
        _bg(sid, R_HEADERS, 0, R_HEADERS + 1, 7, HEADER_BG),
        _bg(sid, R_HEADERS, 9, R_HEADERS + 1, 12, HEADER_BG),
        _fmt(sid, R_HEADERS, 0, R_HEADERS + 1, 7,
             textFormat={"bold": True, "foregroundColor": WHITE},
             horizontalAlignment="CENTER", verticalAlignment="MIDDLE"),
        _fmt(sid, R_HEADERS, 9, R_HEADERS + 1, 12,
             textFormat={"bold": True, "foregroundColor": WHITE},
             horizontalAlignment="CENTER", verticalAlignment="MIDDLE"),
    ]

    # Data rijen: afwisselende achtergrond + uitlijning
    for i in range(n):
        row = R_DATA + i
        bg = LIGHT_BG if i % 2 == 0 else ROW_ALT
        requests += [
            _bg(sid, row, 0, row + 1, 7, bg),
            _align(sid, row, 1, row + 1, 7, h="RIGHT"),
            _fmt(sid, row, 0, row + 1, 1,
                 textFormat={"bold": True},
                 verticalAlignment="MIDDLE"),
        ]

    # Stand kolom kleuren (G = kolom 6)
    for i, r in enumerate(ur):
        row = R_DATA + i
        bg = GREEN_BG if r["stand"] >= 0 else RED_BG
        requests += [
            _bg(sid, row, 6, row + 1, 7, bg),
            _fmt(sid, row, 6, row + 1, 7,
                 textFormat={"bold": True, "foregroundColor": WHITE},
                 horizontalAlignment="RIGHT", verticalAlignment="MIDDLE"),
        ]

    # Totaalrij
    requests += [
        _bg(sid, R_TOTAAL, 0, R_TOTAAL + 1, 7, HEADER_BG),
        _fmt(sid, R_TOTAAL, 0, R_TOTAAL + 1, 7,
             textFormat={"bold": True, "foregroundColor": WHITE},
             horizontalAlignment="RIGHT", verticalAlignment="MIDDLE"),
        _fmt(sid, R_TOTAAL, 0, R_TOTAAL + 1, 1,
             textFormat={"bold": True, "foregroundColor": WHITE},
             horizontalAlignment="LEFT", verticalAlignment="MIDDLE"),
    ]

    # Borders om de hele tabel
    requests.append(_border(sid, R_HEADERS, 0, R_TOTAAL + 1, 7))

    # Merges: titel kolommen
    requests += [
        _merge(sid, R_TITLE, 0, R_TITLE + 1, 6),
        _merge(sid, R_TITLE, 9, R_TITLE + 1, 12),
        _merge(sid, R_SUBKOP, 0, R_SUBKOP + 1, 7),
        _merge(sid, R_IBAN, 0, R_IBAN + 1, 7),
    ]

    # Timestamp opmaak
    requests.append(_fmt(sid, R_STAMP, 0, R_STAMP + 1, 7,
                         textFormat={"italic": True, "foregroundColor": GRAY_TEXT}))

    spreadsheet.batch_update({"requests": requests})


def _sync_turfdata(spreadsheet, overview):
    ws = _ws(spreadsheet, "Turfdata", rows=200, cols=30)
    ws.clear()
    sid = ws.id

    products = overview["products"]
    ur = overview["user_rows"]
    period = overview["period"]
    eind = period.end_date.strftime("%-d %b %Y") if period.end_date else "heden"

    # Kolomkoppen: Naam | product1 | product2 | ... | Totaal stuks | Totaal (€)
    product_names = [f"{p.emoji} {p.name}" for p in products]
    headers = ["Naam"] + product_names + ["Totaal stuks", "Totaal (€)"]

    rows = []
    for r in ur:
        tally_map = r["tallies_per_product"]
        product_counts = [tally_map.get(p.id, 0) for p in products]
        total_stuks = sum(product_counts)
        total_eur = _euro(r["geturfd"])
        rows.append([r["user"].name] + product_counts + [total_stuks, total_eur])

    # Totaalrij
    totaal_counts = [sum(r["tallies_per_product"].get(p.id, 0) for r in ur) for p in products]
    totaal_stuks = sum(totaal_counts)
    totaal_eur = _euro(sum(r["geturfd"] for r in ur))
    rows.append(["Totaal"] + totaal_counts + [totaal_stuks, totaal_eur])

    n_cols = len(headers)
    n_rows = len(rows)

    ws.update([headers] + rows, "A1", value_input_option="USER_ENTERED")

    requests = [
        # Kolombreedtes
        _col_width(sid, 0, 1, 150),
        _col_width(sid, 1, n_cols, 90),
        # Rijhoogte header
        _row_height(sid, 0, 1, 32),
        _row_height(sid, 1, 1 + n_rows, 26),
        # Header opmaak
        _bg(sid, 0, 0, 1, n_cols, HEADER_BG),
        _fmt(sid, 0, 0, 1, n_cols,
             textFormat={"bold": True, "foregroundColor": WHITE},
             horizontalAlignment="CENTER", verticalAlignment="MIDDLE"),
        # Naam kolom links uitlijnen
        _align(sid, 0, 0, 1 + n_rows, 1, h="LEFT"),
        _fmt(sid, 1, 0, n_rows, 1, textFormat={"bold": True}),
        # Cijfers rechts uitlijnen
        _align(sid, 1, 1, 1 + n_rows, n_cols, h="RIGHT"),
        # Totaalrij opmaak
        _bg(sid, n_rows, 0, n_rows + 1, n_cols, HEADER_BG),
        _fmt(sid, n_rows, 0, n_rows + 1, n_cols,
             textFormat={"bold": True, "foregroundColor": WHITE}),
        # Borders
        _border(sid, 0, 0, n_rows + 1, n_cols),
        # Afwisselende rijen
    ]

    # Afwisselende achtergrondkleur per rij
    for i in range(n_rows - 1):
        bg = LIGHT_BG if i % 2 == 0 else ROW_ALT
        requests.append(_bg(sid, 1 + i, 0, 2 + i, n_cols, bg))

    # Laatste kolommen (Totaal stuks + €) iets accentueren
    requests += [
        _bg(sid, 0, n_cols - 2, n_rows + 1, n_cols, DARK_BG),
        _fmt(sid, 0, n_cols - 2, n_rows + 1, n_cols,
             textFormat={"bold": True, "foregroundColor": WHITE}),
    ]

    spreadsheet.batch_update({"requests": requests})


def sync_all(app):
    from models import Tally
    from calculations import get_active_period, get_period_overview

    with app.app_context():
        period = get_active_period()
        if not period:
            return {"ok": False, "error": "Geen actieve periode"}

        overview = get_period_overview(period.id)

        client = _client()

        # Nieuwe overzichtssheet
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        _sync_maandoverzicht(spreadsheet, overview)
        _sync_turfdata(spreadsheet, overview)

        # Bestaande sheet van de gebruiker
        existing = client.open_by_key(EXISTING_SPREADSHEET_ID)
        _sync_existing_overview(existing, overview)
        _sync_existing_invullen(existing, overview)
        _sync_betalingen(existing, overview)

        total_tallies = sum(
            sum(r["tallies_per_product"].values())
            for r in overview["user_rows"]
        )
        return {
            "ok": True,
            "synced_at": datetime.now().strftime("%d-%m-%Y %H:%M"),
            "tallies": total_tallies,
        }
