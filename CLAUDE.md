# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Turfrekening — een drank-bijhoudsysteem voor een studentenhuis. Gebruikt op een iPad om bij te houden wie wat heeft gedronken. Data staat in Supabase, de app draait op Netlify.

## Lokaal draaien

```bash
# Kopieer .env.example naar .env en vul DATABASE_URL in
python3 app.py        # start op poort 8080
# of
bash start.sh
```

De app verbindt automatisch met Supabase als `DATABASE_URL` in `.env` staat. Zonder die variabele valt hij terug op lokale SQLite (`turfrekening.db`).

## Deploy workflow

**Lokaal → GitHub → Netlify (automatisch)**

```bash
git add .
git commit -m "omschrijving"
git push origin main   # Netlify deployt automatisch (~1-2 min)
```

GitHub repo: `https://github.com/christoffelou-coder/Turfrekening-1.0`

Netlify draait Flask via serverless functions (`netlify/functions/app.py` + `serverless-wsgi`). De `netlify.toml` stuurt alle requests door naar die function.

## Architectuur

```
app.py          — Flask routes en API endpoints
models.py       — SQLAlchemy modellen (Period, User, Product, Tally, ...)
calculations.py — Alle financiële berekeningen (stand, HO, turfverlies)
sheets_sync.py  — Google Sheets sync (gspread v6)
templates/      — Jinja2 HTML templates
  turf.html     — Hoofdscherm (iPad interface)
  rapport.html  — Maandoverzicht
  admin/        — Beheerpagina's
```

### Berekeningsformule per persoon
`Stand = vorige_stand + overgemaakt − geturfd − HO_aandeel + correctie`

### HO (Huishoudelijke Onkosten)
Turfverlies + HO-events worden verdeeld via `distribution_type`:
- `equal_all` — gelijk over alle actieve gebruikers
- `equal_selected` — gelijk over geselecteerde gebruikers (via `HOEventShare`)
- `manual` — handmatig bedrag per persoon

## Google Sheets sync

Spreadsheet ID (bestaand): `10MGTFssPTg6GUmMp0sEp-O43wY8_aUd8BqvI190W4FY`

Tabs die gesynchroniseerd worden: **Overview** (C/D/E/F/G/I), **Invullen** (voorraad + turfcounts + HO), **Betalingen** (kolom H).

**gspread v6 let op:** argument volgorde is `ws.update(values, range_name)` — NIET `ws.update(range_name, values)`.

Credentials: `google_credentials.json` (niet in git). Op Netlify via env var `GOOGLE_CREDENTIALS_JSON`.

Manueel triggeren: `POST /api/sync-sheets`

### Product aliassen
"Bier", "Pils", "biertje" zijn synoniemen — zie `PRODUCT_ALIASES` in `sheets_sync.py`.

## Omgevingsvariabelen

| Variabele | Waar |
|---|---|
| `DATABASE_URL` | Supabase pooler URL (eu-west-1) |
| `SECRET_KEY` | Flask session key |
| `GOOGLE_CREDENTIALS_JSON` | Service account JSON als string (Netlify) |

Supabase connectie: `aws-0-eu-west-1.pooler.supabase.com:5432`, gebruikersnaam formaat: `postgres.[project-id]`

## Gebruikersvolgorde

Altijd op `sort_order` dan `name` sorteren: `User.query.order_by(User.sort_order, User.name)`. Volgorde: Stos, Teun, Godard, Ruben, Thomas, Wessel, Kaastra, Stijn, Moffel, Beukers, Luis, De bie, Noah, Romeijn, Pablo, Jorge.

Inactieve gebruikers (Thomas, Noah, Pablo) moeten WEL meegenomen worden in berekeningen — zij kunnen schulden hebben.
