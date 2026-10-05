# CLAUDE.md

Richtlijnen voor Claude Code in deze repository.

## Project

Turfrekening: een drank-bijhoudsysteem voor een studentenhuis. Op een iPad turft iedereen wat hij of zij pakt; de beheerder voert betalingen, inkoop en gedeelde kosten (HO) in en sluit elke periode af. Data staat in Supabase (Postgres), de app draait op Railway (gunicorn).

## Lokaal draaien

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env            # DATABASE_URL leeg laten voor lokale SQLite
export FLASK_DEBUG=1            # alleen lokaal: SECRET_KEY mag dan ontbreken
flask --app app db upgrade      # maakt het schema
python scripts/seed_dev.py      # optioneel: testdata (weigert Postgres)
python3 app.py                  # poort 8080
pytest                          # alle tests, eigen SQLite, nooit de echte database
```

Zonder `DATABASE_URL` gebruikt de app lokaal `turfrekening.db` (staat in `.gitignore`). Zet `DATABASE_URL` in `.env` nooit op de productie-database om te testen.

## Deploy

Lokaal → GitHub → Railway (deployt vanaf `main`). Eerst op een branch werken. Start: `gunicorn app:app` (zie `railway.json`).

Omgevingsvariabelen op Railway: `DATABASE_URL`, `SECRET_KEY` (verplicht, de app start zonder niet), `ADMIN_PASSWORD` (login voor `/admin` en `/ho`).

**Databasewijzigingen** gaan via Flask-Migrate (`migrations/`). Draai ze niet automatisch bij het starten:

1. Maak eerst een backup in Supabase.
2. `flask --app app db upgrade` met `DATABASE_URL` van de doeldatabase.
3. Nieuwe wijziging maken: model aanpassen, `flask --app app db migrate -m "omschrijving"`, de gegenereerde migratie nakijken (vooral data-omzettingen), testen op een kopie.

Een bestaande database die al het oude schema had, eenmalig markeren met `flask --app app db stamp 7615fb1eb8e0` (baseline) en daarna `upgrade`.

## Architectuur

```
app.py          Flask-routes en API (turfscherm, rapport, admin)
models.py       SQLAlchemy-modellen
calculations.py compute_period(): het ENIGE rekenpad voor standen, HO en voorraad
periods.py      afsluiten/bevriezen, nieuwe periode starten, datumcontroles, get_period_view()
money.py        centen-helpers (parse, weergave, split_even)
filters.py      Jinja-filters euro / euro_cls
forms.py        parse_int / parse_date helpers met nette foutmeldingen
auth.py         admin-login (gedeeld wachtwoord)
static/css/app.css  het hele stylesheet (tokens bovenaan); geen CSS-framework
templates/      base.html, turf.html, rapport.html, ho.html, admin/*
tests/          pytest
```

## Regels die niet gebroken mogen worden

**Geld is altijd in hele centen (`int`)**, kolommen heten `*_cents`. Formulieren parsen met `parse_cents`, tonen met het `euro`-filter (`−€1,01`, `+€1,01`, `—` bij nul). Nooit `float` voor bedragen. Verdelingen gaan met `split_even`, zodat de aandelen exact optellen.

**Eén rekenpad.** Alles gaat via `compute_period(period_id)`:
`Stand = beginstand + overgemaakt − geturfd − HO + correctie`.
- Geen beginstand in een periode = 0, nooit een terugval op iets anders.
- Een turfje onthoudt zijn prijs (`Tally.unit_price_cents`); prijswijzigingen raken oude turfjes niet.
- Turfverlies is **voorlopig en wordt niet verdeeld** zolang niet elk product met voorraad of turfjes een eindtelling heeft.
- Gedeactiveerde producten tellen mee in een periode waarin ze turfjes, inkoop of voorraad hebben.

**Afsluiten = bevriezen.** `periods.close_period` slaat het rapport één keer op als `PeriodReport` (JSON), zet `closed_at`, start de nieuwe periode met eindstanden als beginstanden en de eindtelling als beginvoorraad. Rapporten van afgesloten periodes lezen alleen uit `PeriodReport` (`get_period_view`) en worden nooit opnieuw berekend. Afsluiten kan niet zonder volledige eindtelling. Een afgesloten periode kan niet meer worden gewijzigd of geactiveerd. Periodes zonder `closed_at` die niet actief zijn, zijn "historisch" (van vóór deze werkwijze).

**Bewoners worden nooit verwijderd.** Vertrekt iemand: `is_active = False` en `left_at`. Zo blijven geschiedenis en eventuele schuld bewaard. Een vertrokken bewoner met saldo blijft in de volgende periodes staan tot hij of zij vereffend is. Nieuwe bewoners starten op €0,00.

**Producten** met turfjes, voorraad of koppelingen worden niet verwijderd, alleen op inactief gezet.

**Ongedaan maken** van een turfje (`DELETE /api/tally/<id>`) mag alleen in de actieve periode en binnen 10 minuten. Daarna gebruik je een correctie (met verplichte omschrijving).

**Beveiliging:** `/admin/*` en `/ho` vereisen login (`@admin_required`); turfscherm en rapport zijn open. Alle POST/DELETE hebben CSRF-bescherming (formulieren `csrf_token`, turfscherm header `X-CSRFToken`).

## Gebruikersvolgorde

Altijd sorteren op `User.sort_order`, dan `User.name`. De huidige bewoners en hun volgorde staan in de admin (Bewoners); zet ze niet in deze file, ze veranderen.

## Stijl van de interface

Eén stylesheet met tokens in `:root` (kleuren, ruimtes, radius); geen losse hexwaarden of `<style>`-blokken in templates. Eén accentkleur (amber) per scherm, groen/rood alleen voor standen. Labels in gewone zinnen, geen hoofdletters. Op het turfscherm geen saldo's tonen.
