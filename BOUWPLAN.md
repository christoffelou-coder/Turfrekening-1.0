# Bouwplan Turfrekening

Uitwerking van `VERBETERPLAN.md` naar een bouwvolgorde, gebaseerd op de huidige code (`app.py` 744 regels, `calculations.py` 425, `models.py` 154, 12 templates, Tailwind via CDN).

Per stap: wat er verandert, welke bestanden, hoe we het controleren. Elke stap is één branch-commit (of PR) die los te bekijken en terug te draaien is.

---

## 1. Wat ik in de code heb gevonden (naast het verbeterplan)

Dingen die het verbeterplan niet noemt of die anders liggen dan beschreven:

| Bevinding | Gevolg voor het plan |
|---|---|
| `google_credentials.json` en `turfrekening.db` staan **niet** in git en hebben er ook nooit in gestaan (`git log --all` is leeg, `.gitignore` dekt ze). | Fase 7 punt 4/6: alleen lokaal verwijderen, geschiedenis herschrijven is niet nodig. |
| Er staan **twee** deploy-configs naast elkaar (`Procfile` + `railway.json`) plus Netlify (`netlify.toml`, `netlify/functions/handler.py`, `functions/handler.py`, `public/`, `_site/`, `DEPLOY_GUIDE.md`). CLAUDE.md zegt nog Netlify. | Fase 7.1 is groter dan één bestand; ik kies `railway.json` en verwijder de rest. |
| Er is geen migratiesysteem. `create_tables()` met `ALTER TABLE ... try/except: rollback` draait alleen via `python app.py`, dus niet onder gunicorn. | Fase 7.3 (Flask-Migrate) moet **vóór** de schemawijzigingen van fase 1 en 2. Dat is een volgorde-wijziging t.o.v. het verbeterplan. |
| De scheduler (`BackgroundScheduler`) start alleen in `__main__`, dus draait hij in productie niet. | Sheets-sync verwijderen is veilig, niets in productie hangt eraan. |
| Naast `get_stand` / `get_ho_share_for_user` / `get_total_ho_per_person` zijn er `get_stands_bulk`, `get_ho_shares_bulk` en `get_period_overview`, die nog eens dezelfde formule dupliceren (vooral `get_stands_bulk` en `get_period_overview`). | Fase 3.2: één functie `compute_period(period_id)` die alles levert; de rest verdwijnt. |
| `/api/tally` valideert niets (`data["user_id"]` geeft een 500 bij een ontbrekend veld, hoeveelheid is onbegrensd, inactieve gebruiker/product wordt geaccepteerd). | Fase 4.3. |
| `index()` toont **alle** gebruikers (ook inactieve) op het turfscherm. | Fase 6.7: filter op actief + niet vertrokken. |
| `get_inventory_data` filtert `is_active=True` op producten, en `get_total_tallied_per_product` laadt child-producten ongefilterd. | Fase 1.6, en de parent/child-regel (halve krat = 12 pils) moet in de tests. |
| HO-events met bier: `total_cost` bevat de prijs van het bier op het moment van aanmaken (goed), maar het turfverlies rekent `verlies_qty * p.price` met de huidige prijs. | Valt onder 1.5: voorraadwaardering moet ook de prijs van de periode vastleggen (zie 3.7 hieronder). |
| `/api/balance/<id>` bestaat nog en is ongebruikt op het turfscherm. | Weg in fase 3.2. |
| Dev-server draait met `debug=True` op `0.0.0.0`. | Alleen lokaal; in fase 4 achter `FLASK_DEBUG` zetten. |
| Remote branch `origin/claude/brave-fermat-6extxs` bestaat. | Uitzoeken of daar iets op staat dat we nodig hebben, anders opruimen. |

---

## 2. Open vragen (nodig vóór de genoemde stap)

1. **Fase 0 afgerond?** Vooral de backup. Voor stap A heb ik een database nodig om op te testen. Voorkeur: jij maakt een dump uit Supabase (`pg_dump`) en ik laad die in een lokale Postgres. Ik raak productie niet aan.
2. **Bestaande periodes (stap D):** historisch markeren (PDF is leidend) of de officiële eindstanden handmatig importeren als `PeriodReport`? Mijn voorkeur: **importeren van de officiële eindstanden per persoon** voor 9 jun – 10 sep, zodat de beginstanden van de volgende periode kloppen, en daarbij het rapport als "historisch, alleen standen" markeren.
3. **Hosting:** het is Railway, klopt dat? Deployt Railway op `main`? (Bepaalt of ik migraties in de startcommand zet.)
4. **Geld opslaan:** ik stel **integer centen** voor (geen Decimal/float-mix, makkelijk in JSON-snapshots, exact optellen). Alternatief `Numeric(10,2)`. Akkoord met centen?
5. **Login:** één gedeeld wachtwoord uit `ADMIN_PASSWORD` is genoeg? Of per persoon?
6. **HO per periode (3.5):** nu overslaan (bevroren rapporten lossen het grootste deel op) en later oppakken?

---

## 3. Bouwvolgorde

Afwijking van de fasenummering in het verbeterplan, omdat sommige dingen anderen blokkeren:

```
A  Fundament        branch, testdatabase, Flask-Migrate, tests
B  Beveiliging      login, SECRET_KEY, invoercontrole   (productie staat nu open)
C  Opruimen         Sheets/Netlify eruit, minder code om te verbouwen
D  Rekenkern        centen, één rekenpad, unit_price, nul-fallback
E  Periodemodel     bevriezen, vergrendelen, afsluiten-flow, vertrokken
F  Navigatie + CSS  base.html, app.css, euro-filter, admin-dashboard
G  Schermen         turfscherm, rapport, overige admin
H  Afronding        CLAUDE.md, testscenario's, uitrol
```

### Stap A — Fundament (verbeterplan: werkafspraken + 7.3)

- Branch `verbeterplan` vanaf `main`. Alles gaat via die branch.
- Lokale testdatabase uit de dump; `.env` wijst lokaal daarheen, nooit naar Supabase.
- `pytest` toevoegen met een eigen in-memory SQLite en fixtures (periode, 4 gebruikers, 3 producten). Eerst **karakteriseringstests** op de huidige rekenlogica, zodat ik in stap D kan bewijzen dat standen niet onbedoeld veranderen.
- Flask-Migrate (Alembic) toevoegen. Baseline-migratie die overeenkomt met het huidige schema (`stamp`), zodat productie niet opnieuw tabellen probeert aan te maken. `create_tables()` en de `ALTER TABLE`-blokken verdwijnen.
- Bestanden: `requirements.txt`, `app.py` (factory-achtig opzetten blijft klein), `migrations/`, `tests/`.
- **Klaar als:** `flask db upgrade` op een lege en op de gedumpte database geeft hetzelfde schema; testsuite draait groen.

### Stap B — Beveiliging (fase 4)

- `auth.py` met `admin_required`-decorator, login-/logoutpagina, sessiecookie (httponly, samesite=Lax, secure in productie). Beschermt `/admin/*` en `/ho` (en de Sheets-routes bestaan na stap C niet meer).
- `SECRET_KEY` verplicht buiten lokaal (`FLASK_ENV`/`DEBUG`-check), geen vaste fallback.
- Validatie in `/api/tally`: JSON-vorm, actieve gebruiker, actief product, `1 ≤ quantity ≤ 24`, nette JSON-fouten.
- Formulierparsing via kleine helpers (`parse_amount`, `parse_date`, `parse_int`) die een flash-melding geven in plaats van een 500.
- Flask-WTF `CSRFProtect`; het turfscherm stuurt de token mee in de `fetch`-header.
- **Waarom nu al:** de huidige productie-app is voor iedereen met de URL te wijzigen.
- **Klaar als:** testscenario 8 slaagt, en turfen vanaf de iPad werkt nog met de CSRF-header.

### Stap C — Opruimen (fase 7, het veilige deel)

- Verwijderen: `sheets_sync.py`, routes `/api/sync-sheets` en `/api/setup-tabs`, `_scheduled_sync`, scheduler, `gspread`/`google-auth`/`apscheduler`, Netlify-bestanden (`netlify.toml`, `netlify/`, `functions/`, `public/`, `_site/`, `DEPLOY_GUIDE.md`), `Procfile` (of `railway.json`; ik houd `railway.json`).
- Lokaal weghalen: `google_credentials.json`, `turfrekening.db`. `.gitignore` bijwerken.
- `Model.query.get(id)` → `db.session.get(Model, id)` (alle ~20 plekken).
- **Klaar als:** app start, tests groen, `grep -ri "sheets\|netlify\|gspread"` levert niets meer op behalve changelog.

### Stap D — Rekenkern (fase 1.2, 1.5, 1.6 + fase 3)

Modelwijzigingen (via migratie):

| Tabel | Wijziging |
|---|---|
| alle bedragen (`Product.price`, `Payment.amount`, `Correction.amount`, `HOEvent.total_cost`, `HOEventShare.amount`, `InventoryPurchase.total_cost`, `PeriodStartBalance.balance`) | `Float` → integer centen (`*_cents`). Migratie: `ROUND(x*100)`. |
| `Tally` | `unit_price_cents`, gevuld bij turven; migratie vult bestaande turfjes uit de actieve periode met de huidige prijs. |

Rekenwijzigingen:

- Nieuw `calculations.compute_period(period_id)` als **enige** rekenpad. Levert per persoon: begin, overgemaakt, geturfd, HO (uitgesplitst turfverlies / events), correctie, stand; plus voorraadregels, totalen, waarschuwingen.
- Geen beginstand = **0** (fix 1.2). `previous_balance` wordt nergens meer gelezen.
- Alle personen in de periode meenemen (ook vertrokken met saldo of activiteit), gefilterd op "heeft beginstand of activiteit in deze periode" zodat nieuwe bewoners niet in oude periodes verschijnen.
- Voorraad: gedeactiveerde producten tellen mee zodra ze begin/inkoop/eind/turfjes in de periode hebben (1.6).
- Turfverlies is **voorlopig** zolang de eindtelling ontbreekt (`inventory_complete=False`): wordt getoond maar niet verdeeld, rapport krijgt de vlag "HO voorlopig".
- Verdeling per bedrag met *largest remainder* in centen: de aandelen tellen exact op tot het totaal, dus geen afwijkingen van 2 cent tussen rijen en totaal.
- HO-label: `ho_uniform` (waar/onwaar) voor "gelijk verdeeld"; anders uitsplitsing per post.
- Verwijderen: `get_stand`, `get_ho_share_for_user`, `get_total_ho_per_person`, `get_stands_bulk`, `get_ho_shares_bulk`, `get_geturfd_cost`, `/api/balance`, `new_stand` in `/api/tally` en undo.
- Product kan niet verwijderd worden als er turfjes/voorraad zijn (alleen deactiveren), foutmelding in plaats van 500.
- **Tests:** testscenario's 3 en 5, plus: prijswijziging verandert oude turfjes niet, parent/child (halve krat), HO `equal_selected`/`manual`, afrondingssom, bier-bij-HO telt niet als verlies.
- **Klaar als:** karakteriseringstests uit stap A geven dezelfde standen op de gedumpte data, behalve de bewust gewijzigde gevallen (nul-fallback, voorlopig verlies), en die verschillen staan in een lijst die ik laat zien.

### Stap E — Periodemodel (fase 1.1, 1.3, 1.4 + fase 2)

Modelwijzigingen:

| Tabel | Wijziging |
|---|---|
| `Period` | `closed_at` |
| `User` | `left_at` (datum); `is_active` blijft "op turfscherm". Later `previous_balance` droppen. |
| nieuw `PeriodReport` | `period_id` (unique), `data` (JSON), `created_at`; bevat het volledige `compute_period`-resultaat, bedragen in centen, plus een `schema_version`. |

Gedrag:

- **Vertrokken i.p.v. verwijderen:** delete-actie weg; knop "Vertrokken" met waarschuwing bij saldo ≠ 0 (saldo uit `compute_period`). Vertrokken verdwijnt van turfscherm en uit nieuwe periodes, niet uit oude.
- **Vergrendeling:** één helper `require_open_period(period)` in alle muterende routes (turf, betaling, correctie, voorraad, inkoop, HO, beginstand, producten-met-prijs voor die periode). Ongedaan maken alleen actieve periode én ≤ 10 min (constante).
- **Afsluit-flow** op `/admin/periods` in vier stappen: eindtelling (verplicht) → controle met waarschuwingen → afsluiten (einddatum, `PeriodReport`, `closed_at`, in één transactie) → nieuwe periode automatisch (startdatum, naam, `PeriodStartBalance` uit het rapport, begin-`InventorySnapshot` uit de eindtelling).
- Rapport van gesloten periode leest alleen `PeriodReport`; `rapport()` rekent nooit opnieuw.
- Weg: `copy_balances`, `activate`, verwijderen van periodes met data, `/admin/vorige-stand`, `previous_balance` in het gebruikersformulier.
- Datumcontrole bij aanmaken/bewerken: toekomst, overlap, gat.
- **Bestaande data (volgens jouw antwoord op vraag 2):** migratiescript dat de oude periode(s) markeert/importeert. Periode 10 sep – 1 okt krijgt pas een snapshot na jouw bevestiging.
- **Tests:** scenario's 1, 2, 4, 6, 7 en de acceptatiecheck van fase 2 (afsluiten → gebruiker vertrekken → prijs en HO-instelling wijzigen → rapport byte-identiek).
- **Klaar als:** alle bovenstaande tests groen en de afsluit-flow handmatig doorlopen op de testdatabase.

### Stap F — Navigatie en CSS-fundament (fase 5 + 6.1–6.6)

- Tailwind-CDN eruit (`cdn.tailwindcss.com` is niet bedoeld voor productie en vereist internet bij elke load). `static/css/app.css` met de tokens uit 6.2, componentklassen, `base.html` met `Turfen · Rapport · Admin`, iconenset (Lucide, inline SVG, geen extra request).
- Jinja-filter `euro` (`−€1,01`, `+€1,01`, `—`, NL-notatie) in `app.py`/`filters.py`, met tests.
- Flash-balk, print-stylesheet-skelet, `:focus-visible`, `prefers-reduced-motion`.
- Admin-dashboard: statusblok (`compute_period`-samenvatting) + zes tegels in twee groepen; betalingen en correcties samengevoegd tot één pagina met tabs; tegels Vorige standen en Maandrapport weg; `/ho` onder "Gedeelde kosten" in admin.
- **Klaar als:** geen `<style>`-blokken en geen losse hexwaarden meer in `base.html` en admin-index; scenario 9 (bedragen) groen.

### Stap G — Schermen (fase 6.7–6.9)

Volgorde op belang:

1. **Turfscherm** (`turf.html`, 270 regels): vijf-koloms raster in leesvolgorde, producten in één rij, gedimde namen zonder product, aantal onder de naam, vaste onderbalk met laatste turfje + ongedaan maken, tikfeedback. Past op 1024×768 en 1180×820 liggend zonder scrollen. Verifiëren in de browser/simulator op die formaten.
2. **Rapport** (`rapport.html`, 275 regels): kerncijfers, alleen Stand gekleurd, badge gesloten/lopend, bevroren melding, print-stylesheet.
3. **Overige admin** (`users`, `products`, `periods`, `inventory`, `payments`+`corrections`, `ho`): formulieren, lege staten, nette foutmeldingen, tabellen met sticky kop.
- **Klaar als:** scenario 10 en visuele controle per scherm met screenshots die ik laat zien.

### Stap H — Afronding (fase 7.2, 7.8)

- Ongebruikte imports/functies opruimen, `previous_balance`-kolom droppen (aparte, laatste migratie, pas als niets er nog van afhangt en na backup).
- `CLAUDE.md` herschrijven: Railway, huidige bewoners en volgorde, periodemodel (afsluiten = bevriezen), geen Sheets, geen "inactieve gebruikers meenemen".
- Alle 10 testscenario's als geautomatiseerde tests waar mogelijk, rest als handmatige checklist.
- **Uitrol:** zie punt 4.

---

## 4. Uitrolstrategie voor productie

1. Fase 0 (backup, Google-sleutel, saldi-verrekening, Stijn/Luis, startdatum) is af.
2. Migraties zijn **additief eerst** (nieuwe kolommen/tabellen, centen naast de oude floats), code gaat daarna over, en pas aan het eind vallen oude kolommen weg (expand → migrate → contract). Zo kan elke stap terug.
3. Eerst de volledige keten op de lokale kopie van de dump; daarna op een Railway-testomgeving met een Supabase-kopie als die er is; pas dan `main`.
4. Stap B en C kunnen los van de rest naar productie (klein, laag risico). Stap D en E gaan samen live, omdat de migratie van centen en `PeriodReport` bij elkaar hoort. F en G kunnen daarna per scherm.
5. Na elke uitrol: handmatige rooktest (turfen, undo, rapport openen, admin-login).

## 5. Risico's

| Risico | Mitigatie |
|---|---|
| Centen-migratie zet bedragen fout | Vóór/na-vergelijking van alle standen per periode op de dump; migratie faalt als verschil > 0 cent. |
| Standen veranderen door nul-fallback of voorlopig turfverlies | Verschillenlijst tonen vóór uitrol; jij keurt goed. |
| Turfscherm breekt op de iPad na CSS-omzet | Eigen CSS in kleine stappen, test op iPad-formaat, service worker (`static/sw.js`) cache-versie ophogen zodat de iPad de nieuwe CSS pakt. |
| CSRF/login breekt het turven | Turfen blijft zonder login; CSRF-token via meta-tag, testen vóór uitrol. |
| Productie raakt een migratie die half slaagt | Elke migratie in één transactie (Postgres ondersteunt transactionele DDL), backup vooraf. |

## 6. Wat ik niet ga doen zonder overleg

Productie-Supabase benaderen, saldi of periodes in de echte database wijzigen, `previous_balance` droppen, het rapport van 10 sep – 1 okt bevriezen, pushen naar `main`.
