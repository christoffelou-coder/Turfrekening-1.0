# Netlify Deploy Guide

## Wat is gedaan:

1. ✅ `netlify.toml` geconfigureerd met:
   - Build command: installeert dependencies
   - Functions directory: `netlify/functions`
   - Publish directory: `public`
   - Redirect rule: alles → serverless function

2. ✅ `netlify/functions/handler.py` upgraded:
   - Better error handling
   - Correct Python path setup

3. ✅ `public/` directory created (voor statische output)

4. ✅ `.gitignore` updated (build artifacts)

## Volgende stappen:

### 1. Test lokaal (zeker weten alles werkt)
```bash
python3 app.py
# Test op http://localhost:8080/
```

### 2. Commit naar GitHub
```bash
git add .
git commit -m "Setup Netlify serverless backend + frontend"
git push origin main
```

### 3. Connect op Netlify (één keer)

**A. Via netlify.com UI:**
- Ga naar https://app.netlify.com
- "New site from Git" → kies repo `Turfrekening-1.0`
- Deploy!

**B. Of via CLI:**
```bash
npm install -g netlify-cli
netlify deploy
```

### 4. Environment Variables op Netlify

Zet deze in Netlify UI (Site settings → Environment):

```
DATABASE_URL=<jouw Supabase URL>
SECRET_KEY=<random string>
GOOGLE_CREDENTIALS_JSON=<service account JSON>
```

## Hoe werkt het:

- **Frontend (iPad):** `https://turfrekening.netlify.app/`
- **Backend API:** `https://turfrekening.netlify.app/api/*` (serverless)

Alles draait via serverless functions — geen extra URL's nodig!

## Voordelen:

✅ Alle 21 routes (frontend + backend) op één URL
✅ Automatische deploy via GitHub push
✅ HTTPS + SSL gratis
✅ Gratis tier volstaat

## Troubleshooting:

**Error: module not found (Flask, etc.)**
- Check `requirements.txt` is compleet
- Netlify build command installeert dependencies

**Static files niet laden**
- Static folder wordt geserveerd door Flask
- Zorg dat `static/` map exists

**Environment variables niet werken**
- Check ze in Netlify UI zijn ingesteld
- Deploy opnieuw na env var update
