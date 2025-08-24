# JCBoards_Outreach

# places_to_csv.py — Google Places → Emails → CSV (Berlin)

A single-file Python 3.11 tool that:
- Geocodes an anchor address in Berlin
- Searches Google Places (Text + Nearby) around it
- Fetches Place Details
- (Optionally) visits a few same‑domain pages to find a public email
- Writes everything to CSV

## Install
```bash
pip install requests pandas python-dotenv beautifulsoup4 tldextract tenacity
```

## .env (optional)
Create a `.env` next to the script:
```ini
GOOGLE_API_KEY=YOUR_API_KEY
DEFAULT_QUERIES=Küchengeschäft,Feinkost,Delikatessen,Metzgerei,Haushaltswaren,Geschenkartikelladen,Holzwerkstatt
DEFAULT_TYPES=home_goods_store,store,furniture_store
DEFAULT_RADIUS=7500
DEFAULT_LIMIT=500
```

## Run (examples)
```bash
# default (reads .env)
python places_to_csv.py

# different radius + output file
python places_to_csv.py --radius 10000 --out berlin_stockists.csv

# only specific queries
python places_to_csv.py --queries "Feinkost,Delikatessen,Metzgerei,Küchengeschäft"

# disable website crawling (faster)
python places_to_csv.py --no-crawl

# tune speed
python places_to_csv.py --sleep 0.5 --limit 150 --max-pages-per-site 2
```

## CLI
- `--api-key` (overrides `.env`)
- `--address` (default: “”)
- `--radius` (m, default 7500)
- `--queries` (comma‑separated)
- `--types` (comma‑separated)
- `--limit` (cap, default 500)
- `--out` (CSV path, default berlin_places.csv)
- `--sleep` (seconds between Google API calls, default 2.0)
- `--crawl` / `--no-crawl` (email discovery on/off; default on)
- `--max-pages-per-site` (default 5)
- `--timeout` (HTTP timeout, default 10)

## CSV Columns
`name, formatted_address, international_phone_number (or formatted_phone_number), website, public_email, place_id, lat, lng, types, rating, user_ratings_total, google_maps_url, source_query, robots_respected, email_source_page, scrape_notes`

## Notes
- Uses only a few obvious pages: `/`, `/contact`, `/kontakt`, `/impressum`, `/about`, `/ueber-uns`
- Same‑domain only; basic robots.txt checks; shallow crawl
- Retries transient network errors
- De‑dupes by `place_id` and registrable website domain

## Legal/ToS
- Public contact info only
- Respect robots.txt and site terms
- Google APIs are billed/limited—use your own key
