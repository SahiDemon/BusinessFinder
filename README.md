# Sri Lanka Business Finder Directory

Small full-stack Node web app for searching Sri Lankan companies by area and category, with Excel-friendly CSV export and live provider refresh hooks.

## Features

- Search by free text, city, district, and category
- Filter for phone, website, and rating availability
- Export the current filtered result set to CSV for Excel
- Relational SQLite schema for companies, locations, categories, and source records
- Live ingestion from Google Places API when `GOOGLE_MAPS_API_KEY` is configured
- Live fallback ingestion from OpenStreetMap Overpass

## Setup

Optional environment variable:

```powershell
$env:GOOGLE_MAPS_API_KEY="your-key-here"
```

Or create a `.env` file in the project root:

```env
GOOGLE_MAPS_API_KEY=your-key-here
```

If the Google key is not set, the app still fetches live data from OpenStreetMap Overpass.

## Run

```bash
npm start
```

Open `http://localhost:3000`.
