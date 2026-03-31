const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const isVercel = Boolean(process.env.VERCEL);
const dataDir = isVercel ? path.join('/tmp', 'sl-directory-data') : path.join(process.cwd(), 'data');
const dbPath = path.join(dataDir, 'directory.sqlite');

function ensureDataDir() {
  fs.mkdirSync(dataDir, { recursive: true });
}

function openDatabase() {
  ensureDataDir();
  const db = new DatabaseSync(dbPath);

  db.exec(`
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      city TEXT NOT NULL,
      province TEXT NOT NULL DEFAULT '',
      district TEXT NOT NULL,
      country TEXT NOT NULL DEFAULT 'Sri Lanka',
      UNIQUE(city, district, country)
    );

    CREATE TABLE IF NOT EXISTS companies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      normalized_name TEXT NOT NULL,
      address TEXT NOT NULL,
      location_id INTEGER NOT NULL,
      phone TEXT,
      website TEXT,
      rating REAL,
      review_count INTEGER,
      source TEXT NOT NULL,
      last_updated TEXT NOT NULL,
      dedupe_key TEXT NOT NULL UNIQUE,
      FOREIGN KEY(location_id) REFERENCES locations(id)
    );

    CREATE TABLE IF NOT EXISTS categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS company_categories (
      company_id INTEGER NOT NULL,
      category_id INTEGER NOT NULL,
      PRIMARY KEY(company_id, category_id),
      FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
      FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS source_records (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      source_name TEXT NOT NULL,
      source_key TEXT NOT NULL UNIQUE,
      raw_payload TEXT NOT NULL,
      imported_at TEXT NOT NULL,
      FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
    );
  `);

  const locationColumns = db.prepare('PRAGMA table_info(locations)').all();
  const hasProvince = locationColumns.some((column) => column.name === 'province');
  if (!hasProvince) {
    db.exec("ALTER TABLE locations ADD COLUMN province TEXT NOT NULL DEFAULT ''");
  }

  return db;
}

module.exports = {
  openDatabase,
  dbPath,
};
