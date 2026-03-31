const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { DatabaseSync } = require('node:sqlite');
const { DirectoryRepository } = require('../src/repository');
const { createCsvBuffer } = require('../src/exporter');

const sampleCompanies = [
  {
    name: 'Colombo Engineering Hub',
    address: '12 Union Place, Colombo',
    city: 'Colombo',
    district: 'Colombo',
    phone: '+94 11 200 1000',
    website: 'https://colombo-engineering.example.com',
    rating: 4.7,
    reviewCount: 40,
    categories: ['engineering', 'construction'],
    source: 'test',
    sourceKey: 'test-colombo-engineering-hub',
  },
  {
    name: 'Horana Concrete Yard',
    address: '55 Panadura Road, Horana',
    city: 'Horana',
    district: 'Kalutara',
    phone: '+94 34 220 2200',
    website: 'https://horana-concrete.example.com',
    rating: 4.2,
    reviewCount: 12,
    categories: ['concrete'],
    source: 'test',
    sourceKey: 'test-horana-concrete-yard',
  },
];

function createRepositoryFixture() {
  const dbFile = path.join(os.tmpdir(), `directory-test-${Date.now()}-${Math.random()}.sqlite`);
  const db = new DatabaseSync(dbFile);
  db.exec(`
    PRAGMA foreign_keys = ON;
    CREATE TABLE locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      city TEXT NOT NULL,
      district TEXT NOT NULL,
      country TEXT NOT NULL DEFAULT 'Sri Lanka',
      UNIQUE(city, district, country)
    );
    CREATE TABLE companies (
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
    CREATE TABLE categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    );
    CREATE TABLE company_categories (
      company_id INTEGER NOT NULL,
      category_id INTEGER NOT NULL,
      PRIMARY KEY(company_id, category_id),
      FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
      FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
    );
    CREATE TABLE source_records (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      source_name TEXT NOT NULL,
      source_key TEXT NOT NULL UNIQUE,
      raw_payload TEXT NOT NULL,
      imported_at TEXT NOT NULL,
      FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
    );
  `);

  const repository = new DirectoryRepository(db);
  repository.upsertCompanies(sampleCompanies);
  return { repository, db, dbFile };
}

test('Colombo search only returns Colombo rows', () => {
  const { repository, db, dbFile } = createRepositoryFixture();
  const payload = repository.search({
    q: 'engineering and construction companies',
    city: 'Colombo',
    page: 1,
    pageSize: 50,
  });

  assert.ok(payload.results.length > 0);
  assert.ok(payload.results.every((item) => item.city === 'Colombo'));

  db.close();
  fs.unlinkSync(dbFile);
});

test('Horana concrete filter narrows correctly', () => {
  const { repository, db, dbFile } = createRepositoryFixture();
  const payload = repository.search({
    city: 'Horana',
    category: 'concrete',
    page: 1,
    pageSize: 50,
  });

  assert.ok(payload.results.length > 0);
  assert.ok(payload.results.every((item) => item.city === 'Horana'));
  assert.ok(payload.results.every((item) => item.categories.includes('concrete')));

  db.close();
  fs.unlinkSync(dbFile);
});

test('dedupe keeps repeated imports merged', () => {
  const { repository, db, dbFile } = createRepositoryFixture();
  const before = repository.search({ page: 1, pageSize: 100 }).total;

  repository.upsertCompanies([
    {
      name: 'Colombo Engineering Hub',
      address: '12 Union Place, Colombo',
      city: 'Colombo',
      district: 'Colombo',
      phone: '+94 11 200 1000',
      website: 'https://colombo-engineering.example.com',
      rating: 4.9,
      reviewCount: 90,
      categories: ['engineering'],
      source: 'test',
      sourceKey: 'test-colombo-engineering-hub-duplicate',
    },
  ]);

  const after = repository.search({ page: 1, pageSize: 100 }).total;
  assert.equal(after, before);

  db.close();
  fs.unlinkSync(dbFile);
});

test('csv export produces Excel-friendly content', () => {
  const csv = createCsvBuffer([
    {
      name: 'Horana Concrete Yard',
      categories: ['concrete'],
      address: '55 Panadura Road, Horana',
      city: 'Horana',
      district: 'Kalutara',
      phone: '+94 34 220 2200',
      website: 'https://horana-concrete.example.com',
      rating: 4.2,
      reviewCount: 12,
      source: 'test',
      lastUpdated: new Date().toISOString(),
    },
  ]);

  assert.match(csv.toString('utf8'), /Company Name/);
  assert.match(csv.toString('utf8'), /Horana Concrete Yard/);
});
