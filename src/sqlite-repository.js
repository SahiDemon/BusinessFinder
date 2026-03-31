const fs = require('node:fs');
const { openDatabase } = require('./db');

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function createDedupeKey(company) {
  const phone = normalizeText(company.phone || '');
  const website = normalizeText(company.website || '');
  const address = normalizeText(company.address || '');
  return [normalizeText(company.name), phone || website || address].join('|');
}

function tokenizeSearch(value) {
  return normalizeText(value)
    .split(' ')
    .filter((token) => token && !['and', 'in', 'companies', 'company', 'the'].includes(token));
}

class SQLiteDirectoryRepository {
  constructor(database = openDatabase()) {
    this.db = database;
    this.ready = Promise.resolve();
    this.statements = {
      findLocation: this.db.prepare(
        'SELECT id FROM locations WHERE city = ? AND province = ? AND district = ? AND country = ?'
      ),
      findLocationLegacy: this.db.prepare(
        'SELECT id, province FROM locations WHERE city = ? AND district = ? AND country = ?'
      ),
      insertLocation: this.db.prepare(
        'INSERT INTO locations (city, province, district, country) VALUES (?, ?, ?, ?)'
      ),
      updateLocationProvince: this.db.prepare(
        'UPDATE locations SET province = ? WHERE id = ?'
      ),
      findCompanyByDedupeKey: this.db.prepare('SELECT id FROM companies WHERE dedupe_key = ?'),
      insertCompany: this.db.prepare(`
        INSERT INTO companies (
          name, normalized_name, address, location_id, phone, website, rating,
          review_count, source, last_updated, dedupe_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      updateCompany: this.db.prepare(`
        UPDATE companies
        SET name = ?,
            normalized_name = ?,
            address = ?,
            location_id = ?,
            phone = ?,
            website = ?,
            rating = ?,
            review_count = ?,
            source = ?,
            last_updated = ?,
            dedupe_key = ?
        WHERE id = ?
      `),
      findCategory: this.db.prepare('SELECT id FROM categories WHERE name = ?'),
      insertCategory: this.db.prepare('INSERT INTO categories (name) VALUES (?)'),
      linkCategory: this.db.prepare(
        'INSERT OR IGNORE INTO company_categories (company_id, category_id) VALUES (?, ?)'
      ),
      deleteCompanyCategories: this.db.prepare(
        'DELETE FROM company_categories WHERE company_id = ?'
      ),
      upsertSourceRecord: this.db.prepare(`
        INSERT INTO source_records (company_id, source_name, source_key, raw_payload, imported_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_key)
        DO UPDATE SET
          company_id = excluded.company_id,
          source_name = excluded.source_name,
          raw_payload = excluded.raw_payload,
          imported_at = excluded.imported_at
      `),
      deleteCompaniesBySource: this.db.prepare('DELETE FROM companies WHERE source = ?'),
      countCompanies: this.db.prepare('SELECT COUNT(*) AS total FROM companies'),
    };
  }

  ensureLocation(city, province = '', district, country = 'Sri Lanka') {
    const existing = this.statements.findLocation.get(city, province, district, country);
    if (existing) return existing.id;

    const legacy = this.statements.findLocationLegacy.get(city, district, country);
    if (legacy) {
      if (!legacy.province && province) {
        this.statements.updateLocationProvince.run(province, legacy.id);
      }
      return legacy.id;
    }

    const result = this.statements.insertLocation.run(city, province, district, country);
    return Number(result.lastInsertRowid);
  }

  ensureCategory(name) {
    const normalized = normalizeText(name);
    const existing = this.statements.findCategory.get(normalized);
    if (existing) return existing.id;
    const result = this.statements.insertCategory.run(normalized);
    return Number(result.lastInsertRowid);
  }

  async upsertCompanies(companies) {
    try {
      this.db.exec('BEGIN');
      for (const company of companies) {
        const locationId = this.ensureLocation(
          company.city,
          company.province || '',
          company.district,
          company.country || 'Sri Lanka'
        );
        const dedupeKey = createDedupeKey(company);
        const existing = this.statements.findCompanyByDedupeKey.get(dedupeKey);
        const normalizedName = normalizeText(company.name);
        const source = company.source || 'seed';
        const now = company.lastUpdated || new Date().toISOString();
        let companyId;

        if (existing) {
          this.statements.updateCompany.run(
            company.name,
            normalizedName,
            company.address,
            locationId,
            company.phone || null,
            company.website || null,
            company.rating ?? null,
            company.reviewCount ?? null,
            source,
            now,
            dedupeKey,
            existing.id
          );
          companyId = existing.id;
          this.statements.deleteCompanyCategories.run(companyId);
        } else {
          const inserted = this.statements.insertCompany.run(
            company.name,
            normalizedName,
            company.address,
            locationId,
            company.phone || null,
            company.website || null,
            company.rating ?? null,
            company.reviewCount ?? null,
            source,
            now,
            dedupeKey
          );
          companyId = Number(inserted.lastInsertRowid);
        }

        for (const category of company.categories || []) {
          const categoryId = this.ensureCategory(category);
          this.statements.linkCategory.run(companyId, categoryId);
        }

        this.statements.upsertSourceRecord.run(
          companyId,
          source,
          company.sourceKey || `${source}:${dedupeKey}`,
          JSON.stringify(company),
          now
        );
      }
      this.db.exec('COMMIT');
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
    return companies.length;
  }

  async removeCompaniesBySource(source) {
    this.statements.deleteCompaniesBySource.run(source);
  }

  buildWhere(filters = {}) {
    const conditions = [];
    const params = [];

    if (filters.q) {
      const tokens = tokenizeSearch(filters.q);
      const tokenConditions = [];
      for (const token of tokens) {
        const like = `%${token}%`;
        tokenConditions.push(`(
          LOWER(c.name) LIKE ?
          OR LOWER(c.address) LIKE ?
          OR EXISTS (
            SELECT 1
            FROM company_categories cc2
            JOIN categories cat2 ON cat2.id = cc2.category_id
            WHERE cc2.company_id = c.id
              AND cat2.name LIKE ?
          )
        )`);
        params.push(like, like, like);
      }
      if (tokenConditions.length > 0) conditions.push(`(${tokenConditions.join(' OR ')})`);
    }

    if (filters.city) {
      conditions.push('LOWER(l.city) = ?');
      params.push(String(filters.city).trim().toLowerCase());
    }
    if (filters.district) {
      conditions.push('LOWER(l.district) = ?');
      params.push(String(filters.district).trim().toLowerCase());
    }
    if (filters.province) {
      conditions.push('LOWER(l.province) = ?');
      params.push(String(filters.province).trim().toLowerCase());
    }
    if (filters.category) {
      conditions.push(`EXISTS (
        SELECT 1
        FROM company_categories cc3
        JOIN categories cat3 ON cat3.id = cc3.category_id
        WHERE cc3.company_id = c.id
          AND cat3.name LIKE ?
      )`);
      params.push(`%${String(filters.category).trim().toLowerCase()}%`);
    }
    if (filters.hasPhone === 'true') conditions.push("c.phone IS NOT NULL AND TRIM(c.phone) <> ''");
    if (filters.hasWebsite === 'true') conditions.push("c.website IS NOT NULL AND TRIM(c.website) <> ''");
    if (filters.ratingAvailable === 'true') conditions.push('c.rating IS NOT NULL');

    return {
      whereClause: conditions.length ? `WHERE ${conditions.join(' AND ')}` : '',
      params,
    };
  }

  async search(filters = {}) {
    const page = Math.max(Number(filters.page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(filters.pageSize) || 10, 1), 100);
    const offset = (page - 1) * pageSize;
    const { whereClause, params } = this.buildWhere(filters);
    const baseQuery = `FROM companies c JOIN locations l ON l.id = c.location_id ${whereClause}`;

    const rows = this.db.prepare(`
      SELECT
        c.id, c.name, c.address, l.city, l.province, l.district, c.phone, c.website,
        c.rating, c.review_count AS reviewCount, c.source, c.last_updated AS lastUpdated,
        (
          SELECT GROUP_CONCAT(cat.name, '|')
          FROM company_categories cc
          JOIN categories cat ON cat.id = cc.category_id
          WHERE cc.company_id = c.id
          ORDER BY cat.name
        ) AS categories
      ${baseQuery}
      ORDER BY CASE WHEN c.rating IS NULL THEN 1 ELSE 0 END, c.rating DESC, c.name ASC
      LIMIT ? OFFSET ?
    `).all(...params, pageSize, offset).map((row) => ({
      ...row,
      categories: row.categories ? row.categories.split('|') : [],
    }));

    const totalRow = this.db.prepare(`SELECT COUNT(*) AS total ${baseQuery}`).get(...params);
    return { page, pageSize, total: totalRow.total, results: rows };
  }

  async getSearchSummary(filters = {}) {
    const { whereClause, params } = this.buildWhere(filters);
    return this.db.prepare(`
      SELECT COUNT(*) AS total, MAX(c.last_updated) AS latestUpdated
      FROM companies c
      JOIN locations l ON l.id = c.location_id
      ${whereClause}
    `).get(...params);
  }

  async getMeta() {
    const districts = this.db.prepare('SELECT DISTINCT district FROM locations ORDER BY district ASC').all().map((row) => row.district);
    const provinces = this.db.prepare("SELECT DISTINCT province FROM locations WHERE TRIM(province) <> '' ORDER BY province ASC").all().map((row) => row.province);
    const cities = this.db.prepare('SELECT DISTINCT city FROM locations ORDER BY city ASC').all().map((row) => row.city);
    const categories = this.db.prepare('SELECT name FROM categories ORDER BY name ASC').all().map((row) => row.name);
    return { cities, districts, provinces, categories };
  }
}

module.exports = {
  SQLiteDirectoryRepository,
  normalizeText,
  createDedupeKey,
};
