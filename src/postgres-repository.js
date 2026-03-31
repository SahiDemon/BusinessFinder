const postgres = require('postgres');

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

class PostgresDirectoryRepository {
  constructor(connectionString) {
    this.sql = postgres(connectionString, {
      ssl: 'require',
      max: 3,
      connect_timeout: 60,
      idle_timeout: 30,
      max_lifetime: 60 * 30,
      prepare: false,
    });
    this.ready = this.initialize();
  }

  async initialize() {
    await this.sql`
      CREATE TABLE IF NOT EXISTS locations (
        id SERIAL PRIMARY KEY,
        city TEXT NOT NULL,
        province TEXT NOT NULL DEFAULT '',
        district TEXT NOT NULL,
        country TEXT NOT NULL DEFAULT 'Sri Lanka',
        UNIQUE(city, district, country)
      );
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS companies (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        normalized_name TEXT NOT NULL,
        address TEXT NOT NULL,
        location_id INTEGER NOT NULL REFERENCES locations(id),
        phone TEXT,
        website TEXT,
        rating DOUBLE PRECISION,
        review_count INTEGER,
        source TEXT NOT NULL,
        last_updated TEXT NOT NULL,
        dedupe_key TEXT NOT NULL UNIQUE
      );
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE
      );
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS company_categories (
        company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
        category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
        PRIMARY KEY (company_id, category_id)
      );
    `;
    await this.sql`
      CREATE TABLE IF NOT EXISTS source_records (
        id SERIAL PRIMARY KEY,
        company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
        source_name TEXT NOT NULL,
        source_key TEXT NOT NULL UNIQUE,
        raw_payload TEXT NOT NULL,
        imported_at TEXT NOT NULL
      );
    `;
  }

  async ensureLocation(tx, city, province = '', district, country = 'Sri Lanka') {
    const exact = await tx`
      SELECT id FROM locations
      WHERE city = ${city} AND province = ${province} AND district = ${district} AND country = ${country}
      LIMIT 1
    `;
    if (exact.length) return exact[0].id;

    const legacy = await tx`
      SELECT id, province FROM locations
      WHERE city = ${city} AND district = ${district} AND country = ${country}
      LIMIT 1
    `;
    if (legacy.length) {
      if (!legacy[0].province && province) {
        await tx`UPDATE locations SET province = ${province} WHERE id = ${legacy[0].id}`;
      }
      return legacy[0].id;
    }

    const inserted = await tx`
      INSERT INTO locations (city, province, district, country)
      VALUES (${city}, ${province}, ${district}, ${country})
      RETURNING id
    `;
    return inserted[0].id;
  }

  async ensureCategory(tx, name) {
    const normalized = normalizeText(name);
    const existing = await tx`SELECT id FROM categories WHERE name = ${normalized} LIMIT 1`;
    if (existing.length) return existing[0].id;
    const inserted = await tx`INSERT INTO categories (name) VALUES (${normalized}) RETURNING id`;
    return inserted[0].id;
  }

  async upsertCompanies(companies) {
    await this.ready;
    await this.sql.begin(async (tx) => {
      for (const company of companies) {
        const locationId = await this.ensureLocation(
          tx,
          company.city,
          company.province || '',
          company.district,
          company.country || 'Sri Lanka'
        );
        const dedupeKey = createDedupeKey(company);
        const existing = await tx`SELECT id FROM companies WHERE dedupe_key = ${dedupeKey} LIMIT 1`;
        const normalizedName = normalizeText(company.name);
        const source = company.source || 'seed';
        const now = company.lastUpdated || new Date().toISOString();
        let companyId;

        if (existing.length) {
          companyId = existing[0].id;
          await tx`
            UPDATE companies
            SET name = ${company.name},
                normalized_name = ${normalizedName},
                address = ${company.address},
                location_id = ${locationId},
                phone = ${company.phone || null},
                website = ${company.website || null},
                rating = ${company.rating ?? null},
                review_count = ${company.reviewCount ?? null},
                source = ${source},
                last_updated = ${now},
                dedupe_key = ${dedupeKey}
            WHERE id = ${companyId}
          `;
          await tx`DELETE FROM company_categories WHERE company_id = ${companyId}`;
        } else {
          const inserted = await tx`
            INSERT INTO companies (
              name, normalized_name, address, location_id, phone, website, rating,
              review_count, source, last_updated, dedupe_key
            ) VALUES (
              ${company.name}, ${normalizedName}, ${company.address}, ${locationId},
              ${company.phone || null}, ${company.website || null}, ${company.rating ?? null},
              ${company.reviewCount ?? null}, ${source}, ${now}, ${dedupeKey}
            )
            RETURNING id
          `;
          companyId = inserted[0].id;
        }

        for (const category of company.categories || []) {
          const categoryId = await this.ensureCategory(tx, category);
          await tx`
            INSERT INTO company_categories (company_id, category_id)
            VALUES (${companyId}, ${categoryId})
            ON CONFLICT DO NOTHING
          `;
        }

        await tx`
          INSERT INTO source_records (company_id, source_name, source_key, raw_payload, imported_at)
          VALUES (
            ${companyId},
            ${source},
            ${company.sourceKey || `${source}:${dedupeKey}`},
            ${JSON.stringify(company)},
            ${now}
          )
          ON CONFLICT (source_key)
          DO UPDATE SET
            company_id = EXCLUDED.company_id,
            source_name = EXCLUDED.source_name,
            raw_payload = EXCLUDED.raw_payload,
            imported_at = EXCLUDED.imported_at
        `;
      }
    });
    return companies.length;
  }

  async removeCompaniesBySource(source) {
    await this.ready;
    await this.sql`DELETE FROM companies WHERE source = ${source}`;
  }

  buildWhere(filters = {}) {
    const conditions = [];
    const params = [];

    if (filters.q) {
      const tokens = tokenizeSearch(filters.q);
      const tokenConditions = [];
      for (const token of tokens) {
        tokenConditions.push(`(
          LOWER(c.name) LIKE $${params.length + 1}
          OR LOWER(c.address) LIKE $${params.length + 2}
          OR EXISTS (
            SELECT 1
            FROM company_categories cc2
            JOIN categories cat2 ON cat2.id = cc2.category_id
            WHERE cc2.company_id = c.id
              AND cat2.name LIKE $${params.length + 3}
          )
        )`);
        const like = `%${token}%`;
        params.push(like, like, like);
      }
      if (tokenConditions.length) conditions.push(`(${tokenConditions.join(' OR ')})`);
    }
    if (filters.city) {
      conditions.push(`LOWER(l.city) = $${params.length + 1}`);
      params.push(String(filters.city).trim().toLowerCase());
    }
    if (filters.district) {
      conditions.push(`LOWER(l.district) = $${params.length + 1}`);
      params.push(String(filters.district).trim().toLowerCase());
    }
    if (filters.province) {
      conditions.push(`LOWER(l.province) = $${params.length + 1}`);
      params.push(String(filters.province).trim().toLowerCase());
    }
    if (filters.category) {
      conditions.push(`EXISTS (
        SELECT 1
        FROM company_categories cc3
        JOIN categories cat3 ON cat3.id = cc3.category_id
        WHERE cc3.company_id = c.id
          AND cat3.name LIKE $${params.length + 1}
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
    await this.ready;
    const page = Math.max(Number(filters.page) || 1, 1);
    const pageSize = Math.min(Math.max(Number(filters.pageSize) || 10, 1), 100);
    const offset = (page - 1) * pageSize;
    const { whereClause, params } = this.buildWhere(filters);

    const rows = await this.sql.unsafe(`
      SELECT
        c.id, c.name, c.address, l.city, l.province, l.district, c.phone, c.website,
        c.rating, c.review_count AS "reviewCount", c.source, c.last_updated AS "lastUpdated",
        COALESCE(
          (
            SELECT STRING_AGG(cat.name, '|')
            FROM company_categories cc
            JOIN categories cat ON cat.id = cc.category_id
            WHERE cc.company_id = c.id
          ),
          ''
        ) AS categories
      FROM companies c
      JOIN locations l ON l.id = c.location_id
      ${whereClause}
      ORDER BY CASE WHEN c.rating IS NULL THEN 1 ELSE 0 END, c.rating DESC, c.name ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `, params);

    const totalRows = await this.sql.unsafe(`
      SELECT COUNT(*)::int AS total
      FROM companies c
      JOIN locations l ON l.id = c.location_id
      ${whereClause}
    `, params);

    return {
      page,
      pageSize,
      total: totalRows[0]?.total || 0,
      results: rows.map((row) => ({
        ...row,
        categories: row.categories ? row.categories.split('|').filter(Boolean) : [],
      })),
    };
  }

  async getSearchSummary(filters = {}) {
    await this.ready;
    const { whereClause, params } = this.buildWhere(filters);
    const rows = await this.sql.unsafe(`
      SELECT COUNT(*)::int AS total, MAX(c.last_updated) AS "latestUpdated"
      FROM companies c
      JOIN locations l ON l.id = c.location_id
      ${whereClause}
    `, params);
    return rows[0] || { total: 0, latestUpdated: null };
  }

  async getMeta() {
    await this.ready;
    const [districts, provinces, cities, categories] = await Promise.all([
      this.sql`SELECT DISTINCT district FROM locations ORDER BY district ASC`,
      this.sql`SELECT DISTINCT province FROM locations WHERE TRIM(province) <> '' ORDER BY province ASC`,
      this.sql`SELECT DISTINCT city FROM locations ORDER BY city ASC`,
      this.sql`SELECT name FROM categories ORDER BY name ASC`,
    ]);

    return {
      cities: cities.map((row) => row.city),
      districts: districts.map((row) => row.district),
      provinces: provinces.map((row) => row.province),
      categories: categories.map((row) => row.name),
    };
  }
}

module.exports = {
  PostgresDirectoryRepository,
  normalizeText,
  createDedupeKey,
};
