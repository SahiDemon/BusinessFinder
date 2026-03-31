const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { URL } = require('node:url');
const { loadEnvFile } = require('./env');
const { DirectoryRepository } = require('./repository');
const { ingestFromApprovedSources } = require('./providers');
const { createCsvBuffer } = require('./exporter');

loadEnvFile();

const publicDir = path.join(process.cwd(), 'public');
const repository = new DirectoryRepository();
repository.removeCompaniesBySource('seed').catch(() => {});

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8'
};

function logServer(event, data = {}) {
  console.log(`[directory-server] ${event}`, data);
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      if (chunks.length === 0) {
        resolve({});
        return;
      }

      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

function buildFilters(url) {
  return {
    q: url.searchParams.get('q') || '',
    city: url.searchParams.get('city') || '',
    province: url.searchParams.get('province') || '',
    district: url.searchParams.get('district') || '',
    category: url.searchParams.get('category') || '',
    hasPhone: url.searchParams.get('hasPhone') || '',
    hasWebsite: url.searchParams.get('hasWebsite') || '',
    ratingAvailable: url.searchParams.get('ratingAvailable') || '',
    page: url.searchParams.get('page') || '1',
    pageSize: url.searchParams.get('pageSize') || '10'
  };
}

function sanitizeFilenamePart(value) {
  return String(value || 'all')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 30) || 'all';
}

function shouldUseCachedResults(body, existing) {
  const hasCity = Boolean(String(body.city || '').trim());
  const hasCategory = Boolean(String(body.category || '').trim());
  const isNationwide = !hasCity;

  if (isNationwide) {
    return existing.total >= 500;
  }

  if (hasCategory) {
    return existing.total >= 25;
  }

  return existing.total > 0;
}

async function handleApi(req, res, url) {
  if (req.method === 'GET' && url.pathname === '/api/search') {
    const filters = buildFilters(url);
    logServer('search:start', filters);
    const payload = await repository.search(filters);
    logServer('search:done', { total: payload.total, page: payload.page, pageSize: payload.pageSize });
    sendJson(res, 200, payload);
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/meta') {
    logServer('meta');
    sendJson(res, 200, await repository.getMeta());
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/api/export') {
    const filters = buildFilters(url);
    const payload = await repository.search({
      ...filters,
      page: 1,
      pageSize: 1000
    });
    const csv = createCsvBuffer(payload.results);
    const fileName = `companies-${sanitizeFilenamePart(filters.city)}-${sanitizeFilenamePart(filters.category || filters.q)}.csv`;

    res.writeHead(200, {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${fileName}"`
    });
    res.end(csv);
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/api/ingest/search') {
    try {
      const body = await readBody(req);
      logServer('ingest:start', body);
      if (!String(body.q || '').trim()) {
        sendJson(res, 400, {
          error: 'Query required',
          details: 'Enter a search query before refreshing live sources.'
        });
        return true;
      }

      const existing = await repository.getSearchSummary({
        q: body.q || '',
        city: body.city || '',
        category: body.category || '',
      });

      if (existing.total > 0 && shouldUseCachedResults(body, existing)) {
        logServer('ingest:cache-hit', {
          total: existing.total,
          latestUpdated: existing.latestUpdated,
          q: body.q || '',
          city: body.city || '',
          category: body.category || '',
        });
        sendJson(res, 200, {
          imported: 0,
          fetched: 0,
          cached: true,
          cachedTotal: existing.total,
          latestUpdated: existing.latestUpdated,
          providers: {
            googlePlacesEnabled: Boolean(process.env.GOOGLE_MAPS_API_KEY),
            overpassEnabled: true,
          },
          errors: [],
          nationwide: !body.city,
          message: 'Using saved database results. Live refresh was skipped because data already exists.'
        });
        return true;
      }

      const liveIngest = await ingestFromApprovedSources(body);
      const imported = await repository.upsertCompanies(liveIngest.records);
      logServer('ingest:done', {
        imported,
        fetched: liveIngest.records.length,
        nationwide: liveIngest.nationwide,
        errors: liveIngest.errors,
      });
      sendJson(res, 200, {
        imported,
        fetched: liveIngest.records.length,
        providers: liveIngest.providers,
        errors: liveIngest.errors,
        nationwide: liveIngest.nationwide,
        message: liveIngest.records.length > 0
          ? liveIngest.nationwide
            ? 'Sri Lanka live provider results were refreshed and saved.'
            : 'Live provider results were refreshed and saved.'
          : liveIngest.providers.googlePlacesEnabled
            ? 'No live provider results matched this search yet.'
            : 'No live provider results matched this search yet. Add GOOGLE_MAPS_API_KEY to use Google Places.'
      });
    } catch (error) {
      logServer('ingest:error', { message: error.message });
      sendJson(res, 400, { error: 'Invalid request body', details: error.message });
    }
    return true;
  }

  return false;
}

async function handleApiRequest(req, res, pathnameOverride) {
  const url = new URL(req.url || pathnameOverride || '/', 'http://localhost');
  if (pathnameOverride) {
    url.pathname = pathnameOverride;
  }

  const handled = await handleApi(req, res, url);
  if (!handled) {
    sendJson(res, 404, { error: 'Not found' });
  }
}

function serveStatic(res, url) {
  const requestedPath = url.pathname === '/' ? '/index.html' : url.pathname;
  const safePath = path.normalize(requestedPath).replace(/^(\.\.[/\\])+/, '');
  const filePath = path.join(publicDir, safePath);

  if (!filePath.startsWith(publicDir)) {
    sendJson(res, 403, { error: 'Forbidden' });
    return;
  }

  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    sendJson(res, 404, { error: 'Not found' });
    return;
  }

  res.writeHead(200, {
    'Content-Type': mimeTypes[path.extname(filePath)] || 'application/octet-stream'
  });
  fs.createReadStream(filePath).pipe(res);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  try {
    const handled = await handleApi(req, res, url);
    if (!handled) {
      serveStatic(res, url);
    }
  } catch (error) {
    logServer('request:error', { message: error.message, stack: error.stack });
    sendJson(res, 500, { error: 'Internal server error' });
  }
});

if (require.main === module) {
  const port = Number(process.env.PORT) || 3000;
  server.listen(port, () => {
    console.log(`Sri Lanka directory app running at http://localhost:${port}`);
  });
}

module.exports = {
  server,
  repository,
  handleApiRequest,
};
