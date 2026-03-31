const GOOGLE_PLACES_URL = 'https://places.googleapis.com/v1/places:searchText';
const OVERPASS_URL = 'https://overpass-api.de/api/interpreter';

const SRI_LANKA_SCOPES = [
  { city: 'Colombo', district: 'Colombo', province: 'Western Province' },
  { city: 'Dehiwala-Mount Lavinia', district: 'Colombo', province: 'Western Province' },
  { city: 'Sri Jayawardenepura Kotte', district: 'Colombo', province: 'Western Province' },
  { city: 'Moratuwa', district: 'Colombo', province: 'Western Province' },
  { city: 'Negombo', district: 'Gampaha', province: 'Western Province' },
  { city: 'Gampaha', district: 'Gampaha', province: 'Western Province' },
  { city: 'Kalutara', district: 'Kalutara', province: 'Western Province' },
  { city: 'Horana', district: 'Kalutara', province: 'Western Province' },
  { city: 'Kandy', district: 'Kandy', province: 'Central Province' },
  { city: 'Matale', district: 'Matale', province: 'Central Province' },
  { city: 'Nuwara Eliya', district: 'Nuwara Eliya', province: 'Central Province' },
  { city: 'Galle', district: 'Galle', province: 'Southern Province' },
  { city: 'Matara', district: 'Matara', province: 'Southern Province' },
  { city: 'Hambantota', district: 'Hambantota', province: 'Southern Province' },
  { city: 'Jaffna', district: 'Jaffna', province: 'Northern Province' },
  { city: 'Kilinochchi', district: 'Kilinochchi', province: 'Northern Province' },
  { city: 'Vavuniya', district: 'Vavuniya', province: 'Northern Province' },
  { city: 'Trincomalee', district: 'Trincomalee', province: 'Eastern Province' },
  { city: 'Batticaloa', district: 'Batticaloa', province: 'Eastern Province' },
  { city: 'Ampara', district: 'Ampara', province: 'Eastern Province' },
  { city: 'Kurunegala', district: 'Kurunegala', province: 'North Western Province' },
  { city: 'Puttalam', district: 'Puttalam', province: 'North Western Province' },
  { city: 'Anuradhapura', district: 'Anuradhapura', province: 'North Central Province' },
  { city: 'Polonnaruwa', district: 'Polonnaruwa', province: 'North Central Province' },
  { city: 'Badulla', district: 'Badulla', province: 'Uva Province' },
  { city: 'Monaragala', district: 'Monaragala', province: 'Uva Province' },
  { city: 'Ratnapura', district: 'Ratnapura', province: 'Sabaragamuwa Province' },
  { city: 'Kegalle', district: 'Kegalle', province: 'Sabaragamuwa Province' },
];

function compact(values) {
  return values.filter(Boolean).map((value) => String(value).trim()).filter(Boolean);
}

function logProvider(event, data = {}) {
  console.log(`[directory-provider] ${event}`, data);
}

function uniqueStrings(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function normalizeCategoryText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[_-]+/g, ' ')
    .trim();
}

function normalizeKeyword(value) {
  return normalizeCategoryText(value).replace(/[^a-z0-9 ]+/g, ' ').trim();
}

function buildGoogleFieldMask() {
  return [
    'places.id',
    'places.displayName',
    'places.formattedAddress',
    'places.addressComponents',
    'places.nationalPhoneNumber',
    'places.websiteUri',
    'places.googleMapsUri',
    'places.rating',
    'places.userRatingCount',
    'places.primaryType',
    'places.primaryTypeDisplayName',
    'nextPageToken',
  ].join(',');
}

function buildCategoryVariants(q, category) {
  const base = normalizeKeyword(compact([q, category]).join(' '));
  const variants = [];

  if (base.includes('engineering') || base.includes('construction')) {
    variants.push('engineering companies');
    variants.push('construction companies');
    variants.push('general contractors');
    variants.push('civil engineering companies');
  }
  if (base.includes('concrete')) {
    variants.push('concrete companies');
    variants.push('ready mix concrete');
    variants.push('concrete suppliers');
  }
  if (!variants.length) {
    variants.push(base || 'engineering companies');
    variants.push(category ? `${category} companies` : 'construction companies');
  }

  if (!base.includes('concrete')) {
    variants.push(base || 'engineering and construction companies');
  }

  return uniqueStrings(variants.map((item) => item.trim()).filter(Boolean)).slice(0, 4);
}

function buildSearchScopes(city) {
  if (city) {
    return [{ city, district: city, province: '' }];
  }

  return SRI_LANKA_SCOPES;
}

function extractFromAddressComponents(components = [], type) {
  const match = components.find((component) => Array.isArray(component.types) && component.types.includes(type));
  return match ? componentText(match) : '';
}

function componentText(component) {
  return component.longText || component.shortText || '';
}

function parseGooglePlace(place, scope = {}) {
  const displayName = place.displayName && place.displayName.text ? place.displayName.text : '';
  const city =
    extractFromAddressComponents(place.addressComponents, 'locality') ||
    extractFromAddressComponents(place.addressComponents, 'postal_town') ||
    scope.city ||
    '';
  const district =
    extractFromAddressComponents(place.addressComponents, 'administrative_area_level_2') ||
    scope.district ||
    '';
  const province =
    extractFromAddressComponents(place.addressComponents, 'administrative_area_level_1') ||
    scope.province ||
    '';
  const primaryType = normalizeCategoryText(place.primaryTypeDisplayName && place.primaryTypeDisplayName.text);
  const fallbackType = normalizeCategoryText(place.primaryType);

  return {
    name: displayName,
    address: place.formattedAddress || '',
    city: city || 'Sri Lanka',
    province: province || '',
    district: district || city || 'Sri Lanka',
    phone: place.nationalPhoneNumber || '',
    website: place.websiteUri || place.googleMapsUri || '',
    mapsUrl: place.googleMapsUri || '',
    rating: place.rating ?? null,
    reviewCount: place.userRatingCount ?? null,
    categories: compact([primaryType, fallbackType]),
    source: 'google-places',
    sourceKey: `google-places:${place.id}`,
    lastUpdated: new Date().toISOString(),
  };
}

function getRequiredTerms(q, category) {
  return compact([q, category]).flatMap((part) =>
    String(part)
      .toLowerCase()
      .split(/\s+/)
      .filter((token) =>
        token &&
        ![
          'and',
          'in',
          'companies',
          'company',
          'the',
          'services',
          'service',
          'lanka',
          'sri',
          'pvt',
          'ltd',
        ].includes(token)
      )
  );
}

function matchesTextFilter(record, { q = '', city = '', category = '' }) {
  const haystack = compact([
    record.name,
    record.address,
    record.city,
    record.province,
    record.district,
    ...(record.categories || []),
  ]).join(' ').toLowerCase();

  const requiredTerms = getRequiredTerms(q, category);
  const synonymGroups = {
    engineering: ['engineering', 'engineer'],
    construction: ['construction', 'contractor', 'general contractor', 'builder', 'builders'],
    concrete: ['concrete', 'ready mix', 'cement'],
    fabrication: ['fabrication', 'steel', 'manufacturer'],
    civil: ['civil', 'contractor', 'engineering'],
  };

  const matchedTerms = requiredTerms.filter((term) => {
    const synonyms = synonymGroups[term] || [term];
    return synonyms.some((synonym) => haystack.includes(synonym));
  });

  const cityText = String(city || '').toLowerCase();
  const cityMatch =
    !cityText ||
    record.city.toLowerCase().includes(cityText) ||
    record.district.toLowerCase().includes(cityText) ||
    record.province.toLowerCase().includes(cityText);
  const minimumMatches = requiredTerms.length ? Math.max(1, Math.ceil(requiredTerms.length / 2)) : 0;
  const textMatch = !requiredTerms.length || matchedTerms.length >= minimumMatches;
  return cityMatch && textMatch;
}

async function fetchGooglePage({ apiKey, textQuery, pageToken = '' }) {
  logProvider('google-page:start', { textQuery, hasPageToken: Boolean(pageToken) });
  const response = await fetch(GOOGLE_PLACES_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': apiKey,
      'X-Goog-FieldMask': buildGoogleFieldMask(),
    },
    body: JSON.stringify({
      textQuery,
      languageCode: 'en',
      regionCode: 'LK',
      pageSize: 20,
      pageToken: pageToken || undefined,
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    logProvider('google-page:error', { textQuery, status: response.status, message });
    throw new Error(`Google Places request failed with ${response.status}: ${message}`);
  }

  const payload = await response.json();
  logProvider('google-page:done', {
    textQuery,
    places: Array.isArray(payload.places) ? payload.places.length : 0,
    hasNextPageToken: Boolean(payload.nextPageToken),
  });
  return payload;
}

async function fetchGooglePlaces({ q = '', city = '', category = '' }) {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  if (!apiKey) {
    return [];
  }

  const recordsByKey = new Map();
  const scopes = buildSearchScopes(city);
  const searchVariants = buildCategoryVariants(q, category);
  const queryPairs = [];

  for (const scope of scopes) {
    for (const variant of searchVariants) {
      queryPairs.push({
        scope,
        textQuery: compact([variant, scope.city, scope.district, 'Sri Lanka']).join(', '),
      });
    }
  }

  const limitedPairs = queryPairs.slice(0, city ? 12 : 80);
  logProvider('google-search:start', {
    city,
    category,
    q,
    scopeCount: scopes.length,
    variantCount: searchVariants.length,
    queryCount: limitedPairs.length,
  });
  for (const pair of limitedPairs) {
    let nextPageToken = '';
    for (let pageNumber = 0; pageNumber < 2; pageNumber += 1) {
      const payload = await fetchGooglePage({
        apiKey,
        textQuery: pair.textQuery,
        pageToken: nextPageToken,
      });

      for (const place of payload.places || []) {
        const parsed = parseGooglePlace(place, pair.scope);
        if (parsed.name && parsed.address) {
          recordsByKey.set(parsed.sourceKey, parsed);
        }
      }

      if (!payload.nextPageToken) {
        break;
      }

      nextPageToken = payload.nextPageToken;
    }
  }

  const filtered = Array.from(recordsByKey.values()).filter((record) =>
    matchesTextFilter(record, { q, city, category })
  );
  logProvider('google-search:done', { raw: recordsByKey.size, filtered: filtered.length });
  return filtered;
}

function escapeOverpassRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildOverpassQuery({ q = '', city = '', category = '' }) {
  const textBits = compact([q, category]);
  const textPattern = textBits.length
    ? textBits.map(escapeOverpassRegex).join('|')
    : 'engineering|construction|concrete|contractor|builder|fabrication|ready mix';
  const areaBlock = city
    ? `area["name"="Sri Lanka"]["boundary"="administrative"]->.country;
area(area.country)["name"="${String(city).replace(/"/g, '\\"')}"]["boundary"="administrative"]->.searchArea;`
    : `area["name"="Sri Lanka"]["boundary"="administrative"]->.searchArea;`;

  return `
[out:json][timeout:25];
${areaBlock}
(
  nwr(area.searchArea)["name"~"${textPattern}",i];
  nwr(area.searchArea)["office"~"company|engineer|construction",i];
  nwr(area.searchArea)["craft"~"builder|carpenter|electrician|plumber|welder",i];
  nwr(area.searchArea)["industrial"~"concrete|construction|steel|engineering",i];
);
out center tags;
  `.trim();
}

function parseOverpassCategories(tags = {}, queryCategory = '') {
  const values = compact([
    normalizeCategoryText(queryCategory),
    normalizeCategoryText(tags.office),
    normalizeCategoryText(tags.craft),
    normalizeCategoryText(tags.industrial),
    normalizeCategoryText(tags.shop),
    normalizeCategoryText(tags['company:type']),
  ]);

  const categories = new Set(values);
  const name = normalizeCategoryText(tags.name);
  for (const keyword of ['engineering', 'construction', 'concrete', 'contractor', 'builder', 'fabrication']) {
    if (name.includes(keyword)) {
      categories.add(keyword);
    }
  }

  return Array.from(categories);
}

function parseOverpassAddress(tags = {}) {
  const street = compact([tags['addr:housenumber'], tags['addr:street']]).join(' ');
  const locality = compact([tags['addr:city'], tags['addr:suburb'], tags['addr:place']]).join(', ');
  return compact([street, locality]).join(', ');
}

function parseOverpassElement(element, city, category) {
  const tags = element.tags || {};
  const parsedCity = tags['addr:city'] || tags['is_in:city'] || city || '';
  const parsedProvince = tags['addr:state'] || tags['is_in:state'] || tags['addr:province'] || '';
  const parsedDistrict = tags['addr:district'] || parsedCity || '';

  return {
    name: tags.name || '',
    address: parseOverpassAddress(tags) || `${parsedCity || 'Sri Lanka'}`,
    city: parsedCity || 'Sri Lanka',
    province: parsedProvince || '',
    district: parsedDistrict || 'Sri Lanka',
    phone: tags.phone || tags['contact:phone'] || '',
    website: tags.website || tags['contact:website'] || '',
    mapsUrl: '',
    rating: null,
    reviewCount: null,
    categories: parseOverpassCategories(tags, category),
    source: 'openstreetmap-overpass',
    sourceKey: `overpass:${element.type}:${element.id}`,
    lastUpdated: new Date().toISOString(),
  };
}

async function fetchOverpass({ q = '', city = '', category = '' }) {
  logProvider('overpass:start', { q, city, category });
  const response = await fetch(OVERPASS_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
    },
    body: buildOverpassQuery({ q, city, category }),
  });

  if (!response.ok) {
    logProvider('overpass:error', { status: response.status });
    throw new Error(`Overpass request failed with ${response.status}`);
  }

  const payload = await response.json();
  const filtered = (Array.isArray(payload.elements) ? payload.elements : [])
    .map((element) => parseOverpassElement(element, city, category))
    .filter((record) => record.name)
    .filter((record) => matchesTextFilter(record, { q, city, category }));
  logProvider('overpass:done', {
    raw: Array.isArray(payload.elements) ? payload.elements.length : 0,
    filtered: filtered.length,
  });
  return filtered;
}

async function ingestFromApprovedSources({ q = '', city = '', category = '' }) {
  logProvider('ingest:start', { q, city, category });
  const errors = [];
  const providerResults = await Promise.allSettled([
    fetchGooglePlaces({ q, city, category }),
    fetchOverpass({ q, city, category }),
  ]);

  const recordsByKey = new Map();
  for (const result of providerResults) {
    if (result.status === 'fulfilled') {
      for (const record of result.value) {
        recordsByKey.set(record.sourceKey, record);
      }
    } else {
      errors.push(result.reason.message);
    }
  }

  return {
    records: Array.from(recordsByKey.values()),
    providers: {
      googlePlacesEnabled: Boolean(process.env.GOOGLE_MAPS_API_KEY),
      overpassEnabled: true,
    },
    errors,
    nationwide: !city,
  };
}

module.exports = {
  ingestFromApprovedSources,
};
