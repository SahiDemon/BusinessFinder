const state = {
  page: 1,
  pageSize: 9,
  total: 0,
  liveRefreshing: false,
};

const elements = {
  searchForm: document.getElementById('searchForm'),
  q: document.getElementById('q'),
  city: document.getElementById('city'),
  category: document.getElementById('category'),
  province: document.getElementById('province'),
  district: document.getElementById('district'),
  categoryQuick: document.getElementById('categoryQuick'),
  hasPhone: document.getElementById('hasPhone'),
  hasWebsite: document.getElementById('hasWebsite'),
  ratingAvailable: document.getElementById('ratingAvailable'),
  exportButton: document.getElementById('exportButton'),
  refreshButton: document.getElementById('refreshButton'),
  resetFilters: document.getElementById('resetFilters'),
  prevPage: document.getElementById('prevPage'),
  nextPage: document.getElementById('nextPage'),
  pageIndicator: document.getElementById('pageIndicator'),
  resultsGrid: document.getElementById('resultsGrid'),
  resultSummary: document.getElementById('resultSummary'),
  message: document.getElementById('message'),
  cardTemplate: document.getElementById('companyCardTemplate'),
};

function logClient(event, data = {}) {
  console.log(`[directory-ui] ${event}`, data);
}

function setLiveRefreshing(isRefreshing, detailText = '') {
  state.liveRefreshing = isRefreshing;
  elements.refreshButton.disabled = isRefreshing;
  elements.exportButton.disabled = isRefreshing;
  elements.searchForm.querySelector('button[type="submit"]').disabled = isRefreshing;
  if (isRefreshing) {
    showStatusMessage(detailText || 'Searching live sources...', 'loading');
  }
}

function buildQueryParams() {
  const category = elements.categoryQuick.value || elements.category.value.trim();
  return new URLSearchParams({
    q: elements.q.value.trim(),
    city: elements.city.value.trim(),
    province: elements.province.value,
    district: elements.district.value,
    category,
    hasPhone: String(elements.hasPhone.checked),
    hasWebsite: String(elements.hasWebsite.checked),
    ratingAvailable: String(elements.ratingAvailable.checked),
    page: String(state.page),
    pageSize: String(state.pageSize),
  });
}

function showMessage(text) {
  elements.message.hidden = !text;
  elements.message.textContent = text || '';
  elements.message.className = 'message';
}

function showStatusMessage(text, tone = 'neutral') {
  showMessage(text);
  elements.message.classList.add(`message--${tone}`);
}

function renderResults(payload) {
  state.total = payload.total;
  elements.resultsGrid.innerHTML = '';
  elements.pageIndicator.textContent = `Page ${payload.page}`;
  elements.resultSummary.textContent = `${payload.total} companies found`;
  elements.prevPage.disabled = payload.page <= 1;
  elements.nextPage.disabled = payload.page * payload.pageSize >= payload.total;

  if (payload.results.length === 0) {
    elements.resultsGrid.innerHTML =
      '<div class="company-card"><h3>No results found</h3><p>Try another city, category, or refresh approved sources.</p></div>';
    return;
  }

  for (const company of payload.results) {
    const fragment = elements.cardTemplate.content.cloneNode(true);
    fragment.querySelector('.company-card__source').textContent = `Source: ${company.source}`;
    fragment.querySelector('.company-card__title').textContent = company.name;
    fragment.querySelector('.company-card__rating').textContent =
      company.rating != null ? `${company.rating} / 5` : 'No rating';
    fragment.querySelector('.company-card__categories').textContent = company.categories.join(', ');
    fragment.querySelector('.company-card__address').textContent = company.address;
    fragment.querySelector('.company-card__meta').textContent =
      `${company.city}, ${company.district}${company.province ? `, ${company.province}` : ''} • ${company.reviewCount ?? 0} reviews`;

    const actions = fragment.querySelector('.company-card__actions');
    if (company.phone) {
      const phone = document.createElement('a');
      phone.href = `tel:${company.phone.replace(/\s+/g, '')}`;
      phone.textContent = company.phone;
      actions.appendChild(phone);
    }
    if (company.website) {
      const site = document.createElement('a');
      site.href = company.website;
      site.target = '_blank';
      site.rel = 'noreferrer';
      site.textContent = 'Website';
      actions.appendChild(site);
    }

    fragment.querySelector('.company-card__updated').textContent =
      `Updated ${new Date(company.lastUpdated).toLocaleString()}`;
    elements.resultsGrid.appendChild(fragment);
  }
}

async function loadMeta() {
  logClient('load-meta:start');
  const response = await fetch('/api/meta');
  const payload = await response.json();
  logClient('load-meta:done', payload);

  for (const province of payload.provinces || []) {
    const option = document.createElement('option');
    option.value = province;
    option.textContent = province;
    elements.province.appendChild(option);
  }

  for (const district of payload.districts) {
    const option = document.createElement('option');
    option.value = district;
    option.textContent = district;
    elements.district.appendChild(option);
  }

  for (const category of payload.categories) {
    const option = document.createElement('option');
    option.value = category;
    option.textContent = category;
    elements.categoryQuick.appendChild(option);
  }
}

async function loadResults() {
  const currentMessage = elements.message.hidden ? '' : elements.message.textContent;
  const params = buildQueryParams().toString();
  logClient('search:start', { params });
  const response = await fetch(`/api/search?${params}`);
  const payload = await response.json();
  logClient('search:done', { total: payload.total, page: payload.page, pageSize: payload.pageSize });
  renderResults(payload);
  if (currentMessage) {
    showMessage(currentMessage);
  }
}

async function refreshSources() {
  const payloadBody = {
    q: elements.q.value.trim(),
    city: elements.city.value.trim(),
    category: elements.categoryQuick.value || elements.category.value.trim(),
  };

  if (!payloadBody.q) {
    showStatusMessage('Enter a search query before refreshing live sources.', 'error');
    logClient('refresh:blocked-missing-query');
    return;
  }

  logClient('refresh:start', payloadBody);
  setLiveRefreshing(true, 'Fetching live results and saving them into the database...');
  try {
    const response = await fetch('/api/ingest/search', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payloadBody),
    });
    const payload = await response.json();
    logClient('refresh:done', payload);

    if (!response.ok) {
      throw new Error(payload.details || payload.error || 'Live refresh failed.');
    }

    const details = [`Fetched: ${payload.fetched || 0}`, `Imported: ${payload.imported || 0}`];
    if (payload.cached) {
      details.push(`Using saved results: ${payload.cachedTotal || 0}`);
      if (payload.latestUpdated) {
        details.push(`Latest saved update: ${new Date(payload.latestUpdated).toLocaleString()}`);
      }
    }
    if (payload.nationwide) {
      details.push('Coverage: Sri Lanka-wide import across many areas.');
    }
    if (payload.providers && !payload.providers.googlePlacesEnabled) {
      details.push('Google Places is off until GOOGLE_MAPS_API_KEY is set.');
    }
    if (Array.isArray(payload.errors) && payload.errors.length > 0) {
      details.push(`Provider warnings: ${payload.errors.join(' | ')}`);
    }
    setLiveRefreshing(false);
    showStatusMessage(
      [payload.message, ...details].filter(Boolean).join(' '),
      payload.cached ? 'neutral' : 'success'
    );
    state.page = 1;
    await loadResults();
  } catch (error) {
    logClient('refresh:error', { message: error.message });
    setLiveRefreshing(false);
    showStatusMessage(`Live refresh failed: ${error.message}`, 'error');
  } finally {
    if (state.liveRefreshing) {
      setLiveRefreshing(false);
    }
  }
}

elements.searchForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  state.page = 1;
  await loadResults();
});

elements.refreshButton.addEventListener('click', refreshSources);
elements.exportButton.addEventListener('click', () => {
  window.location.href = `/api/export?${buildQueryParams().toString()}`;
});

elements.resetFilters.addEventListener('click', async () => {
  elements.searchForm.reset();
  elements.province.value = '';
  elements.district.value = '';
  elements.categoryQuick.value = '';
  state.page = 1;
  await loadResults();
});

elements.prevPage.addEventListener('click', async () => {
  if (state.page <= 1) return;
  state.page -= 1;
  await loadResults();
});

elements.nextPage.addEventListener('click', async () => {
  if (state.page * state.pageSize >= state.total) return;
  state.page += 1;
  await loadResults();
});

for (const control of [
  elements.district,
  elements.province,
  elements.categoryQuick,
  elements.hasPhone,
  elements.hasWebsite,
  elements.ratingAvailable,
]) {
  control.addEventListener('change', async () => {
    state.page = 1;
    await loadResults();
  });
}

loadMeta().then(loadResults).catch((error) => {
  console.error(error);
  showStatusMessage('Failed to load the directory app.', 'error');
});
