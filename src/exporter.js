function escapeCsvValue(value) {
  const text = String(value ?? '');
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function createCsvBuffer(rows) {
  const headers = [
    'Company Name',
    'Categories',
    'Address',
    'City',
    'District',
    'Phone',
    'Website',
    'Rating',
    'Review Count',
    'Source',
    'Last Updated'
  ];

  const lines = [headers.join(',')];
  for (const row of rows) {
    lines.push(
      [
        row.name,
        row.categories.join(', '),
        row.address,
        row.city,
        row.district,
        row.phone,
        row.website,
        row.rating,
        row.reviewCount,
        row.source,
        row.lastUpdated
      ]
        .map(escapeCsvValue)
        .join(',')
    );
  }

  return Buffer.from(`\uFEFF${lines.join('\n')}`, 'utf8');
}

module.exports = {
  createCsvBuffer,
};
