const { handleApiRequest } = require('../../src/server');

module.exports = async (req, res) => handleApiRequest(req, res, '/api/ingest/search');
