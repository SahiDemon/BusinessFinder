const { SQLiteDirectoryRepository, normalizeText, createDedupeKey } = require('./sqlite-repository');
const { PostgresDirectoryRepository } = require('./postgres-repository');

function createRepository() {
  const connectionString =
    process.env.POSTGRES_URL_NON_POOLING ||
    process.env.DATABASE_URL_UNPOOLED ||
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.POSTGRES_PRISMA_URL;

  if (connectionString) {
    return new PostgresDirectoryRepository(connectionString);
  }

  return new SQLiteDirectoryRepository();
}

module.exports = {
  DirectoryRepository: createRepository,
  normalizeText,
  createDedupeKey,
};
