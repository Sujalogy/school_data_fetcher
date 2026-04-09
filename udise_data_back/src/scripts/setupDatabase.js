'use strict';

require('dotenv').config();
const SchemaManager = require('../db/SchemaManager');
const pool = require('../db/PostgresPool');

class SetupDatabaseCommand {
  constructor() {
    this.schemaManager = new SchemaManager();
  }

  async run() {
    await this.schemaManager.setup();
    // eslint-disable-next-line no-console
    console.log('Database setup complete: micro table, indexes, and download view.');
  }
}

(async () => {
  const cmd = new SetupDatabaseCommand();
  try {
    await cmd.run();
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('Database setup failed:', error.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
