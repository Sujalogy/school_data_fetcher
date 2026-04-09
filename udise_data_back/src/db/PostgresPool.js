'use strict';

require('dotenv').config();
const { Pool } = require('pg');

/**
 * PostgresPool
 * Centralized DB connection pool.
 */
class PostgresPool {
  constructor() {
    this.pool = new Pool({
      user: process.env.PG_USER,
      host: process.env.PG_HOST,
      database: process.env.PG_DATABASE,
      password: process.env.PG_PASSWORD,
      port: parseInt(process.env.PG_PORT ?? '5432', 10),
      ssl: { rejectUnauthorized: false },
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });
  }

  get client() {
    return this.pool;
  }
}

module.exports = new PostgresPool().client;
