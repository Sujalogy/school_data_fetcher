'use strict';

/**
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │  UDISE+ ETL Pipeline — Entry Point                                       │
 * │                                                                          │
 * │  Usage:                                                                  │
 * │    node index.js                              (default: state 128)       │
 * │    STATE_ID=9 YEAR_ID=11 node index.js        (Bihar, year 11)           │
 * │    DRY_RUN=true node index.js                 (flow check, no HTTP)      │
 * │    KEEP_TEMP=true node index.js               (persist extracted CSVs)   │
 * │    CONCURRENT_DISTRICTS=5 node index.js       (raise parallelism)        │
 * └──────────────────────────────────────────────────────────────────────────┘
 */

const fs = require('fs');
const path = require('path');
const EtlPipeline = require('./src/pipeline/EtlPipeline');
const logger = require('./src/utils/logger');
const config = require('./src/config');
const SchemaManager = require('./src/db/SchemaManager');
const pool = require('./src/db/PostgresPool');

// ── Ensure output / temp / logs directories exist ────────────────────────────
for (const dir of [config.output.dir, config.output.tempDir, config.output.logsDir]) {
  fs.mkdirSync(dir, { recursive: true });
}

// ── Run ───────────────────────────────────────────────────────────────────────
(async () => {
  try {
    const schemaManager = new SchemaManager();
    await schemaManager.setup();

    const pipeline = new EtlPipeline();
    const report = await pipeline.run();

    if (report.errors.length > 0) {
      logger.warn(`Pipeline completed with ${report.errors.length} non-fatal error(s):`);
      report.errors.forEach((e, i) => logger.warn(`  [${i + 1}] ${e}`));
    }

    // Exit code 0 even with partial errors — the Excel was still produced.
    process.exit(0);

  } catch (err) {
    logger.error(`[FATAL] Pipeline terminated: ${err.message}`, { stack: err.stack });
    process.exit(1);
  } finally {
    await pool.end();
  }
})();

// ── Guard against unhandled rejections bleeding through ──────────────────────
process.on('unhandledRejection', (reason) => {
  logger.error('[UnhandledRejection]', { reason: String(reason) });
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  logger.error('[UncaughtException]', { message: err.message, stack: err.stack });
  process.exit(1);
});
