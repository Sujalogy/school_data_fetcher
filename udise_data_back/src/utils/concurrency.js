'use strict';

const pLimit = require('p-limit');

/**
 * Creates a concurrency-limited executor.
 *
 * Usage:
 *   const limit = createLimiter(3);
 *   const results = await Promise.all(tasks.map(t => limit(() => processTask(t))));
 *
 * @param {number} concurrency - Max simultaneous in-flight promises.
 * @returns {Function}         - p-limit instance.
 */
function createLimiter(concurrency) {
  return pLimit(concurrency);
}

/**
 * Runs an async mapper over an array with a concurrency cap.
 *
 * @param {any[]}                  items       - Input items.
 * @param {(item: any) => Promise} mapper      - Async transform for each item.
 * @param {number}                 concurrency - Max parallel executions.
 * @returns {Promise<any[]>}
 */
async function mapWithLimit(items, mapper, concurrency) {
  const limit = createLimiter(concurrency);
  return Promise.all(items.map((item) => limit(() => mapper(item))));
}

module.exports = { createLimiter, mapWithLimit };
