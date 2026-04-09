'use strict';

const logger = require('./logger');
const config = require('../config');

/**
 * Executes an async function with exponential back-off retries.
 *
 * @param {() => Promise<any>} fn          - The async operation to attempt.
 * @param {string}             operationId - Human-readable label for logging.
 * @param {object}             [opts]      - Override default retry config.
 * @returns {Promise<any>}
 */
async function withRetry(fn, operationId, opts = {}) {
  const {
    maxAttempts = config.retry.maxAttempts,
    baseDelayMs = config.retry.baseDelayMs,
    backoffFactor = config.retry.backoffFactor,
    retryableStatuses = config.retry.retryableStatuses,
  } = opts;

  let lastError;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;

      const status = err.response?.status;
      const isRetryable =
        !status || // network-level failure (no HTTP response)
        retryableStatuses.has(status);

      if (!isRetryable || attempt === maxAttempts) {
        logger.error(`[Retry] "${operationId}" failed permanently after ${attempt} attempt(s).`, {
          status,
          message: err.message,
        });
        throw err;
      }

      const delay = baseDelayMs * Math.pow(backoffFactor, attempt - 1);
      logger.warn(
        `[Retry] "${operationId}" attempt ${attempt}/${maxAttempts} failed (HTTP ${status ?? 'N/A'}). ` +
          `Retrying in ${delay}ms…`,
      );
      await sleep(delay);
    }
  }

  throw lastError;
}

/**
 * Simple promise-based sleep.
 * @param {number} ms
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { withRetry, sleep };
