'use strict';

const axios = require('axios');
const config = require('../config');
const logger = require('../utils/logger');

/**
 * BaseApiClient
 *
 * Wraps axios with:
 *  - Shared default headers (User-Agent, Accept, Session cookie)
 *  - Request / response logging
 *  - Consistent error normalisation
 *
 * Subclasses extend this to add domain-specific methods.
 */
class BaseApiClient {
  /**
   * @param {string} baseURL  - Root URL for this client instance.
   * @param {object} [extras] - Additional axios config overrides.
   */
  constructor(baseURL, extras = {}) {
    this._client = axios.create({
      baseURL,
      timeout: config.api.timeoutMs,
      headers: {
        'User-Agent': config.api.userAgent,
        Accept: '*/*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        Connection: 'keep-alive',
        Cookie: `null; ${config.api.sessionCookie}`,
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
      },
      ...extras,
    });

    this._attachInterceptors();
  }

  _attachInterceptors() {
    // ── Request logging ───────────────────────────────────────────────────────
    this._client.interceptors.request.use(async (req) => {
      await BaseApiClient._serializeAndDelay(config.api.requestDelayMs);
      logger.debug(`[HTTP] → ${req.method?.toUpperCase()} ${req.baseURL}${req.url}`);
      return req;
    });

    // ── Response / error logging ──────────────────────────────────────────────
    this._client.interceptors.response.use(
      (res) => {
        logger.debug(
          `[HTTP] ← ${res.status} ${res.config.method?.toUpperCase()} ${res.config.url} ` +
            `(${(res.headers['content-length'] ?? '?')} bytes)`,
        );
        return res;
      },
      (err) => {
        const status = err.response?.status ?? 'N/A';
        const url = err.config?.url ?? 'unknown';
        logger.error(`[HTTP] ✗ ${status} ${url} — ${err.message}`);
        return Promise.reject(err);
      },
    );
  }

  /** Expose the raw axios instance for advanced use */
  get http() {
    return this._client;
  }

  static async _serializeAndDelay(delayMs) {
    BaseApiClient._requestQueue = BaseApiClient._requestQueue.then(
      async () =>
        new Promise((resolve) => {
          setTimeout(resolve, Math.max(delayMs, 0));
        }),
    );
    return BaseApiClient._requestQueue;
  }
}

BaseApiClient._requestQueue = Promise.resolve();

module.exports = BaseApiClient;
