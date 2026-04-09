'use strict';

const FormData = require('form-data');
const BaseApiClient = require('./BaseApiClient');
const config = require('../config');
const { withRetry } = require('../utils/retry');
const logger = require('../utils/logger');

/**
 * MicrodataApiClient
 *
 * Handles calls to microdata.udiseplus.gov.in:
 *  - fetchDistrictsByState  → multipart/form-data POST
 *                             Response: { mstDistricts: [{ colId, colValue }] }
 *  - downloadReportZip      → application/x-www-form-urlencoded POST
 *                             Response: ZIP as raw binary OR base64-encoded string
 */
class MicrodataApiClient extends BaseApiClient {
  constructor() {
    super(config.api.microdata.baseUrl);
  }

  // ── District list ──────────────────────────────────────────────────────────

  /**
   * Returns all districts for a given state and year.
   *
   * API response shape: { mstDistricts: [{ colId: "4501", colValue: "ANDAMANS" }, ...] }
   * The sentinel row { colId: "99", colValue: "All District" } is stripped.
   *
   * @param {number} stateId
   * @param {number} [yearId]
   * @returns {Promise<Array<{ districtId: number, districtName: string }>>}
   */
  async fetchDistrictsByState(stateId, yearId = config.pipeline.yearId) {
    logger.info(`[MicrodataApiClient] Fetching districts for state ${stateId} (year ${yearId})…`);

    const form = new FormData();
    form.append('state', String(stateId));
    form.append('year', String(yearId));

    const response = await withRetry(
      () =>
        this.http.post(config.api.microdata.districtPath, form, {
          headers: {
            ...form.getHeaders(),
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            Origin: config.api.microdata.baseUrl,
          },
        }),
      `fetchDistrictsByState(state=${stateId})`,
    );

    const raw = response.data;

    let entries = [];
    if (raw && Array.isArray(raw.mstDistricts)) {
      entries = raw.mstDistricts;
    } else if (Array.isArray(raw)) {
      entries = raw;
    } else if (raw && Array.isArray(raw.data)) {
      entries = raw.data;
    } else {
      logger.warn(
        `[MicrodataApiClient] Unexpected district response shape for state ${stateId}: ` +
          JSON.stringify(raw).slice(0, 200),
      );
    }

    const districts = entries
      .map((entry) => {
        if (entry.colId !== undefined) {
          return {
            districtId:   parseInt(entry.colId, 10),
            districtName: String(entry.colValue ?? '').trim(),
          };
        }
        return {
          districtId:   parseInt(entry.districtId ?? entry.district_id ?? entry.id, 10),
          districtName: String(entry.districtName ?? entry.district_name ?? entry.name ?? '').trim(),
        };
      })
      .filter((d) => {
        if (d.districtId === 99)  return false;   // "All District" sentinel
        if (!d.districtId || isNaN(d.districtId)) return false;
        if (d.districtName.toLowerCase() === 'all district') return false;
        return true;
      });

    logger.info(`[MicrodataApiClient] State ${stateId} — ${districts.length} real district(s).`);
    return districts;
  }

  // ── Report download ────────────────────────────────────────────────────────

  /**
   * Downloads a single report for a district.
   *
   * The UDISE+ API can return the ZIP in two ways:
   *   a) Raw binary ZIP bytes   → first two bytes are PK (0x50 0x4B)
   *   b) Base64-encoded string  → printable ASCII, decodes to a PK-prefixed buffer
   *
   * Both shapes are handled transparently by _decodeZipBuffer().
   *
   * @param {object} params
   * @param {number} params.stateId
   * @param {number} params.districtId
   * @param {number} params.reportId
   * @param {number} [params.yearId]
   * @returns {Promise<Buffer>} Binary ZIP bytes ready for adm-zip.
   */
  async downloadReportZip({ stateId, districtId, reportId, yearId = config.pipeline.yearId }) {
    const label = `downloadZip(state=${stateId},district=${districtId},report=${reportId})`;
    logger.debug(`[MicrodataApiClient] Requesting ${label}…`);

    const payload = new URLSearchParams({
      stateId:    String(stateId),
      districtId: String(districtId),
      reportId:   String(reportId),
      yearId:     String(yearId),
    }).toString();

    const response = await withRetry(
      () =>
        this.http.post(config.api.microdata.downloadPath, payload, {
          headers: {
            'Content-Type':            'application/x-www-form-urlencoded',
            Accept:                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Cache-Control':           'max-age=0',
            'Sec-Fetch-Dest':          'document',
            'Sec-Fetch-Mode':          'navigate',
            'Sec-Fetch-Site':          'same-origin',
            'Sec-Fetch-User':          '?1',
            'Upgrade-Insecure-Requests': '1',
            Origin:                    'null',
          },
          // Receive as arraybuffer — works for both binary and base64 text
          responseType: 'arraybuffer',
        }),
      label,
    );

    const raw = Buffer.from(response.data);
    const zipBuffer = MicrodataApiClient._decodeZipBuffer(raw, label);
    logger.debug(`[MicrodataApiClient] ${label} → ${zipBuffer.length} bytes ready.`);
    return zipBuffer;
  }

  // ── Static helpers ─────────────────────────────────────────────────────────

  /**
   * Normalises the raw HTTP response buffer into a real binary ZIP buffer.
   *
   * Decision tree:
   *   1. Starts with PK magic (0x50 0x4B) → already binary, return as-is.
   *   2. Looks like base64 text → decode → verify PK → return decoded.
   *   3. Neither → return raw and let ZipProcessor surface the error.
   *
   * @param {Buffer} raw    - Raw bytes from axios arraybuffer response.
   * @param {string} label  - Context label for logging.
   * @returns {Buffer}
   * @private
   */
  static _decodeZipBuffer(raw, label) {
    // ── 1. Already binary ZIP (PK magic header 50 4B) ───────────────────────
    if (raw.length >= 2 && raw[0] === 0x50 && raw[1] === 0x4b) {
      logger.debug(`[MicrodataApiClient] ${label} — raw binary ZIP detected.`);
      return raw;
    }

    // ── 1b. Check for PDF (25 50 44 46 = %PDF) ─────────────────────────────
    if (raw.length >= 4 && raw[0] === 0x25 && raw[1] === 0x50 && raw[2] === 0x44 && raw[3] === 0x46) {
      throw new Error(
        `[MicrodataApiClient] ${label} — API returned PDF instead of ZIP. ` +
        `This report may not be available or the API endpoint is misconfigured.`
      );
    }

    // ── 2. Base64-encoded ZIP ────────────────────────────────────────────────
    // The server sends a base64 string; axios stores it as UTF-8 bytes in the
    // arraybuffer. Trim whitespace/newlines that some Java servers append.
    const str = raw.toString('utf8').trim();

    if (MicrodataApiClient._isBase64(str)) {
      try {
        const decoded = Buffer.from(str, 'base64');
        if (decoded.length >= 2 && decoded[0] === 0x50 && decoded[1] === 0x4b) {
          logger.debug(
            `[MicrodataApiClient] ${label} — base64 ZIP decoded ` +
              `(${raw.length} chars → ${decoded.length} bytes).`,
          );
          return decoded;
        }
        logger.warn(`[MicrodataApiClient] ${label} — base64-decoded but no PK magic; using raw.`);
      } catch (err) {
        logger.warn(`[MicrodataApiClient] ${label} — base64 decode error: ${err.message}; using raw.`);
      }
    }

    // ── 3. Unknown format — pass raw to ZipProcessor for a clear error ───────
    logger.warn(
      `[MicrodataApiClient] ${label} — unrecognised response format ` +
        `(first bytes: ${raw.slice(0, 8).toString('hex')}); passing raw buffer.`,
    );
    return raw;
  }

  /**
   * Returns true if the string contains only base64 alphabet characters
   * (A-Z a-z 0-9 + / =) — a fast heuristic, not a full RFC 4648 validator.
   *
   * @param {string} str
   * @returns {boolean}
   * @private
   */
  static _isBase64(str) {
    if (!str || str.length < 4) return false;
    return /^[A-Za-z0-9+/]+=*$/.test(str);
  }
}

module.exports = MicrodataApiClient;
