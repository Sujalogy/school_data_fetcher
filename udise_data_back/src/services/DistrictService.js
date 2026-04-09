'use strict';

const MicrodataApiClient = require('../clients/MicrodataApiClient');
const logger = require('../utils/logger');

/**
 * DistrictService
 *
 * Fetches, validates, and deduplicates the district list for a given state.
 * Returns objects that carry both district and state context so downstream
 * processors never need to look up state info separately.
 */
class DistrictService {
  constructor() {
    this._api = new MicrodataApiClient();
  }

  /**
   * Returns a validated, deduplicated list of districts for a state.
   *
   * @param {number} stateId
   * @param {string} stateName
   * @param {number} [yearId]
   * @returns {Promise<Array<{ stateId, stateName, districtId, districtName }>>}
   */
  async getDistricts(stateId, stateName, yearId) {
    const raw = await this._api.fetchDistrictsByState(stateId, yearId);

    const seen = new Set();
    const unique = [];

    for (const d of raw) {
      const id = String(d.districtId);
      if (!seen.has(id)) {
        seen.add(id);
        unique.push({
          stateId,
          stateName,
          districtId: d.districtId,
          districtName: d.districtName,
        });
      }
    }

    if (unique.length < raw.length) {
      logger.warn(
        `[DistrictService] Removed ${raw.length - unique.length} duplicate district(s) ` +
          `for state ${stateId}.`,
      );
    }

    return unique;
  }
}

module.exports = DistrictService;
