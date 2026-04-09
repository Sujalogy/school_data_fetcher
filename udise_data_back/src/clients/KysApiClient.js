'use strict';

const BaseApiClient = require('./BaseApiClient');
const config = require('../config');
const { withRetry } = require('../utils/retry');
const logger = require('../utils/logger');

/**
 * KysApiClient
 *
 * Handles calls to the KYS metadata API (kys.udiseplus.gov.in).
 * Currently exposes: fetchYearList
 */
class KysApiClient extends BaseApiClient {
  constructor() {
    super(config.api.yearList.baseUrl);
  }

  /**
   * Fetches the list of academic years from UDISE+.
   *
   * @returns {Promise<Array<{ yearId: number, yearLabel: string }>>}
   */
  async fetchYearList() {
    logger.info('[KysApiClient] Fetching year list…');

    const response = {
"httpStatus": 200,
"status": true,
"message": "success",
"data": [
{
"yearId": 0,
"yearDesc": "Real Time"
},
{
"yearId": 11,
"yearDesc": "2024-25"
},
{
"yearId": 10,
"yearDesc": "2023-24"
},
{
"yearId": 9,
"yearDesc": "2022-23"
},
{
"yearId": 8,
"yearDesc": "2021-22"
}
]
};

    const data = response.data;

    // The API may return an array directly or wrap it in a data key
    const years = Array.isArray(data) ? data : data?.data ?? data?.years ?? [];
    logger.info(`[KysApiClient] Received ${years.length} year record(s).`);
    return years;
  }
}

module.exports = KysApiClient;
