'use strict';

const MicrodataApiClient = require('../clients/MicrodataApiClient');
const ZipProcessor  = require('../processors/ZipProcessor');
const CsvProcessor  = require('../processors/CsvProcessor');
const DataNormalizer = require('../processors/DataNormalizer');
const logger = require('../utils/logger');
const config = require('../config');
const { mapWithLimit } = require('../utils/concurrency');

/**
 * ReportService
 *
 * Inner ETL loop — for one district, downloads every report in parallel,
 * unzips, parses CSVs, normalises rows, and returns them as a flat array.
 */
class ReportService {
  constructor() {
    this._api  = new MicrodataApiClient();
    this._zip  = new ZipProcessor();
    this._csv  = new CsvProcessor();
    this._norm = new DataNormalizer();
  }

  /**
   * Processes all configured reports for one district.
   *
   * @param {object} params
   * @param {number} params.stateId
   * @param {string} params.stateName
   * @param {number} params.districtId
   * @param {string} params.districtName
   * @param {number} [params.yearId]
   * @returns {Promise<object[]>}
   */
  async processDistrict({ stateId, stateName, districtId, districtName, yearId = config.pipeline.yearId }) {
    logger.info(
      `[ReportService] ▶ [${stateName}] District ${districtId} (${districtName}) — ` +
        `${config.pipeline.reports.length} report(s)…`,
    );

    const batches = await mapWithLimit(
      config.pipeline.reports,
      (report) =>
        this._processOneReport({ stateId, stateName, districtId, districtName, yearId, ...report }),
      config.pipeline.concurrentReports,
    );

    const allRows = batches.flat();
    logger.info(
      `[ReportService] ✓ [${stateName}] District ${districtId} — ${allRows.length} row(s).`,
    );
    return allRows;
  }

  /** @private */
  async _processOneReport({ stateId, stateName, districtId, districtName, yearId, reportId, label }) {
    const ctx = `[${stateName}] d=${districtId} r=${reportId}(${label})`;

    if (config.flags.dryRun) {
      logger.info(`[ReportService] [DRY RUN] Skipping ${ctx}.`);
      return [];
    }

    try {
      const zipBuffer = await this._api.downloadReportZip({ stateId, districtId, reportId, yearId });

      if (!zipBuffer || zipBuffer.length === 0) {
        logger.warn(`[ReportService] Empty ZIP for ${ctx}. Skipping.`);
        return [];
      }

      const csvFiles = this._zip.extract(
        zipBuffer,
        `s${stateId}_d${districtId}_r${reportId}`,
      );

      if (csvFiles.length === 0) {
        logger.warn(`[ReportService] No CSVs in ZIP for ${ctx}.`);
        return [];
      }

      const parsedArrays = await this._csv.parseMany(csvFiles);

      return parsedArrays.flatMap((rows, idx) =>
        this._norm.normalise(rows, {
          stateId,
          stateName,
          districtId,
          districtName,
          reportId,
          reportLabel: label,
          sourceFile: csvFiles[idx].filename,
        }),
      );

    } catch (err) {
      logger.error(`[ReportService] Failed ${ctx}: ${err.message}`);
      return [];
    }
  }
}

module.exports = ReportService;
