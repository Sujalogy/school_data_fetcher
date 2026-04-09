'use strict';

/**
 * ENROLLMENT MERGE BY PSEUDOCODE (Unique Key)
 *
 * Maps Profile1 + Enrollment2 using PSEUDOCODE as join key
 * One row per school (no stacking, no duplication)
 *
 * Join Logic:
 *   Profile1[pseudocode] = Profile1[pseudocode]
 *            ↓                          ↓
 *   Enrollment2[pseudocode] JOIN WITH Enrollment2[pseudocode]
 *            ↓
 *   One Merged Row with all columns filled
 */

const logger = require('../utils/logger');

class DataNormalizer {
  /**
   * Normalises raw CSV rows and injects metadata.
   *
   * @param {object[]} rows
   * @param {object}   meta
   * @param {number}   meta.stateId
   * @param {string}   meta.stateName
   * @param {number}   meta.districtId
   * @param {string}   meta.districtName
   * @param {number}   meta.reportId
   * @param {string}   meta.reportLabel
   * @param {string}   meta.sourceFile
   * @returns {object[]}
   */
  normalise(rows, meta) {
    if (!rows || rows.length === 0) return [];

    const normalised = rows.map((row) => ({
      // ── ETL metadata (always first) ────────────────────────────────────────
      _state_id:      meta.stateId,
      _state_name:    meta.stateName,
      _district_id:   meta.districtId,
      _district_name: meta.districtName,
      _report_id:     meta.reportId,
      _report_label:  meta.reportLabel,
      _source_file:   meta.sourceFile,
      // ── Original data (keys normalised to snake_case) ─────────────────────
      ...this._normaliseKeys(row),
    }));

    logger.debug(
      `[DataNormalizer] ${normalised.length} row(s) normalised ` +
        `[state=${meta.stateId}, district=${meta.districtId}, report=${meta.reportId}].`,
    );

    return normalised;
  }

  /**
   * Merges multiple batches into one flat array (NO STACKING).
   * Uses PSEUDOCODE as unique key to match Profile1 + Enrollment2.
   *
   * @param {object[][]} batches
   * @returns {{ rows: object[], columns: string[] }}
   */
  merge(batches) {
    const allRows = batches.flat();

    logger.info(`[DataNormalizer] Starting merge with PSEUDOCODE as join key...`);
    logger.debug(`  Total input rows: ${allRows.length.toLocaleString()}`);

    // ── Step 1: Separate rows by report type ──────────────────────────────
    const profile1Rows = allRows.filter((r) => r._report_label === 'Profile1');
    const enrollment2Rows = allRows.filter((r) => r._report_label === 'Enrollment2');
    const enrollment1Rows = allRows.filter((r) => r._report_label === 'Enrollment1');
    const otherRows = allRows.filter(
      (r) =>
        r._report_label !== 'Profile1' &&
        r._report_label !== 'Enrollment1' &&
        r._report_label !== 'Enrollment2',
    );

    logger.info(
      `[DataNormalizer] Report breakdown:` +
        `\n  Profile1:    ${profile1Rows.length.toLocaleString()} rows` +
        `\n  Enrollment1: ${enrollment1Rows.length.toLocaleString()} rows` +
        `\n  Enrollment2: ${enrollment2Rows.length.toLocaleString()} rows` +
        `\n  Other:       ${otherRows.length.toLocaleString()} rows`,
    );

    // ── Step 2: Merge Profile1 + Enrollment2 by PSEUDOCODE ────────────────
    let mergedRows = [];

    if (profile1Rows.length > 0 && enrollment2Rows.length > 0) {
      mergedRows = this._mergeByPseudocode(profile1Rows, enrollment2Rows);
      logger.info(
        `[DataNormalizer] Merged Profile1 + Enrollment2: ${mergedRows.length.toLocaleString()} unified rows`,
      );
    } else if (profile1Rows.length > 0) {
      mergedRows = profile1Rows;
      logger.warn(`[DataNormalizer] No Enrollment2 data found. Using Profile1 only.`);
    }

    // ── Step 3: Handle Enrollment1 separately (if exists) ──────────────────
    if (enrollment1Rows.length > 0) {
      mergedRows.push(...enrollment1Rows);
      logger.info(
        `[DataNormalizer] Added ${enrollment1Rows.length.toLocaleString()} Enrollment1 rows`,
      );
    }

    // ── Step 4: Add other reports as-is ───────────────────────────────────
    if (otherRows.length > 0) {
      mergedRows.push(...otherRows);
      logger.info(
        `[DataNormalizer] Added ${otherRows.length.toLocaleString()} other report rows`,
      );
    }

    // ── Step 5: Build column superset ─────────────────────────────────────
    const metaCols = [
      '_state_id',
      '_state_name',
      '_district_id',
      '_district_name',
      '_report_id',
      '_report_label',
      '_source_file',
    ];

    const colSet = new Set();
    for (const row of mergedRows) {
      for (const key of Object.keys(row)) {
        colSet.add(key);
      }
    }

    const dataCols = [...colSet]
      .filter((c) => !metaCols.includes(c))
      .sort();

    const columns = [...metaCols, ...dataCols];

    logger.info(
      `[DataNormalizer] Merged ${mergedRows.length.toLocaleString()} row(s) ` +
        `across ${columns.length} column(s).`,
    );

    return { rows: mergedRows, columns };
  }

  /**
   * Merge Profile1 + Enrollment2 by matching PSEUDOCODE values.
   * PSEUDOCODE is the unique key for each school.
   *
   * @private
   */
  _mergeByPseudocode(profile1Rows, enrollment2Rows) {
    logger.debug(`[DataNormalizer] Merging by PSEUDOCODE (unique school identifier)...`);

    // Step 1: Build enrollment lookup by pseudocode
    const enrollmentByPseudocode = new Map();

    for (const enrollRow of enrollment2Rows) {
      const pseudocode = enrollRow.pseudocode;

      if (!pseudocode) {
        logger.warn(
          `[DataNormalizer] Enrollment row missing pseudocode: ${JSON.stringify(enrollRow).substring(0, 100)}`,
        );
        continue;
      }

      const key = String(pseudocode).trim();

      if (enrollmentByPseudocode.has(key)) {
        logger.warn(
          `[DataNormalizer] Duplicate pseudocode in Enrollment2: ${key}. Using first occurrence.`,
        );
        continue;
      }

      enrollmentByPseudocode.set(key, enrollRow);
    }

    logger.debug(
      `[DataNormalizer] Built enrollment lookup: ${enrollmentByPseudocode.size} unique pseudocodes`,
    );

    // Step 2: Merge each profile row with matching enrollment
    const merged = [];
    let matchedCount = 0;
    let unmatchedCount = 0;

    for (const profileRow of profile1Rows) {
      const pseudocode = profileRow.pseudocode;

      if (!pseudocode) {
        logger.warn(
          `[DataNormalizer] Profile row missing pseudocode: ${JSON.stringify(profileRow).substring(0, 100)}`,
        );
        merged.push(profileRow);
        unmatchedCount++;
        continue;
      }

      const key = String(pseudocode).trim();
      const enrollmentRow = enrollmentByPseudocode.get(key);

      if (enrollmentRow) {
        // Merge: profile + enrollment
        const mergedRow = this._deepMerge(profileRow, enrollmentRow);
        merged.push(mergedRow);
        matchedCount++;
      } else {
        // No matching enrollment: use profile as-is
        merged.push(profileRow);
        unmatchedCount++;
      }
    }

    logger.debug(
      `[DataNormalizer] Pseudocode merge complete:` +
        `\n    Matched:   ${matchedCount.toLocaleString()}` +
        `\n    Unmatched: ${unmatchedCount.toLocaleString()}`,
    );

    return merged;
  }

  /**
   * Deep merge: profile first, enrollment fills gaps
   * @private
   */
  _deepMerge(profileRow, enrollmentRow) {
    const merged = { ...profileRow };

    for (const [key, value] of Object.entries(enrollmentRow)) {
      // Skip metadata fields (always use profile's)
      if (key.startsWith('_')) {
        continue;
      }

      const profileValue = merged[key];

      // Fill empty cells with enrollment data
      if (
        profileValue === undefined ||
        profileValue === null ||
        profileValue === '' ||
        profileValue === '0'
      ) {
        merged[key] = value;
      }
    }

    return merged;
  }

  // ── Private ────────────────────────────────────────────────────────────────

  _normaliseKeys(row) {
    const result = {};
    for (const [key, value] of Object.entries(row)) {
      const cleanKey = key
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_')
        .replace(/[^a-z0-9_]/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '') || 'col';
      result[cleanKey] = value;
    }
    return result;
  }
}

module.exports = DataNormalizer;
