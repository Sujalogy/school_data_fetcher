'use strict';

const logger = require('../utils/logger');

/**
 * DataNormalizer
 *
 * Transforms raw parsed CSV rows into a uniform record shape:
 *  1. Normalises column names (lowercase, snake_case, no special chars)
 *  2. Injects ETL metadata: stateId, stateName, districtId, districtName, reportId, reportLabel
 *  3. Provides a merge() helper that reconciles the superset of column names
 *     across all report types (schema-agnostic — sparse rows where columns differ)
 */
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
   * Merges multiple batches into one flat array with SEPARATE enrollment data.
   * Uses PSEUDOCODE as unique key to match Profile1 + Enrollment2.
   * Prefixes enrollment columns with enrl_1_ and enrl_2_ to keep data separate.
   * Builds the ordered superset of column names so Excel renders all rows.
   *
   * @param {object[][]} batches
   * @returns {{ rows: object[], columns: string[] }}
   */
  merge(batches) {
    const allRows = batches.flat();

    logger.info(`[DataNormalizer] Starting merge with PSEUDOCODE as join key (keeping enrollments separate)...`);
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

    // ── Step 2: Merge Profile1 + Enrollment2 with PREFIXED enrollment columns ─
    let mergedRows = [];

    if (profile1Rows.length > 0 && enrollment2Rows.length > 0) {
      mergedRows = this._mergeByPseudocodeWithPrefix(profile1Rows, enrollment2Rows, 'enrl_2');
      logger.info(
        `[DataNormalizer] Merged Profile1 + Enrollment2: ${mergedRows.length.toLocaleString()} unified rows (with enrl_2_ prefix)`,
      );
    } else if (profile1Rows.length > 0) {
      mergedRows = profile1Rows;
      logger.warn(`[DataNormalizer] No Enrollment2 data found. Using Profile1 only.`);
    }

    // ── Step 3: Merge Enrollment1 into existing rows by pseudocode ────────
    if (enrollment1Rows.length > 0) {
      mergedRows = this._mergeEnrollment1ByPseudocode(mergedRows, enrollment1Rows);
      logger.info(
        `[DataNormalizer] Merged Enrollment1 by pseudocode: ${mergedRows.length.toLocaleString()} unified rows (with enrl_1_ prefix)`,
      );
    }

    // ── Step 4: Add other reports as-is ───────────────────────────────────
    if (otherRows.length > 0) {
      mergedRows = mergedRows.concat(otherRows);
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
      for (const key of Object.keys(row)) colSet.add(key);
    }

    const dataCols = [...colSet].filter((c) => !metaCols.includes(c)).sort();
    const columns = [...metaCols, ...dataCols];

    logger.info(
      `[DataNormalizer] Merged ${mergedRows.length.toLocaleString()} row(s) ` +
        `across ${columns.length} column(s).`,
    );

    return { rows: mergedRows, columns };
  }

  /**
   * Merge Profile1 + Enrollment2 with PREFIXED enrollment columns only.
   * Key = state_id + '_' + district_id + '_' + pseudocode (unique within context)
   * Only prefixes enrollment-specific columns (c1_b, c1_g, etc.) to reduce memory.
   * School metadata stays unprefixed.
   *
   * @private
   */
  _mergeByPseudocodeWithPrefix(profile1Rows, enrollment2Rows, prefix) {
    logger.debug(`[DataNormalizer] Merging by COMPOSITE KEY with prefix '${prefix}' (state+district+pseudocode)...`);

    const ENROLLMENT_COLUMNS = [
      'c1_b', 'c1_g', 'c2_b', 'c2_g', 'c3_b', 'c3_g',
      'c4_b', 'c4_g', 'c5_b', 'c5_g', 'c6_b', 'c6_g',
      'c7_b', 'c7_g', 'c8_b', 'c8_g', 'c9_b', 'c9_g',
      'c10_b', 'c10_g', 'c11_b', 'c11_g', 'c12_b', 'c12_g',
    ];

    // Helper to build composite key
    const buildKey = (row) => {
      const state = String(row._state_id).trim();
      const district = String(row._district_id).trim();
      const pseudo = String(row.pseudocode).trim();
      return `${state}|${district}|${pseudo}`;
    };

    // Step 1: Build lookups by composite key
    const profileByKey = new Map();
    const enrollmentByKey = new Map();

    for (const profileRow of profile1Rows) {
      if (!profileRow.pseudocode) {
        logger.warn(
          `[DataNormalizer] Profile row missing pseudocode: ${JSON.stringify(profileRow).substring(0, 100)}`,
        );
        continue;
      }

      const key = buildKey(profileRow);
      if (!profileByKey.has(key)) {
        profileByKey.set(key, []);
      }
      profileByKey.get(key).push(profileRow);
    }

    let duplicateProfileCount = 0;
    for (const [key, rows] of profileByKey.entries()) {
      if (rows.length > 1) {
        duplicateProfileCount++;
        logger.debug(
          `[DataNormalizer] Duplicate in Profile1: ${key} (${rows.length} rows)`,
        );
      }
    }

    for (const enrollRow of enrollment2Rows) {
      if (!enrollRow.pseudocode) {
        logger.warn(
          `[DataNormalizer] Enrollment row missing pseudocode: ${JSON.stringify(enrollRow).substring(0, 100)}`,
        );
        continue;
      }

      const key = buildKey(enrollRow);
      if (!enrollmentByKey.has(key)) {
        enrollmentByKey.set(key, []);
      }
      enrollmentByKey.get(key).push(enrollRow);
    }

    let duplicateEnrollCount = 0;
    for (const [key, rows] of enrollmentByKey.entries()) {
      if (rows.length > 1) {
        duplicateEnrollCount++;
        logger.debug(
          `[DataNormalizer] Duplicate in ${prefix}: ${key} (${rows.length} rows)`,
        );
      }
    }

    logger.debug(
      `[DataNormalizer] Lookups built:` +
        `\n  Profile contexts: ${profileByKey.size} (${duplicateProfileCount} with duplicates)` +
        `\n  Enrollment contexts: ${enrollmentByKey.size} (${duplicateEnrollCount} with duplicates)`,
    );

    // Step 2: Merge with prefixed enrollment columns only
    const merged = [];

    for (const [compositeKey, profileRows] of profileByKey.entries()) {
      const enrollmentRows = enrollmentByKey.get(compositeKey) || [];

      // Use first profile row as base
      const baseRow = { ...profileRows[0] };

      // If multiple profiles, sum enrollment values
      if (profileRows.length > 1) {
        for (const col of ENROLLMENT_COLUMNS) {
          let totalVal = 0;
          for (const pRow of profileRows) {
            const val = parseInt(pRow[col], 10) || 0;
            totalVal += val;
          }
          baseRow[col] = totalVal;
        }
      }

      // Add enrollment data with prefix ONLY for enrollment columns
      if (enrollmentRows.length > 0) {
        const enrollmentRow = enrollmentRows[0];
        for (const col of ENROLLMENT_COLUMNS) {
          const prefixedKey = `${prefix}_${col}`;
          baseRow[prefixedKey] = enrollmentRow[col];
        }
      }

      merged.push(baseRow);
    }

    logger.info(
      `[DataNormalizer] Composite-key merge complete: ${merged.length} rows ` +
        `(${duplicateProfileCount} profile duplicates summed, ${duplicateEnrollCount} enrollment duplicates)`,
    );

    return merged;
  }

  /**
   * Merge Enrollment1 data into already-merged rows by matching pseudocode.
   * This keeps all data in ONE row per pseudocode instead of stacking.
   * Enrollment1 columns get enrl_1_ prefix.
   *
   * @private
   */
  _mergeEnrollment1ByPseudocode(mergedRows, enrollment1Rows) {
    const ENROLLMENT_COLUMNS = [
      'c1_b', 'c1_g', 'c2_b', 'c2_g', 'c3_b', 'c3_g',
      'c4_b', 'c4_g', 'c5_b', 'c5_g', 'c6_b', 'c6_g',
      'c7_b', 'c7_g', 'c8_b', 'c8_g', 'c9_b', 'c9_g',
      'c10_b', 'c10_g', 'c11_b', 'c11_g', 'c12_b', 'c12_g',
    ];

    // Build composite key helper
    const buildKey = (row) => {
      const state = String(row._state_id).trim();
      const district = String(row._district_id).trim();
      const pseudo = String(row.pseudocode).trim();
      return `${state}|${district}|${pseudo}`;
    };

    // Create lookup map of merged rows by composite key
    const mergedByKey = new Map();
    for (const row of mergedRows) {
      if (row.pseudocode) {
        const key = buildKey(row);
        if (!mergedByKey.has(key)) {
          mergedByKey.set(key, []);
        }
        mergedByKey.get(key).push(row);
      }
    }

    logger.debug(
      `[DataNormalizer] Enrollment1 merge: ${mergedByKey.size} pseudocode contexts, ${enrollment1Rows.length} enrollment1 rows to merge`,
    );

    // Merge Enrollment1 data into matched merged rows by pseudocode
    let matchedCount = 0;
    let unmatchedCount = 0;
    const result = [];
    const processedMergedKeys = new Set();

    // First, merge all enrollment1 rows INTO matched merged rows
    for (const enrollRow of enrollment1Rows) {
      if (!enrollRow.pseudocode) {
        logger.warn(
          `[DataNormalizer] Enrollment1 row missing pseudocode: ${JSON.stringify(enrollRow).substring(0, 100)}`,
        );
        continue;
      }

      const key = buildKey(enrollRow);
      const matchedMergedRows = mergedByKey.get(key);

      if (matchedMergedRows && matchedMergedRows.length > 0) {
        // Merge into first matched row
        const targetRow = matchedMergedRows[0];
        processedMergedKeys.add(key);

        // Add enrollment1 columns with prefix
        for (const col of ENROLLMENT_COLUMNS) {
          if (col in enrollRow) {
            targetRow[`enrl_1_${col}`] = enrollRow[col];
          }
        }

        matchedCount++;
      } else {
        // No match: create standalone enrollment1 row with prefixed columns
        const standalonRow = { ...enrollRow };
        for (const col of ENROLLMENT_COLUMNS) {
          if (col in standalonRow) {
            standalonRow[`enrl_1_${col}`] = standalonRow[col];
            delete standalonRow[col];
          }
        }
        result.push(standalonRow);
        unmatchedCount++;
      }
    }

    // Add all merged rows (modified with enrollment1 data where matched)
    for (const row of mergedRows) {
      result.push(row);
    }

    logger.debug(
      `[DataNormalizer] Enrollment1 merge complete: ${matchedCount} matched, ${unmatchedCount} unmatched (created as standalone rows)`,
    );

    return result;
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
