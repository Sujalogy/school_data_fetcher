'use strict';

const logger = require('../utils/logger');

/**
 * EnrollmentMerger
 *
 * Merges Profile rows with Enrollment rows into single enriched rows.
 * Intelligently joins by:
 *   1. UDISE Code (if present)
 *   2. Row order within district/block (fallback)
 *   3. School name + location (fuzzy matching fallback)
 *
 * Result: One row per school with BOTH metadata + enrollment data.
 * No stacking by report. Single _report_label per row.
 */
class EnrollmentMerger {
  /**
   * Merge Profile1 + Enrollment data into unified rows.
   *
   * @param {object[]} profileRows    Normalized Profile1 rows with metadata
   * @param {object[]} enrollmentRows Normalized Enrollment1/2 rows with enrollment data
   * @param {object}   options
   * @param {string}   options.joinKey Key to match rows ('udise_code', 'school_name', 'order')
   * @returns {object[]} Merged rows with all columns
   */
  merge(profileRows, enrollmentRows, options = {}) {
    const { joinKey = 'udise_code' } = options;

    if (!profileRows || profileRows.length === 0) {
      logger.warn('[EnrollmentMerger] No profile rows to merge.');
      return enrollmentRows || [];
    }

    if (!enrollmentRows || enrollmentRows.length === 0) {
      logger.warn('[EnrollmentMerger] No enrollment rows; returning profile rows only.');
      return profileRows;
    }

    logger.info(
      `[EnrollmentMerger] Merging ${profileRows.length} profile row(s) ` +
        `with ${enrollmentRows.length} enrollment row(s) on key: ${joinKey}`,
    );

    let mergedRows = [];

    if (joinKey === 'udise_code') {
      mergedRows = this._mergeByUdiseCode(profileRows, enrollmentRows);
    } else if (joinKey === 'order') {
      mergedRows = this._mergeByOrder(profileRows, enrollmentRows);
    } else if (joinKey === 'school_name') {
      mergedRows = this._mergeBySchoolName(profileRows, enrollmentRows);
    } else {
      logger.warn(`[EnrollmentMerger] Unknown join key: ${joinKey}. Using order.`);
      mergedRows = this._mergeByOrder(profileRows, enrollmentRows);
    }

    logger.info(
      `[EnrollmentMerger] Merged ${mergedRows.length} row(s). ` +
        `${profileRows.length - mergedRows.length} profile row(s) not matched.`,
    );

    return mergedRows;
  }

  /**
   * Merge by UDISE Code (most reliable).
   * @private
   */
  _mergeByUdiseCode(profileRows, enrollmentRows) {
    // Build enrollment lookup by UDISE code
    const enrollmentByCode = new Map();

    for (const enrollRow of enrollmentRows) {
      const code = enrollRow.udise_code || enrollRow.udise;
      if (code) {
        enrollmentByCode.set(String(code).trim(), enrollRow);
      }
    }

    logger.debug(
      `[EnrollmentMerger] Built enrollment lookup: ${enrollmentByCode.size} unique codes.`,
    );

    // Merge: for each profile row, find matching enrollment
    const merged = [];
    let matched = 0;
    let unmatched = 0;

    for (const profileRow of profileRows) {
      const code = profileRow.udise_code || profileRow.udise;
      const enrollRow = enrollmentByCode.get(String(code).trim());

      if (enrollRow) {
        // Merge: profile first, then overwrite with enrollment non-empty values
        const mergedRow = this._deepMerge(profileRow, enrollRow);
        // Keep profile's _report_label (don't use enrollment's)
        mergedRow._report_label = profileRow._report_label;
        merged.push(mergedRow);
        matched++;
      } else {
        // No enrollment match: return profile row as-is (with empty enrollment columns)
        merged.push(profileRow);
        unmatched++;
      }
    }

    logger.debug(
      `[EnrollmentMerger] Match rate: ${matched} matched, ${unmatched} unmatched.`,
    );

    return merged;
  }

  /**
   * Merge by row order within district/block (fallback).
   * Assumes profile and enrollment are in same order within context.
   * @private
   */
  _mergeByOrder(profileRows, enrollmentRows) {
    // Group by district + block
    const profilesByContext = this._groupByContext(profileRows);
    const enrollmentsByContext = this._groupByContext(enrollmentRows);

    const merged = [];

    for (const contextKey of profilesByContext.keys()) {
      const pRows = profilesByContext.get(contextKey) || [];
      const eRows = enrollmentsByContext.get(contextKey) || [];

      for (let i = 0; i < pRows.length; i++) {
        const profileRow = pRows[i];
        const enrollRow = eRows[i];

        if (enrollRow) {
          const mergedRow = this._deepMerge(profileRow, enrollRow);
          mergedRow._report_label = profileRow._report_label;
          merged.push(mergedRow);
        } else {
          merged.push(profileRow);
        }
      }
    }

    return merged;
  }

  /**
   * Merge by fuzzy school name matching (last resort).
   * @private
   */
  _mergeBySchoolName(profileRows, enrollmentRows) {
    const enrollmentByName = new Map();

    for (const enrollRow of enrollmentRows) {
      const name = (enrollRow.school_name || enrollRow.school || '').trim().toLowerCase();
      if (name) {
        enrollmentByName.set(name, enrollRow);
      }
    }

    const merged = [];

    for (const profileRow of profileRows) {
      const profileName = (profileRow.school_name || profileRow.school || '').trim().toLowerCase();
      const enrollRow = enrollmentByName.get(profileName);

      if (enrollRow) {
        const mergedRow = this._deepMerge(profileRow, enrollRow);
        mergedRow._report_label = profileRow._report_label;
        merged.push(mergedRow);
      } else {
        merged.push(profileRow);
      }
    }

    return merged;
  }

  /**
   * Deep merge: combine two objects.
   * Profile values are base; enrollment values fill in gaps.
   * Enrollment non-empty values override empty profile values.
   * @private
   */
  _deepMerge(profileRow, enrollmentRow) {
    const result = { ...profileRow };

    for (const [key, value] of Object.entries(enrollmentRow)) {
      // Skip metadata keys (use profile's)
      if (key.startsWith('_')) continue;

      // If profile has this key but it's empty/null, use enrollment value
      const profileValue = result[key];

      if (
        profileValue === undefined ||
        profileValue === null ||
        profileValue === '' ||
        profileValue === '0'
      ) {
        result[key] = value;
      }
      // Otherwise keep profile value (profile is source of truth for duplicate keys)
    }

    return result;
  }

  /**
   * Group rows by district + block context.
   * @private
   */
  _groupByContext(rows) {
    const groups = new Map();

    for (const row of rows) {
      const districtId = row._district_id || row.district_id || '';
      const block = row.block || row.lgd_block_name || '';
      const contextKey = `${districtId}::${block}`;

      if (!groups.has(contextKey)) {
        groups.set(contextKey, []);
      }

      groups.get(contextKey).push(row);
    }

    return groups;
  }

  /**
   * Split master rows by report type and return separated batches.
   * Useful for re-processing.
   * @static
   */
  static splitByReport(masterRows) {
    const byReport = new Map();

    for (const row of masterRows) {
      const label = row._report_label || 'Unknown';
      if (!byReport.has(label)) {
        byReport.set(label, []);
      }
      byReport.get(label).push(row);
    }

    return byReport;
  }

  /**
   * Get enrollment summary statistics from merged rows.
   * @static
   */
  static getEnrollmentStats(mergedRows) {
    const classColumns = [
      'c1_b', 'c1_g', 'c2_b', 'c2_g', 'c3_b', 'c3_g',
      'c4_b', 'c4_g', 'c5_b', 'c5_g', 'c6_b', 'c6_g',
      'c7_b', 'c7_g', 'c8_b', 'c8_g', 'c9_b', 'c9_g',
      'c10_b', 'c10_g', 'c11_b', 'c11_g', 'c12_b', 'c12_g',
    ];

    const stats = {
      total_schools: mergedRows.length,
      total_boys: 0,
      total_girls: 0,
      total_enrollment: 0,
      schools_with_enrollment: 0,
      by_class: {},
    };

    // Initialize class stats
    for (const col of classColumns) {
      stats.by_class[col] = 0;
    }

    // Aggregate
    for (const row of mergedRows) {
      let rowTotal = 0;

      for (const col of classColumns) {
        const val = parseInt(row[col], 10) || 0;
        stats.by_class[col] += val;
        rowTotal += val;

        if (col.endsWith('_b')) stats.total_boys += val;
        if (col.endsWith('_g')) stats.total_girls += val;
      }

      if (rowTotal > 0) {
        stats.schools_with_enrollment++;
      }
    }

    stats.total_enrollment = stats.total_boys + stats.total_girls;

    return stats;
  }
}

module.exports = EnrollmentMerger;
