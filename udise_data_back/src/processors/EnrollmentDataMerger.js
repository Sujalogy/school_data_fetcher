'use strict';

/**
 * EnrollmentDataMerger - Production Ready Implementation
 *
 * Merges Profile1 (metadata) + Enrollment2 (enrollment data) rows
 * into single unified rows per school (no stacking).
 *
 * Can be used as:
 * 1. Drop-in replacement method in DataNormalizer.js
 * 2. Standalone utility import
 * 3. Reference implementation
 *
 * Usage:
 *   const merger = new EnrollmentDataMerger();
 *   const merged = merger.merge(allRows);
 */

const logger = require('../utils/logger');

class EnrollmentDataMerger {
  /**
   * Main merge function - takes all rows and intelligently merges
   * Profile1 + Enrollment2 based on context
   *
   * @param {Array<Object>} batches - Array of row arrays from all reports
   * @returns {Object} { rows: Array, columns: Array }
   */
  merge(batches) {
    const allRows = batches.flat();

    logger.info(`[EnrollmentMerger] Starting merge process...`);
    logger.debug(`  Total input rows: ${allRows.length.toLocaleString()}`);

    // ── Step 1: Separate rows by report type ──────────────────────────────
    const rowsByReport = this._separateByReport(allRows);

    const profile1Rows = rowsByReport.get('Profile1') || [];
    const enrollment2Rows = rowsByReport.get('Enrollment2') || [];
    const enrollment1Rows = rowsByReport.get('Enrollment1') || [];
    const otherRows = Array.from(rowsByReport.entries())
      .filter(
        ([label]) =>
          label !== 'Profile1' && label !== 'Enrollment1' && label !== 'Enrollment2',
      )
      .flatMap(([, rows]) => rows);

    logger.info(
      `[EnrollmentMerger] Report breakdown:` +
        `\n  Profile1:    ${profile1Rows.length.toLocaleString()} rows` +
        `\n  Enrollment1: ${enrollment1Rows.length.toLocaleString()} rows` +
        `\n  Enrollment2: ${enrollment2Rows.length.toLocaleString()} rows` +
        `\n  Other:       ${otherRows.length.toLocaleString()} rows`,
    );

    // ── Step 2: Merge enrollments with profile ─────────────────────────────
    let mergedRows = [];

    if (profile1Rows.length > 0 && enrollment2Rows.length > 0) {
      mergedRows = this._mergeEnrollmentByContext(profile1Rows, enrollment2Rows);
      logger.info(`[EnrollmentMerger] Merged Profile1 + Enrollment2: ${mergedRows.length.toLocaleString()} unified rows`);
    } else if (profile1Rows.length > 0) {
      mergedRows = profile1Rows;
      logger.warn(`[EnrollmentMerger] No Enrollment2 data found. Using Profile1 only.`);
    }

    // ── Step 3: Handle Enrollment1 separately (if it exists) ──────────────
    if (enrollment1Rows.length > 0) {
      mergedRows.push(...enrollment1Rows);
      logger.info(`[EnrollmentMerger] Added ${enrollment1Rows.length.toLocaleString()} Enrollment1 rows`);
    }

    // ── Step 4: Add other reports ──────────────────────────────────────────
    if (otherRows.length > 0) {
      mergedRows.push(...otherRows);
      logger.info(`[EnrollmentMerger] Added ${otherRows.length.toLocaleString()} other report rows`);
    }

    // ── Step 5: Build column superset ──────────────────────────────────────
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
      `[EnrollmentMerger] Final output: ${mergedRows.length.toLocaleString()} rows, ${columns.length} columns`,
    );

    return { rows: mergedRows, columns };
  }

  /**
   * Separate all rows by _report_label
   * @private
   */
  _separateByReport(rows) {
    const byReport = new Map();

    for (const row of rows) {
      const label = row._report_label || 'Unknown';

      if (!byReport.has(label)) {
        byReport.set(label, []);
      }

      byReport.get(label).push(row);
    }

    return byReport;
  }

  /**
   * Merge Profile1 + Enrollment2 by matching rows within context
   * Context = state + district + block
   * Matching = by row position within context (ordinal position)
   *
   * @private
   */
  _mergeEnrollmentByContext(profile1Rows, enrollment2Rows) {
    logger.info(
      `[EnrollmentMerger] Merging by context (state::district::block) + row order...`,
    );

    // Group both sets by context
    const profile1ByContext = this._groupByContext(profile1Rows);
    const enrollment2ByContext = this._groupByContext(enrollment2Rows);

    logger.debug(
      `  Profile1 contexts: ${profile1ByContext.size}` +
        `\n  Enrollment2 contexts: ${enrollment2ByContext.size}`,
    );

    const merged = [];
    let matchedCount = 0;
    let unmatchedCount = 0;

    // Process each context
    for (const [contextKey, profileRows] of profile1ByContext.entries()) {
      const enrollmentRows = enrollment2ByContext.get(contextKey) || [];

      // Merge by position within context
      for (let i = 0; i < profileRows.length; i++) {
        const profileRow = profileRows[i];
        const enrollmentRow = enrollmentRows[i]; // Key: position-based match

        if (enrollmentRow) {
          // Merge found
          const mergedRow = this._deepMerge(profileRow, enrollmentRow);
          merged.push(mergedRow);
          matchedCount++;
        } else {
          // No enrollment for this position: use profile as-is
          merged.push(profileRow);
          unmatchedCount++;
        }
      }
    }

    logger.debug(
      `  Merge results:` +
        `\n    Matched:   ${matchedCount.toLocaleString()}` +
        `\n    Unmatched: ${unmatchedCount.toLocaleString()}`,
    );

    return merged;
  }

  /**
   * Group rows by geographic context
   * @private
   */
  _groupByContext(rows) {
    const groups = new Map();

    for (const row of rows) {
      const stateId = row._state_id || '';
      const districtId = row._district_id || '';
      const block = row.block || row.lgd_block_name || '';

      const contextKey = `${stateId}::${districtId}::${block}`;

      if (!groups.has(contextKey)) {
        groups.set(contextKey, []);
      }

      groups.get(contextKey).push(row);
    }

    return groups;
  }

  /**
   * Deep merge two rows: profile takes precedence, enrollment fills gaps
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
      // Otherwise keep original profile value
    }

    return merged;
  }

  /**
   * Get statistics about enrollment data coverage
   * @static
   */
  static getStats(rows) {
    const enrollmentCols = [
      'c1_b', 'c1_g', 'c2_b', 'c2_g', 'c3_b', 'c3_g',
      'c4_b', 'c4_g', 'c5_b', 'c5_g', 'c6_b', 'c6_g',
      'c7_b', 'c7_g', 'c8_b', 'c8_g', 'c9_b', 'c9_g',
      'c10_b', 'c10_g', 'c11_b', 'c11_g', 'c12_b', 'c12_g',
    ];

    const stats = {
      total_rows: rows.length,
      rows_with_enrollment: 0,
      total_boys: 0,
      total_girls: 0,
      total_enrollment: 0,
      by_class: {},
    };

    for (const col of enrollmentCols) {
      stats.by_class[col] = 0;
    }

    for (const row of rows) {
      let rowTotal = 0;

      for (const col of enrollmentCols) {
        const val = parseInt(row[col], 10) || 0;
        stats.by_class[col] += val;
        rowTotal += val;

        if (col.endsWith('_b')) {
          stats.total_boys += val;
        } else if (col.endsWith('_g')) {
          stats.total_girls += val;
        }
      }

      if (rowTotal > 0) {
        stats.rows_with_enrollment++;
      }
    }

    stats.total_enrollment = stats.total_boys + stats.total_girls;

    return stats;
  }
}

module.exports = EnrollmentDataMerger;

// ── USAGE EXAMPLE ──────────────────────────────────────────────────────────

/*

// Option 1: Use in DataNormalizer.merge()
const DataNormalizer = require('./DataNormalizer');
const merger = new EnrollmentDataMerger();

class DataNormalizer {
  merge(batches) {
    const mergedResult = merger.merge(batches);
    return mergedResult;  // Returns { rows, columns }
  }
}


// Option 2: Standalone
const EnrollmentDataMerger = require('./EnrollmentDataMerger');

const merger = new EnrollmentDataMerger();
const result = merger.merge(allBatches);

console.log(`Total rows: ${result.rows.length}`);
console.log(`Total columns: ${result.columns.length}`);

const stats = EnrollmentDataMerger.getStats(result.rows);
console.log(`Schools with enrollment: ${stats.rows_with_enrollment}`);
console.log(`Total enrollment: ${stats.total_enrollment}`);


// Option 3: For debugging
const DEBUG = true;
if (DEBUG) {
  const stats = EnrollmentDataMerger.getStats(result.rows);
  
  console.table({
    'Total Rows': stats.total_rows,
    'With Enrollment': stats.rows_with_enrollment,
    'Total Boys': stats.total_boys,
    'Total Girls': stats.total_girls,
    'Total Enrollment': stats.total_enrollment,
  });
  
  console.log('\nClass-wise counts:');
  console.table(stats.by_class);
}

*/
