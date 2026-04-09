'use strict';

const { Readable } = require('stream');
const csv = require('csv-parser');
const logger = require('../utils/logger');

/**
 * CsvProcessor
 *
 * Parses a CSV string into an array of row objects.
 * Uses csv-parser's streaming API for memory efficiency —
 * even a 500 MB CSV is processed row-by-row without loading it all at once.
 */
class CsvProcessor {
  /**
   * Parses a CSV string.
   *
   * @param {string} csvContent - Raw CSV text.
   * @param {string} [label]    - Context label for logging.
   * @returns {Promise<object[]>} Array of parsed row objects.
   */
  parse(csvContent, label = 'csv') {
    return new Promise((resolve, reject) => {
      const rows = [];

      if (!csvContent || csvContent.trim().length === 0) {
        logger.warn(`[CsvProcessor] Empty CSV content for "${label}".`);
        return resolve([]);
      }

      // Strip UTF-8 BOM if present (common in Windows-generated CSVs)
      const clean = csvContent.replace(/^\uFEFF/, '');

      Readable.from([clean])
        .pipe(
          csv({
            // Trim whitespace from header names for consistent key access
            mapHeaders: ({ header }) => header.trim(),
            // Trim whitespace from values
            mapValues: ({ value }) => (typeof value === 'string' ? value.trim() : value),
            // Skip empty lines
            skipEmptyLines: true,
          }),
        )
        .on('data', (row) => rows.push(row))
        .on('end', () => {
          logger.debug(`[CsvProcessor] Parsed ${rows.length} row(s) from "${label}".`);
          resolve(rows);
        })
        .on('error', (err) => {
          logger.error(`[CsvProcessor] Parse error for "${label}": ${err.message}`);
          reject(new Error(`CSV parse failed for "${label}": ${err.message}`));
        });
    });
  }

  /**
   * Parses multiple CSV files in sequence and returns a flat array.
   *
   * @param {Array<{ filename: string, content: string }>} files
   * @returns {Promise<object[][]>} One inner array per file.
   */
  async parseMany(files) {
    const results = [];
    for (const { filename, content } of files) {
      const rows = await this.parse(content, filename);
      results.push(rows);
    }
    return results;
  }
}

module.exports = CsvProcessor;
