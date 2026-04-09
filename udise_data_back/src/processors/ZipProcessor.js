'use strict';

const AdmZip = require('adm-zip');
const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const config = require('../config');

/**
 * ZipProcessor
 *
 * Responsibilities:
 *  - Accept a ZIP Buffer
 *  - List and extract all CSV entries (in-memory)
 *  - Optionally persist to temp dir for debugging
 */
class ZipProcessor {
  /**
   * @param {object} [opts]
   * @param {boolean} [opts.keepTempFiles] - Persist extracted CSVs to disk.
   */
  constructor(opts = {}) {
    this._keepTempFiles = opts.keepTempFiles ?? config.flags.keepTempFiles;
    this._tempDir = config.output.tempDir;
  }

  /**
   * Extracts all CSV files from a ZIP buffer.
   *
   * @param {Buffer} zipBuffer
   * @param {string} contextLabel - Used for logging and temp file naming.
   * @returns {Array<{ filename: string, content: string }>}
   */
  extract(zipBuffer, contextLabel = 'archive') {
    let zip;
    try {
      zip = new AdmZip(zipBuffer);
    } catch (err) {
      throw new Error(`[ZipProcessor] Failed to parse ZIP for "${contextLabel}": ${err.message}`);
    }

    const entries = zip.getEntries();
    logger.debug(`[ZipProcessor] "${contextLabel}" — ${entries.length} entr(y/ies) found.`);

    const csvFiles = [];

    for (const entry of entries) {
      if (entry.isDirectory) continue;

      const filename = path.basename(entry.entryName);
      const ext = path.extname(filename).toLowerCase();

      if (ext !== '.csv') {
        logger.debug(`[ZipProcessor] Skipping non-CSV entry: ${entry.entryName}`);
        continue;
      }

      const content = entry.getData().toString('utf-8');
      logger.debug(`[ZipProcessor] Extracted CSV "${filename}" (${content.length} chars).`);

      csvFiles.push({ filename, content });

      if (this._keepTempFiles) {
        this._writeTempFile(contextLabel, filename, content);
      }
    }

    if (csvFiles.length === 0) {
      logger.warn(`[ZipProcessor] No CSV files found inside ZIP for "${contextLabel}".`);
    }

    return csvFiles;
  }

  /**
   * Persists a CSV string to the temp directory for debugging.
   *
   * @private
   */
  _writeTempFile(contextLabel, filename, content) {
    try {
      const safeLabel = contextLabel.replace(/[^a-z0-9_-]/gi, '_');
      const dir = path.join(this._tempDir, safeLabel);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, filename), content, 'utf-8');
    } catch (err) {
      logger.warn(`[ZipProcessor] Could not write temp file: ${err.message}`);
    }
  }
}

module.exports = ZipProcessor;
