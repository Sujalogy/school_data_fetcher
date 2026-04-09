'use strict';

const ExcelJS = require('exceljs');
const path    = require('path');
const fs      = require('fs');
const { createWriteStream } = require('fs');
const logger  = require('../utils/logger');
const config  = require('../config');

// ── Styling constants ────────────────────────────────────────────────────────

const FONTS = {
  header: { name: 'Arial', bold: true, size: 10, color: { argb: 'FFFFFFFF' } },
  meta:   { name: 'Arial', size: 9,  color: { argb: 'FF1F3864' } },
  data:   { name: 'Arial', size: 9 },
  title:  { name: 'Arial', bold: true, size: 11 },
};

const FILLS = {
  headerNavy:   { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3864' } },
  headerGreen:  { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF375623' } },
  headerMaroon: { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF7B2C2C' } },
  metaCol:      { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } },
  altRow:       { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF5F5F5' } },
};

const BORDER = {
  top:    { style: 'thin', color: { argb: 'FFBFBFBF' } },
  left:   { style: 'thin', color: { argb: 'FFBFBFBF' } },
  bottom: { style: 'thin', color: { argb: 'FFBFBFBF' } },
  right:  { style: 'thin', color: { argb: 'FFBFBFBF' } },
};

const META_PREFIX = '_';

/**
 * ExcelExporter
 *
 * Writes a professionally styled multi-sheet XLSX workbook:
 *   • Master     — every row, every district, every state, every report
 *   • Summary    — state × district × report row-count pivot
 *   • Districts  — full district directory with state names
 *   • States     — state catalogue used in this run
 */
class ExcelExporter {
  constructor() {
    this._outputDir  = config.output.dir;
    this._fileName   = config.output.masterFileName;
    this._masterSheet = config.output.masterSheetName;
  }

  /**
   * @param {object}   payload
   * @param {object[]} payload.rows       All normalised rows.
   * @param {string[]} payload.columns    Ordered superset of column names.
   * @param {object[]} payload.districts  [{ stateId, stateName, districtId, districtName }]
   * @param {object[]} payload.states     [{ stateId, stateName }]
   * @returns {Promise<string>} Absolute path to written file.
   */
  async write({ rows, columns, districts, states }) {
    fs.mkdirSync(this._outputDir, { recursive: true });
    const outputPath = path.resolve(this._outputDir, this._fileName);

    logger.info(
      `[ExcelExporter] Building workbook — ` +
        `${rows.length.toLocaleString()} row(s), ${columns.length} column(s), ` +
        `${districts.length} district(s), ${states.length} state(s)…`,
    );

    // For large datasets, use streaming to avoid memory overflow
    // Lowered threshold from 100k to 50k to prevent heap overflow
    const isMassiveDataset = rows.length > 50000;

    if (isMassiveDataset) {
      return await this._writeStreamingLargeDataset(outputPath, rows, columns, districts, states);
    } else {
      return await this._writeStandardWorkbook(outputPath, rows, columns, districts, states);
    }
  }

  /**
   * Standard write for smaller datasets with full styling.
   * @private
   */
  async _writeStandardWorkbook(outputPath, rows, columns, districts, states) {
    const wb = new ExcelJS.Workbook();
    wb.creator  = 'UDISE ETL Pipeline';
    wb.created  = new Date();
    wb.modified = new Date();

    this._buildMasterSheet(wb, rows, columns);
    this._buildSummarySheet(wb, rows);
    this._buildDistrictSheet(wb, districts);
    this._buildStateSheet(wb, states);

    await wb.xlsx.writeFile(outputPath);
    logger.info(`[ExcelExporter] ✅ Workbook saved → ${outputPath}`);
    return outputPath;
  }

  /**
   * Streaming write for large datasets to minimize memory usage.
   * For datasets >= 50k rows, writes master data to CSV instead to avoid heap overflow.
   * @private
   */
  async _writeStreamingLargeDataset(outputPath, rows, columns, districts, states) {
    logger.info(`[ExcelExporter] Using streaming mode for large dataset.`);

    // Force garbage collection before starting
    if (global.gc) {
      logger.debug('[ExcelExporter] Running garbage collection…');
      global.gc();
    }

    // For datasets >= 50k rows, always export to CSV to avoid memory overflow
    // ExcelJS cannot handle materializing 50k+ rows in memory even without styling
    let csvPath = outputPath.replace('.xlsx', '_master.csv');
    logger.info(`[ExcelExporter] Writing ${rows.length.toLocaleString()} rows to CSV: ${csvPath}`);
    await this._writeDataToCsv(csvPath, rows, columns);

    // Build Excel workbook with ONLY summary, districts, and states sheets
    const wb = new ExcelJS.Workbook();
    wb.creator  = 'UDISE ETL Pipeline';
    wb.created  = new Date();
    wb.modified = new Date();

    // Add info sheet explaining where the data is
    const noteWs = wb.addWorksheet('Info');
    noteWs.addRow(['UDISE ETL Pipeline — Master Data Export']);
    noteWs.addRow(['']);
    noteWs.addRow(['Total Rows:', rows.length.toLocaleString()]);
    noteWs.addRow(['Total Columns:', columns.length]);
    noteWs.addRow(['']);
    noteWs.addRow(['Master data has been exported to CSV to handle the large dataset efficiently:']);
    noteWs.addRow([`  File: ${path.basename(csvPath)}`]);
    noteWs.addRow(['']);
    noteWs.addRow(['Other sheets in this workbook:']);
    noteWs.addRow(['  • Summary — Pivot table of row counts by district/report']);
    noteWs.addRow(['  • Districts — District directory']);
    noteWs.addRow(['  • States — State catalogue']);

    // Build Summary, Districts, and States sheets
    logger.info(`[ExcelExporter] Building Summary sheet…`);
    this._buildSummarySheet(wb, rows);

    logger.info(`[ExcelExporter] Building Districts sheet…`);
    this._buildDistrictSheet(wb, districts);

    logger.info(`[ExcelExporter] Building States sheet…`);
    this._buildStateSheet(wb, states);

    // Write Excel (now much smaller since no Master data)
    logger.info(`[ExcelExporter] Writing workbook…`);
    await wb.xlsx.writeFile(outputPath);

    logger.info(`[ExcelExporter] ✅ Export complete:`);
    logger.info(`     Master data → ${csvPath}`);
    logger.info(`     Summary/Index → ${outputPath}`);

    return outputPath;
  }

  /**
   * Writes rows to a CSV file, streaming to minimize memory.
   * @private
   */
  async _writeDataToCsv(csvPath, rows, columns) {
    return new Promise((resolve, reject) => {
      const output = createWriteStream(csvPath, { encoding: 'utf-8' });

      output.on('error', reject);

      // Write header
      const header = columns.map((col) => this._escapeCsvValue(col)).join(',');
      output.write(header + '\n');

      let rowCount = 0;
      for (const row of rows) {
        const values = columns.map((col) => {
          const val = row[col];
          return this._escapeCsvValue(val ?? '');
        });
        output.write(values.join(',') + '\n');
        rowCount++;

        if (rowCount % 100000 === 0) {
          logger.debug(`[ExcelExporter] CSV: Written ${rowCount.toLocaleString()} rows.`);
          if (global.gc) global.gc();
        }
      }

      output.end(() => {
        logger.info(`[ExcelExporter] CSV written: ${rowCount.toLocaleString()} rows → ${csvPath}`);
        resolve();
      });
    });
  }

  /**
   * Escapes values for CSV output.
   * @private
   */
  _escapeCsvValue(val) {
    const str = String(val);
    if (str.includes(',') || str.includes('"') || str.includes('\n')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  }

  // ── Sheet builders ──────────────────────────────────────────────────────────

  _buildMasterSheet(wb, rows, columns) {
    const ws = wb.addWorksheet(this._masterSheet, {
      views: [{ state: 'frozen', ySplit: 1 }],
    });

    ws.columns = columns.map((col) => ({
      header: col, key: col, width: this._colWidth(col),
    }));

    this._styleHeaderRow(ws.getRow(1), FILLS.headerNavy);

    // For datasets > 50k rows, skip per-cell styling to save memory
    if (rows.length > 50000) {
      logger.debug(`[ExcelExporter] Skipping per-cell styling for ${rows.length} rows (memory optimization)`);
      for (const row of rows) {
        ws.addRow(columns.map((col) => row[col] ?? ''));
      }
    } else {
      // For smaller datasets, apply full styling
      let rowIdx = 2;
      for (const row of rows) {
        const wsRow = ws.addRow(columns.map((col) => row[col] ?? ''));
        wsRow.height = 14;
        wsRow.eachCell({ includeEmpty: true }, (cell, colNum) => {
          const colName = columns[colNum - 1] ?? '';
          cell.font   = colName.startsWith(META_PREFIX) ? FONTS.meta : FONTS.data;
          cell.border = BORDER;
          cell.alignment = { vertical: 'top' };
          if (colName.startsWith(META_PREFIX)) cell.fill = FILLS.metaCol;
          else if (rowIdx % 2 === 0)           cell.fill = FILLS.altRow;
        });
        rowIdx++;
      }
    }

    ws.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: columns.length } };
    logger.info(`[ExcelExporter] Master sheet: ${rows.length} data row(s).`);
  }

  _buildSummarySheet(wb, rows) {
    const ws = wb.addWorksheet('Summary', {
      views: [{ state: 'frozen', ySplit: 1 }],
    });

    // Build pivot: key = stateId|districtId  →  { stateName, districtName, [reportLabel]: count }
    const pivot = new Map();
    const reportLabels = new Set();

    for (const row of rows) {
      const key = `${row._state_id}|${row._district_id}`;
      const rLabel = row._report_label ?? String(row._report_id);
      reportLabels.add(rLabel);

      if (!pivot.has(key)) {
        pivot.set(key, {
          _state_id:      row._state_id,
          _state_name:    row._state_name,
          _district_id:   row._district_id,
          _district_name: row._district_name,
        });
      }
      const entry = pivot.get(key);
      entry[rLabel] = (entry[rLabel] ?? 0) + 1;
    }

    // Force garbage collection after building pivot
    if (global.gc) {
      logger.debug('[ExcelExporter] Garbage collection before Summary styling…');
      global.gc();
    }

    const sortedReports = [...reportLabels].sort();
    const columns = ['State ID', 'State Name', 'District ID', 'District Name', ...sortedReports, 'Total Rows'];

    ws.columns = columns.map((c) => ({ header: c, key: c, width: 20 }));
    this._styleHeaderRow(ws.getRow(1), FILLS.headerGreen);

    let rowIdx = 2;
    for (const entry of pivot.values()) {
      const total = sortedReports.reduce((s, r) => s + (entry[r] ?? 0), 0);
      const wsRow = ws.addRow([
        entry._state_id,
        entry._state_name,
        entry._district_id,
        entry._district_name,
        ...sortedReports.map((r) => entry[r] ?? 0),
        total,
      ]);
      wsRow.eachCell((cell) => {
        cell.font = FONTS.data; cell.border = BORDER;
        if (rowIdx % 2 === 0) cell.fill = FILLS.altRow;
      });
      rowIdx++;
    }

    ws.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: columns.length } };
    logger.info(`[ExcelExporter] Summary sheet: ${pivot.size} district(s) pivoted.`);
  }

  _buildDistrictSheet(wb, districts) {
    const ws = wb.addWorksheet('Districts');
    ws.columns = [
      { header: 'State ID',     key: 'stateId',      width: 12 },
      { header: 'State Name',   key: 'stateName',    width: 35 },
      { header: 'District ID',  key: 'districtId',   width: 14 },
      { header: 'District Name',key: 'districtName', width: 30 },
    ];
    this._styleHeaderRow(ws.getRow(1), FILLS.headerNavy);

    let rowIdx = 2;
    for (const d of districts) {
      const wsRow = ws.addRow([d.stateId, d.stateName, d.districtId, d.districtName]);
      wsRow.eachCell((cell) => {
        cell.font = FONTS.data; cell.border = BORDER;
        if (rowIdx % 2 === 0) cell.fill = FILLS.altRow;
      });
      rowIdx++;
    }
    logger.info(`[ExcelExporter] Districts sheet: ${districts.length} row(s).`);
  }

  _buildStateSheet(wb, states) {
    const ws = wb.addWorksheet('States');
    ws.columns = [
      { header: 'State ID',   key: 'stateId',   width: 12 },
      { header: 'State Name', key: 'stateName', width: 40 },
    ];
    this._styleHeaderRow(ws.getRow(1), FILLS.headerMaroon);

    let rowIdx = 2;
    for (const s of states) {
      const wsRow = ws.addRow([s.stateId, s.stateName]);
      wsRow.eachCell((cell) => {
        cell.font = FONTS.data; cell.border = BORDER;
        if (rowIdx % 2 === 0) cell.fill = FILLS.altRow;
      });
      rowIdx++;
    }
    logger.info(`[ExcelExporter] States sheet: ${states.length} row(s).`);
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  _styleHeaderRow(row, fill) {
    row.height = 18;
    row.eachCell((cell) => {
      cell.font      = FONTS.header;
      cell.fill      = fill;
      cell.border    = BORDER;
      cell.alignment = { vertical: 'middle', wrapText: false };
    });
  }

  _colWidth(colName) {
    return Math.min(Math.max(colName.length + 4, 12), 40);
  }
}

module.exports = ExcelExporter;
