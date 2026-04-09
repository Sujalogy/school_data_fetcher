'use strict';

const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');
const logger = require('../utils/logger');
const config = require('../config');

// ── Management Code Lookup ────────────────────────────────────────────────────
const MANAGEMENT_CODES = {
  '1': 'Department of Education',
  '2': 'Tribal Welfare Department',
  '3': 'Local Body',
  '4': 'Government Aided',
  '5': 'Private Unaided (Recognized)',
  '6': 'Other State Govt. Managed',
  '7': 'Partially Govt. Aided',
  '89': 'Minority Affairs Department',
  '90': 'Social Welfare Department',
  '91': 'Ministry of Labour',
  '92': 'Kendriya Vidyalaya Sangathan',
  '93': 'Navodaya Vidyalaya Samiti',
  '94': 'Sainik School',
  '95': 'Railway School',
  '96': 'Central Tibetan School',
  '97': 'Madrasa Private Unaided (Recognized)',
  '99': 'Madrasa Aided (Recognized)',
  '101': 'Other Central Govt./PSU Schools',
  '102': 'Veda Schools/Gurukuls/Pathshalas',
};

// ── School Category Lookup ────────────────────────────────────────────────────
const SCHOOL_CATEGORIES = {
  '1': 'Primary School',
  '2': 'Upper Primary School',
  '3': 'Higher Secondary School',
  '4': 'Upper Primary School',
  '5': 'Higher Secondary School',
  '6': 'Secondary School',
  '7': 'Secondary School',
  '8': 'Secondary School',
  '10': 'Higher Secondary School',
  '11': 'Higher Secondary School',
  '12': 'Pre-Primary School',
};

// ── OPTIMIZED: Only needed columns from Schema ─────────────────────────────────
const NEEDED_COLUMNS = [
  'pseudocode',
  '_state_name',
  '_district_name',
  'lgd_block_name',
  'block',
  'school_category',
  'managment',
  'same_sch_b',
  'same_sch_g',
  'other_sch_b',
  'other_sch_g',
  'anganwadi_ecce_b',
  'anganwadi_ecce_g',
  'c1_b',
  'c1_g',
];

/**
 * PreSchoolExperienceExporter — OPTIMIZED
 *
 * Performance-optimized report generator for pre-school experience analysis.
 * 
 * Generates 4 sheets:
 * 1. Master        — Complete aggregated data (state, district, block, category, management)
 * 2. State Level   — Aggregated by state, management, school_category
 * 3. District Level — Aggregated by state, district, management, school_category
 * 4. Block Level   — Aggregated by state, district, block, management, school_category
 * 
 * Each row shows:
 * - Class 1 Total Enrolment (c1_b + c1_g)
 * - Same School (same_sch_b + same_sch_g)
 * - Other School (other_sch_b + other_sch_g)
 * - Anganwadi/ECCE (anganwadi_ecce_b + anganwadi_ecce_g)
 * 
 * Broken down by: Total | Girls | Boys
 */
class PreSchoolExperienceExporter {
  constructor() {
    this._outputDir = config.output.dir;
    this._preExpDir = path.join(this._outputDir, 'pre_experience');
    this._fileNameTemplate = 'new_admission_pre_exp_2024_25';
  }

  /**
   * @param {object[]} rows - All normalised rows (only needed columns)
   * @returns {Promise<string>} Absolute path to written file, or null if no rows
   */
  async write(rows) {
    if (rows.length === 0) {
      logger.warn('[PreSchoolExperienceExporter] No rows provided - skipping report');
      return null;
    }

    const stateName = rows[0]._state_name || 'AllStates';
    fs.mkdirSync(this._preExpDir, { recursive: true });
    
    const fileName = `${this._fileNameTemplate}_${stateName.replace(/\s+/g, '_')}.xlsx`;
    const outputPath = path.resolve(this._preExpDir, fileName);

    logger.info(`[PreSchoolExperienceExporter] Building optimized pre-school experience report…`);
    const startTime = Date.now();

    // Aggregate data efficiently in a single pass
    const aggregatedData = this._aggregateData(rows);

    // Create workbook with minimal styling for speed
    const wb = new ExcelJS.Workbook();
    wb.creator = 'UDISE ETL Pipeline';
    wb.created = new Date();

    // Build sheets from aggregated data
    this._buildMasterSheet(wb, aggregatedData.master);
    this._buildStateLevelSheet(wb, aggregatedData.state);
    this._buildDistrictLevelSheet(wb, aggregatedData.district);
    this._buildBlockLevelSheet(wb, aggregatedData.block);

    await wb.xlsx.writeFile(outputPath);
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    logger.info(`[PreSchoolExperienceExporter] ✅ Report saved in ${elapsed}s → ${outputPath}`);
    return outputPath;
  }

  /**
   * Single-pass aggregation for all levels
   * @private
   */
  _aggregateData(rows) {
    const master = new Map();
    const state = new Map();
    const district = new Map();
    const block = new Map();

    // Build lookup map: pseudocode -> {c1_b, c1_g}
    // SUM values by pseudocode since there may be multiple rows per school
    const c1Lookup = new Map();
    for (const row of rows) {
      const pseudocode = row.pseudocode;
      const c1_b = this._parseNum(row.c1_b);
      const c1_g = this._parseNum(row.c1_g);
      
      // Store/accumulate C1 data by pseudocode
      if (pseudocode && (c1_b > 0 || c1_g > 0)) {
        if (!c1Lookup.has(pseudocode)) {
          c1Lookup.set(pseudocode, { c1_b: 0, c1_g: 0 });
        }
        const existing = c1Lookup.get(pseudocode);
        existing.c1_b += c1_b;
        existing.c1_g += c1_g;
      }
    }

    logger.info(`[PreSchoolExperienceExporter] Built C1 lookup: ${c1Lookup.size} unique school(s) with enrollment data`);

    for (const row of rows) {
      const stateName = row._state_name || 'Unknown';
      const districtName = row._district_name || 'Unknown';
      const blockName = row.lgd_block_name || row.block || 'Unknown';
      const management = this._getManagementName(row.managment);
      const category = this._getCategoryName(row.school_category);

      // Skip rows with undefined management or category codes
      if (!row.managment || !row.school_category) {
        continue;
      }

      // Parse numeric values
      let c1_b = this._parseNum(row.c1_b);
      let c1_g = this._parseNum(row.c1_g);
      
      const same_sch_b = this._parseNum(row.same_sch_b);
      const same_sch_g = this._parseNum(row.same_sch_g);
      const other_sch_b = this._parseNum(row.other_sch_b);
      const other_sch_g = this._parseNum(row.other_sch_g);
      const anganwadi_b = this._parseNum(row.anganwadi_ecce_b);
      const anganwadi_g = this._parseNum(row.anganwadi_ecce_g);

      // If C1 data is missing but we have school history data, look up by pseudocode
      // Use the aggregated C1 values (summed if multiple rows for same pseudocode)
      if ((c1_b === 0 && c1_g === 0) && (same_sch_b > 0 || same_sch_g > 0 || other_sch_b > 0 || other_sch_g > 0 || anganwadi_b > 0 || anganwadi_g > 0)) {
        const pseudocode = row.pseudocode;
        if (pseudocode && c1Lookup.has(pseudocode)) {
          const lookupData = c1Lookup.get(pseudocode);
          c1_b = lookupData.c1_b;
          c1_g = lookupData.c1_g;
        }
      }

      // 1. Master aggregation
      const masterKey = `${stateName}|${districtName}|${blockName}|${management}|${category}`;
      this._updateAggregation(master, masterKey, {
        state: stateName,
        district: districtName,
        block: blockName,
        management,
        category,
      }, { c1_b, c1_g, same_sch_b, same_sch_g, other_sch_b, other_sch_g, anganwadi_b, anganwadi_g });

      // 2. State level
      const stateKey = `${stateName}|${management}|${category}`;
      this._updateAggregation(state, stateKey, {
        state: stateName,
        management,
        category,
      }, { c1_b, c1_g, same_sch_b, same_sch_g, other_sch_b, other_sch_g, anganwadi_b, anganwadi_g });

      // 3. District level
      const districtKey = `${stateName}|${districtName}|${management}|${category}`;
      this._updateAggregation(district, districtKey, {
        state: stateName,
        district: districtName,
        management,
        category,
      }, { c1_b, c1_g, same_sch_b, same_sch_g, other_sch_b, other_sch_g, anganwadi_b, anganwadi_g });

      // 4. Block level
      const blockKey = `${stateName}|${districtName}|${blockName}|${management}|${category}`;
      this._updateAggregation(block, blockKey, {
        state: stateName,
        district: districtName,
        block: blockName,
        management,
        category,
      }, { c1_b, c1_g, same_sch_b, same_sch_g, other_sch_b, other_sch_g, anganwadi_b, anganwadi_g });
    }

    return { master, state, district, block };
  }

  /**
   * Update aggregation map with row data
   * @private
   */
  _updateAggregation(map, key, dimensions, metrics) {
    if (!map.has(key)) {
      map.set(key, {
        ...dimensions,
        c1_b: 0, c1_g: 0,
        same_sch_b: 0, same_sch_g: 0,
        other_sch_b: 0, other_sch_g: 0,
        anganwadi_b: 0, anganwadi_g: 0,
      });
    }
    const entry = map.get(key);
    entry.c1_b += metrics.c1_b;
    entry.c1_g += metrics.c1_g;
    entry.same_sch_b += metrics.same_sch_b;
    entry.same_sch_g += metrics.same_sch_g;
    entry.other_sch_b += metrics.other_sch_b;
    entry.other_sch_g += metrics.other_sch_g;
    entry.anganwadi_b += metrics.anganwadi_b;
    entry.anganwadi_g += metrics.anganwadi_g;
  }

  /**
   * Build Master sheet with complete hierarchy
   * @private
   */
  _buildMasterSheet(wb, masterData) {
    const ws = wb.addWorksheet('Master', { views: [{ state: 'frozen', ySplit: 1 }] });
    this._populateSheet(
      ws,
      [...masterData.values()],
      ['State', 'District', 'Block', 'Management', 'Category'],
    );
    logger.info(`[PreSchoolExperienceExporter] Master sheet: ${masterData.size} record(s)`);
  }

  /**
   * Build State Level sheet
   * @private
   */
  _buildStateLevelSheet(wb, stateData) {
    const ws = wb.addWorksheet('State Level', { views: [{ state: 'frozen', ySplit: 1 }] });
    this._populateSheet(
      ws,
      [...stateData.values()],
      ['State', 'Management', 'Category'],
    );
    logger.info(`[PreSchoolExperienceExporter] State Level sheet: ${stateData.size} record(s)`);
  }

  /**
   * Build District Level sheet
   * @private
   */
  _buildDistrictLevelSheet(wb, districtData) {
    const ws = wb.addWorksheet('District Level', { views: [{ state: 'frozen', ySplit: 1 }] });
    this._populateSheet(
      ws,
      [...districtData.values()],
      ['State', 'District', 'Management', 'Category'],
    );
    logger.info(`[PreSchoolExperienceExporter] District Level sheet: ${districtData.size} record(s)`);
  }

  /**
   * Build Block Level sheet
   * @private
   */
  _buildBlockLevelSheet(wb, blockData) {
    const ws = wb.addWorksheet('Block Level', { views: [{ state: 'frozen', ySplit: 1 }] });
    this._populateSheet(
      ws,
      [...blockData.values()],
      ['State', 'District', 'Block', 'Management', 'Category'],
    );
    logger.info(`[PreSchoolExperienceExporter] Block Level sheet: ${blockData.size} record(s)`);
  }

  /**
   * Populate sheet with data
   * Structure: Dimensions | [Total | Girls | Boys] × [C1 Total | Same | Other | Anganwadi]
   * Note: dimensions may already include Management and Category, so don't duplicate
   * @private
   */
  _populateSheet(ws, data, dimensions) {
    // Headers - dimensions already include Management and Category where needed
    const headers = [
      ...dimensions,
      // Total section headers
      'Total - C1 Enrolment',
      'Total - Same School',
      'Total - Other School',
      'Total - Anganwadi/ECCE',
      // Girls section
      'Girls - C1 Enrolment',
      'Girls - Same School',
      'Girls - Other School',
      'Girls - Anganwadi/ECCE',
      // Boys section
      'Boys - C1 Enrolment',
      'Boys - Same School',
      'Boys - Other School',
      'Boys - Anganwadi/ECCE',
    ];

    // Set columns
    ws.columns = headers.map((h) => ({
      header: h,
      key: h,
      width: Math.min(Math.max(h.length + 2, 14), 35),
    }));

    // Style header row minimally
    const headerRow = ws.getRow(1);
    headerRow.font = { name: 'Arial', bold: true, size: 10, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F3864' } };
    headerRow.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
    headerRow.height = 20;

    // Sort data by management, then by category
    const sortedData = [...data].sort((a, b) => {
      const mgmtA = (a.management || '').toString();
      const mgmtB = (b.management || '').toString();
      if (mgmtA !== mgmtB) return mgmtA.localeCompare(mgmtB);
      return (a.category || '').toString().localeCompare((b.category || '').toString());
    });

    // Add data rows
    let rowIdx = 2;
    let totals = {
      c1_b: 0, c1_g: 0,
      same_sch_b: 0, same_sch_g: 0,
      other_sch_b: 0, other_sch_g: 0,
      anganwadi_b: 0, anganwadi_g: 0,
    };

    for (const entry of sortedData) {
      const c1_total = entry.c1_b + entry.c1_g;
      const same_total = entry.same_sch_b + entry.same_sch_g;
      const other_total = entry.other_sch_b + entry.other_sch_g;
      const anganwadi_total = entry.anganwadi_b + entry.anganwadi_g;

      const rowData = [
        ...dimensions.map(d => entry[d.toLowerCase()] || ''),
        // Total
        c1_total,
        same_total,
        other_total,
        anganwadi_total,
        // Girls
        entry.c1_g,
        entry.same_sch_g,
        entry.other_sch_g,
        entry.anganwadi_g,
        // Boys
        entry.c1_b,
        entry.same_sch_b,
        entry.other_sch_b,
        entry.anganwadi_b,
      ];

      const wsRow = ws.addRow(rowData);
      wsRow.font = { name: 'Arial', size: 9 };

      // Right-align numbers (minimal styling for speed)
      wsRow.eachCell((cell, colNum) => {
        if (colNum > dimensions.length + 2) {
          cell.numFmt = '#,##0';
          cell.alignment = { horizontal: 'right' };
        }
      });

      // Accumulate totals
      totals.c1_b += entry.c1_b;
      totals.c1_g += entry.c1_g;
      totals.same_sch_b += entry.same_sch_b;
      totals.same_sch_g += entry.same_sch_g;
      totals.other_sch_b += entry.other_sch_b;
      totals.other_sch_g += entry.other_sch_g;
      totals.anganwadi_b += entry.anganwadi_b;
      totals.anganwadi_g += entry.anganwadi_g;

      rowIdx++;
    }

    // Add totals row
    const c1_total = totals.c1_b + totals.c1_g;
    const same_total = totals.same_sch_b + totals.same_sch_g;
    const other_total = totals.other_sch_b + totals.other_sch_g;
    const anganwadi_total = totals.anganwadi_b + totals.anganwadi_g;

    const totalsRowData = [
      ...dimensions.map(() => (dimensions.length === 1 ? 'TOTAL' : '')),
      // Total
      c1_total,
      same_total,
      other_total,
      anganwadi_total,
      // Girls
      totals.c1_g,
      totals.same_sch_g,
      totals.other_sch_g,
      totals.anganwadi_g,
      // Boys
      totals.c1_b,
      totals.same_sch_b,
      totals.other_sch_b,
      totals.anganwadi_b,
    ];

    const totalsRow = ws.addRow(totalsRowData);
    totalsRow.font = { name: 'Arial', bold: true, size: 9, color: { argb: 'FFFFFFFF' } };
    totalsRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF366092' } };
    totalsRow.eachCell((cell, colNum) => {
      cell.numFmt = '#,##0';
      cell.alignment = { horizontal: 'right' };
    });

    // Add autofilter
    ws.autoFilter = {
      from: { row: 1, column: 1 },
      to: { row: rowIdx - 1, column: headers.length },
    };
  }

  /**
   * Safe numeric parsing
   * @private
   */
  _parseNum(val) {
    if (val === null || val === undefined || val === '') return 0;
    const num = Number(val);
    return isNaN(num) ? 0 : num;
  }

  /**
   * Convert management code to name
   * @private
   */
  _getManagementName(code) {
    return MANAGEMENT_CODES[String(code)] || `Management (${code})`;
  }

  /**
   * Convert category code to name
   * @private
   */
  _getCategoryName(code) {
    return SCHOOL_CATEGORIES[String(code)] || `Category (${code})`;
  }
}

module.exports = PreSchoolExperienceExporter;
