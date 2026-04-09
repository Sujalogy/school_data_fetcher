'use strict';

const KysApiClient    = require('../clients/KysApiClient');
const DistrictService = require('../services/DistrictService');
const ReportService   = require('../services/ReportService');
const DataNormalizer  = require('../processors/DataNormalizer');
const ExcelExporter   = require('../output/ExcelExporter');
const PreSchoolExperienceExporter = require('../output/PreSchoolExperienceExporter');
const logger = require('../utils/logger');
const config = require('../config');
const { mapWithLimit } = require('../utils/concurrency');
const MicroSchoolRepository = require('../repositories/MicroSchoolRepository');

/**
 * EtlPipeline
 *
 * Six-stage orchestrator:
 *
 *   Stage 1 — Fetch year list (informational, non-fatal)
 *   Stage 2 — Iterate all active states → fetch districts for each
 *   Stage 3 — For every district → process all reports (parallel, with limits)
 *   Stage 4 — Merge all rows into one flat dataset
 *   Stage 5 — Export to a styled multi-sheet Excel workbook
 *   Stage 6 — Generate pre-school experience analysis report
 */
class EtlPipeline {
  constructor() {
    this._kysClient      = new KysApiClient();
    this._districtService = new DistrictService();
    this._reportService   = new ReportService();
    this._normalizer      = new DataNormalizer();
    this._exporter        = new ExcelExporter();
    this._preSchoolExporter = new PreSchoolExperienceExporter();
    this._microSchoolRepo = new MicroSchoolRepository();
  }

  // ── Public ─────────────────────────────────────────────────────────────────

  async run() {
    const pipelineStart = Date.now();
    const { states, yearId } = config.pipeline;

    logger.info('═══════════════════════════════════════════════════════════');
    logger.info('  UDISE+ ETL Pipeline — Starting');
    logger.info(`  States    : ${states.length} (${states.map((s) => s.stateName).join(', ')})`);
    logger.info(`  Year ID   : ${yearId}`);
    logger.info(`  Reports   : ${config.pipeline.reports.map((r) => `${r.label}(${r.reportId})`).join(', ')}`);
    logger.info(`  Dry Run   : ${config.flags.dryRun}`);
    logger.info('═══════════════════════════════════════════════════════════');

    const errors = [];

    // ── Stage 1: Year list ──────────────────────────────────────────────────
    await this._stage('Stage 1 — Fetch Year List', async () => {
      try {
        const years = await this._kysClient.fetchYearList();
        logger.info(`  Available years: ${years.map((y) => JSON.stringify(y)).join(' | ')}`);
      } catch (err) {
        logger.warn(`  Year list unavailable (${err.message}). Continuing.`);
        errors.push(`Stage 1 non-fatal: ${err.message}`);
      }
    });

    // ── Stage 2: Districts for every state ──────────────────────────────────
    /** @type {Array<{ stateId, stateName, districtId, districtName }>} */
    let allDistricts = [];

    await this._stage('Stage 2 — Fetch Districts (all states)', async () => {
      if (config.flags.dryRun) {
        // Synthetic districts — two per state so concurrency is exercised
        allDistricts = states.flatMap((s) => [
          { stateId: s.stateId, stateName: s.stateName, districtId: 9001, districtName: `${s.stateName}_DRY_A` },
          { stateId: s.stateId, stateName: s.stateName, districtId: 9002, districtName: `${s.stateName}_DRY_B` },
        ]);
        logger.info(`  [DRY RUN] Using ${allDistricts.length} synthetic district(s).`);
        return;
      }

      // Fetch districts state-by-state (sequential — avoids hammering login sessions)
      for (const state of states) {
        try {
          const districts = await this._districtService.getDistricts(
            state.stateId,
            state.stateName,
            yearId,
          );
          allDistricts.push(...districts);
          logger.info(
            `  [${state.stateName}] ${districts.length} district(s) fetched ` +
              `(running total: ${allDistricts.length}).`,
          );
        } catch (err) {
          const msg = `State ${state.stateId} (${state.stateName}) district fetch failed: ${err.message}`;
          logger.error(`[EtlPipeline] ${msg}`);
          errors.push(msg);
        }
      }

      logger.info(`  Total districts across all states: ${allDistricts.length}`);
    });

    if (allDistricts.length === 0) {
      throw new Error('No districts found across all states. Cannot continue.');
    }

    // ── Stage 3: Process districts ──────────────────────────────────────────
    const districtBatches = [];

    await this._stage('Stage 3 — Process Districts (download + extract + parse)', async () => {
      const results = await mapWithLimit(
        allDistricts,
        async (district) => {
          try {
            return await this._reportService.processDistrict({
              stateId:      district.stateId,
              stateName:    district.stateName,
              districtId:   district.districtId,
              districtName: district.districtName,
              yearId,
            });
          } catch (err) {
            const msg =
              `[${district.stateName}] District ${district.districtId} ` +
              `(${district.districtName}) failed: ${err.message}`;
            logger.error(`[EtlPipeline] ${msg}`);
            errors.push(msg);
            return [];
          }
        },
        config.pipeline.concurrentDistricts,
      );

      for (const batch of results) districtBatches.push(batch);
    });

    // ── Stage 4: Merge ──────────────────────────────────────────────────────
    let rows = [], columns = [];

    await this._stage('Stage 4 — Merge Data', async () => {
      const merged = this._normalizer.merge(districtBatches);
      rows    = merged.rows;
      columns = merged.columns;
      logger.info(`  Total rows: ${rows.length.toLocaleString()}, Columns: ${columns.length}`);
    });

    // ── Stage 5: Export ─────────────────────────────────────────────────────
    let outputPath = '';

    await this._stage('Stage 5 — Export Excel', async () => {
      logger.info(`[DEBUG] Before Stage 5: rows.length = ${rows.length}`);
      outputPath = await this._exporter.write({
        rows,
        columns,
        districts: allDistricts,
        states,
      });
      logger.info(`[DEBUG] After Stage 5: rows.length = ${rows.length}`);
    });

    // ── Stage 5b: Persist rows to Postgres ───────────────────────────────────
    await this._stage('Stage 5b — Persist To Postgres', async () => {
      if (rows.length === 0) {
        logger.info('  No rows available to insert.');
        return;
      }

      const chunkSize = 500;
      let inserted = 0;
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        inserted += await this._microSchoolRepo.insertMany(chunk);
      }
      logger.info(`  Inserted ${inserted.toLocaleString()} row(s) into udise_data.micro_school_udise_data.`);
    });

    // ── Stage 6: Pre-school experience analysis ──────────────────────────────
    let analysisOutputPath = '';

    await this._stage('Stage 6 — Generate Pre-school Experience Report', async () => {
      logger.info(`[DEBUG] Before Stage 6: rows.length = ${rows.length}`);
      
      // Filter rows to only needed columns for performance
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
      
      const filteredRows = rows.map(row => {
        const filtered = {};
        NEEDED_COLUMNS.forEach(col => {
          filtered[col] = row[col];
        });
        return filtered;
      });
      
      analysisOutputPath = await this._preSchoolExporter.write(filteredRows);
      logger.info(`[DEBUG] After Stage 6: analysisOutputPath = ${analysisOutputPath}`);
    });

    // ── Final report ────────────────────────────────────────────────────────
    const durationMs = Date.now() - pipelineStart;
    const report = {
      outputPath,
      analysisOutputPath,
      totalRows:     rows.length,
      stateCount:    states.length,
      districtCount: allDistricts.length,
      durationMs,
      errors,
    };

    logger.info('═══════════════════════════════════════════════════════════');
    logger.info('  UDISE+ ETL Pipeline — Complete');
    logger.info(`  Output         : ${outputPath}`);
    logger.info(`  Analysis       : ${analysisOutputPath || 'Not generated (no data available)'}`);
    logger.info(`  Total rows     : ${report.totalRows.toLocaleString()}`);
    logger.info(`  States         : ${report.stateCount}`);
    logger.info(`  Districts      : ${report.districtCount}`);
    logger.info(`  Duration       : ${(durationMs / 1000).toFixed(1)}s`);
    logger.info(`  Errors         : ${errors.length}`);
    logger.info('═══════════════════════════════════════════════════════════');

    return report;
  }

  // ── Private ─────────────────────────────────────────────────────────────────

  async _stage(name, fn) {
    const t0 = Date.now();
    logger.info(`\n▶ ${name}…`);
    await fn();
    logger.info(`✓ ${name} — ${Date.now() - t0}ms.`);
  }
}

module.exports = EtlPipeline;
