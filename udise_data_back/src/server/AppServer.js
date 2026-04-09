'use strict';

const express = require('express');
const MicroSchoolRepository = require('../repositories/MicroSchoolRepository');

class AppServer {
  constructor() {
    this.app = express();
    this.repo = new MicroSchoolRepository();
    this._configure();
    this._routes();
  }

  _configure() {
    this.app.use(express.json({ limit: '2mb' }));
  }

  _routes() {
    this.app.get('/health', (_req, res) => {
      res.json({ ok: true });
    });

    this.app.get('/api/micro-school', async (req, res) => {
      const limit = Math.min(parseInt(req.query.limit ?? '500', 10), 2000);
      const offset = parseInt(req.query.offset ?? '0', 10);
      const rows = await this.repo.getByFilter({
        stateId: req.query.stateId,
        districtId: req.query.districtId,
        block: req.query.block,
        limit,
        offset,
      });
      res.json({ count: rows.length, rows });
    });

    // Download CSV view for filtered dataset.
    this.app.get('/api/micro-school/download.csv', async (req, res) => {
      const rows = await this.repo.getByFilter({
        stateId: req.query.stateId,
        districtId: req.query.districtId,
        block: req.query.block,
        limit: Math.min(parseInt(req.query.limit ?? '100000', 10), 200000),
        offset: parseInt(req.query.offset ?? '0', 10),
      });

      const headers = [
        'id',
        'state_id',
        'state_name',
        'district_id',
        'district_name',
        'block',
        'lgd_block_name',
        'report_id',
        'report_label',
        'pseudocode',
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
        'source_file',
        'created_at',
      ];
      const lines = [headers.join(',')];
      for (const row of rows) {
        lines.push(headers.map((h) => this._csvEscape(row[h])).join(','));
      }

      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="micro_school_udise_data.csv"');
      res.send(lines.join('\n'));
    });
  }

  _csvEscape(value) {
    const text = String(value ?? '');
    if (text.includes(',') || text.includes('"') || text.includes('\n')) {
      return `"${text.replace(/"/g, '""')}"`;
    }
    return text;
  }

  start(port) {
    this.app.listen(port, () => {
      // eslint-disable-next-line no-console
      console.log(`API server listening on http://localhost:${port}`);
    });
  }
}

module.exports = AppServer;
