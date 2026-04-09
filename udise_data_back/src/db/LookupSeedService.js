'use strict';

const fs = require('fs');
const pool = require('./PostgresPool');

/**
 * LookupSeedService
 * Seeds lookup tables from local JSON files when present.
 */
class LookupSeedService {
  constructor() {
    this.seedFiles = {
      master_year: process.env.MASTER_YEAR_FILE ?? 'C:\\Users\\admin\\Downloads\\master_year_202604091606.json',
      school_management:
        process.env.SCHOOL_MANAGEMENT_FILE ?? 'C:\\Users\\admin\\Downloads\\school_management_202604091607.json',
      school_type: process.env.SCHOOL_TYPE_FILE ?? 'C:\\Users\\admin\\Downloads\\school_type_202604091609.json',
      state: process.env.STATE_FILE ?? 'C:\\Users\\admin\\Downloads\\state_202604091618.json',
      district: process.env.DISTRICT_FILE ?? 'C:\\Users\\admin\\Downloads\\district_202604091619.json',
    };
  }

  async seedAll() {
    await this.seedMasterYear();
    await this.seedSchoolManagement();
    await this.seedSchoolType();
    await this.seedState();
    await this.seedDistrict();
  }

  _readJson(filePath, rootKey) {
    if (!fs.existsSync(filePath)) return [];
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return parsed[rootKey] ?? [];
  }

  async seedMasterYear() {
    const items = this._readJson(this.seedFiles.master_year, 'master_year');
    for (const item of items) {
      await pool.query(
        `INSERT INTO udise_data.master_year (yearid, yeardesc)
         VALUES ($1, $2)
         ON CONFLICT (yearid) DO UPDATE SET yeardesc = EXCLUDED.yeardesc`,
        [item.yearid, item.yeardesc],
      );
    }
  }

  async seedSchoolManagement() {
    const items = this._readJson(this.seedFiles.school_management, 'school_management');
    for (const item of items) {
      await pool.query(
        `INSERT INTO udise_data.school_management (code, value)
         VALUES ($1, $2)
         ON CONFLICT (code) DO UPDATE SET value = EXCLUDED.value`,
        [item.code, item.value],
      );
    }
  }

  async seedSchoolType() {
    const items = this._readJson(this.seedFiles.school_type, 'school_type');
    for (const item of items) {
      await pool.query(
        `INSERT INTO udise_data.school_type (code, value)
         VALUES ($1, $2)
         ON CONFLICT (code) DO UPDATE SET value = EXCLUDED.value`,
        [item.code, item.value],
      );
    }
  }

  async seedState() {
    const items = this._readJson(this.seedFiles.state, 'state');
    for (const item of items) {
      await pool.query(
        `INSERT INTO udise_data.state (stcode11, stname, udise_stco)
         VALUES ($1, $2, $3)
         ON CONFLICT (stcode11) DO UPDATE
           SET stname = EXCLUDED.stname, udise_stco = EXCLUDED.udise_stco`,
        [item.stcode11, item.stname, item.udise_stco],
      );
    }
  }

  async seedDistrict() {
    const items = this._readJson(this.seedFiles.district, 'district');
    for (const item of items) {
      await pool.query(
        `INSERT INTO udise_data.district (dtcode11, dtname, stname, stcode11, udise_stco)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (dtcode11) DO UPDATE
           SET dtname = EXCLUDED.dtname,
               stname = EXCLUDED.stname,
               stcode11 = EXCLUDED.stcode11,
               udise_stco = EXCLUDED.udise_stco`,
        [item.dtcode11, item.dtname, item.stname, item.stcode11, item.udise_stco],
      );
    }
  }
}

module.exports = LookupSeedService;
