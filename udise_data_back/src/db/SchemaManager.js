'use strict';

const pool = require('./PostgresPool');

/**
 * SchemaManager
 * Creates udise_data schema + fact tables only.
 */
class SchemaManager {
  async setup() {
    await pool.query(`
      CREATE SCHEMA IF NOT EXISTS udise_data;

      CREATE TABLE IF NOT EXISTS udise_data.micro_school_udise_data (
        id BIGSERIAL PRIMARY KEY,
        state_id INTEGER,
        state_name TEXT,
        district_id INTEGER,
        district_name TEXT,
        report_id INTEGER,
        report_label TEXT,
        source_file TEXT,
        pseudocode TEXT,
        block TEXT,
        lgd_block_name TEXT,
        school_category TEXT,
        managment TEXT,
        same_sch_b TEXT,
        same_sch_g TEXT,
        other_sch_b TEXT,
        other_sch_g TEXT,
        anganwadi_ecce_b TEXT,
        anganwadi_ecce_g TEXT,
        c1_b TEXT,
        c1_g TEXT,
        raw_row JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_micro_school_state ON udise_data.micro_school_udise_data (state_id);
      CREATE INDEX IF NOT EXISTS idx_micro_school_district ON udise_data.micro_school_udise_data (district_id);
      CREATE INDEX IF NOT EXISTS idx_micro_school_block ON udise_data.micro_school_udise_data (block);
      CREATE INDEX IF NOT EXISTS idx_micro_school_report ON udise_data.micro_school_udise_data (report_id);
      CREATE INDEX IF NOT EXISTS idx_micro_school_pseudocode ON udise_data.micro_school_udise_data (pseudocode);

      CREATE OR REPLACE VIEW udise_data.v_micro_school_udise_data_download AS
      SELECT
        id,
        state_id,
        state_name,
        district_id,
        district_name,
        block,
        lgd_block_name,
        report_id,
        report_label,
        pseudocode,
        school_category,
        managment,
        same_sch_b,
        same_sch_g,
        other_sch_b,
        other_sch_g,
        anganwadi_ecce_b,
        anganwadi_ecce_g,
        c1_b,
        c1_g,
        source_file,
        created_at
      FROM udise_data.micro_school_udise_data;
    `);
  }
}

module.exports = SchemaManager;
