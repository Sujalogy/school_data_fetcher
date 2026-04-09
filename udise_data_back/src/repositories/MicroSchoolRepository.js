'use strict';

const pool = require('../db/PostgresPool');

/**
 * MicroSchoolRepository
 * Handles inserts and filtered reads from udise_data.micro_school_udise_data.
 */
class MicroSchoolRepository {
  async insertMany(rows) {
    if (!rows || rows.length === 0) return 0;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const query = `
        INSERT INTO udise_data.micro_school_udise_data (
          state_id, state_name, district_id, district_name, report_id, report_label, source_file,
          pseudocode, block, lgd_block_name, school_category, managment,
          same_sch_b, same_sch_g, other_sch_b, other_sch_g, anganwadi_ecce_b, anganwadi_ecce_g,
          c1_b, c1_g, raw_row
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
        )
      `;

      for (const row of rows) {
        await client.query(query, [
          row._state_id ?? null,
          row._state_name ?? null,
          row._district_id ?? null,
          row._district_name ?? null,
          row._report_id ?? null,
          row._report_label ?? null,
          row._source_file ?? null,
          row.pseudocode ?? null,
          row.block ?? null,
          row.lgd_block_name ?? null,
          row.school_category ?? null,
          row.managment ?? null,
          row.same_sch_b ?? null,
          row.same_sch_g ?? null,
          row.other_sch_b ?? null,
          row.other_sch_g ?? null,
          row.anganwadi_ecce_b ?? null,
          row.anganwadi_ecce_g ?? null,
          row.c1_b ?? null,
          row.c1_g ?? null,
          JSON.stringify(row),
        ]);
      }

      await client.query('COMMIT');
      return rows.length;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getByFilter({ stateId, districtId, block, limit = 500, offset = 0 }) {
    const clauses = [];
    const values = [];
    let p = 1;

    if (stateId) {
      clauses.push(`state_id = $${p++}`);
      values.push(parseInt(stateId, 10));
    }
    if (districtId) {
      clauses.push(`district_id = $${p++}`);
      values.push(parseInt(districtId, 10));
    }
    if (block) {
      clauses.push(`LOWER(COALESCE(block, '')) = LOWER($${p++})`);
      values.push(block);
    }

    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
    values.push(limit);
    values.push(offset);

    const sql = `
      SELECT *
      FROM udise_data.v_micro_school_udise_data_download
      ${where}
      ORDER BY state_name, district_name, block NULLS LAST, id
      LIMIT $${p++}
      OFFSET $${p}
    `;
    const { rows } = await pool.query(sql, values);
    return rows;
  }
}

module.exports = MicroSchoolRepository;
