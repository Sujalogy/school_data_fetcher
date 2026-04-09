'use strict';

/**
 * Central configuration for the UDISE ETL pipeline.
 */

// ── Full UDISE+ state catalogue (all 36 states / UTs) ───────────────────────
const ALL_STATES = [
  // { stateId: 135, stateName: 'Andaman And Nicobar Islands' },
  // { stateId: 128, stateName: 'Andhra Pradesh' },
  // { stateId: 112, stateName: 'Arunachal Pradesh' },
  // { stateId: 118, stateName: 'Assam' },
  { stateId: 110, stateName: 'Bihar' },
  // { stateId: 104, stateName: 'Chandigarh' },
  // { stateId: 122, stateName: 'Chhattisgarh' }, // current working
  // { stateId: 107, stateName: 'Delhi' },
  // { stateId: 130, stateName: 'Goa' },
  // { stateId: 124, stateName: 'Gujarat' },
  // { stateId: 106, stateName: 'Haryana' }, // current working
  // { stateId: 102, stateName: 'Himachal Pradesh' },
  // { stateId: 101, stateName: 'Jammu And Kashmir' },
  // { stateId: 120, stateName: 'Jharkhand' }, // current working
  // { stateId: 129, stateName: 'Karnataka' },
  // { stateId: 132, stateName: 'Kerala' },
  // { stateId: 137, stateName: 'Ladakh' },
  // { stateId: 131, stateName: 'Lakshadweep' },
  // { stateId: 123, stateName: 'Madhya Pradesh' },
  // { stateId: 127, stateName: 'Maharashtra' },
  // { stateId: 114, stateName: 'Manipur' },
  // { stateId: 117, stateName: 'Meghalaya' },
  // { stateId: 115, stateName: 'Mizoram' },
  // { stateId: 113, stateName: 'Nagaland' },
  // { stateId: 121, stateName: 'Odisha' }, // current working
  // { stateId: 134, stateName: 'Puducherry' },
  // { stateId: 103, stateName: 'Punjab' },
  // { stateId: 108, stateName: 'Rajasthan' }, // current working
  // { stateId: 111, stateName: 'Sikkim' },
  // { stateId: 133, stateName: 'Tamil Nadu' },
  // { stateId: 136, stateName: 'Telangana' },
  // { stateId: 138, stateName: 'The Dadra And Nagar Haveli And Daman And Diu' },
  // { stateId: 116, stateName: 'Tripura' },
  // { stateId: 105, stateName: 'Uttarakhand' },
  // { stateId: 109, stateName: 'Uttar Pradesh' }, // current working
  // { stateId: 119, stateName: 'West Bengal' },
];

function resolveActiveStates() {
  const filter = process.env.STATE_IDS;
  if (!filter || filter.trim() === '') return ALL_STATES;
  const allowed = new Set(filter.split(',').map((s) => parseInt(s.trim(), 10)).filter(Boolean));
  const selected = ALL_STATES.filter((s) => allowed.has(s.stateId));
  if (selected.length === 0) throw new Error(`STATE_IDS="${filter}" matched no known states.`);
  return selected;
}

function resolveOutputFileName() {
  const yearId = process.env.YEAR_ID ?? '11';
  const stateIds = process.env.STATE_IDS;
  if (!stateIds || stateIds.trim() === '') return `UDISE_Master_AllStates_Year${yearId}.xlsx`;
  const ids = stateIds.split(',').map((s) => s.trim()).join('-');
  return `UDISE_Master_States${ids}_Year${yearId}.xlsx`;
}

const config = {
  // ─── API ──────────────────────────────────────────────────────────────────
  api: {
    yearList: {
      baseUrl: 'https://kys.udiseplus.gov.in',
      path: '/webapp/api/master/year',
      params: { year: 1 },
    },
    microdata: {
      baseUrl: 'https://microdata.udiseplus.gov.in',
      districtPath: '/fetchDistrictByState',
      downloadPath: '/downloadCsvFile.action',
    },
    sessionCookie: process.env.JSESSIONID
      ? `JSESSIONID=${process.env.JSESSIONID}`
      : 'JSESSIONID=A~211995EB6D4EC5EEECDFA6B56BD108B5',
    timeoutMs: parseInt(process.env.REQUEST_TIMEOUT_MS ?? '60000', 10),
    requestDelayMs: parseInt(process.env.REQUEST_DELAY_MS ?? '1200', 10),
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
  },

  // ─── PIPELINE PARAMETERS ──────────────────────────────────────────────────
  pipeline: {
    /** Active states for this run (all 36 unless STATE_IDS is set). */
    states: resolveActiveStates(),
    allStates: ALL_STATES,

    /** Academic year ID. 11 = 2022-23. */
    yearId: parseInt(process.env.YEAR_ID ?? '11', 10),

    /** Report catalogue — edit here to add/remove report types. */
    reports: [
      { reportId: 1, label: 'Schema' },
      { reportId: 2, label: 'Profile2' },
      { reportId: 3, label: 'Profile1' },
      { reportId: 4, label: 'Facility' },
      { reportId: 5, label: 'Teacher' },
      { reportId: 6, label: 'Enrollment1' },
      { reportId: 7, label: 'Enrollment2' },
    ],

    /** States processed in parallel (keep low — 1 or 2). */
    concurrentStates: parseInt(process.env.CONCURRENT_STATES ?? '1', 10),
    /** Districts processed in parallel within a state (reduce for memory-constrained systems). */
    concurrentDistricts: parseInt(process.env.CONCURRENT_DISTRICTS ?? '1', 10),
    /** Report downloads in parallel within a district (reduce for memory-constrained systems). */
    concurrentReports: parseInt(process.env.CONCURRENT_REPORTS ?? '1', 10),
  },

  // ─── RETRY ────────────────────────────────────────────────────────────────
  retry: {
    maxAttempts: parseInt(process.env.RETRY_ATTEMPTS ?? '3', 10),
    baseDelayMs: parseInt(process.env.RETRY_BASE_DELAY_MS ?? '2000', 10),
    backoffFactor: 2,
    retryableStatuses: new Set([429, 500, 502, 503, 504]),
  },

  // ─── OUTPUT ───────────────────────────────────────────────────────────────
  output: {
    dir: process.env.OUTPUT_DIR ?? './output',
    tempDir: process.env.TEMP_DIR ?? './temp',
    logsDir: process.env.LOGS_DIR ?? './logs',
    masterFileName: resolveOutputFileName(),
    masterSheetName: 'Master',
  },

  // ─── FEATURE FLAGS ────────────────────────────────────────────────────────
  flags: {
    dryRun: process.env.DRY_RUN === 'true',
    keepTempFiles: process.env.KEEP_TEMP === 'true',
  },
};

module.exports = config;
