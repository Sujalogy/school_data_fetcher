# UDISE+ ETL Pipeline

A **production-grade, OOP Node.js ETL pipeline** that pulls school microdata from the UDISE+ government platform, processes all districts in parallel, extracts CSVs from ZIP files, and outputs a single master Excel workbook.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EtlPipeline (Orchestrator)                      │
│                                                                          │
│  Stage 1 → KysApiClient.fetchYearList()                                  │
│  Stage 2 → DistrictService.getDistricts(stateId)                         │
│  Stage 3 → parallel: ReportService.processDistrict(district) × N         │
│  Stage 4 → DataNormalizer.merge(batches)                                  │
│  Stage 5 → ExcelExporter.write({ rows, columns, districts })              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Folder Structure

```
udise-etl/
├── index.js                        # Entry point
├── package.json
├── .env.example                    # All tuneable env vars
│
├── src/
│   ├── config/
│   │   └── index.js                # Centralised config (state, year, reports, etc.)
│   │
│   ├── clients/                    # HTTP layer (axios)
│   │   ├── BaseApiClient.js        # Shared headers, interceptors, logging
│   │   ├── KysApiClient.js         # Year list endpoint
│   │   └── MicrodataApiClient.js   # District list + ZIP download
│   │
│   ├── services/                   # Domain orchestration
│   │   ├── DistrictService.js      # District list with deduplication
│   │   └── ReportService.js        # Inner loop: district → all reports
│   │
│   ├── processors/                 # Data transformation
│   │   ├── ZipProcessor.js         # In-memory ZIP extraction
│   │   ├── CsvProcessor.js         # Streaming CSV parse (csv-parser)
│   │   └── DataNormalizer.js       # Column normalisation + metadata injection
│   │
│   ├── output/
│   │   └── ExcelExporter.js        # Styled multi-sheet XLSX via ExcelJS
│   │
│   ├── pipeline/
│   │   └── EtlPipeline.js          # Stage orchestrator
│   │
│   └── utils/
│       ├── logger.js               # Winston (console + file)
│       ├── retry.js                # Exponential back-off retry
│       └── concurrency.js          # p-limit concurrency helpers
│
├── output/                         # Generated Excel files land here
├── temp/                           # Optional: extracted CSV debug dumps
└── logs/                           # Winston log files
```

---

## Data Flow

```
fetchYearList()
      │
      ▼
fetchDistrictsByState(stateId, yearId)
      │  ← returns: [{ districtId, districtName }, ...]
      │
      ▼
for each district (up to CONCURRENT_DISTRICTS in parallel):
   │
   ├── for each reportId (up to CONCURRENT_REPORTS in parallel):
   │      │
   │      ├── POST /downloadCsvFile.action  → ZIP Buffer
   │      ├── ZipProcessor.extract()        → [{ filename, content }]
   │      ├── CsvProcessor.parse()          → [{ col1: val, col2: val, ... }]
   │      └── DataNormalizer.normalise()    → [{ _state_id, _district_id, ...rowData }]
   │
   └── ← returns: all rows for this district
      │
      ▼
DataNormalizer.merge(allBatches)
      │  ← { rows: [...], columns: [...superset...] }
      │
      ▼
ExcelExporter.write()
      │
      ├── Sheet: "Master"    → all rows, all districts
      ├── Sheet: "Summary"   → district × report pivot table
      └── Sheet: "Districts" → district directory
```

---

## Report IDs

| reportId | Label        | Description          |
|----------|-------------|----------------------|
| 1        | Schema      | Data dictionary      |
| 2        | Profile2    | School profile part 2 |
| 3        | Profile1    | School profile part 1 |
| 4        | Facility    | Infrastructure/facilities |
| 5        | Teacher     | Teacher data         |
| 6        | Enrollment1 | Enrolment part 1     |
| 7        | Enrollment2 | Enrolment part 2     |

---

## Setup

```bash
npm install
cp .env.example .env
# Edit .env to set STATE_ID, YEAR_ID, session cookie, etc.
```

---

## Running

```bash
# Full pipeline (Uttar Pradesh, year 11)
node index.js

# Different state (Bihar = 10)
STATE_ID=10 node index.js

# Dry run — no HTTP calls, tests flow logic
DRY_RUN=true node index.js

# Higher parallelism + debug CSV dumps
CONCURRENT_DISTRICTS=5 KEEP_TEMP=true LOG_LEVEL=debug node index.js

# npm shortcut
npm start
```

---

## Output

The pipeline produces an XLSX workbook in `./output/`:

```
output/UDISE_Master_State128_Year11.xlsx
```

**Sheets:**
- **Master** — every row from every district and every report, with metadata columns (`_state_id`, `_district_id`, `_district_name`, `_report_id`, `_report_label`, `_source_file`)
- **Summary** — district × report row-count pivot
- **Districts** — directory of all districts processed

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| OOP with classes | SOLID: each class has a single responsibility; easy to swap implementations |
| p-limit for parallelism | Prevents hammering the government server; configurable via env |
| Retry with exponential back-off | Handles transient 5xx / network errors gracefully |
| In-memory ZIP extraction | Avoids disk I/O for most runs; temp dump available for debugging |
| Streaming CSV parse | Memory-efficient for large district files |
| Schema-agnostic normalisation | Reports have different columns; sparse rows over rigid mapping |
| ExcelJS (not xlsx) | Supports streaming writes and proper cell styling |
| Config-driven reportIds | Add/remove reports without touching code |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STATE_ID` | `128` | UDISE state code |
| `YEAR_ID` | `11` | Academic year ID |
| `CONCURRENT_DISTRICTS` | `3` | Parallel district jobs |
| `CONCURRENT_REPORTS` | `2` | Parallel report downloads per district |
| `RETRY_ATTEMPTS` | `3` | Max retry attempts |
| `RETRY_BASE_DELAY_MS` | `2000` | Base delay for exponential back-off |
| `OUTPUT_DIR` | `./output` | Excel output directory |
| `TEMP_DIR` | `./temp` | Temp CSV debug directory |
| `LOGS_DIR` | `./logs` | Winston log directory |
| `DRY_RUN` | `false` | Skip HTTP calls |
| `KEEP_TEMP` | `false` | Persist extracted CSVs |
| `LOG_LEVEL` | `info` | Winston log level |

---

## Session Cookie

The UDISE+ microdata API requires a valid `JSESSIONID` cookie.  The default value in `config/index.js` is the one from your curl samples — **rotate it if you receive 401/403 responses**. In production, implement a login flow to programmatically obtain a fresh session before each run.
