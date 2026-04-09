'use strict';

const { createLogger, format, transports } = require('winston');
const path = require('path');
const config = require('../config');

const { combine, timestamp, colorize, printf, errors } = format;

/** Human-readable format for the console */
const consoleFormat = combine(
  colorize({ all: true }),
  timestamp({ format: 'HH:mm:ss' }),
  errors({ stack: true }),
  printf(({ level, message, timestamp: ts, stack, ...meta }) => {
    const extra = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
    return `[${ts}] ${level}: ${stack ?? message}${extra}`;
  }),
);

/** Machine-readable JSON for the log file */
const fileFormat = combine(
  timestamp(),
  errors({ stack: true }),
  format.json(),
);

const logger = createLogger({
  level: process.env.LOG_LEVEL ?? 'info',
  transports: [
    new transports.Console({ format: consoleFormat }),
    new transports.File({
      filename: path.join(config.output.logsDir, 'etl-error.log'),
      level: 'error',
      format: fileFormat,
    }),
    new transports.File({
      filename: path.join(config.output.logsDir, 'etl-combined.log'),
      format: fileFormat,
    }),
  ],
  // Do not crash the process on unhandled logger errors
  exitOnError: false,
});

module.exports = logger;
