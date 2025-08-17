"use strict";

import { mkdir } from "node:fs/promises";
import Database from "better-sqlite3";
import { delay } from "./utils/index.js";
import path from "node:path";

/**
 * Open SQLite database with timeout
 * @param {string} filePath SQLite database file path
 * @param {boolean} isCreate Is create SQLite database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Database>} SQLite database instance
 */
export async function openSQLiteWithTimeout(filePath, isCreate, timeout) {
  if (isCreate) {
    await mkdir(path.dirname(filePath), {
      recursive: true,
    });
  }

  const startTime = Date.now();

  let source;

  while (Date.now() - startTime <= timeout) {
    try {
      source = new Database(filePath);

      source.exec("PRAGMA synchronous = FULL;"); // Set synchronous mode
      source.exec("PRAGMA journal_mode = TRUNCATE;"); // Set truncate mode
      source.exec("PRAGMA mmap_size = 0;"); // Disable memory mapping
      source.exec("PRAGMA busy_timeout = 30000;"); // Set timeout is 30s

      return source;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(25);
      } else {
        if (source) {
          source.close();
        }

        throw error;
      }
    }
  }

  throw new Error("Timeout to access SQLite DB");
}

/**
 * Run a SQL command in SQLite database with timeout
 * @param {Database} source SQLite database instance
 * @param {string} sql SQL command to execute
 * @param {any[]} params Parameters for the SQL command
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function runSQLWithTimeout(source, sql, params, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      source.prepare(sql).run(params);

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(25);
      } else {
        throw error;
      }
    }
  }

  throw new Error("Timeout to access SQLite DB");
}

/**
 * Exec a SQL command in SQLite database with timeout
 * @param {Database} source SQLite database instance
 * @param {string} sql SQL command to execute
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function execSQLWithTimeout(source, sql, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      source.exec(sql);

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(25);
      } else {
        throw error;
      }
    }
  }

  throw new Error("Timeout to access SQLite DB");
}

/**
 * Close SQLite database
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function closeSQLite(source) {
  source.close();
}
