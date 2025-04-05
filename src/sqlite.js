"use strict";

import fsPromise from "node:fs/promises";
import { delay } from "./utils.js";
import sqlite3 from "sqlite3";
import path from "node:path";

/**
 * Open SQLite database
 * @param {string} filePath File path
 * @param {number} mode SQLite mode (e.g: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE | sqlite3.OPEN_READONLY)
 * @param {boolean} wal Use WAL
 * @returns {Promise<sqlite3.Database>} SQLite database instance
 */
export async function openSQLite(filePath, mode, wal) {
  // Create folder if has sqlite3.OPEN_CREATE mode
  if (mode & sqlite3.OPEN_CREATE) {
    await fsPromise.mkdir(path.dirname(filePath), {
      recursive: true,
    });
  }

  // Open DB
  return await new Promise((resolve, reject) => {
    const source = new sqlite3.Database(filePath, mode, async (error) => {
      if (error) {
        return reject(error);
      }

      try {
        // Enable WAL mode if specified
        if (wal === true) {
          await runSQL(source, "PRAGMA journal_mode=WAL;");
        }

        // Disable mmap if specified
        await runSQL(source, "PRAGMA mmap_size = 0;");

        // Set timeout
        await runSQL(source, "PRAGMA busy_timeout = 30000;");

        resolve(source);
      } catch (error) {
        if (source !== undefined) {
          source.close();
        }

        reject(error);
      }
    });
  });
}

/**
 * Run a SQL command in SQLite
 * @param {sqlite3.Database} source SQLite database instance
 * @param {string} sql SQL command to execute
 * @param {any[]} params Parameters for the SQL command
 * @returns {Promise<void>}
 */
export async function runSQL(source, sql, params) {
  await new Promise((resolve, reject) => {
    source.run(sql, params, (error) => {
      if (error) {
        return reject(error);
      }

      resolve();
    });
  });
}

/**
 * Run a SQL command in SQLite with timeout
 * @param {sqlite3.Database} source SQLite database instance
 * @param {string} sql SQL command to execute
 * @param {any[]} params Parameters for the SQL command
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function runSQLWithTimeout(source, sql, params, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      await runSQL(source, sql, params);

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(50);
      } else {
        throw error;
      }
    }
  }

  throw new Error("Timeout to access SQLite DB");
}

/**
 * Fetch one row from SQLite database
 * @param {sqlite3.Database} source SQLite database instance
 * @param {string} sql SQL query string
 * @param {any[]} params Parameters for the SQL query
 * @returns {Promise<Object>} The first row of the query result
 */
export async function fetchOne(source, sql, params) {
  return await new Promise((resolve, reject) => {
    source.get(sql, params, (error, row) => {
      if (error) {
        return reject(error);
      }

      resolve(row);
    });
  });
}

/**
 * Fetch all rows from SQLite database
 * @param {sqlite3.Database} source SQLite database instance
 * @param {string} sql SQL query string
 * @param {any[]} params Parameters for the SQL query
 * @returns {Promise<object[]>} An array of rows
 */
export async function fetchAll(source, sql, params) {
  return await new Promise((resolve, reject) => {
    source.all(sql, params, (error, rows) => {
      if (error) {
        return reject(error);
      }

      resolve(rows);
    });
  });
}

/**
 * Close SQLite database
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function closeSQLite(source) {
  await new Promise((resolve, reject) => {
    source.get("PRAGMA journal_mode;", async (error, row) => {
      if (error) {
        return reject(error);
      }

      try {
        if (row.journal_mode === "wal") {
          await runSQL(source, "PRAGMA wal_checkpoint(PASSIVE);");
        }
      } catch (error) {
        reject(error);
      } finally {
        source.close((error) => {
          if (error) {
            return reject(error);
          }

          resolve();
        });
      }
    });
  });
}
