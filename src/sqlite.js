"use strict";

import { DatabaseSync } from "node:sqlite";
import fsPromise from "node:fs/promises";
import { delay } from "./utils.js";
import path from "node:path";

/**
 * Open SQLite database
 * @param {string} filePath File path
 * @param {boolean} isCreate Is create database?
 * @returns {Promise<DatabaseSync>} SQLite database instance
 */
export async function openSQLite(filePath, isCreate) {
  if (isCreate === true) {
    await fsPromise.mkdir(path.dirname(filePath), {
      recursive: true,
    });
  }

  let source;

  try {
    source = new DatabaseSync(filePath);

    source.exec("PRAGMA mmap_size = 0;"); // Disable memory mapping
    source.exec("PRAGMA busy_timeout = 30000;"); // 30s

    return source;
  } catch (error) {
    if (source !== undefined) {
      source.close();
    }

    throw error;
  }
}

/**
 * Run a SQL command in SQLite with timeout
 * @param {DatabaseSync} source SQLite database instance
 * @param {string} sql SQL command to execute
 * @param {any[]} params Parameters for the SQL command
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function runSQLWithTimeout(source, sql, params, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      source.prepare(sql).run(...params);

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
 * Close SQLite database
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function closeSQLite(source) {
  source.close();
}
