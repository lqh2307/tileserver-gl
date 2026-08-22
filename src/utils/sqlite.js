"use strict";

import { DEFAULT_QUERY_TIMEOUT } from "../defaults/index.js";
import { mkdir } from "node:fs/promises";
import Database from "better-sqlite3";
import { delay } from "./util.js";
import path from "node:path";

/**
 * Open SQLite database
 * @param {string} filePath SQLite database file path
 * @param {boolean} isCreate Is create SQLite database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Database>} SQLite database instance
 */
export async function openSQLite(
  filePath,
  isCreate,
  timeout = DEFAULT_QUERY_TIMEOUT,
) {
  if (isCreate) {
    await mkdir(path.dirname(filePath), {
      recursive: true,
    });
  }

  const startTime = Date.now();

  let source;

  while (Date.now() - startTime <= timeout) {
    try {
      source = new Database(filePath, {
        fileMustExist: !isCreate,
        readonly: !isCreate,
        timeout,
      });

      if (isCreate) {
        // Rollback journal is compatible with a shared NFS volume. WAL relies
        // on shared-memory files and is unsafe when different hosts open the DB.
        source.exec("PRAGMA synchronous = NORMAL;"); // Set synchronous mode
        source.exec("PRAGMA journal_mode = TRUNCATE;"); // Set truncate mode
        source.exec("PRAGMA mmap_size = 0;"); // Disable memory mapping
      } else {
        source.pragma("query_only = ON");
      }

      source.exec("PRAGMA foreign_keys = OFF;"); // Disable foreign keys

      return source;
    } catch (error) {
      if (source) {
        source.close();
        source = undefined;
      }

      if (error.code === "SQLITE_BUSY") {
        await delay(5);
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

/**
 * Open transaction SQLite database
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function openSQLiteTransaction(source) {
  source.exec("BEGIN;");
}

/**
 * Close transaction SQLite database
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function closeSQLiteTransaction(source) {
  source.exec("COMMIT;");
}
