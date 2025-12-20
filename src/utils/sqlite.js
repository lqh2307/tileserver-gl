"use strict";

import { DatabaseSync } from "node:sqlite";
import { mkdir } from "node:fs/promises";
import { delay } from "./util.js";
import path from "node:path";

/**
 * Open SQLite database
 * @param {string} filePath SQLite database file path
 * @param {boolean} isCreate Is create SQLite database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<DatabaseSync>} SQLite database instance
 */
export async function openSQLite(filePath, isCreate, timeout) {
  if (isCreate) {
    await mkdir(path.dirname(filePath), {
      recursive: true,
    });
  }

  const startTime = Date.now();

  let source;

  while (Date.now() - startTime <= timeout) {
    try {
      source = new DatabaseSync(filePath, {
        enableForeignKeyConstraints: false,
        timeout: timeout,
      });

      source.exec("PRAGMA synchronous = FULL;"); // Set synchronous mode
      source.exec("PRAGMA journal_mode = TRUNCATE;"); // Set truncate mode
      source.exec("PRAGMA mmap_size = 0;"); // Disable memory mapping

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
 * Close SQLite database
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function closeSQLite(source) {
  source.close();
}
