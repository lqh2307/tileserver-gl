"use strict";

import pg from "pg";

/**
 * Open PostgreSQL database
 * @param {string} uri Database URI
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @param {{ pool?: boolean, max?: number }} options Connection options
 * @returns {Promise<pg.Client|pg.Pool>} PostgreSQL database instance
 */
export async function openPostgreSQL(uri, isCreate, timeout, options = {}) {
  if (isCreate) {
    const databaseURL = new URL(uri);

    const databaseName = decodeURIComponent(
      databaseURL.pathname.split("/").filter(Boolean).at(-1) ?? "",
    );
    if (!databaseName) {
      throw new Error(`PostgreSQL database name is missing from URI "${uri}"`);
    }

    databaseURL.pathname = "/postgres";
    const quotedDatabaseName = databaseName.replaceAll('"', '""');
    const client = new pg.Client({
      connectionString: databaseURL.toString(),
      statement_timeout: timeout,
      query_timeout: timeout,
    });

    try {
      await client.connect();

      await client.query(`CREATE DATABASE "${quotedDatabaseName}";`);

      await client.end();
    } catch (error) {
      if (client) {
        await client.end();
      }

      if (error.code !== "42P04" && error.code !== "23505") {
        throw error;
      }
    }
  }

  const connectionOptions = {
    connectionString: uri,
    statement_timeout: timeout,
    query_timeout: timeout,
  };

  if (options.pool) {
    const source = new pg.Pool({
      ...connectionOptions,
      max: options.max ?? 4,
      idleTimeoutMillis: 30000,
    });

    await source.query("SELECT 1;");

    return source;
  }

  const source = new pg.Client(connectionOptions);

  await source.connect();

  return source;
}

/**
 * Close PostgreSQL database
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function closePostgreSQL(source) {
  await source.end();
}

/**
 * Open transaction PostgreSQL database
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function openPostgreSQLTransaction(source) {
  await source.query("BEGIN;");
}

/**
 * Close transaction PostgreSQL database
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function closePostgreSQLTransaction(source) {
  await source.query("COMMIT;");
}
