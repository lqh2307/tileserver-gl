"use strict";

import path from "node:path";
import pg from "pg";

/**
 * Open PostgreSQL database
 * @param {string} uri Database URI
 * @param {boolean} isCreate Is create database?
 * @returns {Promise<pg.Client>} PostgreSQL database instance
 */
export async function openPostgreSQL(uri, isCreate) {
  if (isCreate) {
    const client = new pg.Client({
      connectionString: path.dirname(uri),
    });

    try {
      await client.connect();

      await client.query(`CREATE DATABASE "${path.basename(uri)}";`);

      await client.end();
    } catch (error) {
      if (client !== undefined) {
        await client.end();
      }

      if (error.code !== "42P04" && error.code !== "23505") {
        throw error;
      }
    }
  }

  const source = new pg.Client({
    connectionString: uri,
  });

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
