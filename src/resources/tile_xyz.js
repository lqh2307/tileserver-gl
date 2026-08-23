"use strict";

import { getVectorTileProto } from "./vector_tile.js";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import path from "node:path";
import {
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_CONCURRENCY,
} from "../defaults/index.js";
import {
  FALLBACK_VECTOR_LAYERS,
  isFullTransparentImage,
  detectFormatAndHeaders,
  closeSQLiteTransaction,
  openSQLiteTransaction,
  removeFileWithLock,
  createFileWithLock,
  getCenterFromBBox,
  getBBoxFromTiles,
  isErrorNotFound,
  runAllWithLimit,
  runSingleFlight,
  getDataFromURL,
  FALLBACK_BBOX,
  getTileBounds,
  calculateMD5,
  closeSQLite,
  getFileSize,
  openSQLite,
  findFiles,
  walkFiles,
  printLog,
  maxs,
  mins,
} from "../utils/index.js";

const BATCH_SIZE = 1000;

const tileStatementCaches = new WeakMap();

const NUMERIC_PATH_REGEX = /^\d+$/;
const PBF_TILE_FILE_REGEX = /^\d+\.pbf$/;
const TILE_FILE_REGEX = /^\d+\.(png|jpg|jpeg|webp|pbf)$/;

/** Upserts the MD5 metadata associated with an XYZ tile. @type {string} */
export const XYZ_INSERT_MD5_QUERY =
  "INSERT INTO md5s (zoom_level, tile_column, tile_row, hash, created) VALUES (?, ?, ?, ?, ?) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET hash = excluded.hash, created = excluded.created;";
/** Deletes MD5 metadata for an XYZ tile. @type {string} */
export const XYZ_DELETE_MD5_QUERY =
  "DELETE FROM md5s WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;";

function getTileStatement(source, name, query) {
  let statements = tileStatementCaches.get(source);
  if (!statements) {
    statements = {};

    tileStatementCaches.set(source, statements);
  }

  if (!statements[name]) {
    statements[name] = source.prepare(query);
  }

  return statements[name];
}

/*********************************** XYZ *************************************/

/**
 * Get XYZ layers from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[string, string, string, string]>}
 */
async function getXYZLayersFromTiles(sourcePath) {
  const vectorTileProto = await getVectorTileProto();

  const layerNames = new Set();

  async function* getLayerGenerator() {
    for await (const pbfFilePath of walkFiles(
      sourcePath,
      PBF_TILE_FILE_REGEX,
    )) {
      yield async () => {
        const { layers } = vectorTileProto.tile.decode(
          await readFile(pbfFilePath),
        );
        for (const layer of layers) {
          layerNames.add(layer.name);
        }
      };
    }
  }

  // Batch run
  await runAllWithLimit(getLayerGenerator(), DEFAULT_CONCURRENCY);

  return Array.from(layerNames);
}

/**
 * Get XYZ bounding box from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[number, number, number, number]>} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
async function getXYZBBoxFromTiles(sourcePath) {
  const zFolders = await findFiles(
    sourcePath,
    NUMERIC_PATH_REGEX,
    false,
    false,
    true,
  );
  if (!zFolders.length) {
    return;
  }

  const zMax = maxs(zFolders.map(Number));
  const zPath = path.join(sourcePath, String(zMax));

  const xFolders = await findFiles(
    zPath,
    NUMERIC_PATH_REGEX,
    false,
    false,
    true,
  );
  if (!xFolders.length) {
    return;
  }

  const xMin = mins(xFolders.map(Number));
  const xMax = maxs(xFolders.map(Number));

  let yMin;
  let yMax;

  for (const xFolder of xFolders) {
    let yFiles = await findFiles(
      path.join(zPath, xFolder),
      TILE_FILE_REGEX,
      false,
      false,
    );
    if (!yFiles.length) {
      continue;
    }

    const ys = yFiles.map((f) => {
      return Number(f.split(".")[0]);
    });

    const yMinLocal = mins(ys);
    const yMaxLocal = maxs(ys);

    if (yMin === undefined || yMinLocal < yMin) {
      yMin = yMinLocal;
    }
    if (yMax === undefined || yMaxLocal > yMax) {
      yMax = yMaxLocal;
    }
  }

  if (yMin === undefined || yMax === undefined) {
    return;
  }

  return getBBoxFromTiles(xMin, yMin, xMax, yMax, zMax, "xyz");
}

/**
 * Get XYZ zoom level from tiles
 * @param {string} sourcePath XYZ folder path
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {Promise<number>}
 */
async function getXYZZoomLevelFromTiles(sourcePath, zoomType) {
  const folders = await findFiles(
    sourcePath,
    NUMERIC_PATH_REGEX,
    false,
    false,
    true,
  );

  const zooms = folders.map(Number);
  if (zooms.length) {
    let zoom = zooms[0];

    for (let i = 1; i < zooms.length; i++) {
      if (zoomType === "minzoom") {
        if (zooms[i] < zoom) {
          zoom = zooms[i];
        }
      } else {
        if (zooms[i] > zoom) {
          zoom = zooms[i];
        }
      }
    }

    return zoom;
  }
}

/**
 * Get XYZ tile format from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<string>}
 */
export async function getXYZFormatFromTiles(sourcePath) {
  const zFolders = await findFiles(
    sourcePath,
    NUMERIC_PATH_REGEX,
    false,
    false,
    true,
  );

  for (const zFolder of zFolders) {
    const xFolders = await findFiles(
      path.join(sourcePath, zFolder),
      NUMERIC_PATH_REGEX,
      false,
      false,
      true,
    );

    for (const xFolder of xFolders) {
      const yFiles = await findFiles(
        path.join(sourcePath, zFolder, xFolder),
        TILE_FILE_REGEX,
      );
      if (yFiles.length) {
        return yFiles[0].split(".")[1];
      }
    }
  }
}

/**
 * Get XYZ tile extra info from coverages or exact tile bounds
 * @param {{ source: Database, coverages?: { zoom: number, bbox: [number, number, number, number]}[], tileBounds?: { z: number, x: [number, number], y: [number, number] }[], isCreated?: boolean }} options Options
 * @returns {Object<string, string>} Extra info object
 */
export function getXYZTileExtraInfo(options) {
  const tileBounds =
    options.tileBounds ??
    getTileBounds({
      coverages: options.coverages,
    }).tileBounds;

  const extraInfoType = options.isCreated ? "created" : "hash";

  const querySQL = options.source.prepare(
    `
      SELECT
        tile_column, tile_row, ${extraInfoType}
      FROM
        md5s
      WHERE
        zoom_level = ?
      AND
        tile_column BETWEEN ? AND ?
      AND
        tile_row BETWEEN ? AND ?;
    `,
  );

  const result = {};

  tileBounds.forEach((tileBound) => {
    const rows = querySQL.all(
      tileBound.z,
      tileBound.x[0],
      tileBound.x[1],
      tileBound.y[0],
      tileBound.y[1],
    );

    rows.forEach((row) => {
      if (row[extraInfoType]) {
        // XYZ
        result[`${tileBound.z}/${row.tile_column}/${row.tile_row}`] =
          row[extraInfoType];
      }
    });
  });

  return result;
}

/**
 * Calculate XYZ tile extra info
 * @param {string} sourcePath XYZ folder path
 * @param {Database} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function calculateXYZTileExtraInfo(sourcePath, source) {
  const created = Date.now();

  const upsertSQL = source.prepare(XYZ_INSERT_MD5_QUERY);
  const upsertRows = source.transaction((updates) => {
    updates.forEach((update) => {
      upsertSQL.run(update);
    });
  });

  async function processBatch(filePaths) {
    const updates = Array(filePaths.length);

    await runAllWithLimit(
      filePaths.map((filePath, index) => {
        return async () => {
          const relativeParts = path
            .relative(sourcePath, filePath)
            .split(path.sep);
          const z = Number(relativeParts[0]);
          const x = Number(relativeParts[1]);
          const y = Number.parseInt(relativeParts[2], 10);

          updates[index] = [
            z,
            x,
            y,
            calculateMD5(await readFile(filePath)),
            created,
          ];
        };
      }),
      DEFAULT_CONCURRENCY,
    );

    upsertRows(updates);
  }

  let filePaths = [];
  for await (const filePath of walkFiles(sourcePath, TILE_FILE_REGEX)) {
    const relativePath = path.relative(sourcePath, filePath).split(path.sep);
    if (
      relativePath.length !== 3 ||
      relativePath.some((part, index) => {
        return index < 2 && !NUMERIC_PATH_REGEX.test(part);
      })
    ) {
      continue;
    }

    filePaths.push(filePath);

    if (filePaths.length >= BATCH_SIZE) {
      await processBatch(filePaths);

      filePaths = [];
    }
  }

  if (filePaths.length) {
    await processBatch(filePaths);
  }

  source
    .prepare("DELETE FROM md5s WHERE created IS NULL OR created != ?;")
    .run(created);
}

/**
 * Remove XYZ tile data file
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {{ sourcePath: string, statement: BetterSqlite3.Statement, source: Database, format: "jpeg"|"jpg"|"pbf"|"png"|"webp" }} option
 * @returns {Promise<void>}
 */
export async function removeXYZTile(z, x, y, option) {
  await removeFileWithLock(
    path.join(option.sourcePath, String(z), String(x), `${y}.${option.format}`),
    DEFAULT_QUERY_TIMEOUT,
  );

  if (option.statement) {
    option.statement.run([z, x, y]);
  } else {
    getTileStatement(option.source, "delete", XYZ_DELETE_MD5_QUERY).run([
      z,
      x,
      y,
    ]);
  }
}

/**
 * Open XYZ MD5 SQLite database
 * @param {string} filePath MD5 filepath
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Database>}
 */
export async function openXYZMD5DB(filePath, isCreate, timeout) {
  const source = await openSQLite(filePath, isCreate, timeout);

  if (isCreate) {
    source.exec(
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          UNIQUE(name)
        );
      `,
    );

    source.exec(
      `
      CREATE TABLE IF NOT EXISTS
        md5s (
          zoom_level INTEGER NOT NULL,
          tile_column INTEGER NOT NULL,
          tile_row INTEGER NOT NULL,
          hash TEXT,
          created BIGINT,
          UNIQUE(zoom_level, tile_column, tile_row)
        );
      `,
    );

    const tableInfos = source.prepare("PRAGMA table_info(md5s);").all();

    if (
      !tableInfos.some((col) => {
        return col.name === "hash";
      })
    ) {
      try {
        source.exec("ALTER TABLE md5s ADD COLUMN hash TEXT;");
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "hash" for table "md5s" of XYZ MD5 DB "${filePath}": ${error}`,
        );
      }
    }

    if (
      !tableInfos.some((col) => {
        return col.name === "created";
      })
    ) {
      try {
        source.exec("ALTER TABLE md5s ADD COLUMN created BIGINT;");
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "created" for table "md5s" of XYZ MD5 DB "${filePath}": ${error}`,
        );
      }
    }
  }

  return source;
}

/**
 * Get XYZ tile
 * @param {string} sourcePath XYZ folder path
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"} format Tile format
 * @returns {Promise<object>}
 */
export async function getXYZTile(sourcePath, z, x, y, format) {
  try {
    const data = await readFile(
      path.join(sourcePath, String(z), String(x), `${y}.${format}`),
    );

    return {
      data,
      headers: detectFormatAndHeaders(data).headers,
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Not Found");
    }

    throw error;
  }
}

/**
 * Get XYZ metadata
 * @param {string} sourcePath XYZ folder path
 * @param {Database} source SQLite database instance
 * @returns {Promise<object>}
 */
export async function getXYZMetadata(sourcePath, source) {
  /* Default metadata */
  const metadata = {
    name: "Unknown",
    description: "Unknown",
    attribution: "<b>Viettel HighTech</b>",
    version: "1.0.0",
    type: "overlay",
  };

  /* Get metadatas */
  const rows = source.prepare("SELECT name, value FROM metadata;").all();

  rows.forEach((row) => {
    switch (row.name) {
      case "json": {
        Object.assign(metadata, JSON.parse(row.value));

        break;
      }

      case "minzoom": {
        metadata.minzoom = +row.value;

        break;
      }

      case "maxzoom": {
        metadata.maxzoom = +row.value;

        break;
      }

      case "center": {
        metadata.center = row.value.split(",").map(Number);

        break;
      }

      case "format": {
        metadata.format = row.value;

        break;
      }

      case "bounds": {
        metadata.bounds = row.value.split(",").map(Number);

        break;
      }

      case "name": {
        metadata.name = row.value;

        break;
      }

      case "description": {
        metadata.description = row.value;

        break;
      }

      case "attribution": {
        metadata.attribution = row.value;

        break;
      }

      case "version": {
        metadata.version = row.value;

        break;
      }

      case "type": {
        metadata.type = row.value;

        break;
      }
    }
  });

  /* Try get tile format */
  if (metadata.format === undefined) {
    try {
      metadata.format = await getXYZFormatFromTiles(sourcePath);
    } catch (error) {
      metadata.format = "png";
    }
  }

  /* Try get min zoom */
  if (metadata.minzoom === undefined) {
    try {
      metadata.minzoom = await getXYZZoomLevelFromTiles(sourcePath, "minzoom");
    } catch (error) {
      metadata.minzoom = 0;
    }
  }

  /* Try get max zoom */
  if (metadata.maxzoom === undefined) {
    try {
      metadata.maxzoom = await getXYZZoomLevelFromTiles(sourcePath, "maxzoom");
    } catch (error) {
      metadata.maxzoom = 22;
    }
  }

  /* Try get bounds */
  if (metadata.bounds === undefined) {
    try {
      metadata.bounds = await getXYZBBoxFromTiles(sourcePath);
    } catch (error) {
      metadata.bounds = FALLBACK_BBOX;
    }
  }

  /* Calculate center */
  if (metadata.center === undefined) {
    metadata.center = getCenterFromBBox(
      metadata.bounds,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    );
  }

  /* Add missing vector_layers */
  if (metadata.format === "pbf" && metadata.vector_layers === undefined) {
    try {
      const layers = await getXYZLayersFromTiles(sourcePath);

      metadata.vector_layers = layers.map((layer) => {
        return {
          id: layer,
        };
      });
    } catch (error) {
      metadata.vector_layers = FALLBACK_VECTOR_LAYERS;
    }
  }

  return metadata;
}

/**
 * Compact XYZ
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function compactXYZ(source) {
  source.exec("VACUUM;");
}

/**
 * Close the XYZ MD5 SQLite database
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function closeXYZMD5DB(source) {
  closeSQLite(source);
}

/**
 * Update MBTiles metadata table
 * @param {Database} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @returns {void}
 */
export function updateXYZMetadata(source, metadataAdds) {
  const insertSQL = source.prepare(
    `
    INSERT INTO
      metadata (name, value)
    VALUES
      (?, ?)
    ON CONFLICT
      (name)
    DO UPDATE
      SET
        value = excluded.value;
    `,
  );

  openSQLiteTransaction(source);

  for (const [name, value] of Object.entries(metadataAdds)) {
    if (name === "center" || name === "bounds") {
      insertSQL.run([name, value.join(",")]);
    } else {
      insertSQL.run([
        name,
        typeof value === "object" ? JSON.stringify(value) : value,
      ]);
    }
  }

  insertSQL.run(["scheme", "tms"]);

  closeSQLiteTransaction(source);
}

/**
 * Store XYZ tile data file
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data
 * @param {{ statement: BetterSqlite3.Statement, sourcePath: string, source: Database, format: "jpeg"|"jpg"|"pbf"|"png"|"webp", storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storeXYZTileFile(z, x, y, data, option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(data))
  ) {
    return;
  }

  await createFileWithLock(
    path.join(option.sourcePath, String(z), String(x), `${y}.${option.format}`),
    data,
    DEFAULT_QUERY_TIMEOUT,
  );

  if (option.statement) {
    option.statement.run([z, x, y, calculateMD5(data), option.created]);
  } else {
    getTileStatement(option.source, "insert", XYZ_INSERT_MD5_QUERY).run([
      z,
      x,
      y,
      calculateMD5(data),
      Date.now(),
    ]);
  }
}

/**
 * Get the record tile of XYZ folder path
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<number>}
 */
export async function countXYZTiles(sourcePath) {
  let count = 0;

  for await (const _fileName of walkFiles(sourcePath, TILE_FILE_REGEX)) {
    count += 1;
  }

  return count;
}

/**
 * Get the size of XYZ folder path
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<number>}
 */
export async function getXYZSize(sourcePath) {
  let size = 0;

  async function* getSizeGenerator() {
    for await (const fileName of walkFiles(sourcePath, TILE_FILE_REGEX)) {
      yield async () => {
        const fileSize = await getFileSize(fileName);
        size += fileSize;
      };
    }
  }

  await runAllWithLimit(getSizeGenerator(), DEFAULT_CONCURRENCY);

  return size;
}

/**
 * Get and cache XYZ tile data
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<Buffer>}
 */
export async function getAndCacheXYZTileData(id, z, x, y) {
  const item = config.datas[id];
  if (!item) {
    throw new Error(`Data id "${id}" does not exist`);
  }

  const tileName = `${z}/${x}/${y}`;

  try {
    return await getXYZTile(item.source, z, x, y, item.tileJSON.format);
  } catch (error) {
    if (item.sourceURL && isErrorNotFound(error)) {
      const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

      const targetURL = item.sourceURL
        .replace("{z}", `${z}`)
        .replace("{x}", `${x}`)
        .replace("{y}", `${tmpY}`);

      return runSingleFlight(`xyz:${id}:${tileName}`, async () => {
        printLog(
          "info",
          `Forwarding data id "${id}" - Tile "${tileName}" - To "${targetURL}"...`,
        );

        const data = await getDataFromURL(targetURL, {
          method: "GET",
          responseType: "arraybuffer",
          timeout: DEFAULT_QUERY_TIMEOUT,
          headers: item.headers,
          decompress: false,
        });

        if (item.storeCache) {
          try {
            await storeXYZTileFile(z, x, y, data, {
              source: item.md5Source,
              sourcePath: item.source,
              format: item.tileJSON.format,
              storeTransparent: item.storeTransparent,
            });
          } catch (cacheError) {
            printLog(
              "error",
              `Failed to cache data id "${id}" - Tile "${tileName}": ${cacheError}`,
            );
          }
        }

        return {
          data,
          headers: detectFormatAndHeaders(data).headers,
        };
      });
    }

    throw error;
  }
}
