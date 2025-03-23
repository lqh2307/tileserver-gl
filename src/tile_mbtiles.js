"use strict";

import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import sqlite3 from "sqlite3";
import path from "node:path";
import fs from "node:fs";
import {
  isFullTransparentPNGImage,
  detectFormatAndHeaders,
  getBBoxFromTiles,
  getDataFromURL,
  calculateMD5,
  deepClone,
  retry,
  delay,
} from "./utils.js";
import {
  closeSQLite,
  openSQLite,
  fetchAll,
  fetchOne,
  runSQL,
} from "./sqlite.js";

/**
 * Get MBTiles layers from tiles
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getMBTilesLayersFromTiles(source) {
  const layerNames = new Set();
  const batchSize = 200;
  let offset = 0;

  const vectorTileProto = protobuf(
    await fsPromise.readFile("public/protos/vector_tile.proto")
  );

  while (true) {
    const rows = await fetchAll(
      source,
      `
      SELECT
        tile_data
      FROM
        tiles
      LIMIT
        ?
      OFFSET
        ?;
      `,
      batchSize,
      offset
    );

    if (rows.length === 0) {
      break;
    }

    for (const row of rows) {
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach((layer) => layerNames.add(layer));
    }

    offset += batchSize;
  }

  return Array.from(layerNames);
}

/**
 * Get MBTiles bounding box from tiles
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<[number, number, number, number]>} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
async function getMBTilesBBoxFromTiles(source) {
  const rows = await fetchAll(
    source,
    `
    SELECT
      zoom_level,
      MIN(tile_column) AS xMin,
      MAX(tile_column) AS xMax,
      MIN(tile_row) AS yMin,
      MAX(tile_row) AS yMax
    FROM
      tiles
    GROUP BY
      zoom_level;
    `
  );

  let bbox = [-180, -85.051129, 180, 85.051129];

  for (let index = 0; index < rows.length; index++) {
    const _bbox = getBBoxFromTiles(
      rows[index].xMin,
      rows[index].yMin,
      rows[index].xMax,
      rows[index].yMax,
      rows[index].zoom_level,
      "tms"
    );

    if (index === 0) {
      bbox = _bbox;
    } else {
      if (_bbox[0] < bbox[0]) {
        bbox[0] = _bbox[0];
      }

      if (_bbox[1] < bbox[1]) {
        bbox[1] = _bbox[1];
      }

      if (_bbox[2] > bbox[2]) {
        bbox[2] = _bbox[2];
      }

      if (_bbox[3] > bbox[3]) {
        bbox[3] = _bbox[3];
      }
    }
  }

  if (bbox[0] > 180) {
    bbox[0] = 180;
  } else if (bbox[0] < -180) {
    bbox[0] = -180;
  }

  if (bbox[1] > 180) {
    bbox[1] = 180;
  } else if (bbox[1] < -180) {
    bbox[1] = -180;
  }

  if (bbox[2] > 85.051129) {
    bbox[2] = 85.051129;
  } else if (bbox[2] < -85.051129) {
    bbox[2] = -85.051129;
  }

  if (bbox[3] > 85.051129) {
    bbox[3] = 85.051129;
  } else if (bbox[3] < -85.051129) {
    bbox[3] = -85.051129;
  }

  return bbox;
}

/**
 * Get MBTiles zoom level from tiles
 * @param {sqlite3.Database} source SQLite database instance
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {Promise<number>}
 */
async function getMBTilesZoomLevelFromTiles(source, zoomType) {
  const data = await fetchOne(
    source,
    zoomType === "minzoom"
      ? "SELECT MIN(zoom_level) AS zoom FROM tiles;"
      : "SELECT MAX(zoom_level) AS zoom FROM tiles;"
  );

  return data?.zoom;
}

/**
 * Get MBTiles tile format from tiles
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<string>}
 */
async function getMBTilesFormatFromTiles(source) {
  const data = await fetchOne(source, "SELECT tile_data FROM tiles LIMIT 1;");

  if (data !== undefined) {
    return detectFormatAndHeaders(data.tile_data).format;
  }
}

/**
 * Create MBTiles tile
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {boolean} storeMD5 Is store MD5 hashed?
 * @param {Buffer} data Tile data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function createMBTilesTile(source, z, x, y, storeMD5, data, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      await runSQL(
        source,
        `
        INSERT INTO
          tiles (zoom_level, tile_column, tile_row, tile_data, hash, created)
        VALUES
          (?, ?, ?, ?, ?, ?)
        ON CONFLICT (zoom_level, tile_column, tile_row)
        DO UPDATE SET tile_data = excluded.tile_data, hash = excluded.hash, created = excluded.created;
        `,
        z,
        x,
        (1 << z) - 1 - y,
        data,
        storeMD5 === true ? calculateMD5(data) : undefined,
        Date.now()
      );

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(50);
      } else {
        throw error;
      }
    }
  }

  throw new Error(`Timeout to access MBTiles DB`);
}

/**
 * Delete a tile from MBTiles tiles table
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeMBTilesTile(source, z, x, y, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      await runSQL(
        source,
        `
        DELETE FROM
          tiles
        WHERE
          zoom_level = ? AND tile_column = ? AND tile_row = ?;
        `,
        z,
        x,
        (1 << z) - 1 - y
      );

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(50);
      } else {
        throw error;
      }
    }
  }

  throw new Error(`Timeout to access MBTiles DB`);
}

/**
 * Open MBTiles database
 * @param {string} filePath MBTiles filepath
 * @param {number} mode SQLite mode (e.g: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE | sqlite3.OPEN_READONLY)
 * @param {boolean} wal Use WAL
 * @returns {Promise<Object>}
 */
export async function openMBTilesDB(filePath, mode, wal = false) {
  const source = await openSQLite(filePath, mode, wal);

  if (mode & sqlite3.OPEN_CREATE) {
    await Promise.all([
      runSQL(
        source,
        `
        CREATE TABLE IF NOT EXISTS
          metadata (
            name TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (name)
          );
      `
      ),
      runSQL(
        source,
        `
        CREATE TABLE IF NOT EXISTS
          tiles (
            zoom_level INTEGER NOT NULL,
            tile_column INTEGER NOT NULL,
            tile_row INTEGER NOT NULL,
            tile_data BLOB NOT NULL,
            hash TEXT,
            created BIGINT,
            PRIMARY KEY (zoom_level, tile_column, tile_row)
          );
        `
      ),
    ]);
  }

  return source;
}

/**
 * Get MBTiles tile
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<Object>}
 */
export async function getMBTilesTile(source, z, x, y) {
  let data = await fetchOne(
    source,
    `
    SELECT
      tile_data
    FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    z,
    x,
    (1 << z) - 1 - y
  );

  if (!data?.tile_data) {
    throw new Error("Tile does not exist");
  }

  data = Buffer.from(data.tile_data);

  return {
    data: data,
    headers: detectFormatAndHeaders(data).headers,
  };
}

/**
 * Get MBTiles metadata
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<Object>}
 */
export async function getMBTilesMetadata(source) {
  /* Default metadata */
  const metadata = {
    name: "Unknown",
    description: "Unknown",
    attribution: "<b>Viettel HighTech</b>",
    version: "1.0.0",
    type: "overlay",
  };

  /* Get metadatas */
  const rows = await fetchAll(source, "SELECT name, value FROM metadata;");

  rows.forEach((row) => {
    switch (row.name) {
      case "json": {
        Object.assign(metadata, JSON.parse(row.value));

        break;
      }

      case "minzoom": {
        metadata.minzoom = Number(row.value);

        break;
      }

      case "maxzoom": {
        metadata.maxzoom = Number(row.value);

        break;
      }

      case "center": {
        metadata.center = row.value.split(",").map((elm) => Number(elm));

        break;
      }

      case "format": {
        metadata.format = row.value;

        break;
      }

      case "bounds": {
        metadata.bounds = row.value.split(",").map((elm) => Number(elm));

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

  /* Try get min zoom */
  if (metadata.minzoom === undefined) {
    try {
      metadata.minzoom = await getMBTilesZoomLevelFromTiles(source, "minzoom");
    } catch (error) {
      metadata.minzoom = 0;
    }
  }

  /* Try get max zoom */
  if (metadata.maxzoom === undefined) {
    try {
      metadata.maxzoom = await getMBTilesZoomLevelFromTiles(source, "maxzoom");
    } catch (error) {
      metadata.maxzoom = 22;
    }
  }

  /* Try get tile format */
  if (metadata.format === undefined) {
    try {
      metadata.format = await getMBTilesFormatFromTiles(source);
    } catch (error) {
      metadata.format = "png";
    }
  }

  /* Try get bounds */
  if (metadata.bounds === undefined) {
    try {
      metadata.bounds = await getMBTilesBBoxFromTiles(source);
    } catch (error) {
      metadata.bounds = [-180, -85.051129, 180, 85.051129];
    }
  }

  /* Calculate center */
  if (metadata.center === undefined) {
    metadata.center = [
      (metadata.bounds[0] + metadata.bounds[2]) / 2,
      (metadata.bounds[1] + metadata.bounds[3]) / 2,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    ];
  }

  /* Add missing vector_layers */
  if (metadata.format === "pbf" && metadata.vector_layers === undefined) {
    try {
      const layers = await getMBTilesLayersFromTiles(source);

      metadata.vector_layers = layers.map((layer) => {
        return {
          id: layer,
        };
      });
    } catch (error) {
      metadata.vector_layers = [];
    }
  }

  return metadata;
}

/**
 * Create MBTiles metadata
 * @param {Object} metadata Metadata object
 * @returns {Object}
 */
export function createMBTilesMetadata(metadata) {
  const data = {};

  if (metadata.name !== undefined) {
    data.name = metadata.name;
  } else {
    data.name = "Unknown";
  }

  if (metadata.description !== undefined) {
    data.description = metadata.description;
  } else {
    data.description = "Unknown";
  }

  if (metadata.attribution !== undefined) {
    data.attribution = metadata.attribution;
  } else {
    data.attribution = "<b>Viettel HighTech</b>";
  }

  if (metadata.version !== undefined) {
    data.version = metadata.version;
  } else {
    data.version = "1.0.0";
  }

  if (metadata.type !== undefined) {
    data.type = metadata.type;
  } else {
    data.type = "overlay";
  }

  if (metadata.format !== undefined) {
    data.format = metadata.format;
  } else {
    data.format = "png";
  }

  if (metadata.minzoom !== undefined) {
    data.minzoom = metadata.minzoom;
  } else {
    data.minzoom = 0;
  }

  if (metadata.maxzoom !== undefined) {
    data.maxzoom = metadata.maxzoom;
  } else {
    data.maxzoom = 22;
  }

  if (metadata.bounds !== undefined) {
    data.bounds = deepClone(metadata.bounds);
  } else {
    data.bounds = [-180, -85.051129, 180, 85.051129];
  }

  if (metadata.center !== undefined) {
    data.center = [
      (data.bounds[0] + data.bounds[2]) / 2,
      (data.bounds[1] + data.bounds[3]) / 2,
      Math.floor((data.minzoom + data.maxzoom) / 2),
    ];
  }

  if (metadata.vector_layers !== undefined) {
    data.vector_layers = deepClone(metadata.vector_layers);
  } else {
    if (data.format === "pbf") {
      data.vector_layers = [];
    }
  }

  if (metadata.cacheCoverages !== undefined) {
    data.cacheCoverages = deepClone(metadata.cacheCoverages);
  }

  return data;
}

/**
 * Compact MBTiles
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function compactMBTiles(source) {
  await runSQL(source, "VACUUM;");
}

/**
 * Close MBTiles
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function closeMBTilesDB(source) {
  await closeSQLite(source);
}

/**
 * Download MBTiles file with stream
 * @param {string} url The URL to download the file from
 * @param {string} filePath The path where the file will be saved
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function downloadMBTilesFile(url, filePath, maxTry, timeout) {
  await retry(async () => {
    try {
      await fsPromise.mkdir(path.dirname(filePath), {
        recursive: true,
      });

      const response = await getDataFromURL(url, timeout, "stream");

      const tempFilePath = `${filePath}.tmp`;

      const writer = fs.createWriteStream(tempFilePath);

      response.data.pipe(writer);

      return await new Promise((resolve, reject) => {
        writer
          .on("finish", async () => {
            await fsPromise.rename(tempFilePath, filePath);

            resolve();
          })
          .on("error", async (error) => {
            await fsPromise.rm(tempFilePath, {
              force: true,
            });

            reject(error);
          });
      });
    } catch (error) {
      if (error.statusCode !== undefined) {
        if (
          error.statusCode === StatusCodes.NO_CONTENT ||
          error.statusCode === StatusCodes.NOT_FOUND
        ) {
          printLog(
            "error",
            `Failed to download MBTiles file "${filePath}" - From "${url}": ${error}`
          );

          return;
        } else {
          throw new Error(
            `Failed to download MBTiles file "${filePath}" - From "${url}": ${error}`
          );
        }
      } else {
        throw new Error(
          `Failed to download MBTiles file "${filePath}" - From "${url}": ${error}`
        );
      }
    }
  }, maxTry);
}

/**
 * Update MBTiles metadata table
 * @param {sqlite3.Database} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function updateMBTilesMetadata(source, metadataAdds, timeout) {
  const startTime = Date.now();

  while (Date.now() - startTime <= timeout) {
    try {
      await Promise.all(
        Object.entries({
          ...metadataAdds,
          center: metadataAdds.center.join(","),
          bounds: metadataAdds.bounds.join(","),
          scheme: "tms",
        }).map(([name, value]) =>
          runSQL(
            source,
            `
            INSERT INTO
              metadata (name, value)
            VALUES
              (?, ?)
            ON CONFLICT (name)
            DO UPDATE SET value = excluded.value;
            `,
            name,
            typeof value === "object" ? JSON.stringify(value) : value
          )
        )
      );

      return;
    } catch (error) {
      if (error.code === "SQLITE_BUSY") {
        await delay(50);
      } else {
        throw error;
      }
    }
  }

  throw new Error(`Timeout to access MBTiles DB`);
}

/**
 * Get MBTiles tile from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Object>}
 */
export async function getMBTilesTileFromURL(url, timeout) {
  try {
    const response = await getDataFromURL(url, timeout, "arraybuffer");

    return {
      data: response.data,
      headers: detectFormatAndHeaders(response.data).headers,
    };
  } catch (error) {
    if (error.statusCode !== undefined) {
      if (
        error.statusCode === StatusCodes.NO_CONTENT ||
        error.statusCode === StatusCodes.NOT_FOUND
      ) {
        throw new Error("Tile does not exist");
      } else {
        throw new Error(`Failed to get data tile from "${url}": ${error}`);
      }
    } else {
      throw new Error(`Failed to get data tile from "${url}": ${error}`);
    }
  }
}

/**
 * Download MBTiles tile data
 * @param {string} url The URL to download the file from
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeMD5 Is store MD5 hashed?
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function downloadMBTilesTile(
  url,
  source,
  z,
  x,
  y,
  maxTry,
  timeout,
  storeMD5,
  storeTransparent
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data
      await cacheMBtilesTileData(
        source,
        z,
        x,
        y,
        response.data,
        storeMD5,
        storeTransparent
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to download tile data "${z}/${x}/${y}" - From "${url}": ${error}`
      );

      if (error.statusCode !== undefined) {
        if (
          error.statusCode === StatusCodes.NO_CONTENT ||
          error.statusCode === StatusCodes.NOT_FOUND
        ) {
          return;
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }
  }, maxTry);
}

/**
 * Cache MBTiles tile data
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data buffer
 * @param {boolean} storeMD5 Is store MD5 hashed?
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function cacheMBtilesTileData(
  source,
  z,
  x,
  y,
  data,
  storeMD5,
  storeTransparent
) {
  if (
    storeTransparent === false &&
    (await isFullTransparentPNGImage(data)) === true
  ) {
    return;
  } else {
    await createMBTilesTile(
      source,
      z,
      x,
      y,
      storeMD5,
      data,
      300000 // 5 mins
    );
  }
}

/**
 * Get MD5 hash of MBTiles tile
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<string>} Returns the MD5 hash as a string
 */
export async function getMBTilesTileMD5(source, z, x, y) {
  const data = await fetchOne(
    source,
    `
    SELECT
      hash
    FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    z,
    x,
    (1 << z) - 1 - y
  );

  if (!data?.hash) {
    throw new Error("Tile MD5 does not exist");
  }

  return data.hash;
}

/**
 * Get created of MBTiles tile
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<number>} Returns the created as a number
 */
export async function getMBTilesTileCreated(source, z, x, y) {
  const data = await fetchOne(
    source,
    `
    SELECT
      created
    FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    z,
    x,
    (1 << z) - 1 - y
  );

  if (!data?.created) {
    throw new Error("Tile created does not exist");
  }

  return data.created;
}

/**
 * Get the record tile of MBTiles database
 * @param {string} filePath MBTiles filepath
 * @returns {Promise<number>}
 */
export async function countMBTilesTiles(filePath) {
  const source = await openSQLite(filePath, sqlite3.OPEN_READONLY, false);

  const data = await fetchOne(source, "SELECT COUNT(*) AS count FROM tiles;");

  await closeSQLite(source);

  return data?.count;
}

/**
 * Get the size of MBTiles database
 * @param {string} filePath MBTiles filepath
 * @returns {Promise<number>}
 */
export async function getMBTilesSize(filePath) {
  const stat = await fsPromise.stat(filePath);

  return stat.size;
}

/**
 * Validate MBTiles metadata (no validate json field)
 * @param {Object} metadata MBTiles metadata
 * @returns {void}
 */
export function validateMBTiles(metadata) {
  /* Validate name */
  if (metadata.name === undefined) {
    throw new Error(`"name" property is invalid`);
  }

  /* Validate type */
  if (metadata.type !== undefined) {
    if (["baselayer", "overlay"].includes(metadata.type) === false) {
      throw new Error(`"type" property is invalid`);
    }
  }

  /* Validate format */
  if (
    ["jpeg", "jpg", "pbf", "png", "webp", "gif"].includes(metadata.format) ===
    false
  ) {
    throw new Error(`"format" property is invalid`);
  }

  /* Validate json */
  /*
  if (metadata.format === "pbf" && metadata.json === undefined) {
    throw new Error(`"json" property is invalid`);
  }
  */

  /* Validate minzoom */
  if (metadata.minzoom < 0 || metadata.minzoom > 22) {
    throw new Error(`"minzoom" property is invalid`);
  }

  /* Validate maxzoom */
  if (metadata.maxzoom < 0 || metadata.maxzoom > 22) {
    throw new Error(`"maxzoom" property is invalid`);
  }

  /* Validate minzoom & maxzoom */
  if (metadata.minzoom > metadata.maxzoom) {
    throw new Error(`"zoom" property is invalid`);
  }

  /* Validate bounds */
  if (metadata.bounds !== undefined) {
    if (
      metadata.bounds.length !== 4 ||
      Math.abs(metadata.bounds[0]) > 180 ||
      Math.abs(metadata.bounds[2]) > 180 ||
      Math.abs(metadata.bounds[1]) > 90 ||
      Math.abs(metadata.bounds[3]) > 90 ||
      metadata.bounds[0] >= metadata.bounds[2] ||
      metadata.bounds[1] >= metadata.bounds[3]
    ) {
      throw new Error(`"bounds" property is invalid`);
    }
  }

  /* Validate center */
  if (metadata.center !== undefined) {
    if (
      metadata.center.length !== 3 ||
      Math.abs(metadata.center[0]) > 180 ||
      Math.abs(metadata.center[1]) > 90 ||
      metadata.center[2] < 0 ||
      metadata.center[2] > 22
    ) {
      throw new Error(`"center" property is invalid`);
    }
  }
}
