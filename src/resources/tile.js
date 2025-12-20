"use strict";

import { limitValue, maxValue, minValue } from "../utils/number.js";
import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import { PMTiles, FetchSource } from "pmtiles";
import { openSync, readSync } from "node:fs";
import { config } from "../configs/index.js";
import protobuf from "protocol-buffers";
import {
  isFullTransparentImage,
  detectFormatAndHeaders,
  handleTilesConcurrency,
  openSQLiteWithTimeout,
  execSQLWithTimeout,
  removeFileWithLock,
  createFileWithLock,
  getDataTileFromURL,
  handleConcurrency,
  createImageOutput,
  runSQLWithTimeout,
  getImageMetadata,
  getBBoxFromTiles,
  closePostgreSQL,
  getDataFromURL,
  openPostgreSQL,
  getTileBounds,
  calculateMD5,
  getCoverBBox,
  closeSQLite,
  findFiles,
  deepClone,
  printLog,
  retry,
} from "../utils/index.js";

/*********************************** MBTiles *************************************/

/**
 * Get MBTiles layers from tiles
 * @param {DatabaseSync} source SQLite database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getMBTilesLayersFromTiles(source) {
  const layerNames = new Set();
  const batchSize = 1000;
  let offset = 0;

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  const sql = source.prepare(
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
  );

  while (true) {
    const rows = sql.all(batchSize, offset);

    if (!rows.length) {
      break;
    }

    rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach(layerNames.add),
    );

    offset += batchSize;
  }

  return Array.from(layerNames);
}

/**
 * Get MBTiles bounding box from tiles
 * @param {DatabaseSync} source SQLite database instance
 * @returns {[number, number, number, number]} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
function getMBTilesBBoxFromTiles(source) {
  const rows = source
    .prepare(
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
      `,
    )
    .all();

  let bbox;

  if (rows.length) {
    bbox = getBBoxFromTiles(
      rows[0].xMin,
      rows[0].yMin,
      rows[0].xMax,
      rows[0].yMax,
      rows[0].zoom_level,
      "tms",
    );

    for (let index = 1; index < rows.length; index++) {
      bbox = getCoverBBox(
        bbox,
        getBBoxFromTiles(
          rows[index].xMin,
          rows[index].yMin,
          rows[index].xMax,
          rows[index].yMax,
          rows[index].zoom_level,
          "tms",
        ),
      );
    }

    bbox[0] = limitValue(bbox[0], -180, 180);
    bbox[2] = limitValue(bbox[2], -180, 180);
    bbox[1] = limitValue(bbox[1], -85.051129, 85.051129);
    bbox[3] = limitValue(bbox[3], -85.051129, 85.051129);
  } else {
    throw new Error("No row found");
  }

  return bbox;
}

/**
 * Get MBTiles zoom level from tiles
 * @param {DatabaseSync} source SQLite database instance
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {number}
 */
function getMBTilesZoomLevelFromTiles(source, zoomType) {
  const data = source
    .prepare(
      zoomType === "minzoom"
        ? "SELECT MIN(zoom_level) AS zoom FROM tiles;"
        : "SELECT MAX(zoom_level) AS zoom FROM tiles;",
    )
    .get();

  return data?.zoom;
}

/**
 * Get MBTiles tile format from tiles
 * @param {DatabaseSync} source SQLite database instance
 * @returns {string}
 */
export function getMBTilesFormatFromTiles(source) {
  const data = source.prepare("SELECT tile_data FROM tiles LIMIT 1;").get();

  if (data?.tile_data) {
    return detectFormatAndHeaders(data.tile_data).format;
  }
}

/**
 * Get MBTiles tile extra info from coverages
 * @param {DatabaseSync} source SQLite database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {boolean} isCreated Tile created extra info
 * @returns {Object<string, string>} Extra info object
 */
export function getMBTilesTileExtraInfoFromCoverages(
  source,
  coverages,
  isCreated,
) {
  const { tileBounds } = getTileBounds({
    coverages: coverages,
    scheme: "tms",
  });

  let query = "";
  const extraInfoType = isCreated ? "created" : "hash";

  tileBounds.forEach((tileBound, idx) => {
    if (idx) {
      query += " UNION ALL ";
    }

    query += `SELECT zoom_level, tile_column, tile_row, ${extraInfoType} FROM tiles WHERE zoom_level = ${tileBound.z} AND tile_column BETWEEN ${tileBound.x[0]} AND ${tileBound.x[1]} AND tile_row BETWEEN ${tileBound.y[0]} AND ${tileBound.y[1]}`;
  });

  query += ";";

  const result = {};
  const rows = source.prepare(query).all();

  rows.forEach((row) => {
    if (row[extraInfoType] !== null) {
      // TMS -> XYZ
      result[
        `${row.zoom_level}/${row.tile_column}/${
          (1 << row.zoom_level) - 1 - row.tile_row
        }`
      ] = row[extraInfoType];
    }
  });

  return result;
}

/**
 * Calculate MBTiles tile extra info
 * @param {DatabaseSync} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function calculateMBTilesTileExtraInfo(source) {
  const batchSize = 1000;

  const sql = source.prepare(
    `
    SELECT
      zoom_level, tile_column, tile_row, tile_data
    FROM
      tiles
    WHERE
      hash IS NULL
    LIMIT
      ${batchSize};
    `,
  );

  while (true) {
    const rows = sql.all();

    if (!rows.length) {
      break;
    }

    await Promise.all(
      rows.map((row) =>
        runSQLWithTimeout(
          source,
          `
          UPDATE
            tiles
          SET
            hash = ?,
            created = ?
          WHERE
            zoom_level = ? AND tile_column = ? AND tile_row = ?;
          `,
          [
            calculateMD5(row.tile_data),
            Date.now(),
            row.zoom_level,
            row.tile_column,
            row.tile_row,
          ],
          60000, // 1 mins
        ),
      ),
    );
  }
}

/**
 * Delete a tile from MBTiles tiles table
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeMBTilesTile(source, z, x, y, timeout) {
  await runSQLWithTimeout(
    source,
    `
    DELETE FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    z,
    x,
    (1 << z) - 1 - y,
    timeout,
  );
}

/**
 * Open MBTiles database
 * @param {string} filePath MBTiles filepath
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<object>}
 */
export async function openMBTilesDB(filePath, isCreate, timeout) {
  const source = await openSQLiteWithTimeout(filePath, isCreate, timeout);

  if (isCreate) {
    await execSQLWithTimeout(
      source,
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          UNIQUE(name)
        );
      `,
      30000, // 30 secs
    );

    await execSQLWithTimeout(
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
          UNIQUE(zoom_level, tile_column, tile_row)
        );
      `,
      30000, // 30 secs
    );

    const tableInfos = source.prepare("PRAGMA table_info(tiles);").all();

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        await execSQLWithTimeout(
          source,
          `
          ALTER TABLE
            tiles
          ADD COLUMN
            hash TEXT;
          `,
          30000, // 30 secs
        );
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "hash" for table "tiles" of MBTiles DB "${filePath}": ${error}`,
        );
      }
    }

    if (!tableInfos.some((col) => col.name === "created")) {
      try {
        await execSQLWithTimeout(
          source,
          `
          ALTER TABLE
            tiles
          ADD COLUMN
            created BIGINT;
          `,
          30000, // 30 secs
        );
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "created" for table "tiles" of MBTiles DB "${filePath}": ${error}`,
        );
      }
    }
  }

  return source;
}

/**
 * Get MBTiles tile
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {object}
 */
export function getMBTilesTile(source, z, x, y) {
  let data = source
    .prepare(
      `
      SELECT
        tile_data
      FROM
        tiles
      WHERE
        zoom_level = ? AND tile_column = ? AND tile_row = ?;
      `,
    )
    .get(z, x, (1 << z) - 1 - y);

  if (!data?.tile_data) {
    throw new Error("Tile does not exist");
  }

  return {
    data: data.tile_data,
    headers: detectFormatAndHeaders(data.tile_data).headers,
  };
}

/**
 * Get MBTiles metadata
 * @param {DatabaseSync} source SQLite database instance
 * @returns {Promise<Promise<object>>}
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

  /* Try get min zoom */
  if (metadata.minzoom === undefined) {
    try {
      metadata.minzoom = getMBTilesZoomLevelFromTiles(source, "minzoom");
    } catch (error) {
      metadata.minzoom = 0;
    }
  }

  /* Try get max zoom */
  if (metadata.maxzoom === undefined) {
    try {
      metadata.maxzoom = getMBTilesZoomLevelFromTiles(source, "maxzoom");
    } catch (error) {
      metadata.maxzoom = 22;
    }
  }

  /* Try get tile format */
  if (metadata.format === undefined) {
    try {
      metadata.format = getMBTilesFormatFromTiles(source);
    } catch (error) {
      metadata.format = "png";
    }
  }

  /* Try get bounds */
  if (metadata.bounds === undefined) {
    try {
      metadata.bounds = getMBTilesBBoxFromTiles(source);
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

      metadata.vector_layers = layers.map((layer) => ({
        id: layer,
      }));
    } catch (error) {
      metadata.vector_layers = [];
    }
  }

  return metadata;
}

/**
 * Compact MBTiles
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function compactMBTiles(source) {
  source.exec("VACUUM;");
}

/**
 * Close MBTiles
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function closeMBTilesDB(source) {
  closeSQLite(source);
}

/**
 * Update MBTiles metadata table
 * @param {DatabaseSync} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function updateMBTilesMetadata(source, metadataAdds, timeout) {
  await Promise.all(
    Object.entries({
      ...metadataAdds,
      center: metadataAdds.center.join(","),
      bounds: metadataAdds.bounds.join(","),
      scheme: "tms",
    }).map(([name, value]) =>
      runSQLWithTimeout(
        source,
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
        [name, typeof value === "object" ? JSON.stringify(value) : value],
        timeout,
      ),
    ),
  );
}

/**
 * Download MBTiles tile data
 * @param {string} url The URL to download the file from
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {object} headers Headers
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
  storeTransparent,
  headers,
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers,
      );

      // Store data
      await cacheMBtilesTileData(
        source,
        z,
        x,
        y,
        response.data,
        storeTransparent,
      );
    } catch (error) {
      if (error.statusCode) {
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
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data buffer
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function cacheMBtilesTileData(
  source,
  z,
  x,
  y,
  data,
  storeTransparent,
) {
  if (storeTransparent === false && (await isFullTransparentImage(data))) {
    return;
  } else {
    await runSQLWithTimeout(
      source,
      `
      INSERT INTO
        tiles (zoom_level, tile_column, tile_row, tile_data, hash, created)
      VALUES
        (?, ?, ?, ?, ?, ?)
      ON CONFLICT
        (zoom_level, tile_column, tile_row)
      DO UPDATE
        SET
          tile_data = excluded.tile_data,
          hash = excluded.hash,
          created = excluded.created;
      `,
      [z, x, (1 << z) - 1 - y, data, calculateMD5(data), Date.now()],
      30000, // 30 secs
    );
  }
}

/**
 * Get the record tile of MBTiles database
 * @param {string} filePath MBTiles filepath
 * @returns {Promise<number>}
 */
export async function countMBTilesTiles(filePath) {
  const source = await openSQLiteWithTimeout(
    filePath,
    false,
    60000, // 1 mins
  );

  const data = source.prepare("SELECT COUNT(*) AS count FROM tiles;").get();

  closeSQLite(source);

  return data?.count;
}

/**
 * Get the size of MBTiles database
 * @param {string} filePath MBTiles filepath
 * @returns {Promise<number>}
 */
export async function getMBTilesSize(filePath) {
  const stats = await stat(filePath);

  return stats.size;
}

/**
 * Add MBTiles overviews (downsample to lower zoom levels)
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @returns {Promise<void>}
 */
export async function addMBTilesOverviews(source, concurrency, tileSize = 256) {
  /* Get tile width & height */
  const data = source.prepare("SELECT tile_data FROM tiles LIMIT 1;").get();

  if (!data?.tile_data) {
    return;
  }

  const { width, height } = await getImageMetadata(data.tile_data);

  /* Get source width & height */
  const metadata = await getMBTilesMetadata(source);

  const { tileBounds } = getTileBounds({
    bbox: metadata.bounds,
    minZoom: metadata.maxzoom,
    maxZoom: metadata.maxzoom,
    scheme: "tms",
  });

  let sourceWidth = (tileBounds[0].x[1] - tileBounds[0].x[0] + 1) * tileSize;
  let sourceheight = (tileBounds[0].y[1] - tileBounds[0].y[0] + 1) * tileSize;

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    const tiles = source
      .prepare(
        `
        SELECT
          tile_column, tile_row, tile_data
        FROM
          tiles
        WHERE
          zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?;
        `,
      )
      .all(z + 1, minX, maxX, minY, maxY);

    if (tiles.length) {
      // Create composites option
      const compositesOption = [];

      for (const tile of tiles) {
        if (!tile.tile_data) {
          continue;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: await createImageOutput(tile.tile_data, {}),
          left: (tile.tile_column - minX) * width,
          top: (maxY - tile.tile_row) * height,
        });
      }

      if (compositesOption.length) {
        // Create image
        const image = await createImageOutput(undefined, {
          createOption: {
            width: width * 2,
            height: height * 2,
            channels: 4,
            background: { r: 255, g: 255, b: 255, alpha: 0 },
          },
          compositesOption: compositesOption,
          format: metadata.format,
          width: width,
          height: height,
        });

        await runSQLWithTimeout(
          source,
          `
          INSERT INTO
            tiles (zoom_level, tile_column, tile_row, tile_data, hash, created)
          VALUES
            (?, ?, ?, ?, ?, ?)
          ON CONFLICT
            (zoom_level, tile_column, tile_row)
          DO UPDATE
            SET
              tile_data = excluded.tile_data,
              hash = excluded.hash,
              created = excluded.created;
          `,
          [z, x, y, image, calculateMD5(image), Date.now()],
          60000, // 1 mins
        );
      }
    }
  }

  /* Get delta z */
  let deltaZ = 0;
  const targetTileSize = Math.floor(tileSize * 0.95);

  while (
    deltaZ < metadata.maxzoom &&
    (sourceWidth > targetTileSize || sourceheight > targetTileSize)
  ) {
    sourceWidth /= 2;
    sourceheight /= 2;

    deltaZ++;

    const { tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.maxzoom - deltaZ,
      maxZoom: metadata.maxzoom - deltaZ,
      scheme: "tms",
    });

    await handleTilesConcurrency(concurrency, createTileData, tileBounds);
  }

  /* Update minzoom */
  source
    .prepare(
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
    )
    .run("minzoom", metadata.maxzoom - deltaZ);
}

/**
 * Get and cache MBTiles data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCacheMBTilesDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return getMBTilesTile(item.source, z, x, y);
  } catch (error) {
    if (item?.sourceURL && error.message === "Tile does not exist") {
      const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

      const targetURL = item.sourceURL
        .replace("{z}", `${z}`)
        .replace("{x}", `${x}`)
        .replace("{y}", `${tmpY}`);

      printLog(
        "info",
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`,
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        item.headers,
        30000, // 30 secs
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        cacheMBtilesTileData(
          item.source,
          z,
          x,
          tmpY,
          dataTile.data,
          item.storeTransparent,
        ).catch((error) =>
          printLog(
            "error",
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`,
          ),
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/*********************************** PMTiles *************************************/

/**
 * Private class for PMTiles
 */
class PMTilesFileSource {
  constructor(fd) {
    this.fd = fd;
  }

  getKey() {
    return this.fd;
  }

  getBytes(offset, length) {
    const buffer = Buffer.alloc(length);

    readSync(this.fd, buffer, 0, buffer.length, offset);

    return {
      data: buffer.buffer.slice(
        buffer.byteOffset,
        buffer.byteOffset + buffer.byteLength,
      ),
    };
  }
}

/**
 * Open PMTiles
 * @param {string} filePath PMTiles filepath
 * @returns {object}
 */
export function openPMTiles(filePath) {
  let source;

  if (["https://", "http://"].some((scheme) => filePath.startsWith(scheme))) {
    source = new FetchSource(filePath);
  } else {
    source = new PMTilesFileSource(openSync(filePath, "r"));
  }

  return new PMTiles(source);
}

/**
 * Get PMTiles metadata
 * @param {object} pmtilesSource
 * @returns {Promise<object>}
 */
export async function getPMTilesMetadata(pmtilesSource) {
  /* Default metadata */
  const metadata = {};

  /* Get metadatas */
  const [pmtilesHeader, pmtilesMetadata] = await Promise.all([
    pmtilesSource.getHeader(),
    pmtilesSource.getMetadata(),
  ]);

  if (pmtilesMetadata.name !== undefined) {
    metadata.name = pmtilesMetadata.name;
  } else {
    metadata.name = "Unknown";
  }

  if (pmtilesMetadata.description !== undefined) {
    metadata.description = pmtilesMetadata.description;
  } else {
    metadata.description = metadata.name;
  }

  if (pmtilesMetadata.attribution !== undefined) {
    metadata.attribution = pmtilesMetadata.attribution;
  } else {
    metadata.attribution = "<b>Viettel HighTech</b>";
  }

  if (pmtilesMetadata.version !== undefined) {
    metadata.version = pmtilesMetadata.version;
  } else {
    metadata.version = "1.0.0";
  }

  switch (pmtilesHeader.tileType) {
    case 1: {
      metadata.format = "pbf";

      break;
    }

    case 2: {
      metadata.format = "png";

      break;
    }

    case 3: {
      metadata.format = "jpeg";

      break;
    }

    case 4: {
      metadata.format = "webp";

      break;
    }

    case 5: {
      metadata.format = "avif";

      break;
    }

    default: {
      metadata.format = "png";

      break;
    }
  }

  if (pmtilesHeader.minZoom !== undefined) {
    metadata.minzoom = pmtilesHeader.minZoom;
  } else {
    metadata.minzoom = 0;
  }

  if (pmtilesHeader.maxZoom !== undefined) {
    metadata.maxzoom = pmtilesHeader.maxZoom;
  } else {
    metadata.maxzoom = 22;
  }

  if (
    pmtilesHeader.minLon !== undefined &&
    pmtilesHeader.minLat !== undefined &&
    pmtilesHeader.maxLon !== undefined &&
    pmtilesHeader.maxLat !== undefined
  ) {
    metadata.bounds = [
      pmtilesHeader.minLon,
      pmtilesHeader.minLat,
      pmtilesHeader.maxLon,
      pmtilesHeader.maxLat,
    ];
  } else {
    metadata.bounds = [-180, -85.051129, 180, 85.051129];
  }

  if (
    pmtilesHeader.centerLon !== undefined &&
    pmtilesHeader.centerLat !== undefined &&
    pmtilesHeader.centerZoom !== undefined
  ) {
    metadata.center = [
      pmtilesHeader.centerLon,
      pmtilesHeader.centerLat,
      pmtilesHeader.centerZoom,
    ];
  } else {
    metadata.center = [
      (metadata.bounds[0] + metadata.bounds[2]) / 2,
      (metadata.bounds[1] + metadata.bounds[3]) / 2,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    ];
  }

  if (pmtilesMetadata.vector_layers !== undefined) {
    metadata.vector_layers = deepClone(pmtilesMetadata.vector_layers);
  } else {
    if (metadata.format === "pbf") {
      metadata.vector_layers = [];
    }
  }

  return metadata;
}

/**
 * Get PMTiles tile
 * @param {object} pmtilesSource
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getPMTilesTile(pmtilesSource, z, x, y) {
  const zxyTile = await pmtilesSource.getZxy(z, x, y);
  if (!zxyTile?.data) {
    throw new Error("Tile does not exist");
  }

  return {
    data: zxyTile.data,
    headers: detectFormatAndHeaders(zxyTile.data).headers,
  };
}

/**
 * Get the size of PMTiles
 * @param {string} filePath PMTiles filepath
 * @returns {Promise<number>}
 */
export async function getPMTilesSize(filePath) {
  const stats = await stat(filePath);

  return stats.size;
}

/*********************************** PostgreSQL *************************************/

/**
 * Get PostgreSQL layers from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getPostgreSQLLayersFromTiles(source) {
  const layerNames = new Set();
  const batchSize = 1000;
  let offset = 0;

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  while (true) {
    const data = await source.query(
      `
      SELECT
        tile_data
      FROM
        tiles
      LIMIT
        $1
      OFFSET
        $2;
      `,
      [batchSize, offset],
    );

    if (!data.rows.length) {
      break;
    }

    data.rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach(layerNames.add),
    );

    offset += batchSize;
  }

  return Array.from(layerNames);
}

/**
 * Get PostgreSQL bounding box from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<[number, number, number, number]>} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
async function getPostgreSQLBBoxFromTiles(source) {
  const data = await source.query(
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
    `,
  );

  let bbox;

  if (data.rows.length) {
    bbox = getBBoxFromTiles(
      data.rows[0].xMin,
      data.rows[0].yMin,
      data.rows[0].xMax,
      data.rows[0].yMax,
      data.rows[0].zoom_level,
      "xyz",
    );

    for (let index = 1; index < data.rows.length; index++) {
      bbox = getCoverBBox(
        bbox,
        getBBoxFromTiles(
          data.rows[index].xMin,
          data.rows[index].yMin,
          data.rows[index].xMax,
          data.rows[index].yMax,
          data.rows[index].zoom_level,
          "xyz",
        ),
      );
    }

    bbox[0] = limitValue(bbox[0], -180, 180);
    bbox[2] = limitValue(bbox[2], -180, 180);
    bbox[1] = limitValue(bbox[1], -85.051129, 85.051129);
    bbox[3] = limitValue(bbox[3], -85.051129, 85.051129);
  } else {
    throw new Error("No row found");
  }

  return bbox;
}

/**
 * Get PostgreSQL zoom level from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {Promise<number>}
 */
async function getPostgreSQLZoomLevelFromTiles(source, zoomType) {
  const data = await source.query(
    zoomType === "minzoom"
      ? "SELECT MIN(zoom_level) AS zoom FROM tiles;"
      : "SELECT MAX(zoom_level) AS zoom FROM tiles;",
  );

  if (data.rows.length !== 0) {
    return data.rows[0].zoom;
  }
}

/**
 * Get PostgreSQL tile format from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<string>}
 */
export async function getPostgreSQLFormatFromTiles(source) {
  const data = await source.query("SELECT tile_data FROM tiles LIMIT 1;");

  if (data.rows.length !== 0) {
    return detectFormatAndHeaders(data.rows[0].tile_data).format;
  }
}

/**
 * Get PostgreSQL tile extra info from coverages
 * @param {pg.Client} source PostgreSQL database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {boolean} isCreated Tile created extra info
 * @returns {Promise<Object<string, string>>} Extra info object
 */
export async function getPostgreSQLTileExtraInfoFromCoverages(
  source,
  coverages,
  isCreated,
) {
  const { tileBounds } = getTileBounds({ coverages: coverages });

  let query = "";
  const extraInfoType = isCreated ? "created" : "hash";

  tileBounds.forEach((tileBound, idx) => {
    if (idx) {
      query += " UNION ALL ";
    }

    query += `SELECT zoom_level, tile_column, tile_row, ${extraInfoType} FROM tiles WHERE zoom_level = ${tileBound.z} AND tile_column BETWEEN ${tileBound.x[0]} AND ${tileBound.x[1]} AND tile_row BETWEEN ${tileBound.y[0]} AND ${tileBound.y[1]}`;
  });

  query += ";";

  const result = {};
  const data = await source.query(query);

  data.rows.forEach((row) => {
    if (row[extraInfoType] !== null) {
      result[`${row.zoom_level}/${row.tile_column}/${row.tile_row}`] =
        row[extraInfoType];
    }
  });

  return result;
}

/**
 * Calculate PostgreSQL tile extra info
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function calculatePostgreSQLTileExtraInfo(source) {
  const batchSize = 1000;

  while (true) {
    const data = await source.query(
      `
      SELECT
        zoom_level, tile_column, tile_row, tile_data
      FROM
        tiles
      WHERE
        hash IS NULL
      LIMIT
        ${batchSize};
      `,
    );

    if (!data.rows.length) {
      break;
    }

    await Promise.all(
      data.rows.map((row) =>
        source.query(
          `
          UPDATE
            tiles
          SET
            hash = $1,
            created = $2
          WHERE
            zoom_level = $3 AND tile_column = $4 AND tile_row = $5;
          `,
          [
            calculateMD5(row.tile_data),
            Date.now(),
            row.zoom_level,
            row.tile_column,
            row.tile_row,
          ],
        ),
      ),
    );
  }
}

/**
 * Delete a tile from PostgreSQL tiles table
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<void>}
 */
export async function removePostgreSQLTile(source, z, x, y) {
  await source.query(
    `
    DELETE FROM
      tiles
    WHERE
      zoom_level = $1 AND tile_column = $2 AND tile_row = $3;
    `,
    [z, x, y],
  );
}

/**
 * Open PostgreSQL database
 * @param {string} uri Database URI
 * @param {boolean} isCreate Is create database?
 * @returns {Promise<object>}
 */
export async function openPostgreSQLDB(uri, isCreate) {
  const source = await openPostgreSQL(uri, isCreate);

  if (isCreate) {
    await source.query(
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          UNIQUE(name)
        );
      `,
    );

    await source.query(
      `
      CREATE TABLE IF NOT EXISTS
        tiles (
          zoom_level INTEGER NOT NULL,
          tile_column INTEGER NOT NULL,
          tile_row INTEGER NOT NULL,
          tile_data BYTEA NOT NULL,
          hash TEXT,
          created BIGINT,
          UNIQUE(zoom_level, tile_column, tile_row)
        );
      `,
    );

    try {
      await source.query(
        `
        ALTER TABLE
          tiles
        ADD COLUMN IF NOT EXISTS
          hash TEXT;
        `,
      );
    } catch (error) {
      printLog(
        "warn",
        `Failed to create column "hash" for table "tiles" of PostgreSQL DB "${uri}": ${error}`,
      );
    }

    try {
      await source.query(
        `
        ALTER TABLE
          tiles
        ADD COLUMN IF NOT EXISTS
          created BIGINT;
        `,
      );
    } catch (error) {
      printLog(
        "warn",
        `Failed to create column "created" for table "tiles" of PostgreSQL DB "${uri}": ${error}`,
      );
    }
  }

  return source;
}

/**
 * Get PostgreSQL tile
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getPostgreSQLTile(source, z, x, y) {
  let data = await source.query(
    `
    SELECT
      tile_data
    FROM
      tiles
    WHERE
      zoom_level = $1 AND tile_column = $2 AND tile_row = $3;
    `,
    [z, x, y],
  );

  if (!data.rows.length || !data.rows[0].tile_data) {
    throw new Error("Tile does not exist");
  }

  return {
    data: data.rows[0].tile_data,
    headers: detectFormatAndHeaders(data.rows[0].tile_data).headers,
  };
}

/**
 * Get PostgreSQL metadata
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<object>}
 */
export async function getPostgreSQLMetadata(source) {
  /* Default metadata */
  const metadata = {
    name: "Unknown",
    description: "Unknown",
    attribution: "<b>Viettel HighTech</b>",
    version: "1.0.0",
    type: "overlay",
  };

  /* Get metadatas */
  const data = await source.query(`SELECT name, value FROM metadata;`);

  data.rows.forEach((row) => {
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

  /* Try get min zoom */
  if (metadata.minzoom === undefined) {
    try {
      metadata.minzoom = await getPostgreSQLZoomLevelFromTiles(
        source,
        "minzoom",
      );
    } catch (error) {
      metadata.minzoom = 0;
    }
  }

  /* Try get max zoom */
  if (metadata.maxzoom === undefined) {
    try {
      metadata.maxzoom = await getPostgreSQLZoomLevelFromTiles(
        source,
        "maxzoom",
      );
    } catch (error) {
      metadata.maxzoom = 22;
    }
  }

  /* Try get tile format */
  if (metadata.format === undefined) {
    try {
      metadata.format = await getPostgreSQLFormatFromTiles(source);
    } catch (error) {
      metadata.format = "png";
    }
  }

  /* Try get bounds */
  if (metadata.bounds === undefined) {
    try {
      metadata.bounds = await getPostgreSQLBBoxFromTiles(source);
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
      const layers = await getPostgreSQLLayersFromTiles(source);

      metadata.vector_layers = layers.map((layer) => ({
        id: layer,
      }));
    } catch (error) {
      metadata.vector_layers = [];
    }
  }

  return metadata;
}

/**
 * Close PostgreSQL
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function closePostgreSQLDB(source) {
  await closePostgreSQL(source);
}

/**
 * Update PostgreSQL metadata table
 * @param {pg.Client} source PostgreSQL database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @returns {Promise<void>}
 */
export async function updatePostgreSQLMetadata(source, metadataAdds) {
  await Promise.all(
    Object.entries({
      ...metadataAdds,
      center: metadataAdds.center.join(","),
      bounds: metadataAdds.bounds.join(","),
      scheme: "xyz",
    }).map(([name, value]) =>
      source.query(
        `
        INSERT INTO
          metadata (name, value)
        VALUES
          ($1, $2)
        ON CONFLICT
          (name)
        DO UPDATE
          SET
            value = excluded.value;
        `,
        [name, typeof value === "object" ? JSON.stringify(value) : value],
      ),
    ),
  );
}

/**
 * Download PostgreSQL tile data
 * @param {string} url The URL to download the file from
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
export async function downloadPostgreSQLTile(
  url,
  source,
  z,
  x,
  y,
  maxTry,
  timeout,
  storeTransparent,
  headers,
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers,
      );

      // Store data
      await cachePostgreSQLTileData(
        source,
        z,
        x,
        y,
        response.data,
        storeTransparent,
      );
    } catch (error) {
      if (error.statusCode) {
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
 * Cache PostgreSQL tile data
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data buffer
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function cachePostgreSQLTileData(
  source,
  z,
  x,
  y,
  data,
  storeTransparent,
) {
  if (storeTransparent === false && (await isFullTransparentImage(data))) {
    return;
  } else {
    await source.query(
      `
      INSERT INTO
        tiles (zoom_level, tile_column, tile_row, tile_data, hash, created)
      VALUES
        ($1, $2, $3, $4, $5, $6)
      ON CONFLICT
        (zoom_level, tile_column, tile_row)
      DO UPDATE
        SET
          tile_data = excluded.tile_data,
          hash = excluded.hash,
          created = excluded.created;
      `,
      [z, x, y, data, calculateMD5(data), Date.now()],
    );
  }
}

/**
 * Get the size of PostgreSQL database
 * @param {pg.Client} source PostgreSQL database instance
 * @param {string} dbName Database name
 * @returns {Promise<number>}
 */
export async function getPostgreSQLSize(source, dbName) {
  const data = await source.query("SELECT pg_database_size($1) AS size;", [
    dbName,
  ]);

  if (data.rows.length !== 0) {
    return +data.rows[0].size;
  }
}

/**
 * Get the record tile of PostgreSQL database
 * @param {string} uri Database URI
 * @returns {Promise<number>}
 */
export async function countPostgreSQLTiles(uri) {
  const source = await openPostgreSQL(uri, false);

  const data = await source.query("SELECT COUNT(*) AS count FROM tiles;");

  closePostgreSQLDB(source);

  if (data.rows.length !== 0) {
    return +data.rows[0].count;
  }
}

/**
 * Add PostgreSQL overviews (downsample to lower zoom levels)
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @returns {Promise<void>}
 */
export async function addPostgreSQLOverviews(
  source,
  concurrency,
  tileSize = 256,
) {
  /* Get tile width & height */
  const data = await source.query("SELECT tile_data FROM tiles LIMIT 1;");

  if (!data?.rows?.length || !data.rows[0].tile_data) {
    return;
  }

  const { width, height } = await getImageMetadata(data.rows[0].tile_data);

  /* Get source width & height */
  const metadata = await getPostgreSQLMetadata(source);

  const { tileBounds } = getTileBounds({
    bbox: metadata.bounds,
    minZoom: metadata.maxzoom,
    maxZoom: metadata.maxzoom,
    scheme: "xyz",
  });

  let sourceWidth = (tileBounds[0].x[1] - tileBounds[0].x[0] + 1) * tileSize;
  let sourceheight = (tileBounds[0].y[1] - tileBounds[0].y[0] + 1) * tileSize;

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    const tiles = await source
      .query(
        `
        SELECT
          tile_column, tile_row, tile_data
        FROM
          tiles
        WHERE
          zoom_level = $1 AND tile_column BETWEEN $2 AND $3 AND tile_row BETWEEN $4 AND $5;
        `,
      )
      .all([z + 1, minX, maxX, minY, maxY]);

    if (tiles.rows.length) {
      // Create composites option
      const compositesOption = [];

      for (const tile of tiles.rows) {
        if (!tile.tile_data) {
          continue;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: await createImageOutput(tile.tile_data, {}),
          left: (tile.tile_column - minX) * width,
          top: (tile.tile_row - minY) * height,
        });
      }

      if (compositesOption.length) {
        // Create image
        const image = await createImageOutput(undefined, {
          createOption: {
            width: width * 2,
            height: height * 2,
            channels: 4,
            background: { r: 255, g: 255, b: 255, alpha: 0 },
          },
          compositesOption: compositesOption,
          format: metadata.format,
          width: width,
          height: height,
        });

        await source.query(
          `
          INSERT INTO
            tiles (zoom_level, tile_column, tile_row, tile_data, hash, created)
          VALUES
            ($1, $2, $3, $4, $5, $6)
          ON CONFLICT
            (zoom_level, tile_column, tile_row)
          DO UPDATE
            SET
              tile_data = excluded.tile_data,
              hash = excluded.hash,
              created = excluded.created;
          `,
          [z, x, y, image, calculateMD5(image), Date.now()],
        );
      }
    }
  }

  /* Get delta z */
  let deltaZ = 0;
  const targetTileSize = Math.floor(tileSize * 0.95);

  while (
    deltaZ < metadata.maxzoom &&
    (sourceWidth > targetTileSize || sourceheight > targetTileSize)
  ) {
    sourceWidth /= 2;
    sourceheight /= 2;

    deltaZ++;

    const { tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.maxzoom - deltaZ,
      maxZoom: metadata.maxzoom - deltaZ,
      scheme: "xyz",
    });

    await handleTilesConcurrency(concurrency, createTileData, tileBounds);
  }

  /* Update minzoom */
  await source.query(
    `
    INSERT INTO
      metadata (name, value)
    VALUES
      ($1, $2)
    ON CONFLICT
      (name)
    DO UPDATE
      SET
        value = excluded.value;
    `,
    ["minzoom", metadata.maxzoom - deltaZ],
  );
}

/**
 * Get and cache PostgreSQL data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCachePostgreSQLDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return await getPostgreSQLTile(item.source, z, x, y);
  } catch (error) {
    if (item?.sourceURL && error.message === "Tile does not exist") {
      const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

      const targetURL = item.sourceURL
        .replace("{z}", `${z}`)
        .replace("{x}", `${x}`)
        .replace("{y}", `${tmpY}`);

      printLog(
        "info",
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`,
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        item.headers,
        30000, // 30 secs
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        cachePostgreSQLTileData(
          item.source,
          z,
          x,
          tmpY,
          dataTile.data,
          item.storeTransparent,
        ).catch((error) =>
          printLog(
            "error",
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`,
          ),
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/*********************************** XYZ *************************************/

/**
 * Get XYZ layers from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[string, string, string, string]>}
 */
async function getXYZLayersFromTiles(sourcePath) {
  const batchSize = 1000;
  const pbfFilePaths = await findFiles(sourcePath, /^\d+\.pbf$/, true, true);
  const layerNames = new Set();

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  async function getLayer(idx, pbfFilePaths) {
    vectorTileProto.tile
      .decode(await readFile(pbfFilePaths[idx]))
      .layers.map((layer) => layer.name)
      .forEach(layerNames.add);
  }

  // Batch run
  await handleConcurrency(batchSize, getLayer, pbfFilePaths);

  return Array.from(layerNames);
}

/**
 * Get XYZ bounding box from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[number, number, number, number]>} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
async function getXYZBBoxFromTiles(sourcePath) {
  const zFolders = await findFiles(sourcePath, /^\d+$/, false, false, true);
  const boundsArr = [];

  for (const zFolder of zFolders) {
    const xFolders = await findFiles(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false,
      true,
    );

    if (xFolders.length) {
      const xMin = minValue(xFolders.map(Number));
      const xMax = maxValue(xFolders.map(Number));

      for (const xFolder of xFolders) {
        let yFiles = await findFiles(
          `${sourcePath}/${zFolder}/${xFolder}`,
          /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
          false,
          false,
        );

        if (yFiles.length) {
          yFiles = yFiles.map((yFile) => yFile.split(".")[0]);

          const yMin = minValue(yFiles.map(Number));
          const yMax = maxValue(yFiles.map(Number));

          boundsArr.push(
            getBBoxFromTiles(xMin, yMin, xMax, yMax, zFolder, "xyz"),
          );
        }
      }
    }
  }

  if (boundsArr.length) {
    return [
      minValue(boundsArr.map((bbox) => bbox[0])),
      minValue(boundsArr.map((bbox) => bbox[1])),
      maxValue(boundsArr.map((bbox) => bbox[2])),
      maxValue(boundsArr.map((bbox) => bbox[3])),
    ];
  }
}

/**
 * Get XYZ zoom level from tiles
 * @param {string} sourcePath XYZ folder path
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {Promise<number>}
 */
async function getXYZZoomLevelFromTiles(sourcePath, zoomType) {
  const folders = await findFiles(sourcePath, /^\d+$/, false, false, true);

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
  const zFolders = await findFiles(sourcePath, /^\d+$/, false, false, true);

  for (const zFolder of zFolders) {
    const xFolders = await findFiles(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false,
      true,
    );

    for (const xFolder of xFolders) {
      const yFiles = await findFiles(
        `${sourcePath}/${zFolder}/${xFolder}`,
        /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
      );

      if (yFiles.length) {
        return yFiles[0].split(".")[1];
      }
    }
  }
}

/**
 * Get XYZ tile extra info from coverages
 * @param {DatabaseSync} source SQLite database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {boolean} isCreated Tile created extra info
 * @returns {Object<string, string>} Extra info object
 */
export function getXYZTileExtraInfoFromCoverages(source, coverages, isCreated) {
  const { tileBounds } = getTileBounds({ coverages: coverages });

  let query = "";
  const extraInfoType = isCreated ? "created" : "hash";

  tileBounds.forEach((tileBound, idx) => {
    if (idx) {
      query += " UNION ALL ";
    }

    query += `SELECT zoom_level, tile_column, tile_row, ${extraInfoType} FROM md5s WHERE zoom_level = ${tileBound.z} AND tile_column BETWEEN ${tileBound.x[0]} AND ${tileBound.x[1]} AND tile_row BETWEEN ${tileBound.y[0]} AND ${tileBound.y[1]}`;
  });

  query += ";";

  const result = {};
  const rows = source.prepare(query).all();

  rows.forEach((row) => {
    if (row[extraInfoType] !== null) {
      result[`${row.zoom_level}/${row.tile_column}/${row.tile_row}`] =
        row[extraInfoType];
    }
  });

  return result;
}

/**
 * Calculate XYZ tile extra info
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function calculateXYZTileExtraInfo(sourcePath, source) {
  const format = await getXYZFormatFromTiles(sourcePath);

  const batchSize = 1000;

  const sql = source.prepare(
    `
    SELECT
      zoom_level, tile_column, tile_row
    FROM
      md5s
    WHERE
      hash IS NULL
    LIMIT
      ${batchSize};
    `,
  );

  while (true) {
    const rows = sql.all();

    if (!rows.length) {
      break;
    }

    await Promise.all(
      rows.map(async (row) => {
        const data = await getXYZTile(
          sourcePath,
          row.zoom_level,
          row.tile_column,
          row.tile_row,
          format,
        );

        await runSQLWithTimeout(
          source,
          `
          UPDATE
            md5s
          SET
            hash = ?,
            created = ?
          WHERE
            zoom_level = ? AND tile_column = ? AND tile_row = ?;
          `,
          [
            calculateMD5(data),
            Date.now(),
            row.zoom_level,
            row.tile_column,
            row.tile_row,
          ],
          30000, // 30 secs
        );
      }),
    );
  }
}

/**
 * Remove XYZ tile data file
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeXYZTile(
  sourcePath,
  source,
  z,
  x,
  y,
  format,
  timeout,
) {
  await Promise.all([
    removeFileWithLock(`${sourcePath}/${z}/${x}/${y}.${format}`, timeout),
    runSQLWithTimeout(
      source,
      `
      DELETE FROM
        md5s
      WHERE
        zoom_level = ? AND tile_column = ? AND tile_row = ?;
      `,
      [z, x, y],
      timeout,
    ),
  ]);
}

/**
 * Open XYZ MD5 SQLite database
 * @param {string} filePath MD5 filepath
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Database>}
 */
export async function openXYZMD5DB(filePath, isCreate, timeout) {
  const source = await openSQLiteWithTimeout(filePath, isCreate, timeout);

  if (isCreate) {
    await execSQLWithTimeout(
      source,
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          UNIQUE(name)
        );
      `,
      30000, // 30 secs
    );

    await execSQLWithTimeout(
      source,
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
      30000, // 30 secs
    );

    const tableInfos = source.prepare("PRAGMA table_info(md5s);").all();

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        await execSQLWithTimeout(
          source,
          `
          ALTER TABLE
            md5s
          ADD COLUMN
            hash TEXT;
          `,
          30000, // 30 secs
        );
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "hash" for table "md5s" of XYZ MD5 DB "${filePath}": ${error}`,
        );
      }
    }

    if (!tableInfos.some((col) => col.name === "created")) {
      try {
        await execSQLWithTimeout(
          source,
          `
          ALTER TABLE
            md5s
          ADD COLUMN
            created BIGINT;
          `,
          30000, // 30 secs
        );
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
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<object>}
 */
export async function getXYZTile(sourcePath, z, x, y, format) {
  try {
    let data = await readFile(`${sourcePath}/${z}/${x}/${y}.${format}`);

    return {
      data: data,
      headers: detectFormatAndHeaders(data).headers,
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      throw new Error("Tile does not exist");
    } else {
      throw error;
    }
  }
}

/**
 * Get XYZ metadata
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
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
      const layers = await getXYZLayersFromTiles(sourcePath);

      metadata.vector_layers = layers.map((layer) => ({
        id: layer,
      }));
    } catch (error) {
      metadata.vector_layers = [];
    }
  }

  return metadata;
}

/**
 * Compact XYZ
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function compactXYZ(source) {
  source.exec("VACUUM;");
}

/**
 * Close the XYZ MD5 SQLite database
 * @param {DatabaseSync} source SQLite database instance
 * @returns {void}
 */
export function closeXYZMD5DB(source) {
  closeSQLite(source);
}

/**
 * Download XYZ tile data file
 * @param {string} url The URL to download the file from
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
export async function downloadXYZTile(
  url,
  sourcePath,
  source,
  z,
  x,
  y,
  format,
  maxTry,
  timeout,
  storeTransparent,
  headers,
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers,
      );

      // Store data to file
      await cacheXYZTileFile(
        sourcePath,
        source,
        z,
        x,
        y,
        format,
        response.data,
        storeTransparent,
      );
    } catch (error) {
      if (error.statusCode) {
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
 * Update MBTiles metadata table
 * @param {DatabaseSync} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function updateXYZMetadata(source, metadataAdds, timeout) {
  await Promise.all(
    Object.entries({
      ...metadataAdds,
      center: metadataAdds.center.join(","),
      bounds: metadataAdds.bounds.join(","),
      scheme: "xyz",
    }).map(([name, value]) =>
      runSQLWithTimeout(
        source,
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
        [name, typeof value === "object" ? JSON.stringify(value) : value],
        timeout,
      ),
    ),
  );
}

/**
 * Cache XYZ tile data file
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {Buffer} data Tile data buffer
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function cacheXYZTileFile(
  sourcePath,
  source,
  z,
  x,
  y,
  format,
  data,
  storeTransparent,
) {
  if (storeTransparent === false && (await isFullTransparentImage(data))) {
    return;
  } else {
    await Promise.all([
      createFileWithLock(
        `${sourcePath}/${z}/${x}/${y}.${format}`,
        data,
        30000, // 30 secs
      ),
      runSQLWithTimeout(
        source,
        `
        INSERT INTO
          md5s (zoom_level, tile_column, tile_row, hash, created)
        VALUES
          (?, ?, ?, ?, ?)
        ON CONFLICT
          (zoom_level, tile_column, tile_row)
        DO UPDATE
          SET
            hash = excluded.hash,
            created = excluded.created;
        `,
        [z, x, y, calculateMD5(data), Date.now()],
        30000, // 30 secs
      ),
    ]);
  }
}

/**
 * Get the record tile of XYZ folder path
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<number>}
 */
export async function countXYZTiles(sourcePath) {
  const fileNames = await findFiles(
    sourcePath,
    /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
    true,
    false,
  );

  return fileNames.length;
}

/**
 * Get the size of XYZ folder path
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<number>}
 */
export async function getXYZSize(sourcePath) {
  const fileNames = await findFiles(
    sourcePath,
    /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
    true,
    true,
  );

  let size = 0;

  for (const fileName of fileNames) {
    const stats = await stat(fileName);

    size += stats.size;
  }

  return size;
}

/**
 * Add XYZ overviews (downsample to lower zoom levels)
 * @param {string} sourcePath XYZ folder path
 * @param {DatabaseSync} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @returns {Promise<void>}
 */
export async function addXYZOverviews(
  sourcePath,
  source,
  concurrency,
  tileSize = 256,
) {
  /* Get tile width & height */
  let data;
  let found = false;

  const zFolders = await findFiles(sourcePath, /^\d+$/, false, false, true);

  loop: for (const zFolder of zFolders) {
    const xFolders = await findFiles(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false,
      true,
    );

    for (const xFolder of xFolders) {
      const yFiles = await findFiles(
        `${sourcePath}/${zFolder}/${xFolder}`,
        /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
      );

      if (yFiles.length) {
        data = await readFile(
          `${sourcePath}/${zFolder}/${xFolder}/${yFiles[0]}`,
        );

        if (!data) {
          return;
        }

        found = true;

        break loop;
      }
    }
  }

  if (!found) {
    return;
  }

  const { width, height } = await getImageMetadata(data);

  /* Get source width & height */
  const metadata = await getXYZMetadata(sourcePath, source);

  const { tileBounds } = getTileBounds({
    bbox: metadata.bounds,
    minZoom: metadata.maxzoom,
    maxZoom: metadata.maxzoom,
    scheme: "xyz",
  });

  let sourceWidth = (tileBounds[0].x[1] - tileBounds[0].x[0] + 1) * tileSize;
  let sourceheight = (tileBounds[0].y[1] - tileBounds[0].y[0] + 1) * tileSize;

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    // Create composites option
    const compositesOption = [];

    for (let dx = minX; dx <= maxX; dx++) {
      for (let dy = minY; dy <= maxY; dy++) {
        try {
          const tile = await readFile(
            `${sourcePath}/${z + 1}/${dx}/${dy}.${metadata.format}`,
          );

          if (!tile) {
            continue;
          }

          compositesOption.push({
            limitInputPixels: false,
            input: await createImageOutput(tile, {}),
            left: (dx - minX) * width,
            top: (dy - minY) * height,
          });
        } catch (error) {
          continue;
        }
      }
    }

    if (compositesOption.length) {
      // Create image
      const image = await createImageOutput(undefined, {
        createOption: {
          width: width * 2,
          height: height * 2,
          channels: 4,
          background: { r: 255, g: 255, b: 255, alpha: 0 },
        },
        compositesOption: compositesOption,
        format: metadata.format,
        width: width,
        height: height,
      });

      await Promise.all([
        createFileWithLock(
          `${sourcePath}/${z}/${x}/${y}.${metadata.format}`,
          image,
          30000, // 30 secs
        ),
        runSQLWithTimeout(
          source,
          `
          INSERT INTO
            md5s (zoom_level, tile_column, tile_row, hash, created)
          VALUES
            (?, ?, ?, ?, ?)
          ON CONFLICT
            (zoom_level, tile_column, tile_row)
          DO UPDATE
            SET
              hash = excluded.hash,
              created = excluded.created;
          `,
          [z, x, y, calculateMD5(image), Date.now()],
          60000, // 1 mins
        ),
      ]);
    }
  }

  /* Get delta z */
  let deltaZ = 0;
  const targetTileSize = Math.floor(tileSize * 0.95);

  while (
    deltaZ < metadata.maxzoom &&
    (sourceWidth > targetTileSize || sourceheight > targetTileSize)
  ) {
    sourceWidth /= 2;
    sourceheight /= 2;

    deltaZ++;

    const { tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.maxzoom - deltaZ,
      maxZoom: metadata.maxzoom - deltaZ,
      scheme: "xyz",
    });

    await handleTilesConcurrency(concurrency, createTileData, tileBounds);
  }

  /* Update minzoom */
  source
    .prepare(
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
    )
    .run("minzoom", metadata.maxzoom - deltaZ);
}

/**
 * Get and cache XYZ data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCacheXYZDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return await getXYZTile(item.source, z, x, y, item.tileJSON.format);
  } catch (error) {
    if (item?.sourceURL && error.message === "Tile does not exist") {
      const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

      const targetURL = item.sourceURL
        .replace("{z}", `${z}`)
        .replace("{x}", `${x}`)
        .replace("{y}", `${tmpY}`);

      printLog(
        "info",
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`,
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        item.headers,
        30000, // 30 secs
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        cacheXYZTileFile(
          item.source,
          item.md5Source,
          z,
          x,
          tmpY,
          item.tileJSON.format,
          dataTile.data,
          item.storeTransparent,
        ).catch((error) =>
          printLog(
            "error",
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`,
          ),
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/**
 * Validate tile metadata (no validate json field)
 * @param {object} metadata Metadata object
 * @returns {void}
 */
export function validateTileMetadata(metadata) {
  /* Validate name */
  if (metadata.name === undefined) {
    throw new Error(`"name" property is invalid`);
  }

  /* Validate type */
  if (metadata.type !== undefined) {
    if (!["baselayer", "overlay"].includes(metadata.type)) {
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
  if (metadata.format === "pbf" && metadata.vector_layers === undefined) {
    throw new Error(`"vector_layers" property is invalid`);
  }
}
