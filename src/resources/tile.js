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
  removeFileWithLock,
  createFileWithLock,
  getDataTileFromURL,
  handleConcurrency,
  createImageOutput,
  getCenterFromBBox,
  getImageMetadata,
  getBBoxFromTiles,
  BACKGROUND_COLOR,
  closePostgreSQL,
  getDataFromURL,
  openPostgreSQL,
  getTileBounds,
  calculateMD5,
  getCoverBBox,
  closeSQLite,
  openSQLite,
  findFiles,
  printLog,
  retry,
} from "../utils/index.js";

const BATCH_SIZE = 1000;

export const FALLBACK_BBOX = [-180, -85.051129, 180, 85.051129];
export const FALLBACK_VECTOR_LAYERS = [];

export const ALL_FORMATS = new Set([
  "jpeg",
  "jpg",
  "pbf",
  "png",
  "webp",
  "gif",
]);
export const VECTOR_FORMATS = new Set(["pbf"]);
export const RASTER_FORMATS = new Set(["jpeg", "jpg", "png", "webp", "gif"]);
export const SPRITE_FORMATS = new Set(["json", "png"]);
export const TILE_SIZES = new Set(["256", "512"]);
export const LAYER_TYPES = new Set(["baselayer", "overlay"]);

export const MBTILES_INSERT_TILE_QUERY = `INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, hash, created) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET tile_data = excluded.tile_data, hash = excluded.hash, created = excluded.created;`;
const MBTILES_SELECT_TILE_QUERY = `SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;`;
export const MBTILES_DELETE_TILE_QUERY = `DELETE FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;`;

export const XYZ_INSERT_MD5_QUERY = `INSERT INTO md5s (zoom_level, tile_column, tile_row, hash, created) VALUES (?, ?, ?, ?, ?) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET hash = excluded.hash, created = excluded.created;`;
export const XYZ_DELETE_MD5_QUERY = `DELETE FROM md5s WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;`;

export const POSTGRESQL_INSERT_TILE_QUERY = `INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, hash, created) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET tile_data = excluded.tile_data, hash = excluded.hash, created = excluded.created;`;
const POSTGRESQL_SELECT_TILE_QUERY = `SELECT tile_data FROM tiles WHERE zoom_level = $1 AND tile_column = $2 AND tile_row = $3;`;
export const POSTGRESQL_DELETE_TILE_QUERY = `DELETE FROM tiles WHERE zoom_level = $1 AND tile_column = $2 AND tile_row = $3;`;

/*********************************** MBTiles *************************************/

/**
 * Get MBTiles layers from tiles
 * @param {Database} source SQLite database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getMBTilesLayersFromTiles(source) {
  const layerNames = new Set();
  let offset = 0;

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  const sql = source.prepare(`SELECT tile_data FROM tiles LIMIT ? OFFSET ?;`);

  while (true) {
    const rows = sql.all([BATCH_SIZE, offset]);

    if (!rows.length) {
      break;
    }

    rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach(layerNames.add),
    );

    offset += BATCH_SIZE;
  }

  return Array.from(layerNames);
}

/**
 * Get MBTiles bounding box from tiles
 * @param {Database} source SQLite database instance
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
 * @param {Database} source SQLite database instance
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
 * @param {Database} source SQLite database instance
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
 * @param {Database} source SQLite database instance
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
    if (row[extraInfoType]) {
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
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function calculateMBTilesTileExtraInfo(source) {
  const selectSQL = source.prepare(
    `
    SELECT
      zoom_level, tile_column, tile_row, tile_data
    FROM
      tiles
    WHERE
      hash IS NULL
    LIMIT
      ${BATCH_SIZE};
    `,
  );
  const updateSQL = source.prepare(
    `
    UPDATE
      tiles
    SET
      hash = ?,
      created = ?
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
  );

  const created = Date.now();

  while (true) {
    const rows = selectSQL.all();

    if (!rows.length) {
      break;
    }

    rows.forEach((row) =>
      updateSQL.run([
        calculateMD5(row.tile_data),
        created,
        row.zoom_level,
        row.tile_column,
        row.tile_row,
      ]),
    );
  }
}

/**
 * Delete a tile from MBTiles tiles table
 * @param {{ statement: BetterSqlite3.Statement, source: Database, z: number, x: number, y: number }} option
 * @returns {void}
 */
export function removeMBTilesTile(option) {
  if (option.statement) {
    option.statement.run([option.z, option.x, (1 << option.z) - 1 - option.y]);
  } else {
    option.source
      .prepare(MBTILES_DELETE_TILE_QUERY)
      .run([option.z, option.x, (1 << option.z) - 1 - option.y]);
  }
}

/**
 * Open MBTiles database
 * @param {string} filePath MBTiles filepath
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<object>}
 */
export async function openMBTilesDB(filePath, isCreate, timeout) {
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
    );

    const tableInfos = source.prepare("PRAGMA table_info(tiles);").all();

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        source.exec(
          `
          ALTER TABLE
            tiles
          ADD COLUMN
            hash TEXT;
          `,
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
        source.exec(
          `
          ALTER TABLE
            tiles
          ADD COLUMN
            created BIGINT;
          `,
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
 * @param {Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {object}
 */
export function getMBTilesTile(source, z, x, y) {
  const data = source
    .prepare(MBTILES_SELECT_TILE_QUERY)
    .get([z, x, (1 << z) - 1 - y]);

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
 * @param {Database} source SQLite database instance
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
      const layers = await getMBTilesLayersFromTiles(source);

      metadata.vector_layers = layers.map((layer) => ({
        id: layer,
      }));
    } catch (error) {
      metadata.vector_layers = FALLBACK_VECTOR_LAYERS;
    }
  }

  return metadata;
}

/**
 * Compact MBTiles
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function compactMBTiles(source) {
  source.exec("VACUUM;");
}

/**
 * Close MBTiles
 * @param {Database} source SQLite database instance
 * @returns {void}
 */
export function closeMBTilesDB(source) {
  closeSQLite(source);
}

/**
 * Update MBTiles metadata table
 * @param {Database} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @returns {void}
 */
export function updateMBTilesMetadata(source, metadataAdds) {
  const sql = source.prepare(
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

  Object.entries(metadataAdds).map(([name, value]) => {
    if (name === "center" || name === "bounds") {
      sql.run([name, value.join(",")]);
    } else {
      sql.run([
        name,
        typeof value === "object" ? JSON.stringify(value) : value,
      ]);
    }
  });

  sql.run(["scheme", "tms"]);
}

/**
 * Download MBTiles tile data
 * @param {{ headers: object, maxTry: number, timeout: number, url: string, statement: BetterSqlite3.Statement, source: Database, z: number, x: number, y: number, data: Buffer, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function downloadMBTilesTile(option) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        option.url,
        option.timeout,
        "arraybuffer",
        false,
        option.headers,
      );

      option.data = response.data;

      // Store data
      await storeMBtilesTileData(option);
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
  }, option.maxTry);
}

/**
 * Store MBTiles tile data
 * @param {{ statement: BetterSqlite3.Statement, source: Database, z: number, x: number, y: number, data: Buffer, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storeMBtilesTileData(option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(option.data))
  ) {
    return;
  } else {
    if (option.statement) {
      option.statement.run([
        option.z,
        option.x,
        (1 << option.z) - 1 - option.y,
        option.data,
        calculateMD5(option.data),
        option.created,
      ]);
    } else {
      option.source
        .prepare(MBTILES_INSERT_TILE_QUERY)
        .run([
          option.z,
          option.x,
          (1 << option.z) - 1 - option.y,
          option.data,
          calculateMD5(option.data),
          Date.now(),
        ]);
    }
  }
}

/**
 * Get the record tile of MBTiles database
 * @param {string} filePath MBTiles filepath
 * @returns {Promise<number>}
 */
export async function countMBTilesTiles(filePath) {
  const source = await openSQLite(
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
 * @param {Database} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function addMBTilesOverviews(
  source,
  concurrency,
  tileSize,
  storeTransparent,
) {
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

  const sql = source.prepare(
    `
    SELECT
      tile_column, tile_row, tile_data
    FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?;
    `,
  );

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    const tiles = sql.all([z + 1, minX, maxX, minY, maxY]);

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
            background: BACKGROUND_COLOR,
          },
          compositesOption: compositesOption,
          format: metadata.format,
          width: width,
          height: height,
        });

        await storeMBtilesTileData(source, z, x, y, image, storeTransparent);
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
  updateMBTilesMetadata(source, {
    minzoom: metadata.maxzoom - deltaZ,
  });
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
  if (!item) {
    throw new Error("Tile source does not exist");
  }

  const tileName = `${z}/${x}/${y}`;

  try {
    return getMBTilesTile(item.source, z, x, y);
  } catch (error) {
    if (item.sourceURL && error.message === "Tile does not exist") {
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
        30000, // 30 seconds
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        storeMBtilesTileData({
          source: item.source,
          z: z,
          x: x,
          y: tmpY,
          data: dataTile.data,
          storeTransparent: item.storeTransparent,
        }).catch((error) =>
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
  /* Get metadatas */
  const [pmtilesHeader, pmtilesMetadata] = await Promise.all([
    pmtilesSource.getHeader(),
    pmtilesSource.getMetadata(),
  ]);

  /* Default metadata */
  const metadata = {};

  metadata.name = pmtilesMetadata.name ?? "Unknown";
  metadata.description = pmtilesMetadata.description ?? metadata.name;
  metadata.attribution =
    pmtilesMetadata.attribution ?? "<b>Viettel HighTech</b>";
  metadata.version = pmtilesMetadata.version ?? "1.0.0";

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

  metadata.minzoom = pmtilesHeader.minZoom ?? 0;
  metadata.maxzoom = pmtilesHeader.maxZoom ?? 22;

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
    metadata.bounds = FALLBACK_BBOX;
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
    metadata.center = getCenterFromBBox(
      metadata.bounds,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    );
  }

  if (metadata.format === "pbf") {
    metadata.vector_layers =
      pmtilesMetadata.vector_layers ?? FALLBACK_VECTOR_LAYERS;
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
  let offset = 0;

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  const sql = `SELECT tile_data FROM tiles LIMIT $1 OFFSET $2;`;

  while (true) {
    const data = await source.query(sql, [BATCH_SIZE, offset]);

    if (!data.rows.length) {
      break;
    }

    data.rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach(layerNames.add),
    );

    offset += BATCH_SIZE;
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
    if (row[extraInfoType]) {
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
  const selectSQL = `SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE hash IS NULL LIMIT $1;`;
  const updateSQL = `UPDATE tiles SET hash = $1, created = $2 WHERE zoom_level = $3 AND tile_column = $4 AND tile_row = $5;`;

  const created = Date.now();

  while (true) {
    const data = await source.query(selectSQL, [BATCH_SIZE]);

    if (!data.rows.length) {
      break;
    }

    await Promise.all(
      data.rows.map((row) =>
        source.query(updateSQL, [
          calculateMD5(row.tile_data),
          created,
          row.zoom_level,
          row.tile_column,
          row.tile_row,
        ]),
      ),
    );
  }
}

/**
 * Delete a tile from PostgreSQL tiles table
 * @param {{ source: pg.Client, z: number, x: number, y: number }} option
 * @returns {Promise<void>}
 */
export async function removePostgreSQLTile(option) {
  await option.source.query(POSTGRESQL_DELETE_TILE_QUERY, [
    option.z,
    option.x,
    option.y,
  ]);
}

/**
 * Open PostgreSQL database
 * @param {string} uri Database URI
 * @param {boolean} isCreate Is create database?
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<object>}
 */
export async function openPostgreSQLDB(uri, isCreate, timeout) {
  const source = await openPostgreSQL(uri, isCreate, timeout);

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
  let data = await source.query(POSTGRESQL_SELECT_TILE_QUERY, [z, x, y]);

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
      metadata.bounds = FALLBACK_BBOX;
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
      metadata.vector_layers = FALLBACK_VECTOR_LAYERS;
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
  const sql = `INSERT INTO metadata (name, value) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET value = excluded.value;`;

  await Promise.all(
    Object.entries(metadataAdds).map(([name, value]) => {
      if (name === "center" || name === "bounds") {
        source.query(sql, [name, value.join(",")]);
      } else {
        source.query(sql, [
          name,
          typeof value === "object" ? JSON.stringify(value) : value,
        ]);
      }
    }),
  );

  await source.query(sql, ["scheme", "tms"]);
}

/**
 * Download PostgreSQL tile data
 * @param {{ headers: object, maxTry: number, timeout: number, url: string, source: pg.Client, z: number, x: number, y: number, data: Buffer, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function downloadPostgreSQLTile(option) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        option.url,
        option.timeout,
        "arraybuffer",
        false,
        option.headers,
      );

      option.data = response.data;

      // Store data
      await storePostgreSQLTileData(option);
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
  }, option.maxTry);
}

/**
 * Store PostgreSQL tile data
 * @param {{ source: pg.Client, z: number, x: number, y: number, data: Buffer, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storePostgreSQLTileData(option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(option.data))
  ) {
    return;
  } else {
    await option.source.query(POSTGRESQL_INSERT_TILE_QUERY, [
      option.z,
      option.x,
      option.y,
      option.data,
      calculateMD5(option.data),
      option.created ? option.created : Date.now(),
    ]);
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
  const source = await openPostgreSQL(
    uri,
    false,
    60000, // 1 mins
  );

  const data = await source.query("SELECT COUNT(*) AS count FROM tiles;");

  closePostgreSQLDB(source);

  if (data.rows.length !== 0) {
    return +data.rows[0].count;
  }
}

/**
 * Add PostgreSQL overviews (downsample to lower zoom levels)
 * @param {Database} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function addPostgreSQLOverviews(
  source,
  concurrency,
  tileSize,
  storeTransparent,
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

  const sql = `SELECT tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = $1 AND tile_column BETWEEN $2 AND $3 AND tile_row BETWEEN $4 AND $5;`;

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    const tiles = await source.query(sql, [z + 1, minX, maxX, minY, maxY]);

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
            background: BACKGROUND_COLOR,
          },
          compositesOption: compositesOption,
          format: metadata.format,
          width: width,
          height: height,
        });

        await storePostgreSQLTileData({
          source: source,
          z: z,
          x: x,
          y: y,
          data: image,
          storeTransparent: storeTransparent,
        });
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
  await updatePostgreSQLMetadata(source, {
    minzoom: metadata.maxzoom - deltaZ,
  });
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
  if (!item) {
    throw new Error("Tile source does not exist");
  }

  const tileName = `${z}/${x}/${y}`;

  try {
    return await getPostgreSQLTile(item.source, z, x, y);
  } catch (error) {
    if (item.sourceURL && error.message === "Tile does not exist") {
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
        30000, // 30 seconds
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        storePostgreSQLTileData({
          source: item.source,
          z: z,
          x: x,
          y: tmpY,
          data: dataTile.data,
          storeTransparent: item.storeTransparent,
        }).catch((error) =>
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
  await handleConcurrency(BATCH_SIZE, getLayer, pbfFilePaths);

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
 * @param {Database} source SQLite database instance
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
    if (row[extraInfoType]) {
      result[`${row.zoom_level}/${row.tile_column}/${row.tile_row}`] =
        row[extraInfoType];
    }
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
  const format = await getXYZFormatFromTiles(sourcePath);

  const selectSQL = source.prepare(
    `
    SELECT
      zoom_level, tile_column, tile_row
    FROM
      md5s
    WHERE
      hash IS NULL
    LIMIT
      ${BATCH_SIZE};
    `,
  );
  const updateSQL = source.prepare(
    `
    UPDATE
      md5s
    SET
      hash = ?,
      created = ?
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
  );

  const created = Date.now();

  while (true) {
    const rows = selectSQL.all();

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

        updateSQL.run([
          calculateMD5(data),
          created,
          row.zoom_level,
          row.tile_column,
          row.tile_row,
        ]);
      }),
    );
  }
}

/**
 * Remove XYZ tile data file
 * @param {{ sourcePath: string, statement: BetterSqlite3.Statement, source: Database, z: number, x: number, y: number, format: "jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif" }} option
 * @returns {Promise<void>}
 */
export async function removeXYZTile(option) {
  await removeFileWithLock(
    `${option.sourcePath}/${option.z}/${option.x}/${option.y}.${option.format}`,
    30000, // 30 seconds
  );

  if (option.statement) {
    option.statement.run([option.z, option.x, option.y]);
  } else {
    option.source
      .prepare(XYZ_DELETE_MD5_QUERY)
      .run([option.z, option.x, option.y]);
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

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        source.exec(
          `
          ALTER TABLE
            md5s
          ADD COLUMN
            hash TEXT;
          `,
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
        source.exec(
          `
          ALTER TABLE
            md5s
          ADD COLUMN
            created BIGINT;
          `,
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
    const data = await readFile(`${sourcePath}/${z}/${x}/${y}.${format}`);

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

      metadata.vector_layers = layers.map((layer) => ({
        id: layer,
      }));
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
 * Download XYZ tile data file
 * @param {{ headers: object, maxTry: number, timeout: number, url: string, statement: BetterSqlite3.Statement, sourcePath: string, source: Database, z: number, x: number, y: number, data: Buffer, format: "jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif", storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function downloadXYZTile(option) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        option.url,
        option.timeout,
        "arraybuffer",
        false,
        option.headers,
      );

      option.data = response.data;

      // Store data to file
      await storeXYZTileFile(option);
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
  }, option.maxTry);
}

/**
 * Update MBTiles metadata table
 * @param {Database} source SQLite database instance
 * @param {Object<string,string>} metadataAdds Metadata object
 * @returns {void}
 */
export function updateXYZMetadata(source, metadataAdds) {
  const sql = source.prepare(
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

  Object.entries(metadataAdds).map(([name, value]) => {
    if (name === "center" || name === "bounds") {
      sql.run([name, value.join(",")]);
    } else {
      sql.run([
        name,
        typeof value === "object" ? JSON.stringify(value) : value,
      ]);
    }
  });

  sql.run(["scheme", "tms"]);
}

/**
 * Store XYZ tile data file
 * @param {{ statement: BetterSqlite3.Statement, sourcePath: string, source: Database, z: number, x: number, y: number, data: Buffer, format: "jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif", storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storeXYZTileFile(option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(option.data))
  ) {
    return;
  } else {
    await createFileWithLock(
      `${option.sourcePath}/${option.z}/${option.x}/${option.y}.${option.format}`,
      option.data,
      30000, // 30 seconds
    );

    if (option.statement) {
      option.statement.run([
        option.z,
        option.x,
        option.y,
        calculateMD5(option.data),
        option.created,
      ]);
    } else {
      option.source
        .prepare(XYZ_INSERT_MD5_QUERY)
        .run([
          option.z,
          option.x,
          option.y,
          calculateMD5(option.data),
          Date.now(),
        ]);
    }
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
 * @param {Database} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function addXYZOverviews(
  sourcePath,
  source,
  concurrency,
  tileSize,
  storeTransparent,
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
          background: BACKGROUND_COLOR,
        },
        compositesOption: compositesOption,
        format: metadata.format,
        width: width,
        height: height,
      });

      await storeXYZTileFile(
        sourcePath,
        source,
        z,
        x,
        y,
        metadata.format,
        image,
        storeTransparent,
      );
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
  updateXYZMetadata(source, {
    minzoom: metadata.maxzoom - deltaZ,
  });
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
  if (!item) {
    throw new Error("Tile source does not exist");
  }

  const tileName = `${z}/${x}/${y}`;

  try {
    return await getXYZTile(item.source, z, x, y, item.tileJSON.format);
  } catch (error) {
    if (item.sourceURL && error.message === "Tile does not exist") {
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
        30000, // 30 seconds
      );

      /* Cache */
      if (item.storeCache) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

        storeXYZTileFile({
          sourcePath: item.source,
          source: item.md5Source,
          z: z,
          x: x,
          y: tmpY,
          format: item.tileJSON.format,
          data: dataTile.data,
          storeTransparent: item.storeTransparent,
        }).catch((error) =>
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
    if (!LAYER_TYPES.has(metadata.type)) {
      throw new Error(`"type" property is invalid`);
    }
  }

  /* Validate format */
  if (!ALL_FORMATS.has(metadata.format)) {
    throw new Error(`"format" property is invalid`);
  }

  /* Validate json */
  if (metadata.format === "pbf" && metadata.vector_layers === undefined) {
    throw new Error(`"vector_layers" property is invalid`);
  }
}
