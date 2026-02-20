"use strict";

import { limitValue } from "../utils/number.js";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import protobuf from "protocol-buffers";
import {
  FALLBACK_VECTOR_LAYERS,
  isFullTransparentImage,
  detectFormatAndHeaders,
  handleTilesConcurrency,
  closeSQLiteTransaction,
  openSQLiteTransaction,
  getDataTileFromURL,
  createImageOutput,
  getCenterFromBBox,
  getImageMetadata,
  getBBoxFromTiles,
  BACKGROUND_COLOR,
  FALLBACK_BBOX,
  getTileBounds,
  calculateMD5,
  closeSQLite,
  getFileSize,
  openSQLite,
  printLog,
  MAX_LON,
  MAX_LAT,
} from "../utils/index.js";

const BATCH_SIZE = 1000;

export const MBTILES_INSERT_TILE_QUERY =
  "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, hash, created) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET tile_data = excluded.tile_data, hash = excluded.hash, created = excluded.created;";
const MBTILES_SELECT_TILE_QUERY =
  "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;";
export const MBTILES_DELETE_TILE_QUERY =
  "DELETE FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?;";

/*********************************** MBTiles *************************************/

/**
 * Get MBTiles layers from tiles
 * @param {Database} source SQLite database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getMBTilesLayersFromTiles(source) {
  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto"),
  );

  const layerNames = new Set();

  let lastRowID = 0;

  const selectSQL = source.prepare(
    `
    SELECT
      rowid, tile_data
    FROM
      tiles
    WHERE
      rowid > ?
    ORDER BY
      rowid
    LIMIT
      ${BATCH_SIZE};
    `,
  );

  while (true) {
    const rows = selectSQL.all([lastRowID]);

    const len = rows.length;
    if (!len) {
      break;
    }

    rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach(layerNames.add),
    );

    lastRowID = rows[len - 1].rowid;
  }

  return Array.from(layerNames);
}

/**
 * Get MBTiles bounding box from tiles
 * @param {Database} source SQLite database instance
 * @returns {[number, number, number, number]} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
function getMBTilesBBoxFromTiles(source) {
  const zoom = source
    .prepare("SELECT MAX(zoom_level) AS maxzoom FROM tiles;")
    .get();
  if (zoom) {
    const data = source
      .prepare(
        "SELECT MIN(tile_column) AS xMin, MAX(tile_column) AS xMax, MIN(tile_row) AS yMin, MAX(tile_row) AS yMax FROM tiles WHERE zoom_level = ?;",
      )
      .get([zoom.maxzoom]);
    if (data) {
      const bbox = getBBoxFromTiles(
        data.xMin,
        data.yMin,
        data.xMax,
        data.yMax,
        zoom.maxzoom,
        "tms",
      );

      // Claim
      bbox[0] = limitValue(bbox[0], -MAX_LON, MAX_LON);
      bbox[2] = limitValue(bbox[2], -MAX_LON, MAX_LON);
      bbox[1] = limitValue(bbox[1], -MAX_LAT, MAX_LAT);
      bbox[3] = limitValue(bbox[3], -MAX_LAT, MAX_LAT);

      return bbox;
    }
  }
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
      rowid, zoom_level, tile_column, tile_row, tile_data
    FROM
      tiles
    WHERE
      rowid > ?
    ORDER BY
      rowid
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

  let lastRowID = 0;

  const created = Date.now();

  while (true) {
    const rows = selectSQL.all([lastRowID]);

    const len = rows.length;
    if (!len) {
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

    lastRowID = rows[len - 1].rowid;
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
        source.exec("ALTER TABLE tiles ADD COLUMN hash TEXT;");
      } catch (error) {
        printLog(
          "warn",
          `Failed to create column "hash" for table "tiles" of MBTiles DB "${filePath}": ${error}`,
        );
      }
    }

    if (!tableInfos.some((col) => col.name === "created")) {
      try {
        source.exec("ALTER TABLE tiles ADD COLUMN created BIGINT;");
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

  Object.entries(metadataAdds).map(([name, value]) => {
    if (name === "center" || name === "bounds") {
      insertSQL.run([name, value.join(",")]);
    } else {
      insertSQL.run([
        name,
        typeof value === "object" ? JSON.stringify(value) : value,
      ]);
    }
  });

  insertSQL.run(["scheme", "tms"]);

  closeSQLiteTransaction(source);
}

/**
 * Store MBTiles tile data
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data
 * @param {{ statement: BetterSqlite3.Statement, source: Database, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storeMBtilesTileData(z, x, y, data, option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(data))
  ) {
    return;
  } else {
    if (option.statement) {
      option.statement.run([
        z,
        x,
        (1 << z) - 1 - y,
        data,
        calculateMD5(data),
        option.created,
      ]);
    } else {
      option.source
        .prepare(MBTILES_INSERT_TILE_QUERY)
        .run([z, x, (1 << z) - 1 - y, data, calculateMD5(data), Date.now()]);
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
  return await getFileSize(filePath);
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

  const querySQL = source.prepare(
    `
    SELECT
      tile_column, tile_row, tile_data
    FROM
      tiles
    WHERE
      zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?;
    `,
  );

  const insertSQL = source.prepare(MBTILES_INSERT_TILE_QUERY);

  const createOption = {
    width: width * 2,
    height: height * 2,
    channels: 4,
    background: BACKGROUND_COLOR,
  };

  /* Create tile data handler function */
  async function createTileData(z, x, y) {
    const minX = x * 2;
    const maxX = minX + 1;
    const minY = y * 2;
    const maxY = minY + 1;

    const tiles = querySQL.all([z + 1, minX, maxX, minY, maxY]);

    if (tiles.length) {
      // Create composites option
      const compositesOption = [];

      for (const tile of tiles) {
        if (!tile.tile_data) {
          continue;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: await createImageOutput({
            data: tile.tile_data,
          }),
          left: (tile.tile_column - minX) * width,
          top: (maxY - tile.tile_row) * height,
        });
      }

      if (compositesOption.length) {
        // Create image
        const image = await createImageOutput({
          createOption: createOption,
          compositesOption: compositesOption,
          format: metadata.format,
          width: width,
          height: height,
        });

        await storeMBtilesTileData({
          statement: insertSQL,
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

        storeMBtilesTileData(z, x, tmpY, dataTile.data, {
          source: item.source,
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
