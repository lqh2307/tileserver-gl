"use strict";

import { limitValue } from "../utils/number.js";
import { config } from "../configs/index.js";
import { readFile } from "node:fs/promises";
import protobuf from "protocol-buffers";
import {
  closePostgreSQLTransaction,
  openPostgreSQLTransaction,
  FALLBACK_VECTOR_LAYERS,
  isFullTransparentImage,
  detectFormatAndHeaders,
  handleTilesConcurrency,
  getDataTileFromURL,
  createImageOutput,
  getImageMetadata,
  getBBoxFromTiles,
  BACKGROUND_COLOR,
  closePostgreSQL,
  openPostgreSQL,
  FALLBACK_BBOX,
  getTileBounds,
  calculateMD5,
  printLog,
  MAX_LON,
  MAX_LAT,
} from "../utils/index.js";

const BATCH_SIZE = 1000;

export const POSTGRESQL_INSERT_TILE_QUERY =
  "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data, hash, created) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (zoom_level, tile_column, tile_row) DO UPDATE SET tile_data = excluded.tile_data, hash = excluded.hash, created = excluded.created;";
const POSTGRESQL_SELECT_TILE_QUERY =
  "SELECT tile_data FROM tiles WHERE zoom_level = $1 AND tile_column = $2 AND tile_row = $3;";
export const POSTGRESQL_DELETE_TILE_QUERY =
  "DELETE FROM tiles WHERE zoom_level = $1 AND tile_column = $2 AND tile_row = $3;";

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

  const selectSQL = "SELECT tile_data FROM tiles LIMIT $1 OFFSET $2;";

  while (true) {
    const data = await source.query(selectSQL, [BATCH_SIZE, offset]);

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
  const zoom = await source.query(
    "SELECT MAX(zoom_level) AS maxzoom FROM tiles;",
  );
  if (zoom.rows.length) {
    const data = await source.query(
      `
      SELECT
        MIN(tile_column) AS xMin,
        MAX(tile_column) AS xMax,
        MIN(tile_row) AS yMin,
        MAX(tile_row) AS yMax
      FROM
        tiles
      WHERE
        zoom_level = $1;
      `,
      [zoom.rows[0].maxzoom],
    );
    if (data.rows.length) {
      const bbox = getBBoxFromTiles(
        data.rows[0].xMin,
        data.rows[0].yMin,
        data.rows[0].xMax,
        data.rows[0].yMax,
        zoom.rows[0].maxzoom,
        "xyz",
      );

      // Clamp
      bbox[0] = limitValue(bbox[0], -MAX_LON, MAX_LON);
      bbox[2] = limitValue(bbox[2], -MAX_LON, MAX_LON);
      bbox[1] = limitValue(bbox[1], -MAX_LAT, MAX_LAT);
      bbox[3] = limitValue(bbox[3], -MAX_LAT, MAX_LAT);

      return bbox;
    }
  }
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
  const selectSQL =
    "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE hash IS NULL LIMIT $1;";
  const updateSQL =
    "UPDATE tiles SET hash = $1, created = $2 WHERE zoom_level = $3 AND tile_column = $4 AND tile_row = $5;";

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
        "ALTER TABLE tiles ADD COLUMN IF NOT EXISTS hash TEXT;",
      );
    } catch (error) {
      printLog(
        "warn",
        `Failed to create column "hash" for table "tiles" of PostgreSQL DB "${uri}": ${error}`,
      );
    }

    try {
      await source.query(
        "ALTER TABLE tiles ADD COLUMN IF NOT EXISTS created BIGINT;",
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
  const data = await source.query("SELECT name, value FROM metadata;");

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
  const sql =
    "INSERT INTO metadata (name, value) VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET value = excluded.value;";

  await openPostgreSQLTransaction(source);

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

  await closePostgreSQLTransaction(source);
}

/**
 * Store PostgreSQL tile data
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data
 * @param {{ source: pg.Client, storeTransparent: boolean, created: number }} option Option
 * @returns {Promise<void>}
 */
export async function storePostgreSQLTileData(z, x, y, data, option) {
  if (
    option.storeTransparent === false &&
    (await isFullTransparentImage(data))
  ) {
    return;
  } else {
    await option.source.query(POSTGRESQL_INSERT_TILE_QUERY, [
      z,
      x,
      y,
      data,
      calculateMD5(data),
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

  const querySQL =
    "SELECT tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = $1 AND tile_column BETWEEN $2 AND $3 AND tile_row BETWEEN $4 AND $5;";

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

    const tiles = await source.query(querySQL, [z + 1, minX, maxX, minY, maxY]);

    if (tiles.rows.length) {
      // Create composites option
      const compositesOption = [];

      for (const tile of tiles.rows) {
        if (!tile.tile_data) {
          continue;
        }

        compositesOption.push({
          limitInputPixels: false,
          input: await createImageOutput({
            data: tile.tile_data,
          }),
          left: (tile.tile_column - minX) * width,
          top: (tile.tile_row - minY) * height,
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

        storePostgreSQLTileData(z, x, tmpY, dataTile.data, {
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
