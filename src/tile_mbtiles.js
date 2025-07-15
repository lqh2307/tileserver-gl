"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import sharp from "sharp";
import {
  openSQLiteWithTimeout,
  execSQLWithTimeout,
  runSQLWithTimeout,
  closeSQLite,
} from "./sqlite.js";
import {
  isFullTransparentPNGImage,
  detectFormatAndHeaders,
  handleTilesConcurrency,
  createImageOutput,
  getBBoxFromTiles,
  getDataFromURL,
  getTileBounds,
  calculateMD5,
  retry,
} from "./utils.js";

/**
 * Get MBTiles layers from tiles
 * @param {Database} source SQLite database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getMBTilesLayersFromTiles(source) {
  const layerNames = new Set();
  const batchSize = 256;
  let offset = 0;

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto")
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
    `
  );

  while (true) {
    const rows = sql.all([batchSize, offset]);

    if (!rows.length) {
      break;
    }

    rows.forEach((row) =>
      vectorTileProto.tile
        .decode(row.tile_data)
        .layers.map((layer) => layer.name)
        .forEach((layer) => layerNames.add(layer))
    );

    offset += batchSize;
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
      `
    )
    .all();

  let bbox = [-180, -85.051129, 180, 85.051129];

  if (rows.length) {
    bbox = getBBoxFromTiles(
      rows[0].xMin,
      rows[0].yMin,
      rows[0].xMax,
      rows[0].yMax,
      rows[0].zoom_level,
      "tms"
    );

    for (let index = 1; index < rows.length; index++) {
      const _bbox = getBBoxFromTiles(
        rows[index].xMin,
        rows[index].yMin,
        rows[index].xMax,
        rows[index].yMax,
        rows[index].zoom_level,
        "tms"
      );

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
        : "SELECT MAX(zoom_level) AS zoom FROM tiles;"
    )
    .get();

  return data?.zoom;
}

/**
 * Get MBTiles tile format from tiles
 * @param {Database} source SQLite database instance
 * @returns {string}
 */
function getMBTilesFormatFromTiles(source) {
  const data = source.prepare("SELECT tile_data FROM tiles LIMIT 1;").get();

  if (data?.tile_data) {
    return detectFormatAndHeaders(data.tile_data).format;
  }
}

/**
 * Create MBTiles tile
 * @param {Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function createMBTilesTile(source, z, x, y, data, timeout) {
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
    timeout
  );
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
  isCreated
) {
  const { tileBounds } = getTileBounds({ coverages: coverages, scheme: "tms" });

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
 * @returns {Promise<void>}
 */
export async function calculateMBTilesTileExtraInfo(source) {
  const batchSize = 256;

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
    `
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
          30000 // 30 secs
        )
      )
    );
  }
}

/**
 * Delete a tile from MBTiles tiles table
 * @param {Database} source SQLite database instance
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
    [z, x, (1 << z) - 1 - y],
    timeout
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
      30000 // 30 secs
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
      30000 // 30 secs
    );

    const tableInfos = source.prepare("PRAGMA table_info(tiles);").all();

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        await execSQLWithTimeout(
          source,
          `ALTER TABLE
            tiles
          ADD COLUMN
            hash TEXT;
          `,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to create column "hash" for table "tiles" of MBTiles DB "${filePath}": ${error}`
        );
      }
    }

    if (!tableInfos.some((col) => col.name === "created")) {
      try {
        await execSQLWithTimeout(
          source,
          `ALTER TABLE
            tiles
          ADD COLUMN
            created BIGINT;
          `,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to create column "created" for table "tiles" of MBTiles DB "${filePath}": ${error}`
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
  let data = source
    .prepare(
      `
      SELECT
        tile_data
      FROM
        tiles
      WHERE
        zoom_level = ? AND tile_column = ? AND tile_row = ?;
      `
    )
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
        timeout
      )
    )
  );
}

/**
 * Download MBTiles tile data
 * @param {string} url The URL to download the file from
 * @param {Database} source SQLite database instance
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
  headers
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(
        url,
        timeout,
        "arraybuffer",
        false,
        headers
      );

      // Store data
      await cacheMBtilesTileData(
        source,
        z,
        x,
        y,
        response.data,
        storeTransparent
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
 * @param {Database} source SQLite database instance
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
  storeTransparent
) {
  if (storeTransparent === false && (await isFullTransparentPNGImage(data))) {
    return;
  } else {
    await createMBTilesTile(
      source,
      z,
      x,
      y,
      data,
      30000 // 30 secs
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
    30000 // 30 secs
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
 * @returns {Promise<void>}
 */
export async function addMBTilesOverviews(
  source,
  concurrency,
  tileSize = 256
) {
  /* Get tile width & height */
  const data = source.prepare("SELECT tile_data FROM tiles LIMIT 1;").get();

  if (!data?.tile_data) {
    return;
  }

  const { width, height } = await sharp(data.tile_data, {
    limitInputPixels: false,
  }).metadata();

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

  /* Get delta z */
  let deltaZ = 0;
  const targetTileSize = Math.floor(tileSize * 0.95);

  while (deltaZ < metadata.maxzoom && (sourceWidth > targetTileSize || sourceheight > targetTileSize)) {
    sourceWidth /= 2;
    sourceheight /= 2;

    deltaZ++;
  }

  for (let _deltaZ = 1; _deltaZ <= deltaZ; _deltaZ++) {
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
        `
        )
        .all([z + 1, minX, maxX, minY, maxY]);

      const composites = [];

      const compositeImage = sharp({
        create: {
          width: width * 2,
          height: height * 2,
          channels: 4,
          background: { r: 0, g: 0, b: 0, alpha: 0 },
        },
      });

      for (const tile of tiles) {
        if (!tile.tile_data) {
          continue;
        }

        composites.push({
          input: await sharp(tile.tile_data, {
            limitInputPixels: false,
          }).toBuffer(),
          left: (tile.tile_column - minX) * width,
          top: (maxY - tile.tile_row) * height,
        });
      }

      if (!composites.length) {
        return;
      }

      const image = await createImageOutput(
        sharp(
          await compositeImage
            .composite(composites)
            .toFormat(metadata.format)
            .toBuffer(),
          {
            limitInputPixels: false,
          }
        ),
        {
          format: metadata.format,
          width: width,
          height: height,
        }
      );

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
        30000
      );
    }

    const { tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.maxzoom - _deltaZ,
      maxZoom: metadata.maxzoom - _deltaZ,
      scheme: "tms",
    });

    await handleTilesConcurrency(concurrency, createTileData, tileBounds);
  }

  /* Update minzoom */
  source.prepare(
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
    `)
    .run(["minzoom", metadata.maxzoom - deltaZ]);
}
