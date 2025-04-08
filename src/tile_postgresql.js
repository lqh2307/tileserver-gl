"use strict";

import { closePostgreSQL, openPostgreSQL } from "./postgresql.js";
import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import {
  getTileBoundsFromCoverages,
  isFullTransparentPNGImage,
  detectFormatAndHeaders,
  getBBoxFromTiles,
  getDataFromURL,
  calculateMD5,
  deepClone,
  retry,
} from "./utils.js";

/**
 * Get PostgreSQL layers from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<[string, string, string, string]>}
 */
async function getPostgreSQLLayersFromTiles(source) {
  const layerNames = new Set();
  const batchSize = 256;
  let offset = 0;

  const vectorTileProto = protobuf(
    await fsPromise.readFile("public/protos/vector_tile.proto")
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
      [batchSize, offset]
    );

    if (data.rows.length === 0) {
      break;
    }

    data.rows.forEach((row) =>
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
    `
  );

  let bbox = [-180, -85.051129, 180, 85.051129];

  for (let index = 0; index < data.rows.length; index++) {
    const _bbox = getBBoxFromTiles(
      data.rows[index].xMin,
      data.rows[index].yMin,
      data.rows[index].xMax,
      data.rows[index].yMax,
      data.rows[index].zoom_level,
      "xyz"
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
 * Get PostgreSQL zoom level from tiles
 * @param {pg.Client} source PostgreSQL database instance
 * @param {"minzoom"|"maxzoom"} zoomType
 * @returns {Promise<number>}
 */
async function getPostgreSQLZoomLevelFromTiles(source, zoomType) {
  const data = await source.query(
    zoomType === "minzoom"
      ? "SELECT MIN(zoom_level) AS zoom FROM tiles;"
      : "SELECT MAX(zoom_level) AS zoom FROM tiles;"
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
async function getPostgreSQLFormatFromTiles(source) {
  const data = await source.query("SELECT tile_data FROM tiles LIMIT 1;");

  if (data.rows.length !== 0) {
    return detectFormatAndHeaders(data.rows[0].tile_data).format;
  }
}

/**
 * Create PostgreSQL tile
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {Buffer} data Tile data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function createPostgreSQLTile(source, z, x, y, data, timeout) {
  await source.query({
    text: `
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
    values: [z, x, y, data, calculateMD5(data), Date.now()],
    statement_timeout: timeout,
  });
}

/**
 * Get PostgreSQL tile hash from coverages
 * @param {pg.Client} source PostgreSQL database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @returns {Promise<Object<string, string>>} Hash object
 */
export async function getPostgreSQLTileHashFromCoverages(source, coverages) {
  const { tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

  let query = "";

  tileBounds.forEach((tileBound, idx) => {
    if (idx > 0) {
      query += " UNION ALL ";
    }

    query += `SELECT zoom_level, tile_column, tile_row, hash FROM tiles WHERE zoom_level = ${tileBound.z} AND tile_column BETWEEN ${tileBound.x[0]} AND ${tileBound.x[1]} AND tile_row BETWEEN ${tileBound.y[0]} AND ${tileBound.y[1]}`;
  });

  query += ";";

  const result = {};
  const data = await source.query(query);

  data.rows.forEach((row) => {
    if (row.hash !== null) {
      result[`${row.zoom_level}/${row.tile_column}/${row.tile_row}`] = row.hash;
    }
  });

  return result;
}

/**
 * Calculate PostgreSQL tile hash
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<void>}
 */
export async function calculatePostgreSQLTileHash(source) {
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
        256
      OFFSET
        0;
      `
    );

    if (data.rows.length === 0) {
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
          ]
        )
      )
    );
  }
}

/**
 * Delete a tile from PostgreSQL tiles table
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removePostgreSQLTile(source, z, x, y, timeout) {
  await source.query({
    text: `
    DELETE FROM
      tiles
    WHERE
      zoom_level = $1 AND tile_column = $2 AND tile_row = $3;
    `,
    values: [z, x, y],
    statement_timeout: timeout,
  });
}

/**
 * Open PostgreSQL database
 * @param {string} uri Database URI
 * @param {boolean} isCreate Is create database?
 * @returns {Promise<Object>}
 */
export async function openPostgreSQLDB(uri, isCreate) {
  const source = await openPostgreSQL(uri, isCreate);

  if (isCreate === true) {
    await source.query(
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (name)
        );
      `
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
          PRIMARY KEY (zoom_level, tile_column, tile_row)
        );
      `
    );

    try {
      await source.query(
        `
        ALTER TABLE
          tiles
        ADD COLUMN IF NOT EXISTS
          hash TEXT;
        `
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to create column "hash" for table "tiles" of PostgreSQL DB "${uri}": ${error}`
      );
    }

    try {
      await source.query(
        `
        ALTER TABLE
          tiles
        ADD COLUMN IF NOT EXISTS
          created BIGINT;
        `
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to create column "created" for table "tiles" of PostgreSQL DB "${uri}": ${error}`
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
 * @returns {Promise<Object>}
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
    [z, x, y]
  );

  if (data.rows.length === 0) {
    throw new Error("Tile does not exist");
  }

  data = Buffer.from(data.rows[0].tile_data);

  return {
    data: data,
    headers: detectFormatAndHeaders(data).headers,
  };
}

/**
 * Get PostgreSQL metadata
 * @param {pg.Client} source PostgreSQL database instance
 * @returns {Promise<Object>}
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
      metadata.minzoom = await getPostgreSQLZoomLevelFromTiles(
        source,
        "minzoom"
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
        "maxzoom"
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
 * Create PostgreSQL metadata
 * @param {Object} metadata Metadata object
 * @returns {Object}
 */
export function createPostgreSQLMetadata(metadata) {
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
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function updatePostgreSQLMetadata(source, metadataAdds, timeout) {
  await Promise.all(
    Object.entries({
      ...metadataAdds,
      center: metadataAdds.center.join(","),
      bounds: metadataAdds.bounds.join(","),
      scheme: "xyz",
    }).map(([name, value]) =>
      source.query({
        text: `
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
        values: [
          name,
          typeof value === "object" ? JSON.stringify(value) : value,
        ],
        statement_timeout: timeout,
      })
    )
  );
}

/**
 * Get PostgreSQL tile from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Object>}
 */
export async function getPostgreSQLTileFromURL(url, timeout) {
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
 * Download PostgreSQL tile data
 * @param {string} url The URL to download the file from
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
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
  storeTransparent
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data
      await cachePostgreSQLTileData(
        source,
        z,
        x,
        y,
        response.data,
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
  storeTransparent
) {
  if (
    storeTransparent === false &&
    (await isFullTransparentPNGImage(data)) === true
  ) {
    return;
  } else {
    await createPostgreSQLTile(
      source,
      z,
      x,
      y,
      data,
      300000 // 5 mins
    );
  }
}

/**
 * Get MD5 hash of PostgreSQL tile
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<string>} Returns the MD5 hash as a string
 */
export async function getPostgreSQLTileMD5(source, z, x, y) {
  const data = await source.query(
    `
    SELECT
      hash
    FROM
      tiles
    WHERE
      zoom_level = $1 AND tile_column = $2 AND tile_row = $3;
    `,
    [z, x, y]
  );

  if (data.rows.length === 0) {
    throw new Error("Tile MD5 does not exist");
  }

  return data.rows[0].hash;
}

/**
 * Get created of PostgreSQL tile
 * @param {pg.Client} source PostgreSQL database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<number>} Returns the created as a number
 */
export async function getPostgreSQLTileCreated(source, z, x, y) {
  const data = await source.query(
    `
    SELECT
      created
    FROM
      tiles
    WHERE
      zoom_level = $1 AND tile_column = $2 AND tile_row = $3;
    `,
    [z, x, y]
  );

  if (data.rows.length === 0) {
    throw new Error("Tile created does not exist");
  }

  return data.rows[0].created;
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
    return Number(data.rows[0].size);
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

  await closePostgreSQLDB(source);

  if (data.rows.length !== 0) {
    return Number(data.rows[0].count);
  }
}

/**
 * Validate PostgreSQL metadata (no validate json field)
 * @param {Object} metadata PostgreSQL metadata
 * @returns {void}
 */
export function validatePostgreSQL(metadata) {
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
