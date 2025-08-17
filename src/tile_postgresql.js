"use strict";

import { StatusCodes } from "http-status-codes";
import { readFile } from "node:fs/promises";
import protobuf from "protocol-buffers";
import sharp from "sharp";
import {
  isFullTransparentPNGImage,
  handleTilesConcurrency,
  detectFormatAndHeaders,
  createImageOutput,
  getBBoxFromTiles,
  closePostgreSQL,
  openPostgreSQL,
  getDataFromURL,
  getTileBounds,
  calculateMD5,
  printLog,
  retry,
} from "./utils/index.js";

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
    await readFile("public/protos/vector_tile.proto")
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

    if (!data.rows.length) {
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

  if (data.rows.length) {
    bbox = getBBoxFromTiles(
      data.rows[0].xMin,
      data.rows[0].yMin,
      data.rows[0].xMax,
      data.rows[0].yMax,
      data.rows[0].zoom_level,
      "xyz"
    );

    for (let index = 1; index < data.rows.length; index++) {
      const _bbox = getBBoxFromTiles(
        data.rows[index].xMin,
        data.rows[index].yMin,
        data.rows[index].xMax,
        data.rows[index].yMax,
        data.rows[index].zoom_level,
        "xyz"
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
 * @returns {Promise<void>}
 */
async function createPostgreSQLTile(source, z, x, y, data) {
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
    [z, x, y, data, calculateMD5(data), Date.now()]
  );
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
  isCreated
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
  const batchSize = 256;

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
      `
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
    [z, x, y]
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
          UNIQUE(zoom_level, tile_column, tile_row)
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
    [z, x, y]
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
        [name, typeof value === "object" ? JSON.stringify(value) : value]
      )
    )
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
      await cachePostgreSQLTileData(
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
  if (storeTransparent === false && (await isFullTransparentPNGImage(data))) {
    return;
  } else {
    await createPostgreSQLTile(source, z, x, y, data);
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

  closePostgreSQLDB(source);

  if (data.rows.length !== 0) {
    return Number(data.rows[0].count);
  }
}

/**
 * Add PostgreSQL overviews (downsample to lower zoom levels)
 * @param {Database} source SQLite database instance
 * @param {number} concurrency Concurrency to generate overviews
 * @param {256|512} tileSize Tile size
 * @returns {Promise<void>}
 */
export async function addPostgreSQLOverviews(
  source,
  concurrency,
  tileSize = 256
) {
  /* Get tile width & height */
  const data = await source.query("SELECT tile_data FROM tiles LIMIT 1;");

  if (!data.rows.length || !data.rows[0].tile_data) {
    return;
  }

  const { width, height } = await sharp(data.tile_data, {
    limitInputPixels: false,
  }).metadata();

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
        `
      )
      .all([z + 1, minX, maxX, minY, maxY]);

    if (tiles.rows.length) {
      const composites = [];

      for (const tile of tiles.rows) {
        if (!tile.tile_data) {
          continue;
        }

        composites.push({
          input: await sharp(tile.tile_data, {
            limitInputPixels: false,
          }).toBuffer(),
          left: (tile.tile_column - minX) * width,
          top: (tile.tile_row - minY) * height,
        });
      }

      if (composites.length) {
        const compositeImage = sharp({
          create: {
            width: width * 2,
            height: height * 2,
            channels: 4,
            background: { r: 255, g: 255, b: 255, alpha: 0 },
          },
        });

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
          [z, x, y, image, calculateMD5(image), Date.now()]
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
    ["minzoom", metadata.maxzoom - deltaZ]
  );
}
