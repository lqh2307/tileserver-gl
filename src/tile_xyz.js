"use strict";

import { readFile, stat } from "node:fs/promises";
import { StatusCodes } from "http-status-codes";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
import {
  openSQLiteWithTimeout,
  execSQLWithTimeout,
  runSQLWithTimeout,
  closeSQLite,
} from "./sqlite.js";
import {
  getTileBoundsFromCoverages,
  isFullTransparentPNGImage,
  detectFormatAndHeaders,
  removeFileWithLock,
  createFileWithLock,
  getBBoxFromTiles,
  getDataFromURL,
  calculateMD5,
  findFiles,
  delay,
  retry,
} from "./utils.js";

/**
 * Get XYZ layers from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[string, string, string, string]>}
 */
async function getXYZLayersFromTiles(sourcePath) {
  const pbfFilePaths = await findFiles(sourcePath, /^\d+\.pbf$/, true, true);
  const layerNames = new Set();

  const vectorTileProto = protobuf(
    await readFile("public/protos/vector_tile.proto")
  );

  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
  };

  for (const pbfFilePath of pbfFilePaths) {
    /* Wait slot for a task */
    while (tasks.activeTasks >= 256) {
      await delay(25);
    }

    await tasks.mutex.runExclusive(() => {
      tasks.activeTasks++;
    });

    /* Run a task */
    (async () => {
      try {
        vectorTileProto.tile
          .decode(await readFile(pbfFilePath))
          .layers.map((layer) => layer.name)
          .forEach((layer) => layerNames.add(layer));
      } catch (error) {
        throw error;
      } finally {
        await tasks.mutex.runExclusive(() => {
          tasks.activeTasks--;
        });
      }
    })();
  }

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(25);
  }

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
      true
    );

    if (xFolders.length) {
      const xMin = Math.min(...xFolders.map((folder) => Number(folder)));
      const xMax = Math.max(...xFolders.map((folder) => Number(folder)));

      for (const xFolder of xFolders) {
        let yFiles = await findFiles(
          `${sourcePath}/${zFolder}/${xFolder}`,
          /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
          false,
          false
        );

        if (yFiles.length) {
          yFiles = yFiles.map((yFile) => yFile.split(".")[0]);

          const yMin = Math.min(...yFiles.map((file) => Number(file)));
          const yMax = Math.max(...yFiles.map((file) => Number(file)));

          boundsArr.push(
            getBBoxFromTiles(xMin, yMin, xMax, yMax, zFolder, "xyz")
          );
        }
      }
    }
  }

  if (boundsArr.length) {
    return [
      Math.min(...boundsArr.map((bbox) => bbox[0])),
      Math.min(...boundsArr.map((bbox) => bbox[1])),
      Math.max(...boundsArr.map((bbox) => bbox[2])),
      Math.max(...boundsArr.map((bbox) => bbox[3])),
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

  return zoomType === "minzoom"
    ? Math.min(...folders.map((folder) => Number(folder)))
    : Math.max(...folders.map((folder) => Number(folder)));
}

/**
 * Get XYZ tile format from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<string>}
 */
async function getXYZFormatFromTiles(sourcePath) {
  const zFolders = await findFiles(sourcePath, /^\d+$/, false, false, true);

  for (const zFolder of zFolders) {
    const xFolders = await findFiles(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false,
      true
    );

    for (const xFolder of xFolders) {
      const yFiles = await findFiles(
        `${sourcePath}/${zFolder}/${xFolder}`,
        /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/
      );

      if (yFiles.length) {
        return yFiles[0].split(".")[1];
      }
    }
  }
}

/**
 * Create XYZ tile
 * @param {string} sourcePath XYZ folder path
 * @param {Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {Buffer} data Tile data buffer
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function createXYZTile(
  sourcePath,
  source,
  z,
  x,
  y,
  format,
  data,
  timeout
) {
  await Promise.all([
    createFileWithLock(
      `${sourcePath}/${z}/${x}/${y}.${format}`,
      data,
      30000 // 30 secs
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
      timeout
    ),
  ]);
}

/**
 * Get XYZ tile extra info from coverages
 * @param {Database} source SQLite database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {boolean} isCreated Tile created extra info
 * @returns {Object<string, string>} Extra info object
 */
export function getXYZTileExtraInfoFromCoverages(source, coverages, isCreated) {
  const { tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

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
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<void>}
 */
export async function calculatXYZTileExtraInfo(sourcePath, source, format) {
  const sql = source.prepare(
    `
    SELECT
      zoom_level, tile_column, tile_row
    FROM
      md5s
    WHERE
      hash IS NULL
    LIMIT
      256;
    `
  );

  while (true) {
    const rows = sql.all();

    if (rows.length === 0) {
      break;
    }

    await Promise.all(
      rows.map(async (row) => {
        const data = await getXYZTile(
          sourcePath,
          row.zoom_level,
          row.tile_column,
          row.tile_row,
          format
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
          30000 // 30 secs
        );
      })
    );
  }
}

/**
 * Remove XYZ tile data file
 * @param {string} id XYZ ID
 * @param {Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
export async function removeXYZTile(id, source, z, x, y, format, timeout) {
  await Promise.all([
    removeFileWithLock(
      `${process.env.DATA_DIR}/caches/xyzs/${id}/${z}/${x}/${y}.${format}`,
      timeout
    ),
    runSQLWithTimeout(
      source,
      `
      DELETE FROM
        md5s
      WHERE
        zoom_level = ? AND tile_column = ? AND tile_row = ?;
      `,
      [z, x, y],
      timeout
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
      30000 // 30 secs
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
      30000 // 30 secs
    );

    const tableInfos = source.prepare("PRAGMA table_info(md5s);").all();

    if (!tableInfos.some((col) => col.name === "hash")) {
      try {
        await execSQLWithTimeout(
          source,
          `ALTER TABLE
            md5s
          ADD COLUMN
            hash TEXT;
          `,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to create column "hash" for table "md5s" of XYZ MD5 DB "${filePath}": ${error}`
        );
      }
    }

    if (!tableInfos.some((col) => col.name === "created")) {
      try {
        await execSQLWithTimeout(
          source,
          `ALTER TABLE
            md5s
          ADD COLUMN
            created BIGINT;
          `,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to create column "created" for table "md5s" of XYZ MD5 DB "${filePath}": ${error}`
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
 * @param {Database} source SQLite database instance
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<object>}
 */
export async function getXYZMetadata(source, sourcePath) {
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
 * @param {string} url The URL to download the file from
 * @param {string} id XYZ ID
 * @param {Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @returns {Promise<void>}
 */
export async function downloadXYZTile(
  url,
  id,
  source,
  z,
  x,
  y,
  format,
  maxTry,
  timeout,
  storeTransparent
) {
  await retry(async () => {
    try {
      // Get data from URL
      const response = await getDataFromURL(url, timeout, "arraybuffer");

      // Store data to file
      await cacheXYZTileFile(
        `${process.env.DATA_DIR}/caches/xyzs/${id}`,
        source,
        z,
        x,
        y,
        format,
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
 * Update MBTiles metadata table
 * @param {Database} source SQLite database instance
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
        timeout
      )
    )
  );
}

/**
 * Cache XYZ tile data file
 * @param {string} sourcePath XYZ folder path
 * @param {Database} source SQLite database instance
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
  storeTransparent
) {
  if (storeTransparent === false && (await isFullTransparentPNGImage(data))) {
    return;
  } else {
    await createXYZTile(
      sourcePath,
      source,
      z,
      x,
      y,
      format,
      data,
      30000 // 30 secs
    );
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
    false
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
    true
  );

  let size = 0;

  for (const fileName of fileNames) {
    const stats = await stat(fileName);

    size += stats.size;
  }

  return size;
}
