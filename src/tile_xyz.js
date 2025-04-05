"use strict";

import { StatusCodes } from "http-status-codes";
import fsPromise from "node:fs/promises";
import protobuf from "protocol-buffers";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
import sqlite3 from "sqlite3";
import {
  runSQLWithTimeout,
  closeSQLite,
  openSQLite,
  fetchOne,
  fetchAll,
  runSQL,
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
  findFolders,
  findFiles,
  deepClone,
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
    await fsPromise.readFile("public/protos/vector_tile.proto")
  );

  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
  };

  for (const pbfFilePath of pbfFilePaths) {
    /* Wait slot for a task */
    while (tasks.activeTasks >= 200) {
      await delay(50);
    }

    await tasks.mutex.runExclusive(() => {
      tasks.activeTasks++;
    });

    /* Run a task */
    (async () => {
      try {
        vectorTileProto.tile
          .decode(await fsPromise.readFile(pbfFilePath))
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
    await delay(50);
  }

  return Array.from(layerNames);
}

/**
 * Get XYZ bounding box from tiles
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<[number, number, number, number]>} Bounding box in format [minLon, minLat, maxLon, maxLat]
 */
async function getXYZBBoxFromTiles(sourcePath) {
  const zFolders = await findFolders(sourcePath, /^\d+$/, false, false);
  const boundsArr = [];

  for (const zFolder of zFolders) {
    const xFolders = await findFolders(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false
    );

    if (xFolders.length > 0) {
      const xMin = Math.min(...xFolders.map((folder) => Number(folder)));
      const xMax = Math.max(...xFolders.map((folder) => Number(folder)));

      for (const xFolder of xFolders) {
        let yFiles = await findFiles(
          `${sourcePath}/${zFolder}/${xFolder}`,
          /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
          false,
          false
        );

        if (yFiles.length > 0) {
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

  if (boundsArr.length > 0) {
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
  const folders = await findFolders(sourcePath, /^\d+$/, false, false);

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
  const zFolders = await findFolders(sourcePath, /^\d+$/, false, false);

  for (const zFolder of zFolders) {
    const xFolders = await findFolders(
      `${sourcePath}/${zFolder}`,
      /^\d+$/,
      false,
      false
    );

    for (const xFolder of xFolders) {
      const yFiles = await findFiles(
        `${sourcePath}/${zFolder}/${xFolder}`,
        /^\d+\.(gif|png|jpg|jpeg|webp|pbf)$/,
        false,
        false
      );

      if (yFiles.length > 0) {
        return yFiles[0].split(".")[1];
      }
    }
  }
}

/**
 * Create XYZ tile
 * @param {string} sourcePath XYZ folder path
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
  data,
  timeout
) {
  await Promise.all([
    createFileWithLock(
      `${sourcePath}/${z}/${x}/${y}.${format}`,
      data,
      300000 // 5 mins
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
      [z, x, y, storeMD5 === true ? calculateMD5(data) : undefined, Date.now()],
      timeout
    ),
  ]);
}

/**
 * Get XYZ tile hash from coverages
 * @param {sqlite3.Database} source SQLite database instance
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @returns {Promise<Object<string, string>>} Hash object
 */
export async function getXYZTileHashFromCoverages(source, coverages) {
  const { tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

  let query = "";
  const params = [];
  tileBounds.forEach((tileBound, idx) => {
    const { z, x, y } = tileBound;

    if (idx > 0) {
      query += " UNION ALL ";
    }

    query +=
      "SELECT zoom_level, tile_column, tile_row, hash FROM md5s WHERE zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?";

    params.push(z, ...x, ...y);
  });

  query += ";";

  const rows = await fetchAll(source, query, params);

  const result = {};
  rows.forEach((row) => {
    if (row.hash !== null) {
      result[`${row.zoom_level}/${row.tile_column}/${row.tile_row}`] = row.hash;
    }
  });

  return result;
}

/**
 * Remove XYZ tile data file
 * @param {string} id XYZ ID
 * @param {sqlite3.Database} source SQLite database instance
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
 * @param {number} mode SQLite mode (e.g: sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE | sqlite3.OPEN_READONLY)
 * @param {boolean} wal Use WAL
 * @returns {Promise<sqlite3.Database>}
 */
export async function openXYZMD5DB(filePath, mode, wal = false) {
  const source = await openSQLite(filePath, mode, wal);

  if (mode & sqlite3.OPEN_CREATE) {
    await runSQL(
      source,
      `
      CREATE TABLE IF NOT EXISTS
        metadata (
          name TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (name)
        );
      `
    );

    await runSQL(
      source,
      `
      CREATE TABLE IF NOT EXISTS
        md5s (
          zoom_level INTEGER NOT NULL,
          tile_column INTEGER NOT NULL,
          tile_row INTEGER NOT NULL,
          hash TEXT,
          created BIGINT,
          PRIMARY KEY (zoom_level, tile_column, tile_row)
        );
      `
    );

    const tableInfos = await fetchAll(source, "PRAGMA table_info(md5s)");

    if (tableInfos.some((col) => col.name === "hash") === false) {
      await runSQL(
        source,
        `ALTER TABLE
          md5s
        ADD COLUMN IF NOT EXISTS
          hash TEXT;
        `
      );
    }

    if (tableInfos.some((col) => col.name === "created") === false) {
      await runSQL(
        source,
        `ALTER TABLE
          md5s
        ADD COLUMN IF NOT EXISTS
          created BIGINT;
        `
      );
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
 * @returns {Promise<Object>}
 */
export async function getXYZTile(sourcePath, z, x, y, format) {
  try {
    let data = await fsPromise.readFile(
      `${sourcePath}/${z}/${x}/${y}.${format}`
    );
    if (!data) {
      throw new Error("Tile does not exist");
    }

    data = Buffer.from(data);

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
 * @param {sqlite3.Database} source SQLite database instance
 * @param {string} sourcePath XYZ folder path
 * @returns {Promise<Object>}
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
 * Create XYZ metadata
 * @param {Object} metadata Metadata object
 * @returns {Object}
 */
export function createXYZMetadata(metadata) {
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
 * Close the XYZ MD5 SQLite database
 * @param {sqlite3.Database} source SQLite database instance
 * @returns {Promise<void>}
 */
export async function closeXYZMD5DB(source) {
  await closeSQLite(source);
}

/**
 * Download XYZ tile data file
 * @param {string} url The URL to download the file from
 * @param {string} id XYZ ID
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
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
        storeMD5,
        storeTransparent
      );
    } catch (error) {
      printLog(
        "error",
        `Failed to download tile data file "${z}/${x}/${y}" - From "${url}": ${error}`
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
 * Update MBTiles metadata table
 * @param {sqlite3.Database} source SQLite database instance
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
 * Get XYZ tile from a URL
 * @param {string} url The URL to fetch data from
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<Object>}
 */
export async function getXYZTileFromURL(url, timeout) {
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
 * Cache XYZ tile data file
 * @param {string} sourcePath XYZ folder path
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {Buffer} data Tile data buffer
 * @param {boolean} storeMD5 Is store MD5 hashed?
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
  storeMD5,
  storeTransparent
) {
  if (
    storeTransparent === false &&
    (await isFullTransparentPNGImage(data)) === true
  ) {
    return;
  } else {
    await createXYZTile(
      sourcePath,
      source,
      z,
      x,
      y,
      format,
      storeMD5,
      data,
      300000 // 5 mins
    );
  }
}

/**
 * Get MD5 hash of XYZ tile
 * @param {sqlite3.Database} source SQLite database instance
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<string>} Returns the MD5 hash as a string
 */
export async function getXYZTileMD5(source, z, x, y) {
  const data = await fetchOne(
    source,
    `
    SELECT
      hash
    FROM
      md5s
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    [z, x, y]
  );

  if (data === undefined || data.hash === null) {
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
export async function getXYZTileCreated(source, z, x, y) {
  const data = await fetchOne(
    source,
    `
    SELECT
      created
    FROM
      md5s
    WHERE
      zoom_level = ? AND tile_column = ? AND tile_row = ?;
    `,
    [z, x, y]
  );

  if (data === undefined || data.created === null) {
    throw new Error("Tile created does not exist");
  }

  return data.created;
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
    const stat = await fsPromise.stat(fileName);

    size += stat.size;
  }

  return size;
}

/**
 * Validate XYZ metadata (no validate json field)
 * @param {Object} metadata XYZ metadata
 * @returns {void}
 */
export function validateXYZ(metadata) {
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
