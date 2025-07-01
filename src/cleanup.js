"use strict";

import { getGeoJSONCreated, removeGeoJSONFile } from "./geojson.js";
import { getSpriteCreated, removeSpriteFile } from "./sprite.js";
import { removeStyleFile, getStyleCreated } from "./style.js";
import { getFontCreated, removeFontFile } from "./font.js";
import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { printLog } from "./logger.js";
import {
  getXYZTileExtraInfoFromCoverages,
  removeXYZTile,
  closeXYZMD5DB,
  openXYZMD5DB,
  compactXYZ,
} from "./tile_xyz.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  removeMBTilesTile,
  compactMBTiles,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  handleTilesConcurrency,
  createFileWithLock,
  removeEmptyFolders,
  getJSONSchema,
  validateJSON,
} from "./utils.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  removePostgreSQLTile,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";

let cleanUp;

/* Load cleanup.json */
if (cleanUp === undefined) {
  try {
    cleanUp = JSON.parse(
      readFileSync(`${process.env.DATA_DIR || "data"}/cleanup.json`, "utf8")
    );
  } catch (error) {
    printLog("error", `Failed to load cleanup.json file: ${error}`);

    cleanUp = {};
  }
}

/**
 * Validate cleanup.json file
 * @returns {Promise<void>}
 */
async function validateCleanUpFile() {
  validateJSON(await getJSONSchema("cleanup"), cleanUp);
}

/**
 * Read cleanup.json file
 * @param {boolean} isParse Parse JSON?
 * @returns {Promise<object>}
 */
async function readCleanUpFile(isParse) {
  const data = await readFile(`${process.env.DATA_DIR}/cleanup.json`, "utf8");

  if (isParse) {
    return JSON.parse(data);
  } else {
    return data;
  }
}

/**
 * Update cleanup.json file content with lock
 * @param {object} cleanUp Cleanup object
 * @param {number} timeout Timeout in milliseconds
 * @returns {Promise<void>}
 */
async function updateCleanUpFile(cleanUp, timeout) {
  await createFileWithLock(
    `${process.env.DATA_DIR}/cleanup.json`,
    JSON.stringify(cleanUp, null, 2),
    timeout
  );
}

/**
 * Cleanup MBTiles tiles
 * @param {string} id Cleanup MBTiles ID
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpMBTilesTiles(id, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const concurrency = 256;

    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of mbtiles "${id}" with:`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tCleanup before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open MBTiles SQLite database */
    const filePath = `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`;

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          coverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Remove tiles */
    async function cleanUpMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
        return;
      }

      const completeTasks = tasks.completeTasks;

      printLog(
        "info",
        `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        await removeMBTilesTile(
          source,
          z,
          x,
          y,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    await handleTilesConcurrency(concurrency, cleanUpMBTilesTileData, tileBounds);

    /* Compact MBTiles (Block DB) */
    // compactMBTiles(source);

    printLog(
      "info",
      `Completed cleanup ${total} tiles of mbtiles "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close MBTiles SQLite database */
    if (source) {
      closeMBTilesDB(source);
    }
  }
}

/**
 * Cleanup PostgreSQL tiles
 * @param {string} id Cleanup PostgreSQL ID
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpPostgreSQLTiles(id, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const concurrency = 256;

    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of postgresql "${id}" with:`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tCleanup before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open PostgreSQL database */
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          coverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Remove tiles */
    async function cleanUpPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
        return;
      }

      const completeTasks = tasks.completeTasks;

      printLog(
        "info",
        `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        await removePostgreSQLTile(
          source,
          z,
          x,
          y,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    await handleTilesConcurrency(concurrency, cleanUpPostgreSQLTileData, tileBounds);

    printLog(
      "info",
      `Completed cleanup ${total} tiles of postgresql "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close PostgreSQL database */
    if (source) {
      closePostgreSQLDB(source);
    }
  }
}

/**
 * Cleanup XYZ tiles
 * @param {string} id Cleanup XYZ ID
 * @param {"jpeg"|"jpg"|"pbf"|"png"|"webp"|"gif"} format Tile format
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpXYZTiles(id, format, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const concurrency = 256;

    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of xyz "${id}" with:`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tCleanup before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open XYZ MD5 SQLite database */
    const sourcePath = `${process.env.DATA_DIR}/caches/xyzs/${id}`;
    const filePath = `${sourcePath}/${id}.sqlite`;

    source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          coverages,
          true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Remove tile files */
    async function cleanUpXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
        return;
      }

      const completeTasks = tasks.completeTasks;

      printLog(
        "info",
        `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        await removeXYZTile(
          id,
          source,
          z,
          x,
          y,
          format,
          30000 // 30 secs
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    await handleTilesConcurrency(concurrency, cleanUpXYZTileData, tileBounds);

    /* Compact XYZ (Block DB) */
    // compactXYZ(source);

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp|pbf)$/);

    printLog(
      "info",
      `Completed cleanup ${total} tiles of xyz "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close XYZ MD5 SQLite database */
    if (source) {
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Cleanup geojson
 * @param {string} id Cleanup geojson ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpGeoJSON(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up geojson "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tCleanup before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove GeoJSON file */
  const sourcePath = `${process.env.DATA_DIR}/caches/geojsons/${id}`;
  const filePath = `${sourcePath}/${id}.geojson`;

  try {
    let needRemove = false;

    if (cleanUpTimestamp) {
      try {
        const created = await getGeoJSONCreated(filePath);

        if (created === undefined || created < cleanUpTimestamp) {
          needRemove = true;
        }
      } catch (error) {
        if (error.message === "GeoJSON created does not exist") {
          needRemove = true;
        } else {
          throw error;
        }
      }
    } else {
      needRemove = true;
    }

    printLog("info", "Removing geojson...");

    if (needRemove) {
      printLog("info", `Removing geojson "${id}" - File "${filePath}"...`);

      await removeGeoJSONFile(
        filePath,
        30000 // 30 secs
      );
    }
  } catch (error) {
    printLog("error", `Failed to cleanup geojson "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.geojson$/);

  printLog(
    "info",
    `Completed cleanup geojson "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Cleanup sprite
 * @param {string} id Cleanup sprite ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpSprite(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up sprite "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tCleanup before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove sprite files */
  const sourcePath = `${process.env.DATA_DIR}/caches/sprites/${id}`;

  async function cleanUpSpriteData(fileName) {
    const filePath = `${sourcePath}/${fileName}`;

    try {
      let needRemove = false;

      if (cleanUpTimestamp) {
        try {
          const created = await getSpriteCreated(filePath);

          if (created === undefined || created < cleanUpTimestamp) {
            needRemove = true;
          }
        } catch (error) {
          if (error.message === "Sprite created does not exist") {
            needRemove = true;
          } else {
            throw error;
          }
        }
      } else {
        needRemove = true;
      }

      if (needRemove) {
        printLog("info", `Removing sprite "${id}" - File "${fileName}"...`);

        await removeSpriteFile(
          filePath,
          30000 // 30 secs
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to cleanup sprite "${id}" - File "${fileName}": ${error}`
      );
    }
  }

  printLog("info", "Removing sprites...");

  await Promise.all(
    ["sprite.json", "sprite.png", "sprite@2x.json", "sprite@2x.png"].map(
      (fileName) => cleanUpSpriteData(fileName)
    )
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.(json|png)$/);

  printLog(
    "info",
    `Completed cleanup sprite "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Cleanup font
 * @param {string} id Cleanup font ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpFont(id, cleanUpBefore) {
  const startTime = Date.now();

  const total = 256;

  let log = `Cleaning up ${total} fonts of font "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tCleanup before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove font files */
  const sourcePath = `${process.env.DATA_DIR}/caches/fonts/${id}`;

  async function cleanUpFontData(start, end) {
    const range = `${start}-${end}`;
    const filePath = `${sourcePath}/${range}.pbf`;

    try {
      let needRemove = false;

      if (cleanUpTimestamp) {
        try {
          const created = await getFontCreated(filePath);

          if (created === undefined || created < cleanUpTimestamp) {
            needRemove = true;
          }
        } catch (error) {
          if (error.message === "Font created does not exist") {
            needRemove = true;
          } else {
            throw error;
          }
        }
      } else {
        needRemove = true;
      }

      if (needRemove) {
        printLog("info", `Removing font "${id}" - Range "${range}"...`);

        await removeFontFile(
          filePath,
          30000 // 30 secs
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to cleanup font "${id}" -  Range "${range}": ${error}`
      );
    }
  }

  printLog("info", "Removing fonts...");

  await Promise.all(
    Array.from({ length: total }, (_, i) =>
      cleanUpFontData(i * 256, i * 256 + 255)
    )
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.pbf$/);

  printLog(
    "info",
    `Completed cleanup ${total} fonts of font "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Cleanup style
 * @param {string} id Cleanup style ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpStyle(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up style "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tCleanup before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove style.json file */
  const sourcePath = `${process.env.DATA_DIR}/caches/styles/${id}`;
  const filePath = `${sourcePath}/style.json`;

  try {
    let needRemove = false;

    if (cleanUpTimestamp) {
      try {
        const created = await getStyleCreated(filePath);

        if (created === undefined || created < cleanUpTimestamp) {
          needRemove = true;
        }
      } catch (error) {
        if (error.message === "Style created does not exist") {
          needRemove = true;
        } else {
          throw error;
        }
      }
    } else {
      needRemove = true;
    }

    printLog("info", "Removing style...");

    if (needRemove) {
      printLog("info", `Removing style "${id}" - File "${filePath}"...`);

      await removeStyleFile(
        filePath,
        30000 // 30 secs
      );
    }
  } catch (error) {
    printLog("error", `Failed to cleanup style "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.json$/);

  printLog(
    "info",
    `Completed cleanup style "${id}" after ${(Date.now() - startTime) / 1000}s!`
  );
}

export {
  cleanUpPostgreSQLTiles,
  validateCleanUpFile,
  cleanUpMBTilesTiles,
  updateCleanUpFile,
  readCleanUpFile,
  cleanUpXYZTiles,
  cleanUpGeoJSON,
  cleanUpSprite,
  cleanUpStyle,
  cleanUpFont,
  cleanUp,
};
