"use strict";

import { getGeoJSONCreated, removeGeoJSONFile } from "./geojson.js";
import { getSpriteCreated, removeSpriteFile } from "./sprite.js";
import { removeStyleFile, getStyleCreated } from "./style.js";
import { getFontCreated, removeFontFile } from "./font.js";
import fsPromise from "node:fs/promises";
import { printLog } from "./logger.js";
import { Mutex } from "async-mutex";
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
  createFileWithLock,
  removeEmptyFolders,
  getJSONSchema,
  validateJSON,
  delay,
} from "./utils.js";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  removePostgreSQLTile,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";

let cleanUp = {};

/**
 * Read cleanup.json file
 * @param {boolean} isValidate Is validate file content?
 * @returns {Promise<Object>}
 */
async function readCleanUpFile(isValidate) {
  /* Read cleanup.json file */
  const data = await fsPromise.readFile(
    `${process.env.DATA_DIR}/cleanup.json`,
    "utf8"
  );

  const cleanUp = JSON.parse(data);

  /* Validate cleanup.json file */
  if (isValidate === true) {
    validateJSON(await getJSONSchema("cleanup"), cleanUp);
  }

  return cleanUp;
}

/**
 * Load cleanup.json file content to global variable
 * @returns {Promise<void>}
 */
async function loadCleanUpFile() {
  Object.assign(cleanUp, await readCleanUpFile(true));
}

/**
 * Update cleanup.json file content with lock
 * @param {Object} cleanUp Clean up object
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
 * Clean up MBTiles tiles
 * @param {string} id Clean up MBTiles ID
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpMBTilesTiles(id, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of mbtiles "${id}" with:\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tClean up before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open MBTiles SQLite database */
    source = await openMBTilesDB(
      `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp !== undefined) {
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
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function cleanUpMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;
      const completeTasks = tasks.completeTasks;

      try {
        if (
          cleanUpTimestamp === undefined ||
          (cleanUpTimestamp !== undefined &&
            (tileExtraInfo[tileName] === undefined ||
              tileExtraInfo[tileName] < cleanUpTimestamp))
        ) {
          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          await removeMBTilesTile(
            source,
            z,
            x,
            y,
            30000 // 30 secs
          );
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to clean up data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= 256) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          cleanUpMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    /* Compact MBTiles (Block DB) */
    // compactMBTiles(source);

    printLog(
      "info",
      `Completed clean up ${total} tiles of mbtiles "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      /* Close MBTiles SQLite database */
      closeMBTilesDB(source);
    }
  }
}

/**
 * Clean up PostgreSQL tiles
 * @param {string} id Clean up PostgreSQL ID
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpPostgreSQLTiles(id, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of postgresql "${id}" with:\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tClean up before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open PostgreSQL database */
    source = await openPostgreSQLDB(
      `${process.env.POSTGRESQL_BASE_URI}/${id}`,
      true
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp !== undefined) {
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
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function cleanUpPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;
      const completeTasks = tasks.completeTasks;

      try {
        if (
          cleanUpTimestamp === undefined ||
          (cleanUpTimestamp !== undefined &&
            (tileExtraInfo[tileName] === undefined ||
              tileExtraInfo[tileName] < cleanUpTimestamp))
        ) {
          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          await removePostgreSQLTile(
            source,
            z,
            x,
            y,
            30000 // 30 secs
          );
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to clean up data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= 256) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          cleanUpPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    printLog(
      "info",
      `Completed clean up ${total} tiles of postgresql "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      /* Close PostgreSQL database */
      await closePostgreSQLDB(source);
    }
  }
}

/**
 * Clean up XYZ tiles
 * @param {string} id Clean up XYZ ID
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
    const { total, tileBounds } = getTileBoundsFromCoverages(coverages, "xyz");

    let log = `Cleaning up ${total} tiles of xyz "${id}" with:\n\tCoverages: ${JSON.stringify(
      coverages
    )}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tClean up before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    printLog("info", log);

    /* Open XYZ MD5 SQLite database */
    source = await openXYZMD5DB(
      `${process.env.DATA_DIR}/caches/xyzs/${id}/${id}.sqlite`,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (cleanUpTimestamp !== undefined) {
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
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function cleanUpXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;
      const completeTasks = tasks.completeTasks;

      try {
        if (
          cleanUpTimestamp === undefined ||
          (cleanUpTimestamp !== undefined &&
            (tileExtraInfo[tileName] === undefined ||
              tileExtraInfo[tileName] < cleanUpTimestamp))
        ) {
          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          await removeXYZTile(
            id,
            source,
            z,
            x,
            y,
            format,
            30000 // 30 secs
          );
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to clean up data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    printLog("info", "Removing datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= 256) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          cleanUpXYZTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await delay(50);
    }

    /* Compact XYZ (Block DB) */
    // compactXYZ(source);

    /* Remove parent folders if empty */
    await removeEmptyFolders(
      `${process.env.DATA_DIR}/caches/xyzs/${id}`,
      /^.*\.(gif|png|jpg|jpeg|webp|pbf)$/
    );

    printLog(
      "info",
      `Completed clean up ${total} tiles of xyz "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    throw error;
  } finally {
    if (source !== undefined) {
      /* Close XYZ MD5 SQLite database */
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Clean up geojson
 * @param {string} id Clean up geojson ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpGeoJSON(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up geojson "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tClean up before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove GeoJSON file */
  const filePath = `${process.env.DATA_DIR}/caches/geojsons/${id}/${id}.geojson`;

  try {
    let needRemove = false;

    if (cleanUpTimestamp !== undefined) {
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

    if (needRemove === true) {
      printLog("info", `Removing geojson "${id}" - File "${filePath}"...`);

      await removeGeoJSONFile(
        filePath,
        30000 // 30 secs
      );
    }
  } catch (error) {
    printLog("error", `Failed to clean up geojson "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/geojsons/${id}`,
    /^.*\.geojson$/
  );

  printLog(
    "info",
    `Completed clean up geojson "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Clean up sprite
 * @param {string} id Clean up sprite ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpSprite(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up sprite "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tClean up before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove sprite files */
  async function cleanUpSpriteData(fileName) {
    const filePath = `${process.env.DATA_DIR}/caches/sprites/${id}/${fileName}`;

    try {
      let needRemove = false;

      if (cleanUpTimestamp !== undefined) {
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

      if (needRemove === true) {
        printLog("info", `Removing sprite "${id}" - File "${fileName}"...`);

        await removeSpriteFile(
          filePath,
          30000 // 30 secs
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to clean up sprite "${id}" - File "${fileName}": ${error}`
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/sprites/${id}`,
    /^.*\.(json|png)$/
  );

  printLog(
    "info",
    `Completed clean up sprite "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Clean up font
 * @param {string} id Clean up font ID
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

    log += `\n\tClean up before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove font files */
  async function cleanUpFontData(start, end) {
    const range = `${start}-${end}`;
    const filePath = `${process.env.DATA_DIR}/caches/fonts/${id}/${range}.pbf`;

    try {
      let needRemove = false;

      if (cleanUpTimestamp !== undefined) {
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

      if (needRemove === true) {
        printLog("info", `Removing font "${id}" - Range "${range}"...`);

        await removeFontFile(
          filePath,
          30000 // 30 secs
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to clean up font "${id}" -  Range "${range}": ${error}`
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
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/fonts/${id}`,
    /^.*\.pbf$/
  );

  printLog(
    "info",
    `Completed clean up ${total} fonts of font "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Clean up style
 * @param {string} id Clean up style ID
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpStyle(id, cleanUpBefore) {
  const startTime = Date.now();

  let log = `Cleaning up style "${id}" with:`;

  let cleanUpTimestamp;
  if (typeof cleanUpBefore === "string") {
    cleanUpTimestamp = new Date(cleanUpBefore).getTime();

    log += `\n\tClean up before: ${cleanUpBefore}`;
  } else if (typeof cleanUpBefore === "number") {
    const now = new Date();

    cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

    log += `\n\tOld than: ${cleanUpBefore} days`;
  }

  printLog("info", log);

  /* Remove style.json file */
  const filePath = `${process.env.DATA_DIR}/caches/styles/${id}/style.json`;

  try {
    let needRemove = false;

    if (cleanUpTimestamp !== undefined) {
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

    if (needRemove === true) {
      printLog("info", `Removing style "${id}" - File "${filePath}"...`);

      await removeStyleFile(
        filePath,
        30000 // 30 secs
      );
    }
  } catch (error) {
    printLog("error", `Failed to clean up style "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/styles/${id}`,
    /^.*\.json$/
  );

  printLog(
    "info",
    `Completed clean up style "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

export {
  cleanUpPostgreSQLTiles,
  cleanUpMBTilesTiles,
  updateCleanUpFile,
  readCleanUpFile,
  loadCleanUpFile,
  cleanUpXYZTiles,
  cleanUpGeoJSON,
  cleanUpSprite,
  cleanUpStyle,
  cleanUpFont,
  cleanUp,
};
