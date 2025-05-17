"use strict";

import { printLog } from "./logger.js";
import { config } from "./config.js";
import { Mutex } from "async-mutex";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  updatePostgreSQLMetadata,
  cachePostgreSQLTileData,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";
import {
  getAndCachePostgreSQLDataTile,
  getAndCacheMBTilesDataTile,
  getAndCacheXYZDataTile,
} from "./data.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  updateMBTilesMetadata,
  cacheMBtilesTileData,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  removeEmptyFolders,
  processCoverages,
  wait25ms,
} from "./utils.js";
import {
  getXYZTileExtraInfoFromCoverages,
  updateXYZMetadata,
  cacheXYZTileFile,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./tile_xyz.js";

/**
 * Export MBTiles tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportMBTilesTiles(
  id,
  filePath,
  metadata,
  coverages,
  concurrency,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Exporting ${total} tiles of data "${id}" to mbtiles with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open MBTiles SQLite database */
    const item = config.datas[id];

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          item.source,
          targetCoverages,
          false
        );

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateMBTilesMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          const dataTile = await getAndCacheMBTilesDataTile(id, z, x, y);

          // Store data
          await cacheMBtilesTileData(
            source,
            z,
            x,
            y,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await wait25ms();
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await wait25ms();
    }

    printLog(
      "info",
      `Completed export ${total} tiles of datas "${id}" to mbtiles after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to mbtiles after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  } finally {
    if (source !== undefined) {
      // Close MBTiles SQLite database
      closeMBTilesDB(source);
    }
  }
}

/**
 * Export XYZ tiles
 * @param {string} id Style ID
 * @param {string} sourcePath Exported source path
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportXYZTiles(
  id,
  sourcePath,
  filePath,
  metadata,
  coverages,
  concurrency,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Exporting ${total} tiles of data "${id}" to xyz with:`;
    log += `\n\tSource path: ${sourcePath}`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open MD5 SQLite database */
    const item = config.datas[id];

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getXYZTileExtraInfoFromCoverages(
          item.md5Source,
          targetCoverages,
          false
        );

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateXYZMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tile files */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          const dataTile = await getAndCacheXYZDataTile(id, z, x, y);

          // Store data
          await cacheXYZTileFile(
            sourcePath,
            source,
            z,
            x,
            y,
            metadata.format,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await wait25ms();
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportXYZTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await wait25ms();
    }

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp)$/);

    printLog(
      "info",
      `Completed export ${total} tiles of data "${id}" to xyz after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to xyz after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  } finally {
    if (source !== undefined) {
      /* Close MD5 SQLite database */
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Export PostgreSQL tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function exportPostgreSQLTiles(
  id,
  filePath,
  metadata,
  coverages,
  concurrency,
  storeTransparent,
  refreshBefore
) {
  const startTime = Date.now();

  let source;

  try {
    /* Calculate summary */
    const targetCoverages = processCoverages(coverages);
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Exporting ${total} tiles of data "${id}" to postgresql with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (typeof refreshBefore === "boolean") {
      refreshTimestamp = true;

      log += `\n\tRefresh before: check MD5`;
    }
    const refreshTimestampType = typeof refreshTimestamp;

    printLog("info", log);

    /* Open PostgreSQL database */
    const item = config.datas[id];

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let targetTileExtraInfo;
    let tileExtraInfo;

    if (refreshTimestampType === "boolean") {
      try {
        printLog(
          "info",
          `Get target tile extra info from "${item.path}" and tile extra info from "${filePath}"...`
        );

        targetTileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          item.source,
          targetCoverages,
          false
        );

        tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          false
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get target tile extra info from "${item.path}" and tile extra info from "${filePath}": ${error}`
        );

        targetTileExtraInfo = {};
        tileExtraInfo = {};
      }
    } else if (refreshTimestampType === "number") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
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

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updatePostgreSQLMetadata(
      source,
      metadata,
      30000 // 30 secs
    );

    /* Export tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    async function exportPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      if (
        refreshTimestampType === "undefined" ||
        (refreshTimestampType === "boolean" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] !== targetTileExtraInfo[tileName])) ||
        (refreshTimestampType === "number" &&
          (tileExtraInfo[tileName] === undefined ||
            tileExtraInfo[tileName] < refreshTimestamp))
      ) {
        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Exporting data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Export data
          const dataTile = await getAndCachePostgreSQLDataTile(id, z, x, y);

          // Store data
          await cachePostgreSQLTileData(
            source,
            z,
            x,
            y,
            dataTile.data,
            storeTransparent
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to export data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      }
    }

    printLog("info", "Exporting datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await wait25ms();
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          exportPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
            tasks.mutex.runExclusive(() => {
              tasks.activeTasks--;
            })
          );
        }
      }
    }

    /* Wait all tasks done */
    while (tasks.activeTasks > 0) {
      await wait25ms();
    }

    printLog(
      "info",
      `Completed export ${total} tiles of data "${id}" to postgresql after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to export data "${id}" to postgresql after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );
  } finally {
    if (source !== undefined) {
      /* Close PostgreSQL database */
      await closePostgreSQLDB(source);
    }
  }
}
