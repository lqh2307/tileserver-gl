"use strict";

import { cleanUp, seed } from "./configs/index.js";
import path from "node:path";
import {
  DEFAULT_STORE_TRANSPARENT,
  DEFAULT_TILE_BATCH_SIZE,
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_INFO_TIMEOUT,
  DEFAULT_CONCURRENCY,
  DEFAULT_MAX_TRY,
} from "./defaults/index.js";
import {
  getPostgreSQLTileExtraInfo,
  MBTILES_INSERT_TILE_QUERY,
  MBTILES_DELETE_TILE_QUERY,
  updatePostgreSQLMetadata,
  getMBTilesTileExtraInfo,
  storePostgreSQLTileData,
  updateMBTilesMetadata,
  getXYZFormatFromTiles,
  removePostgreSQLTile,
  XYZ_DELETE_MD5_QUERY,
  XYZ_INSERT_MD5_QUERY,
  storeMBtilesTileData,
  getXYZTileExtraInfo,
  updateXYZMetadata,
  removeMBTilesTile,
  closePostgreSQLDB,
  getGeoJSONCreated,
  removeGeoJSONFile,
  storeXYZTileFile,
  openPostgreSQLDB,
  getSpriteCreated,
  removeSpriteFile,
  storeGeoJSONFile,
  getStyleCreated,
  removeStyleFile,
  storeSpriteFile,
  getFontCreated,
  removeFontFile,
  closeMBTilesDB,
  compactMBTiles,
  storeStyleFile,
  removeXYZTile,
  closeXYZMD5DB,
  openMBTilesDB,
  storeFontFile,
  getGeoJSONMD5,
  openXYZMD5DB,
  getSpriteMD5,
  getStyleMD5,
  compactXYZ,
  getFontMD5,
} from "./resources/index.js";
import {
  getTileBoundsBatches,
  removeEmptyFolders,
  runAllWithLimit,
  isErrorNotFound,
  getDataFromURL,
  getTileBounds,
  requestToURL,
  getDuration,
  getTaskIds,
  TASK_TYPES,
  printLog,
} from "./utils/index.js";

const SPRITE_FILES = [
  "sprite.json",
  "sprite.png",
  "sprite@2x.json",
  "sprite@2x.png",
];
const GEOJSON_FILE_REGEX = /^.*\.geojson$/;
const SPRITE_FILE_REGEX = /^.*\.(json|png)$/;
const FONT_FILE_REGEX = /^.*\.pbf$/;
const STYLE_FILE_REGEX = /^.*\.json$/;

/**
 * Run cleanup and seed tasks
 * @param {{ type?: "sprite"|"font"|"style"|"geojson"|"data", id?: string }} opts Options
 * @returns {Promise<void>}
 */
export async function runTasks(opts) {
  try {
    printLog("info", "Starting seed and cleanup tasks...");

    const { id, type } = opts;

    if (!type || TASK_TYPES.has(type)) {
      /* Cleanup sprites */
      if (!type || type === "sprite") {
        try {
          if (!cleanUp.sprites) {
            printLog("info", "No sprites in cleanup. Skipping...");
          } else {
            const spriteIds = getTaskIds(cleanUp, "sprite", id);

            printLog("info", `Starting cleanup ${spriteIds.length} sprites...`);

            const startTime = Date.now();

            for (const spriteId of spriteIds) {
              const item = cleanUp.sprites[spriteId];

              if (item.skip) {
                printLog("info", `Skipping cleanup sprite id "${spriteId}"...`);

                continue;
              }

              try {
                await cleanUpSprite(
                  spriteId,
                  item.cleanUpBefore?.time ?? item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup sprite id "${spriteId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${spriteIds.length} sprites after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup sprites: ${error}. Exited!`);
        }
      }

      /* Cleanup fonts */
      if (!type || type === "font") {
        try {
          if (!cleanUp.fonts) {
            printLog("info", "No fonts in cleanup. Skipping...");
          } else {
            const fontIds = getTaskIds(cleanUp, "font", id);

            printLog("info", `Starting cleanup ${fontIds.length} fonts...`);

            const startTime = Date.now();

            for (const fontId of fontIds) {
              const item = cleanUp.fonts[fontId];

              if (item.skip) {
                printLog("info", `Skipping cleanup font id "${fontId}"...`);

                continue;
              }

              try {
                await cleanUpFont(
                  fontId,
                  item.concurrency || DEFAULT_CONCURRENCY,
                  item.skipWhenError,
                  item.cleanUpBefore?.time ?? item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup font id "${fontId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${fontIds.length} fonts after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup fonts: ${error}. Exited!`);
        }
      }

      /* Cleanup styles */
      if (!type || type === "style") {
        try {
          if (!cleanUp.styles) {
            printLog("info", "No styles in cleanup. Skipping...");
          } else {
            const styleIds = getTaskIds(cleanUp, "style", id);

            printLog("info", `Starting cleanup ${styleIds.length} styles...`);

            const startTime = Date.now();

            for (const styleId of styleIds) {
              const item = cleanUp.styles[styleId];

              if (item.skip) {
                printLog("info", `Skipping cleanup style id "${styleId}"...`);

                continue;
              }

              try {
                await cleanUpStyle(
                  styleId,
                  item.cleanUpBefore?.time ?? item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup style id "${styleId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${styleIds.length} styles after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup styles: ${error}. Exited!`);
        }
      }

      /* Cleanup geojsons */
      if (!type || type === "geojson") {
        try {
          if (!cleanUp.geojsons) {
            printLog("info", "No geojsons in cleanup. Skipping...");
          } else {
            const geojsonIds = getTaskIds(cleanUp, "geojson", id);

            printLog(
              "info",
              `Starting cleanup ${geojsonIds.length} geojsons...`,
            );

            const startTime = Date.now();

            for (const geojsonId of geojsonIds) {
              const item = cleanUp.geojsons[geojsonId];

              if (item.skip) {
                printLog(
                  "info",
                  `Skipping cleanup geojson id "${geojsonId}"...`,
                );

                continue;
              }

              try {
                await cleanUpGeoJSON(
                  geojsonId,
                  item.cleanUpBefore?.time ?? item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup geojson id "${geojsonId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${geojsonIds.length} geojsons after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup geojsons: ${error}. Exited!`);
        }
      }

      /* Cleanup datas */
      if (!type || type === "data") {
        try {
          if (!cleanUp.datas) {
            printLog("info", "No datas in cleanup. Skipping...");
          } else {
            const dataIds = getTaskIds(cleanUp, "data", id);

            printLog("info", `Starting cleanup ${dataIds.length} datas...`);

            const startTime = Date.now();

            for (const dataId of dataIds) {
              const seedDataItem = seed.datas[dataId];
              const cleanUpDataItem = cleanUp.datas[dataId];

              if (cleanUpDataItem.skip) {
                printLog("info", `Skipping cleanup data id "${dataId}"...`);

                continue;
              }

              try {
                await cleanUpTileDatas({
                  id: dataId,
                  storeType: seedDataItem.storeType,
                  metadata: seedDataItem.metadata,
                  coverages: cleanUpDataItem.coverages,
                  concurrency: cleanUpDataItem.concurrency,
                  batch: cleanUpDataItem.batch,
                  skipWhenError: cleanUpDataItem.skipWhenError,
                  cleanUpBefore:
                    cleanUpDataItem.cleanUpBefore?.time ??
                    cleanUpDataItem.cleanUpBefore?.day,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup data id "${dataId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${dataIds.length} datas after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup datas: ${error}. Exited!`);
        }
      }

      /* Run seed sprites */
      if (!type || type === "sprite") {
        try {
          if (!seed.sprites) {
            printLog("info", "No sprites in seed. Skipping...");
          } else {
            const spriteIds = getTaskIds(seed, "sprite", id);

            printLog("info", `Starting seed ${spriteIds.length} sprites...`);

            const startTime = Date.now();

            for (const spriteId of spriteIds) {
              const item = seed.sprites[spriteId];

              if (item.skip) {
                printLog("info", `Skipping seed font id "${spriteId}"...`);

                continue;
              }

              try {
                await seedSprite({
                  id: spriteId,
                  url: item.url,
                  maxTry: item.maxTry,
                  timeout: item.timeout,
                  headers: item.headers,
                  refreshBefore:
                    item.refreshBefore?.time ??
                    item.refreshBefore?.day ??
                    item.refreshBefore?.md5,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed sprite id "${spriteId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${spriteIds.length} sprites after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed sprites: ${error}. Exited!`);
        }
      }

      /* Run seed fonts */
      if (!type || type === "font") {
        try {
          if (!seed.fonts) {
            printLog("info", "No fonts in seed. Skipping...");
          } else {
            const fontIds = getTaskIds(seed, "font", id);

            printLog("info", `Starting seed ${fontIds.length} fonts...`);

            const startTime = Date.now();

            for (const fontId of fontIds) {
              const item = seed.fonts[fontId];

              if (item.skip) {
                printLog("info", `Skipping seed font id "${fontId}"...`);

                continue;
              }

              try {
                await seedFont({
                  id: fontId,
                  url: item.url,
                  concurrency: item.concurrency,
                  maxTry: item.maxTry,
                  timeout: item.timeout,
                  headers: item.headers,
                  skipWhenError: item.skipWhenError,
                  refreshBefore:
                    item.refreshBefore?.time ??
                    item.refreshBefore?.day ??
                    item.refreshBefore?.md5,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed font id "${fontId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${fontIds.length} fonts after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed fonts: ${error}. Exited!`);
        }
      }

      /* Run seed styles */
      if (!type || type === "style") {
        try {
          if (!seed.styles) {
            printLog("info", "No styles in seed. Skipping...");
          } else {
            const styleIds = getTaskIds(seed, "style", id);

            printLog("info", `Starting seed ${styleIds.length} styles...`);

            const startTime = Date.now();

            for (const styleId of styleIds) {
              const item = seed.styles[styleId];

              if (item.skip) {
                printLog("info", `Skipping seed style id "${styleId}"...`);

                continue;
              }

              try {
                await seedStyle({
                  id: styleId,
                  url: item.url,
                  maxTry: item.maxTry,
                  timeout: item.timeout,
                  headers: item.headers,
                  refreshBefore:
                    item.refreshBefore?.time ??
                    item.refreshBefore?.day ??
                    item.refreshBefore?.md5,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed style id "${styleId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${styleIds.length} styles after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed styles: ${error}. Exited!`);
        }
      }

      /* Run seed geojsons */
      if (!type || type === "geojson") {
        try {
          if (!seed.geojsons) {
            printLog("info", "No geojsons in seed. Skipping...");
          } else {
            const geojsonIds = getTaskIds(seed, "geojson", id);

            printLog("info", `Starting seed ${geojsonIds.length} geojsons...`);

            const startTime = Date.now();

            for (const geojsonId of geojsonIds) {
              const item = seed.geojsons[geojsonId];

              if (item.skip) {
                printLog("info", `Skipping seed geojson id "${geojsonId}"...`);

                continue;
              }

              try {
                await seedGeoJSON({
                  id: geojsonId,
                  url: item.url,
                  maxTry: item.maxTry,
                  timeout: item.timeout,
                  headers: item.headers,
                  refreshBefore:
                    item.refreshBefore?.time ??
                    item.refreshBefore?.day ??
                    item.refreshBefore?.md5,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed geojson id "${geojsonId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${geojsonIds.length} geojsons after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed geojsons: ${error}. Exited!`);
        }
      }

      /* Run seed datas */
      if (!type || type === "data") {
        try {
          if (!seed.datas) {
            printLog("info", "No datas in seed. Skipping...");
          } else {
            const dataIds = getTaskIds(seed, "data", id);

            printLog("info", `Starting seed ${dataIds.length} datas...`);

            const startTime = Date.now();

            for (const dataId of dataIds) {
              const item = seed.datas[dataId];

              if (item.skip) {
                printLog("info", `Skipping seed data id "${dataId}"...`);

                continue;
              }

              try {
                await seedTileDatas({
                  id: dataId,
                  storeType: item.storeType,
                  metadata: item.metadata,
                  url: item.url,
                  scheme: item.scheme,
                  coverages: item.coverages,
                  concurrency: item.concurrency,
                  maxTry: item.maxTry,
                  timeout: item.timeout,
                  infoTimeout: item.infoTimeout,
                  batch: item.batch,
                  storeTransparent: item.storeTransparent,
                  headers: item.headers,
                  skipWhenError: item.skipWhenError,
                  refreshBefore:
                    item.refreshBefore?.time ??
                    item.refreshBefore?.day ??
                    item.refreshBefore?.md5,
                });
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed data id "${dataId}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${dataIds.length} datas after: ${getDuration(
                startTime,
              )}s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed datas: ${error}. Exited!`);
        }
      }
    } else {
      printLog("info", "No task assigned. Skipping...");
    }
  } catch (error) {
    printLog("error", `Failed to run tasks: ${error}`);
  } finally {
    printLog("info", "Completed seed and cleanup tasks!");
  }
}

/*********************************** Seed *************************************/

/**
 * Seed tile datas
 * @param {{ id: string, storeType: "mbtiles"|"xyz"|"pg", metadata: object, url: string, scheme: "tms"|"xyz", coverages: { zoom: number, bbox: [number, number, number, number]}[], concurrency?: number, maxTry?: number, timeout?: number, infoTimeout?: number, batch?: number, storeTransparent?: boolean, headers?: object, skipWhenError?: object, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
async function seedTileDatas({
  id,
  storeType,
  metadata,
  url,
  scheme,
  coverages,
  concurrency = DEFAULT_CONCURRENCY,
  maxTry = DEFAULT_MAX_TRY,
  timeout = DEFAULT_QUERY_TIMEOUT,
  infoTimeout = DEFAULT_INFO_TIMEOUT,
  batch = DEFAULT_TILE_BATCH_SIZE,
  storeTransparent = DEFAULT_STORE_TRANSPARENT,
  headers,
  skipWhenError,
  refreshBefore,
}) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { total, targetCoverages, tileBounds } = getTileBounds({
      coverages,
      limitedBBox: metadata.bounds,
    });

    let log = `Seeding ${total} tiles of ${storeType} "${id}" with:`;
    log += `\n\tURL: ${url} - Header: ${JSON.stringify(
      headers,
    )} - Scheme: ${scheme}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Batch: ${batch} - Max try: ${maxTry} - Timeout: ${timeout} - Info timeout: ${infoTimeout} - Skip when error: ${JSON.stringify(skipWhenError)}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)} - Target coverages: ${JSON.stringify(targetCoverages)}`;

    let refreshTimestamp;
    if (typeof refreshBefore === "string") {
      refreshTimestamp = new Date(refreshBefore).getTime();

      log += `\n\tRefresh before: ${refreshBefore}`;
    } else if (typeof refreshBefore === "number") {
      const now = new Date();

      refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

      log += `\n\tOld than: ${refreshBefore} days`;
    } else if (refreshBefore === true) {
      refreshTimestamp = true;

      log += `\n\tRefresh before: Check MD5`;
    }

    const skipExistingTiles = refreshTimestamp === undefined;

    printLog("info", log);

    let getTileExtraInfoFunc;
    let storeTileDataFunc;
    let tileOption;

    const hashURL =
      refreshTimestamp === true
        ? `${url.slice(0, url.indexOf("/{z}/{x}/{y}"))}/extra-info?compression=true`
        : undefined;

    switch (storeType) {
      default: {
        throw new Error(`Invalid store type "${storeType}"`);
      }

      case "mbtiles": {
        const filePath = path.join(
          process.env.DATA_DIR,
          "caches",
          "mbtiles",
          id,
          `${id}.mbtiles`,
        );

        /* Open database */
        printLog("info", "Creating database...");

        source = await openMBTilesDB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateMBTilesMetadata(source, metadata);

        getTileExtraInfoFunc = (batchTileBounds, isCreated) => {
          return getMBTilesTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          method: "GET",
          responseType: "arraybuffer",
          statement: source.prepare(MBTILES_INSERT_TILE_QUERY),
          maxTry,
          timeout,
          created: Date.now(),
          storeTransparent,
          headers,
          decompress: false,
        };

        /* Store tile data function */
        storeTileDataFunc = (z, x, y, data) => {
          return storeMBtilesTileData(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = () => {
          return closeMBTilesDB(source);
        };

        break;
      }

      case "pg": {
        const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openPostgreSQLDB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        /* Update metadata */
        printLog("info", "Updating metadata...");

        await updatePostgreSQLMetadata(source, metadata);

        getTileExtraInfoFunc = (batchTileBounds, isCreated) => {
          return getPostgreSQLTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          method: "GET",
          responseType: "arraybuffer",
          source,
          maxTry,
          timeout,
          created: Date.now(),
          storeTransparent,
          headers,
          decompress: false,
        };

        /* Store tile data function */
        storeTileDataFunc = (z, x, y, data) => {
          return storePostgreSQLTileData(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = () => {
          return closePostgreSQLDB(source);
        };

        break;
      }

      case "xyz": {
        const sourcePath = path.join(
          process.env.DATA_DIR,
          "caches",
          "xyzs",
          id,
        );
        const filePath = path.join(sourcePath, `${id}.sqlite`);

        /* Create database */
        printLog("info", "Creating database...");

        source = await openXYZMD5DB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateXYZMetadata(source, metadata);

        getTileExtraInfoFunc = (batchTileBounds, isCreated) => {
          return getXYZTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated,
          });
        };

        /* Assign tile option */
        tileOption = {
          method: "GET",
          responseType: "arraybuffer",
          sourcePath,
          statement: source.prepare(XYZ_INSERT_MD5_QUERY),
          format: metadata.format,
          maxTry,
          timeout,
          created: Date.now(),
          storeTransparent,
          headers,
          decompress: false,
        };

        storeTileDataFunc = (z, x, y, data) => {
          return storeXYZTileFile(z, x, y, data, tileOption);
        };

        /* Close database function */
        closeDatabaseFunc = () => {
          return closeXYZMD5DB(source);
        };

        break;
      }
    }

    let completeTasks = 0;

    if (skipWhenError) {
      skipWhenError.errCount = 0;
      skipWhenError.skipLoop = 0;
    }

    /* Download and store one tile data batch */
    function* downloadAndStoreTileDataGenerator(
      batchTileBounds,
      targetTileExtraInfo,
      tileExtraInfo,
    ) {
      for (const { z, x, y } of batchTileBounds) {
        const maxTileRow = scheme === "tms" ? (1 << z) - 1 : undefined;

        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            const taskNumber = completeTasks;
            const tileName = `${z}/${xCount}/${yCount}`;

            if (skipWhenError && skipWhenError.skipLoop > 0) {
              skipWhenError.skipLoop--;

              delete tileExtraInfo[tileName];
              delete targetTileExtraInfo[tileName];

              continue;
            }

            yield async () => {
              const currentTileExtraInfo = tileExtraInfo[tileName];
              const currentTargetTileExtraInfo = targetTileExtraInfo[tileName];

              try {
                if (
                  refreshTimestamp === true
                    ? currentTileExtraInfo &&
                      currentTileExtraInfo === currentTargetTileExtraInfo
                    : refreshTimestamp
                      ? currentTileExtraInfo >= refreshTimestamp
                      : skipExistingTiles && currentTileExtraInfo !== undefined
                ) {
                  return;
                }

                const tmpY = scheme === "tms" ? maxTileRow - yCount : yCount;

                const targetURL = url
                  .replace("{z}", `${z}`)
                  .replace("{x}", `${xCount}`)
                  .replace("{y}", `${tmpY}`);

                printLog(
                  "debug",
                  `Downloading data id "${id}" - Tile "${tileName}" - From "${targetURL}" - ${taskNumber}/${total}...`,
                );

                try {
                  await storeTileDataFunc(
                    z,
                    xCount,
                    yCount,
                    await getDataFromURL(targetURL, tileOption),
                    tileOption,
                  );

                  if (skipWhenError) {
                    skipWhenError.errCount = 0;
                  }
                } catch (error) {
                  printLog(
                    "error",
                    `Failed to seed data id "${id}" - Tile "${tileName}" - From "${targetURL}" - ${taskNumber}/${total}: ${error}`,
                  );

                  if (skipWhenError) {
                    skipWhenError.errCount++;

                    if (skipWhenError.errCount >= skipWhenError.count) {
                      skipWhenError.skipLoop = skipWhenError.skip;

                      printLog(
                        "warn",
                        `Encountered ${skipWhenError.errCount} errors. Skipping download next ${skipWhenError.skipLoop} tiles...`,
                      );

                      skipWhenError.errCount = 0;
                    }
                  }
                }
              } finally {
                delete tileExtraInfo[tileName];
                delete targetTileExtraInfo[tileName];
              }
            };
          }
        }
      }
    }

    /* Download and store tile datas */
    printLog("info", "Downloading and storing tiles...");

    let batchNumber = 0;

    for (const batchTileBounds of getTileBoundsBatches(tileBounds, batch)) {
      batchNumber++;

      let targetTileExtraInfo = {};
      let tileExtraInfo = {};
      const batchTotal = batchTileBounds.reduce((total, tileBound) => {
        return total + tileBound.total;
      }, 0);
      const batchDescription = `batch #${batchNumber} (${batchTileBounds.length} ranges, ${batchTotal} tiles)`;

      if (refreshTimestamp === true) {
        try {
          printLog(
            "info",
            `Getting target tile extra info from "${hashURL}" and local tile extra info for ${batchDescription}...`,
          );

          [targetTileExtraInfo, tileExtraInfo] = await Promise.all([
            getDataFromURL(hashURL, {
              method: "POST",
              timeout: infoTimeout,
              body: {
                tileBounds: batchTileBounds,
              },
              responseType: "json",
              headers: {
                ...(headers ?? {}),
                "content-type": "application/json",
              },
              maxTry,
              decompress: true,
            }),
            getTileExtraInfoFunc(batchTileBounds, false),
          ]);
        } catch (error) {
          if (error.statusCode >= 500) {
            printLog(
              "error",
              `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed ${storeType} "${id}"...`,
            );

            return;
          }

          printLog(
            "error",
            `Failed to get target or local tile extra info for ${batchDescription}: ${error}`,
          );

          targetTileExtraInfo = {};
          tileExtraInfo = {};
        }
      } else if (refreshTimestamp || skipExistingTiles) {
        try {
          printLog(
            "info",
            `Getting local tile extra info for ${batchDescription}...`,
          );

          tileExtraInfo = await getTileExtraInfoFunc(
            batchTileBounds,
            refreshTimestamp !== true,
          );

          printLog(
            "info",
            `Completed getting local tile extra info for ${batchDescription}.`,
          );
        } catch (error) {
          printLog(
            "error",
            `Failed to get local tile extra info for ${batchDescription}: ${error}`,
          );

          tileExtraInfo = {};
        }
      }

      await runAllWithLimit(
        downloadAndStoreTileDataGenerator(
          batchTileBounds,
          targetTileExtraInfo,
          tileExtraInfo,
        ),
        concurrency,
      );
    }

    printLog(
      "info",
      `Completed seed ${total} tiles of ${storeType} "${id}" after ${getDuration(
        startTime,
      )}s!`,
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
    }
  }
}

/**
 * Seed geojson
 * @param {{ id: string, url: string, maxTry?: number, timeout?: number, headers?: object, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
async function seedGeoJSON({
  id,
  url,
  maxTry = DEFAULT_MAX_TRY,
  timeout = DEFAULT_QUERY_TIMEOUT,
  headers,
  refreshBefore,
}) {
  const startTime = Date.now();

  let log = `Seeding geojson id "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
    refreshTimestamp = true;

    log += `\n\tRefresh before: Check MD5`;
  }

  printLog("info", log);

  /* Download and store GeoJSON file */
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "geojsons", id);
  const filePath = path.join(sourcePath, `${id}.geojson`);

  printLog("info", "Get extra info...");

  let needDownload = false;

  if (refreshTimestamp === true) {
    try {
      const [response, md5] = await Promise.all([
        requestToURL(`${url.slice(0, url.indexOf(".geojson"))}/md5`, {
          method: "GET",
          timeout,
          responseType: "arraybuffer",
          headers,
          decompress: false,
        }),
        getGeoJSONMD5(filePath),
      ]);

      if (!response.headers["etag"] || response.headers["etag"] !== md5) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else if (refreshTimestamp) {
    try {
      const created = await getGeoJSONCreated(filePath);

      if (created === undefined || created < refreshTimestamp) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else {
    needDownload = true;
  }

  printLog("info", "Downloading and storing geojson...");

  if (needDownload) {
    const option = {
      method: "GET",
      responseType: "arraybuffer",
      maxTry,
      timeout,
      headers,
      decompress: true,
    };

    async function downloadAndStoreGeoJSONData() {
      try {
        printLog(
          "info",
          `Downloading geojson id "${id}" - File "${filePath}" - From "${url}"...`,
        );

        await storeGeoJSONFile(filePath, await getDataFromURL(url, option));
      } catch (error) {
        printLog("error", `Failed to seed geojson id "${id}": ${error}`);
      }
    }

    await downloadAndStoreGeoJSONData();
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, GEOJSON_FILE_REGEX);

  printLog(
    "info",
    `Completed seed geojson id "${id}" after ${getDuration(startTime)}s!`,
  );
}

/**
 * Seed sprite
 * @param {{ id: string, url: string, maxTry?: number, timeout?: number, headers?: object, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
async function seedSprite({
  id,
  url,
  maxTry = DEFAULT_MAX_TRY,
  timeout = DEFAULT_QUERY_TIMEOUT,
  headers,
  refreshBefore,
}) {
  const startTime = Date.now();

  let log = `Seeding sprite id "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
    refreshTimestamp = true;

    log += `\n\tRefresh before: Check MD5`;
  }

  printLog("info", log);

  /* Download and store sprite files */
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "sprites", id);

  printLog("info", "Get extra info...");

  let needDownload = false;

  if (refreshTimestamp === true) {
    try {
      const [response, md5] = await Promise.all([
        requestToURL(`${url.slice(0, url.indexOf("/{name}"))}/md5`, {
          method: "GET",
          timeout,
          responseType: "arraybuffer",
          headers,
          decompress: false,
        }),
        getSpriteMD5(sourcePath),
      ]);

      if (!response.headers["etag"] || response.headers["etag"] !== md5) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else if (refreshTimestamp) {
    try {
      const created = await getSpriteCreated(sourcePath);

      if (created === undefined || created < refreshTimestamp) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else {
    needDownload = true;
  }

  printLog("info", "Downloading and storing sprites...");

  if (needDownload) {
    const option = {
      method: "GET",
      responseType: "arraybuffer",
      maxTry,
      timeout,
      headers,
    };

    async function downloadAndStoreSpriteData(fileName) {
      try {
        const targetURL = url.replace("{name}", `${fileName}`);

        printLog(
          "info",
          `Downloading sprite id "${id}" - File "${fileName}" - From "${targetURL}"...`,
        );

        await storeSpriteFile(
          path.join(sourcePath, fileName),
          await getDataFromURL(targetURL, {
            ...option,
            decompress: fileName.endsWith(".json") ? true : false,
          }),
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to seed sprite id "${id}" - File "${fileName}": ${error}`,
        );
      }
    }

    // Batch run
    await Promise.all(SPRITE_FILES.map(downloadAndStoreSpriteData));
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, SPRITE_FILE_REGEX);

  printLog(
    "info",
    `Completed seed sprite id "${id}" after ${getDuration(startTime)}s!`,
  );
}

/**
 * Seed font
 * @param {{ id: string, url: string, concurrency?: number, maxTry?: number, timeout?: number, headers?: object, skipWhenError?: object, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
async function seedFont({
  id,
  url,
  concurrency = DEFAULT_CONCURRENCY,
  maxTry = DEFAULT_MAX_TRY,
  timeout = DEFAULT_QUERY_TIMEOUT,
  headers,
  skipWhenError,
  refreshBefore,
}) {
  const startTime = Date.now();

  const total = 256;

  let log = `Seeding ${total} fonts of font id "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout} - Skip when error: ${JSON.stringify(skipWhenError)}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
    refreshTimestamp = true;

    log += `\n\tRefresh before: Check MD5`;
  }

  printLog("info", log);

  /* Download and store font files */
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "fonts", id);

  printLog("info", "Get extra info...");

  let needDownload = false;

  if (refreshTimestamp === true) {
    try {
      const [response, md5] = await Promise.all([
        requestToURL(`${url.slice(0, url.indexOf("/{range}.pbf"))}/md5`, {
          method: "GET",
          timeout,
          responseType: "arraybuffer",
          headers,
          decompress: false,
        }),
        getFontMD5(sourcePath),
      ]);

      if (!response.headers["etag"] || response.headers["etag"] !== md5) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else if (refreshTimestamp) {
    try {
      const created = await getFontCreated(sourcePath);

      if (created === undefined || created < refreshTimestamp) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else {
    needDownload = true;
  }

  printLog("info", "Downloading and storing fonts...");

  if (needDownload) {
    const option = {
      method: "GET",
      responseType: "arraybuffer",
      maxTry,
      timeout,
      headers,
      decompress: true,
    };

    /* Seed font data generator */
    function* downloadAndStoreFontDataGenerator() {
      let completeTasks = 0;

      if (skipWhenError) {
        skipWhenError.errCount = 0;
        skipWhenError.skipLoop = 0;
      }

      for (let idx = 0; idx < total; idx++) {
        completeTasks++;

        if (skipWhenError && skipWhenError.skipLoop > 0) {
          skipWhenError.skipLoop--;

          continue;
        }

        yield async () => {
          const rangeStart = idx * 256;
          const rangeEnd = rangeStart + 255;

          const fileName = `${rangeStart}-${rangeEnd}.pbf`;

          try {
            const targetURL = url.replace("{range}.pbf", fileName);

            printLog(
              "info",
              `Downloading font id "${id}" - Filename "${fileName}" - From "${targetURL}" - ${completeTasks}/${total}...`,
            );

            await storeFontFile(
              path.join(sourcePath, fileName),
              await getDataFromURL(targetURL, option),
            );

            if (skipWhenError) {
              skipWhenError.errCount = 0;
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to seed font id "${id}" - Filename "${fileName}" - ${completeTasks}/${total}: ${error}`,
            );

            if (skipWhenError) {
              skipWhenError.errCount++;

              if (skipWhenError.errCount >= skipWhenError.count) {
                skipWhenError.skipLoop = skipWhenError.skip;

                printLog(
                  "warn",
                  `Encountered ${skipWhenError.errCount} errors. Skipping download next ${skipWhenError.skipLoop} tiles...`,
                );

                skipWhenError.errCount = 0;
              }
            }
          }
        };
      }
    }

    // Batch run
    await runAllWithLimit(downloadAndStoreFontDataGenerator(), concurrency);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, FONT_FILE_REGEX);

  printLog(
    "info",
    `Completed seed ${total} fonts of font id "${id}" after ${getDuration(
      startTime,
    )}s!`,
  );
}

/**
 * Seed style
 * @param {{ id: string, url: string, maxTry?: number, timeout?: number, headers?: object, refreshBefore?: string|number|boolean }} options Options
 * @returns {Promise<void>}
 */
async function seedStyle({
  id,
  url,
  maxTry = DEFAULT_MAX_TRY,
  timeout = DEFAULT_QUERY_TIMEOUT,
  headers,
  refreshBefore,
}) {
  const startTime = Date.now();

  let log = `Seeding style id "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tMax try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  } else if (refreshBefore === true) {
    refreshTimestamp = true;

    log += `\n\tRefresh before: Check MD5`;
  }

  printLog("info", log);

  /* Download and store StyleJSON file */
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "styles", id);
  const filePath = path.join(sourcePath, "style.json");

  printLog("info", "Get extra info...");

  let needDownload = false;

  if (refreshTimestamp === true) {
    try {
      const [response, md5] = await Promise.all([
        requestToURL(`${url.slice(0, url.indexOf("/style.json"))}/md5`, {
          method: "GET",
          timeout,
          responseType: "arraybuffer",
          headers,
        }),
        getStyleMD5(filePath),
      ]);

      if (!response.headers["etag"] || response.headers["etag"] !== md5) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else if (refreshTimestamp) {
    try {
      const created = await getStyleCreated(filePath);

      if (created === undefined || created < refreshTimestamp) {
        needDownload = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needDownload = true;
      } else {
        throw error;
      }
    }
  } else {
    needDownload = true;
  }

  printLog("info", "Downloading and storing style...");

  if (needDownload) {
    const option = {
      method: "GET",
      responseType: "arraybuffer",
      maxTry,
      timeout,
      headers,
      decompress: true,
    };

    async function downloadAndStoreStyleData() {
      try {
        printLog(
          "info",
          `Downloading style id "${id}" - File "${filePath}" - From "${url}"...`,
        );

        await storeStyleFile(filePath, await getDataFromURL(url, option));
      } catch (error) {
        printLog("error", `Failed to seed style id "${id}": ${error}`);
      }
    }

    await downloadAndStoreStyleData();
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, STYLE_FILE_REGEX);

  printLog(
    "info",
    `Completed seed style id "${id}" after ${getDuration(startTime)}s!`,
  );
}

/*********************************** Clean up *************************************/

/**
 * Cleanup tile datas
 * @param {{ storeType: "mbtiles"|"xyz"|"pg", id: string, metadata: object, coverages: { zoom: number, bbox: [number, number, number, number]}[], concurrency?: number, batch?: number, skipWhenError?: object, cleanUpBefore?: string|number }} options Options
 * @returns {Promise<void>}
 */
async function cleanUpTileDatas({
  storeType,
  id,
  metadata,
  coverages,
  concurrency = DEFAULT_CONCURRENCY,
  batch = DEFAULT_TILE_BATCH_SIZE,
  skipWhenError,
  cleanUpBefore,
}) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { total, targetCoverages, tileBounds } = getTileBounds({
      coverages,
      limitedBBox: metadata.bounds,
    });

    let log = `Cleaning up ${total} tiles of ${storeType} "${id}" with:`;
    log += `\n\tConcurrency: ${concurrency} - Batch: ${batch} - Skip when error: ${JSON.stringify(skipWhenError)}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)} - Target coverages: ${JSON.stringify(targetCoverages)}`;

    let cleanUpTimestamp;
    if (typeof cleanUpBefore === "string") {
      cleanUpTimestamp = new Date(cleanUpBefore).getTime();

      log += `\n\tCleanup before: ${cleanUpBefore}`;
    } else if (typeof cleanUpBefore === "number") {
      const now = new Date();

      cleanUpTimestamp = now.setDate(now.getDate() - cleanUpBefore);

      log += `\n\tOld than: ${cleanUpBefore} days`;
    }

    const cleanExistingTilesOnly = cleanUpTimestamp === undefined;

    printLog("info", log);

    let getTileExtraInfoFunc;
    let removeTileDataFunc;
    let compactDatabase;
    let tileOption;

    switch (storeType) {
      default: {
        throw new Error(`Invalid store type "${storeType}"`);
      }

      case "mbtiles": {
        const filePath = path.join(
          process.env.DATA_DIR,
          "caches",
          "mbtiles",
          id,
          `${id}.mbtiles`,
        );

        /* Open database */
        printLog("info", "Opening database...");

        source = await openMBTilesDB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        getTileExtraInfoFunc = (batchTileBounds) => {
          return getMBTilesTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: true,
          });
        };

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(MBTILES_DELETE_TILE_QUERY),
        };

        /* Remove tile data function */
        removeTileDataFunc = (z, x, y) => {
          return removeMBTilesTile(z, x, y, tileOption);
        };

        /* Compact database function */
        compactDatabase = () => {
          return compactMBTiles(source);
        };

        /* Close database function */
        closeDatabaseFunc = () => {
          return closeMBTilesDB(source);
        };

        break;
      }

      case "pg": {
        const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

        /* Open database */
        printLog("info", "Opening database...");

        source = await openPostgreSQLDB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        getTileExtraInfoFunc = (batchTileBounds) => {
          return getPostgreSQLTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: true,
          });
        };

        /* Assign tile option */
        tileOption = {
          source,
        };

        /* Remove tile data function */
        removeTileDataFunc = (z, x, y) => {
          return removePostgreSQLTile(z, x, y, tileOption);
        };

        /* Compact database function */
        compactDatabase = () => {};

        /* Close database function */
        closeDatabaseFunc = () => {
          return closePostgreSQLDB(source);
        };

        break;
      }

      case "xyz": {
        const sourcePath = path.join(
          process.env.DATA_DIR,
          "caches",
          "xyzs",
          id,
        );
        const filePath = path.join(sourcePath, `${id}.sqlite`);

        /* Open database */
        printLog("info", "Opening database...");

        source = await openXYZMD5DB(filePath, true, DEFAULT_QUERY_TIMEOUT);

        getTileExtraInfoFunc = (batchTileBounds) => {
          return getXYZTileExtraInfo({
            source,
            tileBounds: batchTileBounds,
            isCreated: true,
          });
        };

        const format = await getXYZFormatFromTiles(sourcePath);

        /* Assign tile option */
        tileOption = {
          sourcePath,
          statement: source.prepare(XYZ_DELETE_MD5_QUERY),
        };

        /* Remove tile data function */
        removeTileDataFunc = (z, x, y) => {
          return removeXYZTile(z, x, y, tileOption);
        };

        /* Compact database function */
        compactDatabase = async () => {
          /* Compact database */
          compactXYZ(source);

          /* Remove parent folders if empty */
          await removeEmptyFolders(sourcePath, new RegExp(`^.*\\.${format}$`));
        };

        /* Close database function */
        closeDatabaseFunc = () => {
          return closeXYZMD5DB(source);
        };

        break;
      }
    }

    let completeTasks = 0;

    if (skipWhenError) {
      skipWhenError.errCount = 0;
      skipWhenError.skipLoop = 0;
    }

    /* Remove one tile data batch */
    function* removeTileDataGenerator(batchTileBounds, tileExtraInfo) {
      for (const { z, x, y } of batchTileBounds) {
        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            const taskNumber = completeTasks;

            if (skipWhenError && skipWhenError.skipLoop > 0) {
              skipWhenError.skipLoop--;

              continue;
            }

            yield async () => {
              const tileName = `${z}/${xCount}/${yCount}`;
              const currentTileExtraInfo = tileExtraInfo[tileName];

              try {
                if (
                  (cleanUpTimestamp &&
                    currentTileExtraInfo >= cleanUpTimestamp) ||
                  (cleanExistingTilesOnly && currentTileExtraInfo === undefined)
                ) {
                  return;
                }

                printLog(
                  "debug",
                  `Removing data id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}...`,
                );

                try {
                  await removeTileDataFunc(z, xCount, yCount, tileOption);

                  if (skipWhenError) {
                    skipWhenError.errCount = 0;
                  }
                } catch (error) {
                  printLog(
                    "error",
                    `Failed to cleanup data id "${id}" - Tile "${tileName}" - ${taskNumber}/${total}: ${error}`,
                  );

                  if (skipWhenError) {
                    skipWhenError.errCount++;

                    if (skipWhenError.errCount >= skipWhenError.count) {
                      skipWhenError.skipLoop = skipWhenError.skip;

                      printLog(
                        "warn",
                        `Encountered ${skipWhenError.errCount} errors. Skipping download next ${skipWhenError.skipLoop} tiles...`,
                      );

                      skipWhenError.errCount = 0;
                    }
                  }
                }
              } finally {
                delete tileExtraInfo[tileName];
              }
            };
          }
        }
      }
    }

    /* Remove tile datas */
    printLog("info", "Removing tiles...");

    for (const batchTileBounds of getTileBoundsBatches(tileBounds, batch)) {
      let tileExtraInfo = {};

      if (cleanUpTimestamp || cleanExistingTilesOnly) {
        try {
          tileExtraInfo = await getTileExtraInfoFunc(batchTileBounds);
        } catch (error) {
          printLog(
            "error",
            `Failed to get local tile extra info for a cleanup batch: ${error}`,
          );
        }
      }

      await runAllWithLimit(
        removeTileDataGenerator(batchTileBounds, tileExtraInfo),
        concurrency,
      );
    }

    /* Compact database */
    printLog("info", "Compacting database...");

    await compactDatabase();

    printLog(
      "info",
      `Completed cleanup ${total} tiles of ${storeType} "${id}" after ${getDuration(
        startTime,
      )}s!`,
    );
  } catch (error) {
    throw error;
  } finally {
    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
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

  let log = `Cleaning up geojson id "${id}" with:`;

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
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "geojsons", id);
  const filePath = path.join(sourcePath, `${id}.geojson`);

  printLog("info", "Get extra info...");

  let needRemove = false;

  if (cleanUpTimestamp) {
    try {
      const created = await getGeoJSONCreated(filePath);

      if (created === undefined || created < cleanUpTimestamp) {
        needRemove = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needRemove = true;
      } else {
        throw error;
      }
    }
  } else {
    needRemove = true;
  }

  if (needRemove) {
    async function removeGeoJSONData() {
      try {
        printLog("info", `Removing geojson id "${id}" - File "${filePath}"...`);

        await removeGeoJSONFile(filePath);
      } catch (error) {
        printLog("error", `Failed to cleanup geojson id "${id}": ${error}`);
      }
    }

    await removeGeoJSONData();
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, GEOJSON_FILE_REGEX);

  printLog(
    "info",
    `Completed cleanup geojson id "${id}" after ${getDuration(startTime)}s!`,
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

  let log = `Cleaning up sprite id "${id}" with:`;

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
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "sprites", id);

  printLog("info", "Get extra info...");

  let needRemove = false;

  if (cleanUpTimestamp) {
    try {
      const created = await getSpriteCreated(sourcePath);

      if (created === undefined || created < cleanUpTimestamp) {
        needRemove = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needRemove = true;
      } else {
        throw error;
      }
    }
  } else {
    needRemove = true;
  }

  printLog("info", "Removing sprites...");

  if (needRemove) {
    async function removeSpriteData(fileName) {
      try {
        printLog("info", `Removing sprite id "${id}" - File "${fileName}"...`);

        await removeSpriteFile(path.join(sourcePath, fileName));
      } catch (error) {
        printLog(
          "error",
          `Failed to cleanup sprite id "${id}" - File "${fileName}": ${error}`,
        );
      }
    }

    // Batch run
    await Promise.all(SPRITE_FILES.map(removeSpriteData));
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, SPRITE_FILE_REGEX);

  printLog(
    "info",
    `Completed cleanup sprite id "${id}" after ${getDuration(startTime)}s!`,
  );
}

/**
 * Cleanup font
 * @param {string} id Cleanup font ID
 * @param {number} concurrency Concurrency for removing font files
 * @param {object} skipWhenError Skip when error
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpFont(id, concurrency, skipWhenError, cleanUpBefore) {
  const startTime = Date.now();

  const total = 256;

  let log = `Cleaning up ${total} fonts of font id "${id}" with:`;
  log += `\n\tConcurrency: ${concurrency} - Skip when error: ${JSON.stringify(skipWhenError)}`;

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
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "fonts", id);

  printLog("info", "Get extra info...");

  let needRemove = false;

  if (cleanUpTimestamp) {
    try {
      const created = await getFontCreated(sourcePath);

      if (created === undefined || created < cleanUpTimestamp) {
        needRemove = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
        needRemove = true;
      } else {
        throw error;
      }
    }
  } else {
    needRemove = true;
  }

  printLog("info", "Removing fonts...");

  if (needRemove) {
    /* Remove font data generator */
    function* removeFontDataGenerator() {
      let completeTasks = 0;

      if (skipWhenError) {
        skipWhenError.errCount = 0;
        skipWhenError.skipLoop = 0;
      }

      for (let idx = 0; idx < total; idx++) {
        completeTasks++;

        if (skipWhenError && skipWhenError.skipLoop > 0) {
          skipWhenError.skipLoop--;

          continue;
        }

        yield async () => {
          const rangeStart = idx * 256;
          const rangeEnd = rangeStart + 255;

          const fileName = `${`${rangeStart}-${rangeEnd}`}.pbf`;

          try {
            printLog(
              "info",
              `Removing font id "${id}" - Filename "${fileName}" - ${completeTasks}/${total}...`,
            );

            await removeFontFile(path.join(sourcePath, fileName));

            if (skipWhenError) {
              skipWhenError.errCount = 0;
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to cleanup font id "${id}" - Filename "${fileName}" - ${completeTasks}/${total}: ${error}`,
            );

            if (skipWhenError) {
              skipWhenError.errCount++;

              if (skipWhenError.errCount >= skipWhenError.count) {
                skipWhenError.skipLoop = skipWhenError.skip;

                printLog(
                  "warn",
                  `Encountered ${skipWhenError.errCount} errors. Skipping download next ${skipWhenError.skipLoop} tiles...`,
                );

                skipWhenError.errCount = 0;
              }
            }
          }
        };
      }
    }

    // Batch run
    await runAllWithLimit(removeFontDataGenerator(), concurrency);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, FONT_FILE_REGEX);

  printLog(
    "info",
    `Completed cleanup ${total} fonts of font id "${id}" after ${getDuration(
      startTime,
    )}s!`,
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

  let log = `Cleaning up style id "${id}" with:`;

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

  /* Remove StyleJSON file */
  const sourcePath = path.join(process.env.DATA_DIR, "caches", "styles", id);
  const filePath = path.join(sourcePath, "style.json");

  printLog("info", "Get extra info...");

  let needRemove = false;

  if (cleanUpTimestamp) {
    try {
      const created = await getStyleCreated(filePath);

      if (created === undefined || created < cleanUpTimestamp) {
        needRemove = true;
      }
    } catch (error) {
      if (isErrorNotFound(error)) {
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
    async function removeStyleData() {
      try {
        printLog("info", `Removing style id "${id}" - File "${filePath}"...`);

        await removeStyleFile(filePath);
      } catch (error) {
        printLog("error", `Failed to cleanup style id "${id}": ${error}`);
      }
    }

    await removeStyleData();
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, STYLE_FILE_REGEX);

  printLog(
    "info",
    `Completed cleanup style id "${id}" after ${getDuration(startTime)}s!`,
  );
}
