"use strict";

import { cleanUp, seed } from "./configs/index.js";
import os from "os";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  getXYZTileExtraInfoFromCoverages,
  updatePostgreSQLMetadata,
  downloadPostgreSQLTile,
  updateMBTilesMetadata,
  getXYZFormatFromTiles,
  removePostgreSQLTile,
  downloadMBTilesTile,
  downloadGeoJSONFile,
  downloadSpriteFile,
  updateXYZMetadata,
  removeMBTilesTile,
  closePostgreSQLDB,
  downloadStyleFile,
  getGeoJSONCreated,
  removeGeoJSONFile,
  openPostgreSQLDB,
  downloadFontFile,
  getSpriteCreated,
  removeSpriteFile,
  getStyleCreated,
  removeStyleFile,
  downloadXYZTile,
  getFontCreated,
  removeFontFile,
  closeMBTilesDB,
  compactMBTiles,
  closeXYZMD5DB,
  openMBTilesDB,
  removeXYZTile,
  openXYZMD5DB,
  compactXYZ,
  getGeoJSON,
  getStyle,
} from "./resources/index.js";
import {
  handleTilesConcurrency,
  removeEmptyFolders,
  handleConcurrency,
  getDataFromURL,
  postDataToURL,
  getTileBounds,
  calculateMD5,
  unzipAsync,
  printLog,
} from "./utils/index.js";

/**
 * Run cleanup and seed tasks
 * @param {{ cleanUpSprites: boolean, cleanUpFonts: boolean, cleanUpStyles: boolean, cleanUpGeoJSONs: boolean, cleanUpDatas: boolean, seedSprites: boolean, seedFonts: boolean, seedStyles: boolean, seedGeoJSONs: boolean, seedDatas: boolean }} opts Options
 * @returns {Promise<void>}
 */
export async function runTasks(opts) {
  try {
    printLog("info", "Starting seed and cleanup tasks...");

    if (
      opts.cleanUpSprites ||
      opts.cleanUpFonts ||
      opts.cleanUpStyles ||
      opts.cleanUpGeoJSONs ||
      opts.cleanUpDatas ||
      opts.seedSprites ||
      opts.seedFonts ||
      opts.seedStyles ||
      opts.seedGeoJSONs ||
      opts.seedDatas
    ) {
      /* Cleanup sprites */
      if (opts.cleanUpSprites) {
        try {
          if (!cleanUp.sprites) {
            printLog("info", "No sprites in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.sprites);

            printLog("info", `Starting cleanup ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.sprites[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup sprite "${id}"...`);

                continue;
              }

              try {
                await cleanUpSprite(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup sprite "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} sprites after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup sprites: ${error}. Exited!`);
        }
      }

      /* Cleanup fonts */
      if (opts.cleanUpFonts) {
        try {
          if (!cleanUp.fonts) {
            printLog("info", "No fonts in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.fonts);

            printLog("info", `Starting cleanup ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.fonts[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup font "${id}"...`);

                continue;
              }

              try {
                await cleanUpFont(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup font "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} fonts after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup fonts: ${error}. Exited!`);
        }
      }

      /* Cleanup styles */
      if (opts.cleanUpStyles) {
        try {
          if (!cleanUp.styles) {
            printLog("info", "No styles in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.styles);

            printLog("info", `Starting cleanup ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.styles[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup style "${id}"...`);

                continue;
              }

              try {
                await cleanUpStyle(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup style "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} styles after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup styles: ${error}. Exited!`);
        }
      }

      /* Cleanup geojsons */
      if (opts.cleanUpGeoJSONs) {
        try {
          if (!cleanUp.geojsons) {
            printLog("info", "No geojsons in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.geojsons);

            printLog("info", `Starting cleanup ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = cleanUp.geojsons[id];

              if (item.skip) {
                printLog("info", `Skipping cleanup geojson "${id}"...`);

                continue;
              }

              try {
                await cleanUpGeoJSON(
                  id,
                  item.cleanUpBefore?.time || item.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup geojson "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} geojsons after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup geojsons: ${error}. Exited!`);
        }
      }

      /* Cleanup datas */
      if (opts.cleanUpDatas) {
        try {
          if (!cleanUp.datas) {
            printLog("info", "No datas in cleanup. Skipping...");
          } else {
            const ids = Object.keys(cleanUp.datas);

            printLog("info", `Starting cleanup ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const seedDataItem = seed.datas[id];
              const cleanUpDataItem = cleanUp.datas[id];

              if (cleanUpDataItem.skip) {
                printLog("info", `Skipping cleanup data "${id}"...`);

                continue;
              }

              try {
                await cleanUpDataTiles(
                  seedDataItem.storeType,
                  id,
                  cleanUpDataItem.coverages,
                  cleanUpDataItem.cleanUpBefore?.time ||
                    cleanUpDataItem.cleanUpBefore?.day,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to cleanup data "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed cleanup ${ids.length} datas after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to cleanup datas: ${error}. Exited!`);
        }
      }

      /* Run seed sprites */
      if (opts.seedSprites) {
        try {
          if (!seed.sprites) {
            printLog("info", "No sprites in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.sprites);

            printLog("info", `Starting seed ${ids.length} sprites...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.sprites[id];

              if (item.skip) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedSprite(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed font "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} sprites after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed sprites: ${error}. Exited!`);
        }
      }

      /* Run seed fonts */
      if (opts.seedFonts) {
        try {
          if (!seed.fonts) {
            printLog("info", "No fonts in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.fonts);

            printLog("info", `Starting seed ${ids.length} fonts...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.fonts[id];

              if (item.skip) {
                printLog("info", `Skipping seed font "${id}"...`);

                continue;
              }

              try {
                await seedFont(
                  id,
                  item.url,
                  item.concurrency || os.cpus().length,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed font "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} fonts after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed fonts: ${error}. Exited!`);
        }
      }

      /* Run seed styles */
      if (opts.seedStyles) {
        try {
          if (!seed.styles) {
            printLog("info", "No styles in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.styles);

            printLog("info", `Starting seed ${ids.length} styles...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.styles[id];

              if (item.skip) {
                printLog("info", `Skipping seed style "${id}"...`);

                continue;
              }

              try {
                await seedStyle(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed style "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} styles after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed styles: ${error}. Exited!`);
        }
      }

      /* Run seed geojsons */
      if (opts.seedGeoJSONs) {
        try {
          if (!seed.geojsons) {
            printLog("info", "No geojsons in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.geojsons);

            printLog("info", `Starting seed ${ids.length} geojsons...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.geojsons[id];

              if (item.skip) {
                printLog("info", `Skipping seed geojson "${id}"...`);

                continue;
              }

              try {
                await seedGeoJSON(
                  id,
                  item.url,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed geojson "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} geojsons after: ${
                (Date.now() - startTime) / 1000
              }s!`,
            );
          }
        } catch (error) {
          printLog("error", `Failed to seed geojsons: ${error}. Exited!`);
        }
      }

      /* Run seed datas */
      if (opts.seedDatas) {
        try {
          if (!seed.datas) {
            printLog("info", "No datas in seed. Skipping...");
          } else {
            const ids = Object.keys(seed.datas);

            printLog("info", `Starting seed ${ids.length} datas...`);

            const startTime = Date.now();

            for (const id of ids) {
              const item = seed.datas[id];

              if (item.skip) {
                printLog("info", `Skipping seed data "${id}"...`);

                continue;
              }

              try {
                await seedDataTiles(
                  id,
                  item.storeType,
                  item.metadata,
                  item.url,
                  item.scheme,
                  item.coverages,
                  item.concurrency || os.cpus().length,
                  item.maxTry || 5,
                  item.timeout ?? 60000,
                  item.storeTransparent ?? true,
                  item.refreshBefore?.time ||
                    item.refreshBefore?.day ||
                    item.refreshBefore?.md5,
                  item.headers,
                );
              } catch (error) {
                printLog(
                  "error",
                  `Failed to seed data "${id}": ${error}. Skipping...`,
                );
              }
            }

            printLog(
              "info",
              `Completed seed ${ids.length} datas after: ${
                (Date.now() - startTime) / 1000
              }s!`,
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
 * Seed data tiles
 * @param {string} id Cache data ID
 * @param {"mbtiles"|"xyz"|"pg"} storeType Store type
 * @param {object} metadata Metadata object
 * @param {string} url Tile URL to download
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedDataTiles(
  id,
  storeType,
  metadata,
  url,
  scheme,
  coverages,
  concurrency,
  maxTry,
  timeout,
  storeTransparent,
  refreshBefore,
  headers,
) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { total, tileBounds } = getTileBounds({
      coverages: coverages,
    });

    let log = `Seeding ${total} tiles of ${storeType} "${id}" with:`;
    log += `\n\tURL: ${url} - Header: ${JSON.stringify(
      headers,
    )} - Scheme: ${scheme}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

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

    let downloadDataTileFunc;

    switch (storeType) {
      case "mbtiles": {
        const filePath = `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`;

        /* Open database */
        printLog("info", "Creating database...");

        source = await openMBTilesDB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateMBTilesMetadata(source, metadata);

        /* Get tile extra info */
        let targetTileExtraInfo;
        let tileExtraInfo;

        if (refreshTimestamp === true) {
          const hashURL = `${url.slice(
            0,
            url.indexOf("/{z}/{x}/{y}"),
          )}/extra-info?compression=true`;

          try {
            printLog(
              "info",
              `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`,
            );

            const res = await postDataToURL(
              hashURL,
              3600000, // 1 hours
              coverages,
              "arraybuffer",
              false,
              {
                "Content-Type": "application/json",
              },
            );

            if (res.headers["content-encoding"] === "gzip") {
              targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
            } else {
              targetTileExtraInfo = JSON.parse(res.data);
            }

            tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
              source,
              coverages,
              false,
            );
          } catch (error) {
            if (error.statusCode >= 500) {
              printLog(
                "error",
                `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed mbtiles "${id}"...`,
              );

              return;
            }

            printLog(
              "error",
              `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Download data tile function */
        downloadDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (
            (refreshTimestamp === true &&
              tileExtraInfo[tileName] &&
              tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
            (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
          ) {
            return;
          }

          const completeTasks = tasks.completeTasks;
          const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

          const targetURL = url
            .replace("{z}", `${z}`)
            .replace("{x}", `${x}`)
            .replace("{y}", `${tmpY}`);

          printLog(
            "info",
            `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`,
          );

          try {
            await downloadMBTilesTile(
              targetURL,
              source,
              z,
              x,
              tmpY,
              maxTry,
              timeout,
              storeTransparent,
              headers,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Close database function */
        closeDatabaseFunc = async () => closeMBTilesDB(source);

        break;
      }

      case "pg": {
        const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openPostgreSQLDB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        await updatePostgreSQLMetadata(source, metadata);

        /* Get tile extra info */
        let targetTileExtraInfo;
        let tileExtraInfo;

        if (refreshTimestamp === true) {
          const hashURL = `${url.slice(
            0,
            url.indexOf("/{z}/{x}/{y}"),
          )}/extra-info?compression=true`;

          try {
            printLog(
              "info",
              `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`,
            );

            const res = await postDataToURL(
              hashURL,
              3600000, // 1 hours
              coverages,
              "arraybuffer",
              false,
              {
                "Content-Type": "application/json",
              },
            );

            if (res.headers["content-encoding"] === "gzip") {
              targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
            } else {
              targetTileExtraInfo = JSON.parse(res.data);
            }

            tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
              source,
              coverages,
              false,
            );
          } catch (error) {
            if (error.statusCode >= 500) {
              printLog(
                "error",
                `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed postgresql "${id}"...`,
              );

              return;
            }

            printLog(
              "error",
              `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Download data tile function */
        downloadDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (
            (refreshTimestamp === true &&
              tileExtraInfo[tileName] &&
              tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
            (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
          ) {
            return;
          }

          const completeTasks = tasks.completeTasks;
          const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

          const targetURL = url
            .replace("{z}", `${z}`)
            .replace("{x}", `${x}`)
            .replace("{y}", `${tmpY}`);

          printLog(
            "info",
            `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`,
          );

          try {
            await downloadPostgreSQLTile(
              targetURL,
              source,
              z,
              x,
              tmpY,
              maxTry,
              timeout,
              storeTransparent,
              headers,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Close database function */
        closeDatabaseFunc = async () => await closePostgreSQLDB(source);

        break;
      }

      case "xyz": {
        const sourcePath = `${process.env.DATA_DIR}/caches/xyzs/${id}`;
        const filePath = `${sourcePath}/${id}.mbtiles`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openXYZMD5DB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateXYZMetadata(source, metadata);

        /* Get tile extra info */
        let targetTileExtraInfo;
        let tileExtraInfo;

        if (refreshTimestamp === true) {
          const hashURL = `${url.slice(
            0,
            url.indexOf("/{z}/{x}/{y}"),
          )}/extra-info?compression=true`;

          try {
            printLog(
              "info",
              `Get target tile extra info from "${hashURL}" and tile extra info from "${filePath}"...`,
            );

            const res = await postDataToURL(
              hashURL,
              3600000, // 1 hours
              coverages,
              "arraybuffer",
              false,
              {
                "Content-Type": "application/json",
              },
            );

            if (res.headers["content-encoding"] === "gzip") {
              targetTileExtraInfo = JSON.parse(await unzipAsync(res.data));
            } else {
              targetTileExtraInfo = JSON.parse(res.data);
            }

            tileExtraInfo = getXYZTileExtraInfoFromCoverages(
              source,
              coverages,
              false,
            );
          } catch (error) {
            if (error.statusCode >= 500) {
              printLog(
                "error",
                `Failed to get target tile extra info from "${hashURL}": ${error}. Skipping seed xyz "${id}"...`,
              );

              return;
            }

            printLog(
              "error",
              `Failed to get target tile extra info from "${hashURL}" and tile extra info from "${filePath}": ${error}`,
            );

            targetTileExtraInfo = {};
            tileExtraInfo = {};
          }
        } else if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getXYZTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Download data tile function */
        downloadDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (
            (refreshTimestamp === true &&
              tileExtraInfo[tileName] &&
              tileExtraInfo[tileName] === targetTileExtraInfo[tileName]) ||
            (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp)
          ) {
            return;
          }

          const completeTasks = tasks.completeTasks;
          const tmpY = scheme === "tms" ? (1 << z) - 1 - y : y;

          const targetURL = url
            .replace("{z}", `${z}`)
            .replace("{x}", `${x}`)
            .replace("{y}", `${tmpY}`);

          printLog(
            "info",
            `Downloading data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}...`,
          );

          try {
            await downloadXYZTile(
              targetURL,
              sourcePath,
              source,
              z,
              x,
              tmpY,
              metadata.format,
              maxTry,
              timeout,
              storeTransparent,
              headers,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to seed data "${id}" - Tile "${tileName}" - From "${targetURL}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Close database function */
        closeDatabaseFunc = async () => closeXYZMD5DB(source);

        break;
      }
    }

    /* Download data tiles */
    printLog("info", "Downloading data tiles...");

    await handleTilesConcurrency(concurrency, downloadDataTileFunc, tileBounds);

    printLog(
      "info",
      `Completed seed ${total} tiles of ${storeType} "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`,
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
 * @param {string} id Cache geojson ID
 * @param {string} url GeoJSON URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedGeoJSON(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding geojson "${id}" with:`;
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

  /* Download GeoJSON file */
  const sourcePath = `${process.env.DATA_DIR}/caches/geojsons/${id}`;
  const filePath = `${sourcePath}/${id}.geojson`;

  try {
    let needDownload = false;

    if (refreshTimestamp === true) {
      try {
        const [response, geoJSONData] = await Promise.all([
          getDataFromURL(
            `${url.slice(0, url.indexOf(`/${id}.geojson`))}/md5`,
            timeout,
            "arraybuffer",
          ),
          getGeoJSON(filePath),
        ]);

        if (
          !response.headers["etag"] ||
          response.headers["etag"] !== calculateMD5(geoJSONData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "JSON does not exist") {
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
        if (error.message === "GeoJSON created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    printLog("info", "Downloading geojson...");

    if (needDownload) {
      printLog(
        "info",
        `Downloading geojson "${id}" - File "${filePath}" - From "${url}"...`,
      );

      await downloadGeoJSONFile(url, filePath, maxTry, timeout, headers);
    }
  } catch (error) {
    printLog("error", `Failed to seed geojson "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.geojson$/);

  printLog(
    "info",
    `Completed seed geojson "${id}" after ${(Date.now() - startTime) / 1000}s!`,
  );
}

/**
 * Seed sprite
 * @param {string} id Cache sprite ID
 * @param {string} url Sprite URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedSprite(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding sprite "${id}" with:`;
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
  }

  printLog("info", log);

  /* Download sprite files */
  const sourcePath = `${process.env.DATA_DIR}/caches/sprites/${id}`;

  async function seedSpriteData(fileName) {
    const filePath = `${sourcePath}/${fileName}`;

    try {
      let needDownload = false;

      if (refreshTimestamp) {
        try {
          const created = await getSpriteCreated(filePath);

          if (created === undefined || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Sprite created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload) {
        const targetURL = url.replace("{name}", `${fileName}`);

        printLog(
          "info",
          `Downloading sprite "${id}" - File "${fileName}" - From "${targetURL}"...`,
        );

        await downloadSpriteFile(targetURL, filePath, maxTry, timeout, headers);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed sprite "${id}" - File "${fileName}": ${error}`,
      );
    }
  }

  printLog("info", "Downloading sprites...");

  await Promise.all(
    ["sprite.json", "sprite.png", "sprite@2x.json", "sprite@2x.png"].map(
      seedSpriteData,
    ),
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.(json|png)$/);

  printLog(
    "info",
    `Completed seed sprite "${id}" after ${(Date.now() - startTime) / 1000}s!`,
  );
}

/**
 * Seed font
 * @param {string} id Cache font ID
 * @param {string} url Font URL
 * @param {number} concurrency Concurrency
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedFont(
  id,
  url,
  concurrency,
  maxTry,
  timeout,
  refreshBefore,
  headers,
) {
  const startTime = Date.now();

  const total = 256;

  let log = `Seeding ${total} fonts of font "${id}" with:`;
  log += `\n\tURL: ${url} - Header: ${JSON.stringify(headers)}`;
  log += `\n\tConcurrency: ${concurrency} - Max try: ${maxTry} - Timeout: ${timeout}`;

  let refreshTimestamp;
  if (typeof refreshBefore === "string") {
    refreshTimestamp = new Date(refreshBefore).getTime();

    log += `\n\tRefresh before: ${refreshBefore}`;
  } else if (typeof refreshBefore === "number") {
    const now = new Date();

    refreshTimestamp = now.setDate(now.getDate() - refreshBefore);

    log += `\n\tOld than: ${refreshBefore} days`;
  }

  printLog("info", log);

  /* Download font files */
  const sourcePath = `${process.env.DATA_DIR}/caches/fonts/${id}`;

  async function seedFontData(idx, ranges, tasks) {
    const fileName = `${ranges[idx]}.pbf`;
    const filePath = `${sourcePath}/${fileName}`;
    const completeTasks = tasks.completeTasks;

    try {
      let needDownload = false;

      if (refreshTimestamp) {
        try {
          const created = await getFontCreated(filePath);

          if (created === undefined || created < refreshTimestamp) {
            needDownload = true;
          }
        } catch (error) {
          if (error.message === "Font created does not exist") {
            needDownload = true;
          } else {
            throw error;
          }
        }
      } else {
        needDownload = true;
      }

      if (needDownload) {
        const targetURL = url.replace("{range}.pbf", `${ranges[idx]}.pbf`);

        printLog(
          "info",
          `Downloading font "${id}" - Filename "${fileName}" - From "${targetURL}" - ${completeTasks}/${total}...`,
        );

        await downloadFontFile(targetURL, filePath, maxTry, timeout, headers);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to seed font "${id}" - Filename "${fileName}" - ${completeTasks}/${total}: ${error}`,
      );
    }
  }

  printLog("info", "Downloading fonts...");

  // Batch run
  await handleConcurrency(
    concurrency,
    seedFontData,
    Array.from({ length: 256 }, (_, idx) => `${idx * 256}-${idx * 256 + 255}`),
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.pbf$/);

  printLog(
    "info",
    `Completed seed ${total} fonts of font "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`,
  );
}

/**
 * Seed style
 * @param {string} id Cache style ID
 * @param {string} url Style URL
 * @param {number} maxTry Number of retry attempts on failure
 * @param {number} timeout Timeout in milliseconds
 * @param {string|number} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss" or number of days before which file should be refreshed
 * @param {object} headers Headers
 * @returns {Promise<void>}
 */
async function seedStyle(id, url, maxTry, timeout, refreshBefore, headers) {
  const startTime = Date.now();

  let log = `Seeding style "${id}" with:`;
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

  /* Download style.json file */
  const sourcePath = `${process.env.DATA_DIR}/caches/styles/${id}`;
  const filePath = `${sourcePath}/style.json`;

  try {
    let needDownload = false;

    if (refreshTimestamp === true) {
      try {
        const [response, styleData] = await Promise.all([
          getDataFromURL(
            `${url.slice(0, url.indexOf(`/${id}/style.json?raw=true`))}/md5`,
            timeout,
            "arraybuffer",
          ),
          getStyle(filePath),
        ]);

        if (
          !response.headers["etag"] ||
          response.headers["etag"] !== calculateMD5(styleData)
        ) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "JSON does not exist") {
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
        if (error.message === "Style created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    if (refreshTimestamp) {
      try {
        const created = await getStyleCreated(filePath);

        if (created === undefined || created < refreshTimestamp) {
          needDownload = true;
        }
      } catch (error) {
        if (error.message === "Style created does not exist") {
          needDownload = true;
        } else {
          throw error;
        }
      }
    } else {
      needDownload = true;
    }

    printLog("info", "Downloading style...");

    if (needDownload) {
      printLog(
        "info",
        `Downloading style "${id}" - File "${filePath}" - From "${url}"...`,
      );

      await downloadStyleFile(url, filePath, maxTry, timeout, headers);
    }
  } catch (error) {
    printLog("error", `Failed to seed style "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.json$/);

  printLog(
    "info",
    `Completed seed style "${id}" after ${(Date.now() - startTime) / 1000}s!`,
  );
}

/*********************************** Clean up *************************************/

/**
 * Cleanup data tiles
 * @param {"mbtiles"|"xyz"|"pg"} storeType Store type
 * @param {string} id Cleanup data ID
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {string|number} cleanUpBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be deleted
 * @returns {Promise<void>}
 */
async function cleanUpDataTiles(storeType, id, coverages, cleanUpBefore) {
  const startTime = Date.now();

  let source;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const concurrency = 256;

    const { total, tileBounds } = getTileBounds({ coverages: coverages });

    let log = `Cleaning up ${total} tiles of ${storeType} "${id}" with:`;
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

    let removeDataTileFunc;
    let compactDatabase;

    switch (storeType) {
      case "mbtiles": {
        const filePath = `${process.env.DATA_DIR}/caches/mbtiles/${id}/${id}.mbtiles`;

        /* Open database */
        printLog("info", "Opening database...");

        source = await openMBTilesDB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Get tile extra info */
        let tileExtraInfo;

        if (cleanUpTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Remove data tile function */
        removeDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
            return;
          }

          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
          );

          try {
            removeMBTilesTile(source, z, x, y);
          } catch (error) {
            printLog(
              "error",
              `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Compact database function */
        compactDatabase = async () => compactMBTiles(source);

        /* Close database function */
        closeDatabaseFunc = async () => closeMBTilesDB(source);

        break;
      }

      case "pg": {
        const filePath = `${process.env.POSTGRESQL_BASE_URI}/${id}`;

        /* Open database */
        printLog("info", "Opening database...");

        source = await openPostgreSQLDB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Get tile extra info */
        let tileExtraInfo;

        if (cleanUpTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getPostgreSQLTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Remove data tile function */
        removeDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
            return;
          }

          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
          );

          try {
            await removePostgreSQLTile(source, z, x, y);
          } catch (error) {
            printLog(
              "error",
              `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Compact database function */
        compactDatabase = async () => {};

        /* Close database function */
        closeDatabaseFunc = async () => await closePostgreSQLDB(source);

        break;
      }

      case "xyz": {
        const sourcePath = `${process.env.DATA_DIR}/caches/xyzs/${id}`;
        const filePath = `${sourcePath}/${id}.mbtiles`;

        /* Open database */
        printLog("info", "Opening database...");

        source = await openXYZMD5DB(
          filePath,
          true,
          30000, // 30 seconds
        );

        /* Get tile extra info */
        let tileExtraInfo;

        if (cleanUpTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${filePath}"...`);

            tileExtraInfo = getXYZTileExtraInfoFromCoverages(
              source,
              coverages,
              true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${filePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Detect format tile */
        const format = await getXYZFormatFromTiles(sourcePath);

        /* Remove data tile function */
        removeDataTileFunc = async (z, x, y, tasks) => {
          const tileName = `${z}/${x}/${y}`;

          if (cleanUpTimestamp && tileExtraInfo[tileName] >= cleanUpTimestamp) {
            return;
          }

          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Removing data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
          );

          try {
            await removeXYZTile(
              sourcePath,
              source,
              z,
              x,
              y,
              format,
              30000, // 30 seconds
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to cleanup data "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`,
            );
          }
        };

        /* Compact database function */
        compactDatabase = async () => {
          /* Compact database */
          compactXYZ(source);

          /* Remove parent folders if empty */
          await removeEmptyFolders(sourcePath, new RegExp(`^.*\\.${format}$`));
        };

        /* Close database function */
        closeDatabaseFunc = async () => closeXYZMD5DB(source);

        break;
      }
    }

    /* Remove data tiles */
    printLog("info", "Removing data tiles...");

    await handleTilesConcurrency(concurrency, removeDataTileFunc, tileBounds);

    /* Compact database */
    printLog("info", "Compacting database...");

    await compactDatabase();

    printLog(
      "info",
      `Completed cleanup ${total} tiles of ${storeType} "${id}" after ${
        (Date.now() - startTime) / 1000
      }s!`,
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
        30000, // 30 seconds
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
    }s!`,
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
          30000, // 30 seconds
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to cleanup sprite "${id}" - File "${fileName}": ${error}`,
      );
    }
  }

  printLog("info", "Removing sprites...");

  await Promise.all(
    ["sprite.json", "sprite.png", "sprite@2x.json", "sprite@2x.png"].map(
      cleanUpSpriteData,
    ),
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.(json|png)$/);

  printLog(
    "info",
    `Completed cleanup sprite "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`,
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
          30000, // 30 seconds
        );
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to cleanup font "${id}" -  Range "${range}": ${error}`,
      );
    }
  }

  printLog("info", "Removing fonts...");

  await Promise.all(
    Array.from({ length: total }, (_, i) =>
      cleanUpFontData(i * 256, i * 256 + 255),
    ),
  );

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.pbf$/);

  printLog(
    "info",
    `Completed cleanup ${total} fonts of font "${id}" after ${
      (Date.now() - startTime) / 1000
    }s!`,
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
        30000, // 30 seconds
      );
    }
  } catch (error) {
    printLog("error", `Failed to cleanup style "${id}": ${error}`);
  }

  /* Remove parent folders if empty */
  await removeEmptyFolders(sourcePath, /^.*\.json$/);

  printLog(
    "info",
    `Completed cleanup style "${id}" after ${(Date.now() - startTime) / 1000}s!`,
  );
}
