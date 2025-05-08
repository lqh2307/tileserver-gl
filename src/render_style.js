"use strict";

import { getPMTilesTile } from "./tile_pmtiles.js";
import { getRenderedStyleJSON } from "./style.js";
import { createPool } from "generic-pool";
import { printLog } from "./logger.js";
import { getFonts } from "./font.js";
import { config } from "./config.js";
import { Mutex } from "async-mutex";
import cluster from "cluster";
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
  getAndCacheDataGeoJSON,
  getAndCacheXYZDataTile,
  getAndCacheDataSprite,
} from "./data.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  calculateMBTilesTileExtraInfo,
  updateMBTilesMetadata,
  cacheMBtilesTileData,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundsFromCoverages,
  detectFormatAndHeaders,
  createFallbackTileData,
  renderImageTileData,
  removeEmptyFolders,
  getLonLatFromXYZ,
  processCoverages,
  getDataFromURL,
  calculateMD5,
  unzipAsync,
  runCommand,
  delay,
} from "./utils.js";
import {
  getXYZTileExtraInfoFromCoverages,
  updateXYZMetadata,
  cacheXYZTileFile,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./tile_xyz.js";

let mlgl;

if (cluster.isPrimary !== true) {
  import("@maplibre/maplibre-gl-native")
    .then((module) => {
      mlgl = module.default;

      printLog(
        "info",
        `Success to import "@maplibre/maplibre-gl-native". Enable backend render!`
      );

      config.enableBackendRender = true;
    })
    .catch((error) => {
      printLog(
        "error",
        `Failed to import "@maplibre/maplibre-gl-native": ${error}. Disable backend render!`
      );
    });
}

/**
 * Create tile render
 * @param {number} tileScale Tile scale
 * @param {object} styleJSON StyleJSON
 * @returns {object}
 */
function createTileRenderer(tileScale, styleJSON) {
  const renderer = new mlgl.Map({
    mode: "tile",
    ratio: tileScale,
    request: async (req, callback) => {
      const url = decodeURIComponent(req.url);
      const parts = url.split("/");
      let data = null;
      let err = null;

      switch (parts[0]) {
        /* Get sprite */
        case "sprites:": {
          try {
            data = await getAndCacheDataSprite(parts[2], parts[3]);
          } catch (error) {
            printLog(
              "warn",
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty sprite...`
            );

            err = error;
          }

          break;
        }

        /* Get font */
        case "fonts:": {
          try {
            data = await getFonts(parts[2], parts[3]);

            const headers = detectFormatAndHeaders(data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty font...`
            );

            err = error;
          }

          break;
        }

        /* Get geojson */
        case "geojson:": {
          try {
            data = await getAndCacheDataGeoJSON(parts[2], parts[3]);
          } catch (error) {
            printLog(
              "warn",
              `Failed to get GeoJSON group "${parts[2]}" - Layer "${parts[3]}": ${error}. Serving empty geojson...`
            );

            err = error;
          }

          break;
        }

        /* Get pmtiles tile */
        case "pmtiles:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getPMTilesTile(item.source, z, x, y);

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get mbtiles tile */
        case "mbtiles:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCacheMBTilesDataTile(
              parts[2],
              z,
              x,
              y
            );

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get xyz tile */
        case "xyz:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCacheXYZDataTile(parts[2], z, x, y);

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get pg tile */
        case "pg:": {
          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCachePostgreSQLDataTile(
              parts[2],
              z,
              x,
              y
            );

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataTile.data);
            } else {
              data = dataTile.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
            err = error;
          }

          break;
        }

        /* Get data from remote */
        case "http:":
        case "https:": {
          try {
            printLog("info", `Getting data from "${url}"...`);

            const dataRemote = await getDataFromURL(
              url,
              30000, // 30 secs
              "arraybuffer"
            );

            /* Unzip pbf data */
            const headers = detectFormatAndHeaders(dataRemote.data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"] !== undefined
            ) {
              data = await unzipAsync(dataRemote.data);
            } else {
              data = dataRemote.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data from "${url}": ${error}. Serving empty data...`
            );

            data = createFallbackTileData(url.slice(url.lastIndexOf(".") + 1));
            err = error;
          }

          break;
        }

        /* Default */
        default: {
          err = new Error(`Unknown scheme: "${parts[0]}`);

          printLog("warn", `Failed to render: ${err}. Skipping...`);

          break;
        }
      }

      callback(err, {
        data: data,
      });
    },
  });

  renderer.load(styleJSON);

  return renderer;
}

/**
 * Render image tile
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {object} styleJSON StyleJSON
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageTile(
  tileScale,
  tileSize,
  styleJSON,
  z,
  x,
  y,
  format
) {
  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

  const data = await new Promise((resolve, reject) => {
    const renderer = createTileRenderer(tileScale, styleJSON);

    renderer.render(
      {
        zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
      },
      (error, data) => {
        if (renderer !== undefined) {
          renderer.release();
        }

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  return await renderImageTileData(
    data,
    hackTileSize * tileScale,
    isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
    format
  );
}

/**
 * Render MBTiles tiles
 * @param {string} id Style ID
 * @param {string} fileID Exported file id
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderMBTilesTiles(
  id,
  fileID,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to mbtiles with:`;
    log += `\n\tID: ${fileID}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const filePath = `${process.env.DATA_DIR}/exports/style_renders/mbtiles/${fileID}/${fileID}.mbtiles`;

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
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

    /* Render tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const item = config.styles[id];
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);
    let pool;
    let renderMBTilesTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createTileRenderer(tileScale, renderedStyleJSON),
          destroy: (renderer) => renderer.release(),
        },
        {
          min: 1,
          max: maxRendererPoolSize,
        }
      );

      renderMBTilesTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const isNeedHack = z === 0 && tileSize === 256;
            const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

            const renderer = await pool.acquire();

            const dataRaw = await new Promise((resolve, reject) => {
              renderer.render(
                {
                  zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                  center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                  width: hackTileSize,
                  height: hackTileSize,
                },
                (error, dataRaw) => {
                  if (renderer !== undefined) {
                    pool.release(renderer);
                  }

                  if (error) {
                    return reject(error);
                  }

                  resolve(dataRaw);
                }
              );
            });

            const data = await renderImageTileData(
              dataRaw,
              hackTileSize * tileScale,
              isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    } else {
      renderMBTilesTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const data = await renderImageTile(
              tileScale,
              tileSize,
              renderedStyleJSON,
              z,
              x,
              y,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderMBTilesTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0) {
      await pool.drain();
      await pool.clear();
    }

    /* Create overviews */
    if (createOverview === true) {
      printLog("info", "Creating overviews...");

      const command = `gdaladdo -r lanczos -oo ZLEVEL=9 ${filePath} 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304`;

      printLog("info", `Gdal command: ${command}`);

      const commandOutput = await runCommand(command);

      printLog("info", `Gdal command output: ${commandOutput}`);

      printLog("info", "Calculating tile extra info...");

      await calculateMBTilesTileExtraInfo(source);
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to mbtiles after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to mbtiles after ${
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
 * Render XYZ tiles
 * @param {string} id Style ID
 * @param {string} fileID Exported file id
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderXYZTiles(
  id,
  fileID,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tID: ${fileID}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const sourcePath = `${process.env.DATA_DIR}/exports/style_renders/xyzs/${fileID}`;
    const filePath = `${sourcePath}/${fileID}.sqlite`;

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
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

    /* Render tile files */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const item = config.styles[id];
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);
    let pool;
    let renderXYZTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createTileRenderer(tileScale, renderedStyleJSON),
          destroy: (renderer) => renderer.release(),
        },
        {
          min: 1,
          max: maxRendererPoolSize,
        }
      );

      renderXYZTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const isNeedHack = z === 0 && tileSize === 256;
            const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

            const renderer = await pool.acquire();

            const dataRaw = await new Promise((resolve, reject) => {
              renderer.render(
                {
                  zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                  center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                  width: hackTileSize,
                  height: hackTileSize,
                },
                (error, dataRaw) => {
                  if (renderer !== undefined) {
                    pool.release(renderer);
                  }

                  if (error) {
                    return reject(error);
                  }

                  resolve(dataRaw);
                }
              );
            });

            const data = await renderImageTileData(
              dataRaw,
              hackTileSize * tileScale,
              isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cacheXYZTileFile(
              sourcePath,
              source,
              z,
              x,
              y,
              metadata.format,
              data,
              storeTransparent
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    } else {
      renderXYZTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const data = await renderImageTile(
              tileScale,
              tileSize,
              renderedStyleJSON,
              z,
              x,
              y,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cacheXYZTileFile(
              sourcePath,
              source,
              z,
              x,
              y,
              metadata.format,
              data,
              storeTransparent
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderXYZTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0) {
      await pool.drain();
      await pool.clear();
    }

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp)$/);

    /* Create overviews */
    if (createOverview === true) {
      // Do nothing
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to xyz after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to xyz after ${
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
 * Render PostgreSQL tiles
 * @param {string} id Style ID
 * @param {string} fileID Exported file id
 * @param {object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {{ zoom: number, bbox: [number, number, number, number]}[]} coverages Specific coverages
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderPostgreSQLTiles(
  id,
  fileID,
  metadata,
  tileScale,
  tileSize,
  coverages,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
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

    let log = `Rendering ${total} tiles of style "${id}" to postgresql with:`;
    log += `\n\tID: ${fileID}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tCreate overview: ${createOverview}`;
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
    const filePath = `${process.env.POSTGRESQL_BASE_URI}/${fileID}`;

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestampType !== "undefined") {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestampType === "number"
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

    /* Render tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    const item = config.styles[id];
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);
    let pool;
    let renderPostgreSQLTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createTileRenderer(tileScale, renderedStyleJSON),
          destroy: (renderer) => renderer.release(),
        },
        {
          min: 1,
          max: maxRendererPoolSize,
        }
      );

      renderPostgreSQLTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const isNeedHack = z === 0 && tileSize === 256;
            const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

            const renderer = await pool.acquire();

            const dataRaw = await new Promise((resolve, reject) => {
              renderer.render(
                {
                  zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
                  center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
                  width: hackTileSize,
                  height: hackTileSize,
                },
                (error, dataRaw) => {
                  if (renderer !== undefined) {
                    pool.release(renderer);
                  }

                  if (error) {
                    return reject(error);
                  }

                  resolve(dataRaw);
                }
              );
            });

            const data = await renderImageTileData(
              dataRaw,
              hackTileSize * tileScale,
              isNeedHack === false ? undefined : (hackTileSize / 2) * tileScale,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cachePostgreSQLTileData(
              source,
              z,
              x,
              y,
              data,
              storeTransparent
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    } else {
      renderPostgreSQLTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        if (
          refreshTimestampType !== "number" ||
          tileExtraInfo[tileName] === undefined ||
          tileExtraInfo[tileName] < refreshTimestamp
        ) {
          const completeTasks = tasks.completeTasks;

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          try {
            // Rendered data
            const data = await renderImageTile(
              tileScale,
              tileSize,
              renderedStyleJSON,
              z,
              x,
              y,
              metadata.format
            );

            if (
              refreshTimestampType === "boolean" &&
              tileExtraInfo[tileName] === calculateMD5(data)
            ) {
              return;
            }

            // Store data
            await cachePostgreSQLTileData(
              source,
              z,
              x,
              y,
              data,
              storeTransparent
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
            );
          }
        }
      };
    }

    printLog("info", "Rendering datas...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(50);
          }

          await tasks.mutex.runExclusive(() => {
            tasks.activeTasks++;
            tasks.completeTasks++;
          });

          /* Run a task */
          renderPostgreSQLTileData(z, xCount, yCount, tasks).finally(() =>
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

    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0) {
      await pool.drain();
      await pool.clear();
    }

    /* Create overviews */
    if (createOverview === true) {
      // Do nothing
    }

    printLog(
      "info",
      `Completed render ${total} tiles of style "${id}" to postgresql after ${
        (Date.now() - startTime) / 1000
      }s!`
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style "${id}" to postgresql after ${
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
