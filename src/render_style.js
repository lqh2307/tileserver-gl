"use strict";

import mlgl from "@maplibre/maplibre-gl-native";
import { config } from "./configs/index.js";
import { createPool } from "generic-pool";
import { nanoid } from "nanoid";
import path from "path";
import os from "os";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  getXYZTileExtraInfoFromCoverages,
  getAndCachePostgreSQLTileData,
  getAndCacheMBTilesTileData,
  MBTILES_INSERT_TILE_QUERY,
  updatePostgreSQLMetadata,
  storePostgreSQLTileData,
  getAndCacheDataGeoJSON,
  getAndCacheXYZTileData,
  getAndCacheDataSprite,
  updateMBTilesMetadata,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  storeMBtilesTileData,
  XYZ_INSERT_MD5_QUERY,
  closePostgreSQLDB,
  updateXYZMetadata,
  storeXYZTileFile,
  openPostgreSQLDB,
  getFallbackFont,
  closeMBTilesDB,
  getPMTilesTile,
  openMBTilesDB,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./resources/index.js";
import {
  detectFormatAndHeaders,
  FALLBACK_TILE_DATA,
  lonLat4326ToXY3857,
  xy3857ToLonLat4326,
  createImageOutput,
  getLonLatFromXYZ,
  BACKGROUND_COLOR,
  runAllWithLimit,
  calculateSizes,
  getDataFromURL,
  base64ToBuffer,
  getTileBounds,
  calculateMD5,
  unzipAsync,
  printLog,
} from "./utils/index.js";

mlgl.on("message", (msg) => {
  switch (msg.severity) {
    case "ERROR": {
      printLog("error", `Render StyleJSON: ${msg.text}`);

      break;
    }

    case "WARNING": {
      printLog("warn", `Render StyleJSON: ${msg.text}`);

      break;
    }

    default: {
      printLog("info", `Render StyleJSON: ${msg.text}`);

      break;
    }
  }
});

/**
 * Create render
 * @param {{ mode: "tile"|"static", styleJSON: object, ratio: number }} option Option object
 * @returns {object}
 */
function createRenderer(option) {
  const renderer = new mlgl.Map({
    mode: option.mode,
    ratio: option.ratio ?? 1,
    request: async (req, callback) => {
      const scheme = req.url.slice(0, req.url.indexOf(":"));

      let data = null;
      let err = null;

      // Handle get resource
      switch (scheme) {
        /* Get sprite */
        case "sprites": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataSprite(parts[2], parts[3]);

            /* Unzip data */
            const headers = detectFormatAndHeaders(data).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}`,
            );

            err = error;
          }

          break;
        }

        /* Get font */
        case "fonts": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataFonts(parts[2], parts[3]);

            /* Unzip data */
            const headers = detectFormatAndHeaders(data).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}`,
            );

            err = error;
          }

          break;
        }

        /* Get geojson */
        case "geojson": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataGeoJSON(parts[2], parts[3]);

            /* Unzip data */
            const headers = detectFormatAndHeaders(data).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "error",
              `Failed to get GeoJSON group "${parts[2]}" - Layer "${parts[3]}": ${error}.`,
            );

            err = error;
          }

          break;
        }

        /* Get pmtiles tile */
        case "pmtiles": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = parts[3];
          const x = parts[4];
          const y = parts[5].slice(0, parts[5].indexOf("."));
          const item = config.datas[parts[2]];

          try {
            const tileData = await getPMTilesTile(item.source, +z, +x, +y);

            /* Unzip data */
            data = tileData.headers["content-encoding"]
              ? await unzipAsync(tileData.data)
              : tileData.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`,
            );

            data = FALLBACK_TILE_DATA[item.tileJSON.format];
          }

          break;
        }

        /* Get mbtiles tile */
        case "mbtiles": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = parts[3];
          const x = parts[4];
          const y = parts[5].slice(0, parts[5].indexOf("."));
          const item = config.datas[parts[2]];

          try {
            const tileData = await getAndCacheMBTilesTileData(
              parts[2],
              +z,
              +x,
              +y,
            );

            /* Unzip data */
            data = tileData.headers["content-encoding"]
              ? await unzipAsync(tileData.data)
              : tileData.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`,
            );

            data = FALLBACK_TILE_DATA[item.tileJSON.format];
          }

          break;
        }

        /* Get xyz tile */
        case "xyz": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = parts[3];
          const x = parts[4];
          const y = parts[5].slice(0, parts[5].indexOf("."));
          const item = config.datas[parts[2]];

          try {
            const tileData = await getAndCacheXYZTileData(parts[2], +z, +x, +y);

            /* Unzip data */
            data = tileData.headers["content-encoding"]
              ? await unzipAsync(tileData.data)
              : tileData.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`,
            );

            data = FALLBACK_TILE_DATA[item.tileJSON.format];
          }

          break;
        }

        /* Get pg tile */
        case "pg": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = parts[3];
          const x = parts[4];
          const y = parts[5].slice(0, parts[5].indexOf("."));
          const item = config.datas[parts[2]];

          try {
            const tileData = await getAndCachePostgreSQLTileData(
              parts[2],
              +z,
              +x,
              +y,
            );

            /* Unzip data */
            data = tileData.headers["content-encoding"]
              ? await unzipAsync(tileData.data)
              : tileData.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`,
            );

            data = FALLBACK_TILE_DATA[item.tileJSON.format];
          }

          break;
        }

        /* Get data from remote */
        case "http":
        case "https": {
          try {
            data = await getDataFromURL(req.url, {
              method: "GET",
              timeout: 30000, // 30 seconds
              responseType: "arraybuffer",
              decompress: true,
            });
          } catch (error) {
            if (req.kind === 3) {
              const result = req.url.match(/(png|jpg|jpeg|webp|pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get tile from "${req.url}": ${error}. Serving empty tile...`,
                );

                data = FALLBACK_TILE_DATA[result[0]];
              } else {
                printLog("error", `Failed to detect tile from "${req.url}"`);

                err = error;
              }
            } else if (req.kind === 4) {
              const result = req.url.match(/([^/]+\/\d+-\d+\.pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get font from "${req.url}": ${error}. Serving fallback font "Open Sans"...`,
                );

                const parts = result[0].split("/");

                data = await getFallbackFont(parts[0], parts[1]);

                /* Unzip data */
                const headers = detectFormatAndHeaders(data).headers;

                if (headers["content-encoding"]) {
                  data = await unzipAsync(data);
                }
              } else {
                printLog("error", `Failed to detect font from "${req.url}"`);

                err = error;
              }
            } else {
              printLog(
                "error",
                `Failed to get data from "${req.url}": ${error}`,
              );

              err = error;
            }
          }

          break;
        }

        /* Get base64 data */
        case "data": {
          try {
            const dataBase64 = base64ToBuffer(req.url);

            /* Unzip data */
            const headers = detectFormatAndHeaders(dataBase64).headers;

            if (headers["content-encoding"]) {
              data = await unzipAsync(dataBase64);
            } else {
              data = dataBase64;
            }
          } catch (error) {
            printLog("error", `Failed to decode base64 data: ${error}`);

            err = error;
          }

          break;
        }

        /* Default */
        default: {
          err = new Error(`Unknown scheme: "${scheme}"`);

          printLog("error", `Failed to render: ${err}`);

          break;
        }
      }

      // Call callback fn
      callback(err, {
        data: data,
      });
    },
  });

  // Load style
  renderer.load(option.styleJSON);

  return renderer;
}

/**
 * Render image tile data
 * @param {number} z Zoom level
 * @param {number} x Tile column
 * @param {number} y Tile row
 * @param {{ pool: object, styleJSON: object, pitch: number, bearing: number, tileScale: number, tileSize: 256|512, format: "jpeg"|"jpg"|"png"|"webp", grayscale: boolean, filePath: string }} option Option object
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageTileData(z, x, y, option) {
  const renderer = option.pool
    ? await option.pool.acquire()
    : createRenderer({
        mode: "tile",
        ratio: option.tileScale,
        styleJSON: option.styleJSON,
      });

  return await new Promise((resolve, reject) => {
    const isNeedHack = z === 0 && option.tileSize === 256;
    const hackTileSize = isNeedHack ? option.tileSize * 2 : option.tileSize;

    renderer.render(
      {
        zoom: z > 0 && option.tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
        pitch: option.pitch ?? 0,
        bearing: option.bearing ?? 0,
      },
      (error, data) => {
        option.pool ? option.pool.release(renderer) : renderer.release();

        if (error) {
          return reject(error);
        }

        const tileSize = hackTileSize * option.tileScale;
        const originTileSize = Math.round(tileSize);
        const targetTileSize = isNeedHack
          ? Math.round(tileSize / 2)
          : undefined;

        createImageOutput({
          data: data,
          rawOption: {
            premultiplied: true,
            width: originTileSize,
            height: originTileSize,
            channels: 4,
          },
          format: option.format,
          grayscale: option.grayscale,
          filePath: option.filePath,
          width: targetTileSize,
          height: targetTileSize,
        })
          .then(resolve)
          .catch(reject);
      },
    );
  });
}

/**
 * Render image static data
 * @param {{ pool: object, styleJSON: object, pitch: number, bearing: number, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp", grayscale: boolean, width: number, height: number, filePath: string }} option Option object
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageStaticData(option) {
  const renderer = option.pool
    ? await option.pool.acquire()
    : createRenderer({
        mode: "static",
        ratio: option.tileScale,
        styleJSON: option.styleJSON,
      });

  return await new Promise((resolve, reject) => {
    const sizes = calculateSizes(option.zoom, option.bbox, option.tileSize);

    renderer.render(
      {
        zoom: option.zoom,
        center: [
          (option.bbox[0] + option.bbox[2]) / 2,
          (option.bbox[1] + option.bbox[3]) / 2,
        ],
        width: sizes.width,
        height: sizes.height,
        pitch: option.pitch ?? 0,
        bearing: option.bearing ?? 0,
      },
      (error, data) => {
        option.pool ? option.pool.release(renderer) : renderer.release();

        if (error) {
          return reject(error);
        }

        createImageOutput({
          data: data,
          rawOption: {
            premultiplied: true,
            width: Math.round(option.tileScale * sizes.width),
            height: Math.round(option.tileScale * sizes.height),
            channels: 4,
          },
          format: option.format,
          grayscale: option.grayscale,
          width: option.width,
          height: option.height,
          filePath: option.filePath,
        })
          .then(resolve)
          .catch(reject);
      },
    );
  });
}

/**
 * Render StyleJSON
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp", grayscale: boolean, width: number, height: number }} option Option object
 * @returns {Promise<string>}
 */
export async function renderStyleJSON(option) {
  const MAX_TILE_PX = 8192;

  const sizes = calculateSizes(option.zoom, option.bbox, option.tileSize);
  const totalWidth = Math.round(option.tileScale * sizes.width);
  const totalHeight = Math.round(option.tileScale * sizes.height);

  const id = nanoid();
  const dirPath = `${process.env.DATA_DIR}/exports/style_renders/${option.format}s/${id}`;
  const filePath = `${dirPath}/${id}.${option.format}`;

  if (totalWidth <= MAX_TILE_PX && totalHeight <= MAX_TILE_PX) {
    return await renderImageStaticData({
      ...option,
      filePath: filePath,
    });
  } else {
    const [minX, minY] = lonLat4326ToXY3857(option.bbox[0], option.bbox[1]);
    const [maxX, maxY] = lonLat4326ToXY3857(option.bbox[2], option.bbox[3]);

    const xSplits = Math.ceil(totalWidth / MAX_TILE_PX);
    const ySplits = Math.ceil(totalHeight / MAX_TILE_PX);

    const xStep = (maxX - minX) / xSplits;
    const yStep = (maxY - minY) / ySplits;

    const pxStep = totalWidth / xSplits;
    const pyStep = totalHeight / ySplits;

    // Create composite options
    const total = xSplits * ySplits;
    const compositesOption = Array(total);

    function* createCompositeOptionGenerator() {
      for (let idx = 0; idx < total; idx++) {
        yield async () => {
          const xi = Math.floor(idx / ySplits);
          const yi = idx % ySplits;

          const subMinX = minX + xi * xStep;
          const subMinY = minY + yi * yStep;
          const subFilePath = `${dirPath}/${idx}.${option.format}`;

          await renderImageStaticData({
            styleJSON: option.styleJSON,
            tileScale: option.tileScale,
            tileSize: option.tileSize,
            format: option.format,
            pitch: option.pitch,
            bearing: option.bearing,
            bbox: [
              ...xy3857ToLonLat4326(subMinX, subMinY),
              ...xy3857ToLonLat4326(subMinX + xStep, subMinY + yStep),
            ],
            filePath: subFilePath,
            zoom: option.zoom,
          });

          compositesOption[idx] = {
            limitInputPixels: false,
            input: subFilePath,
            left: Math.round(xi * pxStep),
            top: Math.round(totalHeight - (yi + 1) * pyStep),
          };
        };
      }
    }

    // Batch run
    await runAllWithLimit(createCompositeOptionGenerator(), os.cpus().length);

    // Create image output
    return await createImageOutput({
      createOption: {
        width: totalWidth,
        height: totalHeight,
        channels: 4,
        background: BACKGROUND_COLOR,
      },
      filePath: filePath,
      compositesOption: compositesOption,
      format: option.format,
      width: option.width,
      height: option.height,
      grayscale: option.grayscale,
    });
  }
}

/**
 * Render tile datas
 * @param {string} id Style ID
 * @param {"mbtiles"|"xyz"|"pg"} storeType Store type
 * @param {string} storePath Exported path
 * @param {object} metadata Metadata object
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderTileDatas(
  id,
  storeType,
  storePath,
  metadata,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  tileScale,
  tileSize,
  refreshBefore,
) {
  const startTime = Date.now();

  let source;
  let pool;
  let closeDatabaseFunc;

  try {
    /* Calculate summary */
    const { targetCoverages, realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });

    let log = `Rendering ${total} tiles of style id "${id}" to ${storeType} with:`;
    log += `\n\tStore path: ${storePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tFormat: ${metadata.format} - Tile scale: ${tileScale} - Tile size: ${tileSize}`;
    log += `\n\tBBox: ${JSON.stringify(metadata.bounds)}- Minzoom: ${metadata.minzoom} - Maxzoom: ${metadata.maxzoom}`;

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

    let tileExtraInfo;
    let storeTileDataFunc;
    let tileOption;

    const item = config.styles[id];
    const styleJSON = await getRenderedStyleJSON(item.path);
    const newMetadata = {
      ...metadata,
      bounds: realBBox,
    };

    /* Create renderer pool */
    if (maxRendererPoolSize) {
      pool = createPool(
        {
          create: () =>
            createRenderer({
              mode: "tile",
              ratio: tileScale,
              styleJSON: styleJSON,
            }),
          destroy: (renderer) => renderer.release(),
        },
        {
          min: 1,
          max: maxRendererPoolSize,
        },
      );
    }

    switch (storeType) {
      default: {
        throw new Error(`Invalid store type "${storeType}"`);
      }

      case "mbtiles": {
        /* Create database */
        printLog("info", "Creating database...");

        source = await openMBTilesDB(
          storePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateMBTilesMetadata(source, newMetadata);

        /* Get tile extra info */
        if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${storePath}"...`);

            tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
              source,
              targetCoverages,
              refreshTimestamp === true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${storePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(MBTILES_INSERT_TILE_QUERY),
          created: Date.now(),
          storeTransparent: storeTransparent,
          pool: pool,
          styleJSON: styleJSON,
          tileScale: tileScale,
          tileSize: tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storeMBtilesTileData(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => closeMBTilesDB(source);

        break;
      }

      case "pg": {
        /* Create database */
        printLog("info", "Creating database...");

        source = await openPostgreSQLDB(
          storePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        await updatePostgreSQLMetadata(source, newMetadata);

        /* Get tile extra info */
        if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${storePath}"...`);

            tileExtraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
              source,
              targetCoverages,
              refreshTimestamp === true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${storePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Assign tile option */
        tileOption = {
          created: Date.now(),
          storeTransparent: storeTransparent,
          pool: pool,
          styleJSON: styleJSON,
          tileScale: tileScale,
          tileSize: tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storePostgreSQLTileData(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => await closePostgreSQLDB(source);

        break;
      }

      case "xyz": {
        const sqliteFilePath = `${storePath}/${path.basename(storePath)}.sqlite`;

        /* Create database */
        printLog("info", "Creating database...");

        source = await openXYZMD5DB(
          sqliteFilePath,
          true,
          30000, // 30 seconds
        );

        /* Update metadata */
        printLog("info", "Updating metadata...");

        updateXYZMetadata(source, newMetadata);

        /* Get tile extra info */
        if (refreshTimestamp) {
          try {
            printLog("info", `Get tile extra info from "${sqliteFilePath}"...`);

            tileExtraInfo = getXYZTileExtraInfoFromCoverages(
              source,
              targetCoverages,
              refreshTimestamp === true,
            );
          } catch (error) {
            printLog(
              "error",
              `Failed to get tile extra info from "${sqliteFilePath}": ${error}`,
            );

            tileExtraInfo = {};
          }
        }

        /* Assign tile option */
        tileOption = {
          statement: source.prepare(XYZ_INSERT_MD5_QUERY),
          created: Date.now(),
          sourcePath: storePath,
          storeTransparent: storeTransparent,
          pool: pool,
          styleJSON: styleJSON,
          tileScale: tileScale,
          tileSize: tileSize,
          format: metadata.format,
        };

        /* Store data function */
        storeTileDataFunc = async (z, x, y, data) =>
          await storeXYZTileFile(z, x, y, data, tileOption);

        /* Close database function */
        closeDatabaseFunc = async () => closeXYZMD5DB(source);

        break;
      }
    }

    /* Render and store tile data generator */
    function* renderAndStoreTileDataGenerator() {
      let completeTasks = 0;

      for (const { z, x, y } of tileBounds) {
        for (let xCount = x[0]; xCount <= x[1]; xCount++) {
          for (let yCount = y[0]; yCount <= y[1]; yCount++) {
            completeTasks++;

            yield async () => {
              const tileName = `${z}/${xCount}/${yCount}`;

              try {
                if (refreshTimestamp === true) {
                  printLog(
                    "info",
                    `Rendering style id "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
                  );

                  // Get tile data
                  const data = await renderImageTileData(
                    z,
                    xCount,
                    yCount,
                    tileOption,
                  );

                  if (tileExtraInfo[tileName] === calculateMD5(data)) {
                    return;
                  }

                  // Store tile data
                  await storeTileDataFunc(z, xCount, yCount, data);
                } else {
                  if (
                    refreshTimestamp &&
                    tileExtraInfo[tileName] >= refreshTimestamp
                  ) {
                    return;
                  }

                  printLog(
                    "info",
                    `Rendering style id "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`,
                  );

                  // Store tile data
                  await storeTileDataFunc(
                    z,
                    xCount,
                    yCount,
                    await renderImageTileData(z, xCount, yCount, tileOption),
                  );
                }
              } catch (error) {
                printLog(
                  "error",
                  `Failed to render style id "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`,
                );
              }
            };
          }
        }
      }
    }

    /* Render and store tile datas */
    printLog("info", "Rendering and storing tile datas...");

    await runAllWithLimit(renderAndStoreTileDataGenerator(), concurrency, item);

    printLog(
      "info",
      `Completed render ${total} tiles of style id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s!`,
    );
  } catch (error) {
    printLog(
      "error",
      `Failed to render style id "${id}" to ${storeType} after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`,
    );
  } finally {
    /* Destroy renderer pool */
    if (pool) {
      pool.drain().then(pool.clear);
    }

    /* Close database */
    if (source && closeDatabaseFunc) {
      await closeDatabaseFunc();
    }
  }
}
