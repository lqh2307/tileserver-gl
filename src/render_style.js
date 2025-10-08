"use strict";

import mlgl from "@maplibre/maplibre-gl-native";
import { config } from "./configs/index.js";
import { createPool } from "generic-pool";
import os from "os";
import {
  getPostgreSQLTileExtraInfoFromCoverages,
  getMBTilesTileExtraInfoFromCoverages,
  calculatePostgreSQLTileExtraInfo,
  getXYZTileExtraInfoFromCoverages,
  calculateMBTilesTileExtraInfo,
  getAndCachePostgreSQLDataTile,
  getAndCacheMBTilesDataTile,
  calculateXYZTileExtraInfo,
  updatePostgreSQLMetadata,
  cachePostgreSQLTileData,
  getAndCacheDataGeoJSON,
  getAndCacheXYZDataTile,
  addPostgreSQLOverviews,
  getAndCacheDataSprite,
  updateMBTilesMetadata,
  getAndCacheDataFonts,
  getRenderedStyleJSON,
  cacheMBtilesTileData,
  addMBTilesOverviews,
  closePostgreSQLDB,
  updateXYZMetadata,
  cacheXYZTileFile,
  openPostgreSQLDB,
  addXYZOverviews,
  getFallbackFont,
  closeMBTilesDB,
  getPMTilesTile,
  openMBTilesDB,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./resources/index.js";
import {
  renderImageToHighQualityPDF,
  detectFormatAndHeaders,
  createFallbackTileData,
  handleTilesConcurrency,
  removeEmptyFolders,
  lonLat4326ToXY3857,
  xy3857ToLonLat4326,
  handleConcurrency,
  createImageOutput,
  getLonLatFromXYZ,
  renderImageToPDF,
  addFrameToImage,
  getDataFromURL,
  calculateSizes,
  base64ToBuffer,
  getTileBounds,
  calculateMD5,
  unzipAsync,
  printLog,
} from "./utils/index.js";

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
          const parts = decodeURI(req.url).split("/");

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
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}`
            );

            err = error;
          }

          break;
        }

        /* Get font */
        case "fonts": {
          const parts = decodeURI(req.url).split("/");

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
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}`
            );

            err = error;
          }

          break;
        }

        /* Get geojson */
        case "geojson": {
          const parts = decodeURI(req.url).split("/");

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
              `Failed to get GeoJSON group "${parts[2]}" - Layer "${parts[3]}": ${error}.`
            );

            err = error;
          }

          break;
        }

        /* Get pmtiles tile */
        case "pmtiles": {
          const parts = decodeURI(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getPMTilesTile(item.source, z, x, y);

            /* Unzip data */
            data = dataTile.headers["content-encoding"]
              ? await unzipAsync(dataTile.data)
              : dataTile.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
          }

          break;
        }

        /* Get mbtiles tile */
        case "mbtiles": {
          const parts = decodeURI(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCacheMBTilesDataTile(
              parts[2],
              z,
              x,
              y
            );

            /* Unzip data */
            data = dataTile.headers["content-encoding"]
              ? await unzipAsync(dataTile.data)
              : dataTile.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
          }

          break;
        }

        /* Get xyz tile */
        case "xyz": {
          const parts = decodeURI(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCacheXYZDataTile(parts[2], z, x, y);

            /* Unzip data */
            data = dataTile.headers["content-encoding"]
              ? await unzipAsync(dataTile.data)
              : dataTile.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
          }

          break;
        }

        /* Get pg tile */
        case "pg": {
          const parts = decodeURI(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCachePostgreSQLDataTile(
              parts[2],
              z,
              x,
              y
            );

            /* Unzip data */
            data = dataTile.headers["content-encoding"]
              ? await unzipAsync(dataTile.data)
              : dataTile.data;
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data "${parts[2]}" - Tile "${`${z}/${x}/${y}`}": ${error}. Serving empty tile...`
            );

            data = createFallbackTileData(item.tileJSON.format);
          }

          break;
        }

        /* Get data from remote */
        case "http":
        case "https": {
          const url = decodeURI(req.url);

          try {
            const dataRemote = await getDataFromURL(
              url,
              30000, // 30 secs
              "arraybuffer"
            );

            /* Unzip data */
            const headers = detectFormatAndHeaders(dataRemote.data).headers;

            data = headers["content-encoding"]
              ? await unzipAsync(dataRemote.data)
              : dataRemote.data;
          } catch (error) {
            if (req.kind === 3) {
              const result = url.match(/(gif|png|jpg|jpeg|webp|pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get tile from "${url}": ${error}. Serving empty tile...`
                );

                data = createFallbackTileData(result[0]);
              } else {
                printLog("error", `Failed to detect tile from "${url}"`);

                err = error;
              }
            } else if (req.kind === 4) {
              const result = url.match(/([^/]+\/\d+-\d+\.pbf)/g);
              if (result) {
                printLog(
                  "warn",
                  `Failed to get font from "${url}": ${error}. Serving fallback font "Open Sans"...`
                );

                const parts = result[0].split("/");

                data = await getFallbackFont(parts[0], parts[1]);

                /* Unzip data */
                const headers = detectFormatAndHeaders(data).headers;

                if (headers["content-encoding"]) {
                  data = await unzipAsync(data);
                }
              } else {
                printLog("error", `Failed to detect font from "${url}"`);

                err = error;
              }
            } else {
              printLog("error", `Failed to get data from "${url}": ${error}`);

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
 * @param {{ pool: object, styleJSON: object, pitch: number, bearing: number, tileScale: number, tileSize: 256|512, z: number, x: number, y: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean, filePath: string }} option Option object
 * @returns {Promise<Buffer|string|void>}
 */
export async function renderImageTileData(option) {
  const renderer = option.pool
    ? await option.pool.acquire()
    : createRenderer({
        mode: "tile",
        ratio: option.tileScale,
        styleJSON: option.styleJSON,
      });

  return await new Promise((resolve, reject) => {
    const isNeedHack = option.z === 0 && option.tileSize === 256;
    const hackTileSize = isNeedHack ? option.tileSize * 2 : option.tileSize;

    renderer.render(
      {
        zoom: option.z > 0 && option.tileSize === 256 ? option.z - 1 : option.z,
        center: getLonLatFromXYZ(option.x, option.y, option.z, "center", "xyz"),
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

        createImageOutput(data, {
          rawOption: {
            premultiplied: true,
            width: originTileSize,
            height: originTileSize,
            channels: 4,
          },
          format: option.format,
          base64: option.base64,
          grayscale: option.grayscale,
          filePath: option.filePath,
          width: targetTileSize,
          height: targetTileSize,
        })
          .then(resolve)
          .catch(reject);
      }
    );
  });
}

/**
 * Render image static data
 * @param {{ pool: object, styleJSON: object, pitch: number, bearing: number, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean, width: number, height: number, filePath: string }} option Option object
 * @returns {Promise<Buffer|string|void>}
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

        createImageOutput(data, {
          rawOption: {
            premultiplied: true,
            width: Math.round(option.tileScale * sizes.width),
            height: Math.round(option.tileScale * sizes.height),
            channels: 4,
          },
          format: option.format,
          base64: option.base64,
          grayscale: option.grayscale,
          width: option.width,
          height: option.height,
          filePath: option.filePath,
        })
          .then(resolve)
          .catch(reject);
      }
    );
  });
}

/**
 * Render StyleJSON
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean, width: number, height: number, filePath: string }} option Option object
 * @returns {Promise<Buffer|string|void>}
 */
async function renderStyleJSON(option) {
  const MAX_TILE_PX = 8192;

  const sizes = calculateSizes(option.zoom, option.bbox, option.tileSize);
  const totalWidth = Math.round(option.tileScale * sizes.width);
  const totalHeight = Math.round(option.tileScale * sizes.height);

  if (totalWidth <= MAX_TILE_PX && totalHeight <= MAX_TILE_PX) {
    return await renderImageStaticData(option);
  } else {
    const [minX, minY] = lonLat4326ToXY3857(option.bbox[0], option.bbox[1]);
    const [maxX, maxY] = lonLat4326ToXY3857(option.bbox[2], option.bbox[3]);

    const xSplits = Math.ceil(totalWidth / MAX_TILE_PX);
    const ySplits = Math.ceil(totalHeight / MAX_TILE_PX);

    const xStep = (maxX - minX) / xSplits;
    const yStep = (maxY - minY) / ySplits;

    const compositesOption = await Promise.all(
      Array.from({ length: xSplits * ySplits }, async (_, i) => {
        const xi = Math.floor(i / ySplits);
        const yi = i % ySplits;

        const subMinX = minX + xi * xStep;
        const subMinY = minY + yi * yStep;
        const subMaxX = subMinX + xStep;
        const subMaxY = subMinY + yStep;

        const subBBox = [
          ...xy3857ToLonLat4326(subMinX, subMinY),
          ...xy3857ToLonLat4326(subMaxX, subMaxY),
        ];

        const subSizes = calculateSizes(option.zoom, subBBox, option.tileSize);

        return {
          limitInputPixels: false,
          input: await renderImageStaticData({
            styleJSON: option.styleJSON,
            tileScale: option.tileScale,
            tileSize: option.tileSize,
            format: option.format,
            pitch: option.pitch,
            bearing: option.bearing,
            bbox: subBBox,
            zoom: option.zoom,
          }),
          left: xi * Math.round(option.tileScale * subSizes.width),
          top:
            totalHeight -
            (yi + 1) * Math.round(option.tileScale * subSizes.height),
        };
      })
    );

    return await createImageOutput(undefined, {
      createOption: {
        width: totalWidth,
        height: totalHeight,
        channels: 4,
        background: { r: 255, g: 255, b: 255, alpha: 0 },
      },
      compositesOption: compositesOption,
      format: option.format,
      width: option.width,
      height: option.height,
      base64: option.base64,
      grayscale: option.grayscale,
    });
  }
}

/**
 * Render StyleJSONs
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean }[]} overlays StyleJSON overlays
 * @returns {Promise<Buffer[]|string[]>} Response
 */
export async function renderStyleJSONs(overlays) {
  const targetOverlays = Array(overlays.length);

  async function renderImageData(idx) {
    targetOverlays[idx] = await renderStyleJSON(overlays[idx]);
  }

  // Batch run
  await handleConcurrency(os.cpus().length, renderImageData, overlays);

  return targetOverlays;
}

/**
 * Render SVG
 * @param {{ image: string, width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean, filePath: string }} option Option object
 * @returns {Promise<Buffer|string|void>}
 */
async function renderSVG(option) {
  return await createImageOutput(base64ToBuffer(option.image), {
    format: option.format,
    width: option.width,
    height: option.height,
    base64: option.base64,
    grayscale: option.grayscale,
    filePath: option.filePath,
  });
}

/**
 * Render SVGs
 * @param {{ image: string, width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean }[]} overlays SVG overlays
 * @returns {Promise<Buffer[]|string[]>}
 */
export async function renderSVGs(overlays) {
  const targetOverlays = Array(overlays.length);

  async function renderImageData(idx) {
    targetOverlays[idx] = await renderSVG(overlays[idx]);
  }

  // Batch run
  await handleConcurrency(os.cpus().length, renderImageData, overlays);

  return targetOverlays;
}

/**
 * Add frame to Image
 * @param {{ filePath: string|Buffer, bbox: [number, number, number, number] }} input Input object
 * @param {{ image: Buffer, bbox: [number, number, number, number] }[]} overlays Array of overlay object
 * @param {{ frameMargin: number, frameInnerColor: string, frameInnerWidth: number, frameInnerStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", frameOuterColor: string, frameOuterWidth: number, frameOuterStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", frameSpace: number, tickLabelFormat: "D"|"DMS"|"DMSD", majorTickStep: number, minorTickStep: number, majorTickWidth: number, minorTickWidth: number, majorTickSize: number, minorTickSize: number, majorTickLabelSize: number, minorTickLabelSize: number, majorTickColor: string, minorTickColor: string, majorTickLabelColor: string, minorTickLabelColor: string, majorTickLabelFont: string, minorTickLabelFont: string, xTickLabelOffset: number, yTickLabelOffset: number, xTickEnd: boolean, xTickMajorLabelRotation: number, xTickMinorLabelRotation: number, yTickMajorLabelRotation: number, yTickEnd: boolean, yTickMinorLabelRotation: number }} frame Frame object
 * @param {{ majorGridStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", majorGridWidth: number, majorGridStep: number, majorGridColor: string, minorGridStyle: "solid"|"dashed"|"longDashed"|"dotted"|"dashedDot", minorGridWidth: number, minorGridStep: number, minorGridColor: string }} grid Grid object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", width: number, height: number, base64: boolean, grayscale: boolean }} output Output object
 * @returns {Promise<Buffer|string>} Response
 */
export async function addFrame(input, overlays, frame, grid, output) {
  return await addFrameToImage(
    {
      image: base64ToBuffer(input.image),
      bbox: input.bbox,
    },
    overlays,
    frame,
    grid,
    output
  );
}

/**
 * Render high quality PDF
 * @param {object} input Input object
 * @param {object} preview Preview object
 * @param {object} output Output object
 * @returns {Promise<Buffer[]|string[]>}
 */
export async function renderHighQualityPDF(input, preview, output) {
  return await renderImageToHighQualityPDF(
    {
      images: input.images.map((item) => ({
        image: base64ToBuffer(item.image),
        res: item.resolution,
      })),
    },
    preview,
    output
  );
}

/**
 * Render PDF
 * @param {object} input Input object
 * @param {object} preview Preview object
 * @param {object} output Output object
 * @returns {Promise<Buffer|string[]>}
 */
export async function renderPDF(input, preview, output) {
  return await renderImageToPDF(
    {
      images: input.images.map(base64ToBuffer),
    },
    preview,
    output
  );
}

/**
 * Render MBTiles tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderMBTilesTiles(
  id,
  filePath,
  metadata,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
  tileScale,
  tileSize,
  refreshBefore
) {
  const startTime = Date.now();

  let source;
  let styleJSON;
  let pool;

  try {
    /* Calculate summary */
    const { targetCoverages, realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });

    let log = `Rendering ${total} tiles of style "${id}" to mbtiles with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
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

    /* Open MBTiles SQLite database */
    printLog("info", "Creating MBTiles...");

    source = await openMBTilesDB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getMBTilesTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestamp === true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update MBTiles metadata */
    printLog("info", "Updating MBTiles metadata...");

    await updateMBTilesMetadata(
      source,
      {
        ...metadata,
        bounds: realBBox,
      },
      30000 // 30 secs
    );

    /* Create renderer pool */
    const item = config.styles[id];
    styleJSON = await getRenderedStyleJSON(item.path);

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
        }
      );
    }

    async function renderTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

          if (tileExtraInfo[tileName] === calculateMD5(data)) {
            return;
          }

          // Store data
          await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
        } else {
          if (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp) {
            return;
          }

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

          // Store data
          await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    // Render tiles with concurrency
    printLog("info", "Rendering tiles to MBTiles...");

    await handleTilesConcurrency(concurrency, renderTileData, tileBounds, item);

    /* Create overviews */
    if (createOverview) {
      printLog("info", `Creating overviews...`);

      await addMBTilesOverviews(source, concurrency, tileSize);

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
    /* Destroy renderer pool */
    if (pool) {
      pool.drain().then(() => pool.clear());
    }

    // Close MBTiles SQLite database
    if (source) {
      closeMBTilesDB(source);
    }
  }
}

/**
 * Render XYZ tiles
 * @param {string} id Style ID
 * @param {string} sourcePath Exported source path
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderXYZTiles(
  id,
  sourcePath,
  filePath,
  metadata,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
  tileScale,
  tileSize,
  refreshBefore
) {
  const startTime = Date.now();

  let source;
  let styleJSON;
  let pool;

  try {
    /* Calculate summary */
    const { targetCoverages, realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tFile path: ${filePath} - Source path: ${sourcePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
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

    /* Open MD5 SQLite database */
    printLog("info", "Creating MBTiles...");

    const source = await openXYZMD5DB(
      filePath,
      true,
      30000 // 30 secs
    );

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = getXYZTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestamp === true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update XYZ metadata */
    printLog("info", "Updating XYZ metadata...");

    await updateXYZMetadata(
      source,
      {
        ...metadata,
        bounds: realBBox,
      },
      30000 // 30 secs
    );

    /* Create renderer pool */
    const item = config.styles[id];
    styleJSON = await getRenderedStyleJSON(item.path);

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
        }
      );
    }

    async function renderTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

          if (tileExtraInfo[tileName] === calculateMD5(data)) {
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
        } else {
          if (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp) {
            return;
          }

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

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
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    // Render tiles with concurrency
    printLog("info", "Rendering tiles to XYZ...");

    await handleTilesConcurrency(concurrency, renderTileData, tileBounds, item);

    /* Remove parent folders if empty */
    await removeEmptyFolders(sourcePath, /^.*\.(gif|png|jpg|jpeg|webp)$/);

    /* Create overviews */
    if (createOverview) {
      printLog("info", `Creating overviews...`);

      await addXYZOverviews(sourcePath, source, concurrency, tileSize);

      printLog("info", "Calculating tile extra info...");

      await calculateXYZTileExtraInfo(sourcePath, source);
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
    /* Destroy renderer pool */
    if (pool) {
      pool.drain().then(() => pool.clear());
    }

    /* Close MD5 SQLite database */
    if (source) {
      closeXYZMD5DB(source);
    }
  }
}

/**
 * Render PostgreSQL tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderPostgreSQLTiles(
  id,
  filePath,
  metadata,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  createOverview,
  tileScale,
  tileSize,
  refreshBefore
) {
  const startTime = Date.now();

  let source;
  let styleJSON;
  let pool;

  try {
    /* Calculate summary */
    const { targetCoverages, realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });

    let log = `Rendering ${total} tiles of style "${id}" to postgresql with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
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

    /* Open PostgreSQL database */
    printLog("info", "Creating PostgreSQL...");

    source = await openPostgreSQLDB(filePath, true);

    /* Get tile extra info */
    let tileExtraInfo;

    if (refreshTimestamp) {
      try {
        printLog("info", `Get tile extra info from "${filePath}"...`);

        tileExtraInfo = await getPostgreSQLTileExtraInfoFromCoverages(
          source,
          targetCoverages,
          refreshTimestamp === true
        );
      } catch (error) {
        printLog(
          "error",
          `Failed to get tile extra info from "${filePath}": ${error}`
        );

        tileExtraInfo = {};
      }
    }

    /* Update PostgreSQL metadata */
    printLog("info", "Updating PostgreSQL metadata...");

    await updatePostgreSQLMetadata(source, {
      ...metadata,
      bounds: realBBox,
    });

    /* Create renderer pool */
    const item = config.styles[id];
    styleJSON = await getRenderedStyleJSON(item.path);

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
        }
      );
    }

    async function renderTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

          if (tileExtraInfo[tileName] === calculateMD5(data)) {
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
        } else {
          if (refreshTimestamp && tileExtraInfo[tileName] >= refreshTimestamp) {
            return;
          }

          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData({
            pool: pool,
            styleJSON: styleJSON,
            tileScale: tileScale,
            tileSize: tileSize,
            z: z,
            x: x,
            y: y,
            format: metadata.format,
          });

          // Store data
          await cachePostgreSQLTileData(
            source,
            z,
            x,
            y,
            data,
            storeTransparent
          );
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );
      }
    }

    // Render tiles with concurrency
    printLog("info", "Rendering tiles to PostgreSQL...");

    await handleTilesConcurrency(concurrency, renderTileData, tileBounds, item);

    /* Create overviews */
    if (createOverview) {
      printLog("info", `Creating overviews...`);

      await addPostgreSQLOverviews(source, concurrency, tileSize);

      printLog("info", "Calculating tile extra info...");

      await calculatePostgreSQLTileExtraInfo(source);
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
    /* Destroy renderer pool */
    if (pool) {
      pool.drain().then(() => pool.clear());
    }

    /* Close PostgreSQL database */
    if (source) {
      closePostgreSQLDB(source);
    }
  }
}
