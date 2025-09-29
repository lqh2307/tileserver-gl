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
  handleConcurrency,
  createImageOutput,
  getLonLatFromXYZ,
  renderImageToPDF,
  addFrameToImage,
  getDataFromURL,
  calculateSizes,
  getTileBounds,
  calculateMD5,
  unzipAsync,
  printLog,
} from "./utils/index.js";

/**
 * Create render
 * @param {"tile"|"static"} mode Render mode
 * @param {number} scale Scale
 * @param {object} styleJSON StyleJSON
 * @returns {object}
 */
function createRenderer(mode, scale, styleJSON) {
  const renderer = new mlgl.Map({
    mode: mode,
    ratio: scale,
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
            const dataBase64 = Buffer.from(
              req.url.slice(req.url.indexOf(",") + 1),
              "base64"
            );

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

      callback(err, {
        data: data,
      });
    },
  });

  // Load style
  renderer.load(styleJSON);

  return renderer;
}

/**
 * Render image tile data
 * @param {object} styleJSONOrPool StyleJSON or Pool
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @param {boolean} usePool Use pool?
 * @param {string} filePath File path
 * @returns {Promise<Buffer>}
 */
export async function renderImageTileData(
  styleJSONOrPool,
  tileScale,
  tileSize,
  z,
  x,
  y,
  format,
  usePool,
  filePath
) {
  const renderer = usePool
    ? await styleJSONOrPool.acquire()
    : createRenderer("tile", tileScale, styleJSONOrPool);

  return await new Promise((resolve, reject) => {
    const isNeedHack = z === 0 && tileSize === 256;
    const hackTileSize = isNeedHack ? tileSize * 2 : tileSize;

    renderer.render(
      {
        zoom: z > 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
      },
      (error, data) => {
        usePool ? styleJSONOrPool.release(renderer) : renderer.release();

        if (error) {
          return reject(error);
        }

        const originTileSize = Math.floor(hackTileSize * tileScale);
        const targetTileSize = isNeedHack
          ? Math.floor(originTileSize / 2)
          : undefined;

        createImageOutput(data, {
          rawOption: {
            premultiplied: true,
            width: originTileSize,
            height: originTileSize,
            channels: 4,
          },
          format: format,
          width: targetTileSize,
          height: targetTileSize,
          filePath: filePath,
        })
          .then(resolve)
          .catch(reject);
      }
    );
  });
}

/**
 * Render image static data
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number] }} input Input object
 * @param {{ format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean, width: number, height: number }} output Output object
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageStaticData(input, output) {
  const renderer = createRenderer("static", input.tileScale, input.styleJSON);

  return await new Promise((resolve, reject) => {
    const sizes = calculateSizes(
      input.zoom,
      input.bbox,
      input.tileScale,
      input.tileSize
    );

    renderer.render(
      {
        zoom: input.zoom,
        center: [
          (input.bbox[0] + input.bbox[2]) / 2,
          (input.bbox[1] + input.bbox[3]) / 2,
        ],
        width: sizes.width,
        height: sizes.height,
      },
      (error, data) => {
        renderer.release();

        if (error) {
          return reject(error);
        }

        createImageOutput(data, {
          rawOption: {
            premultiplied: true,
            width: sizes.width,
            height: sizes.height,
            channels: 4,
          },
          ...output,
        })
          .then(resolve)
          .catch(reject);
      }
    );
  });
}

/**
 * Render image static data
 * @param {{ styleJSON: object, tileScale: number, tileSize: 256|512, zoom: number, bbox: [number, number, number, number], width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean }[]} overlays Input object
 * @returns {Promise<Buffer[]|string[]>} Response
 */
export async function renderStyleJSON(overlays) {
  const targetOverlays = Array(overlays.length);

  async function renderStyleJSONToImageData(idx, overlays) {
    const overlay = overlays[idx];

    // Create image
    targetOverlays[idx] = await renderImageStaticData(
      {
        styleJSON: overlay.styleJSON,
        tileScale: overlay.tileScale || 1,
        tileSize: overlay.tileSize || 512,
        zoom: overlay.zoom,
        bbox: overlay.bbox,
      },
      {
        width: overlay.width,
        height: overlay.height,
        format: overlay.format,
        base64: overlay.base64,
        grayscale: overlay.grayscale,
      }
    );
  }

  // Batch run
  await handleConcurrency(
    os.cpus().length,
    renderStyleJSONToImageData,
    overlays
  );

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
      image: Buffer.from(
        input.image.slice(input.image.indexOf(",") + 1),
        "base64"
      ),
      bbox: input.bbox,
    },
    overlays,
    frame,
    grid,
    output
  );
}

/**
 * Render SVG to Image
 * @param {{ image: string, width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif", base64: boolean, grayscale: boolean }[]} overlays SVG overlays
 * @returns {Promise<Buffer[]|string[]>}
 */
export async function renderSVGToImage(overlays) {
  const targetOverlays = Array(overlays.length);

  async function renderSVGToImageData(idx, overlays) {
    const overlay = overlays[idx];

    // Create image
    targetOverlays[idx] = await createImageOutput(
      Buffer.from(
        overlay.image.slice(overlay.image.indexOf(",") + 1),
        "base64"
      ),
      {
        format: overlay.format,
        width: overlay.width,
        height: overlay.height,
        base64: overlay.base64,
        grayscale: overlay.grayscale,
      }
    );
  }

  // Batch run
  await handleConcurrency(os.cpus().length, renderSVGToImageData, overlays);

  return targetOverlays;
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
      images: input.images.map((item) => {
        return {
          image: Buffer.from(
            item.image.slice(item.image.indexOf(",") + 1),
            "base64"
          ),
          res: item.resolution,
        };
      }),
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
      images: input.images.map((item) =>
        Buffer.from(item.slice(item.indexOf(",") + 1), "base64")
      ),
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
  let styleJSONOrPool;

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
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
    log += `\n\tBBox: ${JSON.stringify(metadata.bounds)}`;
    log += `\n\tMinzoom: ${metadata.minzoom} - Maxzoom: ${metadata.maxzoom}`;

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
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);

    styleJSONOrPool =
      maxRendererPoolSize > 0
        ? createPool(
            {
              create: () =>
                createRenderer("tile", tileScale, renderedStyleJSON),
              destroy: (renderer) => renderer.release(),
            },
            {
              min: 1,
              max: maxRendererPoolSize,
            }
          )
        : renderedStyleJSON;

    async function renderMBTilesTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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

    await handleTilesConcurrency(
      concurrency,
      renderMBTilesTileData,
      tileBounds,
      item
    );

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
    if (maxRendererPoolSize > 0 && styleJSONOrPool) {
      styleJSONOrPool.drain().then(() => styleJSONOrPool.clear());
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
  let styleJSONOrPool;

  try {
    /* Calculate summary */
    const { targetCoverages, realBBox, total, tileBounds } = getTileBounds({
      bbox: metadata.bounds,
      minZoom: metadata.minzoom,
      maxZoom: metadata.maxzoom,
    });

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tSource path: ${sourcePath}`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
    log += `\n\tBBox: ${JSON.stringify(metadata.bounds)}`;
    log += `\n\tMinzoom: ${metadata.minzoom} - Maxzoom: ${metadata.maxzoom}`;

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
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);

    styleJSONOrPool =
      maxRendererPoolSize > 0
        ? createPool(
            {
              create: () =>
                createRenderer("tile", tileScale, renderedStyleJSON),
              destroy: (renderer) => renderer.release(),
            },
            {
              min: 1,
              max: maxRendererPoolSize,
            }
          )
        : renderedStyleJSON;

    async function renderXYZTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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

    await handleTilesConcurrency(
      concurrency,
      renderXYZTileData,
      tileBounds,
      item
    );

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
    if (maxRendererPoolSize > 0 && styleJSONOrPool) {
      styleJSONOrPool.drain().then(() => styleJSONOrPool.clear());
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
  let styleJSONOrPool;

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
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
    log += `\n\tBBox: ${JSON.stringify(metadata.bounds)}`;
    log += `\n\tMinzoom: ${metadata.minzoom} - Maxzoom: ${metadata.maxzoom}`;

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
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);

    styleJSONOrPool =
      maxRendererPoolSize > 0
        ? createPool(
            {
              create: () =>
                createRenderer("tile", tileScale, renderedStyleJSON),
              destroy: (renderer) => renderer.release(),
            },
            {
              min: 1,
              max: maxRendererPoolSize,
            }
          )
        : renderedStyleJSON;

    async function renderPostgreSQLTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      try {
        if (refreshTimestamp === true) {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${total}...`
          );

          // Rendered data
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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
          const data = await renderImageTileData(
            styleJSONOrPool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            metadata.format,
            maxRendererPoolSize > 0
          );

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

    await handleTilesConcurrency(
      concurrency,
      renderPostgreSQLTileData,
      tileBounds,
      item
    );

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
    if (maxRendererPoolSize > 0 && styleJSONOrPool) {
      styleJSONOrPool.drain().then(() => styleJSONOrPool.clear());
    }

    /* Close PostgreSQL database */
    if (source) {
      closePostgreSQLDB(source);
    }
  }
}
