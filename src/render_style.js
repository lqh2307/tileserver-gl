"use strict";

import { getPMTilesTile } from "./tile_pmtiles.js";
import { getRenderedStyleJSON } from "./style.js";
import mlgl from "@maplibre/maplibre-gl-native";
import { createPool } from "generic-pool";
import { emitWSMessage } from "./ws.js";
import { printLog } from "./logger.js";
import { rm } from "node:fs/promises";
import { config } from "./config.js";
import { v4 } from "uuid";
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
  getAndCacheDataFonts,
} from "./data.js";
import {
  getMBTilesTileExtraInfoFromCoverages,
  calculateMBTilesTileExtraInfo,
  updateMBTilesMetadata,
  cacheMBtilesTileData,
  addMBTilesOverviews,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  createCoveragesFromBBoxAndZooms,
  getTileBoundsFromCoverages,
  processImageStaticRawData,
  processImageTileRawData,
  detectFormatAndHeaders,
  createFallbackTileData,
  handleTilesConcurrency,
  calculateResolution,
  removeEmptyFolders,
  mergeTilesToImage,
  handleConcurrency,
  getLonLatFromXYZ,
  addFrameToImage,
  getDataFromURL,
  calculateSizes,
  convertImage,
  calculateMD5,
  createBase64,
  unzipAsync,
  runCommand,
  splitImage,
} from "./utils.js";
import {
  getXYZTileExtraInfoFromCoverages,
  updateXYZMetadata,
  cacheXYZTileFile,
  closeXYZMD5DB,
  openXYZMD5DB,
} from "./tile_xyz.js";

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

      switch (scheme) {
        /* Get sprite */
        case "sprites": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataSprite(parts[2], parts[3]);
          } catch (error) {
            printLog(
              "warn",
              `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty sprite...`
            );

            data = createFallbackTileData(
              parts[3].slice(parts[3].lastIndexOf(".") + 1)
            );
          }

          break;
        }

        /* Get font */
        case "fonts": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataFonts(parts[2], parts[3]);

            const headers = detectFormatAndHeaders(data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"]
            ) {
              data = await unzipAsync(data);
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty font...`
            );

            data = createFallbackTileData("pbf");
          }

          break;
        }

        /* Get geojson */
        case "geojson": {
          const parts = decodeURIComponent(req.url).split("/");

          try {
            data = await getAndCacheDataGeoJSON(parts[2], parts[3]);
          } catch (error) {
            printLog(
              "warn",
              `Failed to get GeoJSON group "${parts[2]}" - Layer "${parts[3]}": ${error}. Serving empty geojson...`
            );

            data = createFallbackTileData("geojson");
          }

          break;
        }

        /* Get pmtiles tile */
        case "pmtiles": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getPMTilesTile(item.source, z, x, y);

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"]
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
          }

          break;
        }

        /* Get mbtiles tile */
        case "mbtiles": {
          const parts = decodeURIComponent(req.url).split("/");

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
              dataTile.headers["content-encoding"]
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
          }

          break;
        }

        /* Get xyz tile */
        case "xyz": {
          const parts = decodeURIComponent(req.url).split("/");

          const z = Number(parts[3]);
          const x = Number(parts[4]);
          const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
          const tileName = `${z}/${x}/${y}`;
          const item = config.datas[parts[2]];

          try {
            const dataTile = await getAndCacheXYZDataTile(parts[2], z, x, y);

            if (
              dataTile.headers["content-type"] === "application/x-protobuf" &&
              dataTile.headers["content-encoding"]
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
          }

          break;
        }

        /* Get pg tile */
        case "pg": {
          const parts = decodeURIComponent(req.url).split("/");

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
              dataTile.headers["content-encoding"]
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
          }

          break;
        }

        /* Get data from remote */
        case "http":
        case "https": {
          try {
            printLog("info", `Getting data from "${req.url}"...`);

            const dataRemote = await getDataFromURL(
              req.url,
              30000, // 30 secs
              "arraybuffer"
            );

            /* Unzip pbf data */
            const headers = detectFormatAndHeaders(dataRemote.data).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"]
            ) {
              data = await unzipAsync(dataRemote.data);
            } else {
              data = dataRemote.data;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to get data from "${req.url}": ${error}. Serving empty data...`
            );

            data = createFallbackTileData(
              req.url.slice(req.url.lastIndexOf(".") + 1)
            );
          }

          break;
        }

        /* Get base64 data */
        case "data": {
          try {
            printLog("info", "Decoding base64 data...");

            const dataBase64 = Buffer.from(
              req.url.slice(req.url.indexOf(",") + 1),
              "base64"
            );

            /* Unzip pbf data */
            const headers = detectFormatAndHeaders(dataBase64).headers;

            if (
              headers["content-type"] === "application/x-protobuf" &&
              headers["content-encoding"]
            ) {
              data = await unzipAsync(dataBase64);
            } else {
              data = dataBase64;
            }
          } catch (error) {
            printLog(
              "warn",
              `Failed to decode base64 data: ${error}. Serving empty data...`
            );

            data = createFallbackTileData(
              req.url.slice(req.url.indexOf("/") + 1, req.url.indexOf(";"))
            );
          }

          break;
        }

        /* Default */
        default: {
          err = new Error(`Unknown scheme: "${scheme}"`);

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
  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack ? tileSize * 2 : tileSize;

  const renderer = usePool
    ? await styleJSONOrPool.acquire()
    : createRenderer("tile", tileScale, styleJSONOrPool);

  const data = await new Promise((resolve, reject) => {
    renderer.render(
      {
        zoom: z > 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
      },
      (error, data) => {
        if (renderer) {
          usePool ? styleJSONOrPool.release(renderer) : renderer.release();
        }

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  const originTileSize = Math.floor(hackTileSize * tileScale);
  const targetTileSize = isNeedHack
    ? Math.floor(originTileSize / 2)
    : undefined;

  return await processImageTileRawData(
    data,
    originTileSize,
    targetTileSize,
    format,
    filePath
  );
}

/**
 * Render image static data
 * @param {object} styleJSON StyleJSON
 * @param {number} tileScale Tile scale
 * @param {number} z Zoom level
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @param {string} filePath File path
 * @returns {Promise<Buffer|>}
 */
export async function renderImageStaticData(
  styleJSON,
  tileScale,
  z,
  bbox,
  format,
  filePath
) {
  const sizes = calculateSizes(z, bbox, 512);

  const renderer = createRenderer("static", tileScale, styleJSON);

  const data = await new Promise((resolve, reject) => {
    renderer.render(
      {
        zoom: z,
        center: [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2],
        width: sizes.width,
        height: sizes.height,
      },
      (error, data) => {
        if (renderer) {
          renderer.release();
        }

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  return await processImageStaticRawData(
    data,
    sizes.width,
    sizes.height,
    undefined,
    undefined,
    format,
    filePath
  );
}

/**
 * Render styleJSON to Image
 * @param {object|string} styleJSON Style JSON
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} zoom Zoom level
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {"tms"|"xyz"} scheme Tile scheme
 * @param {object} frame Add frame options?
 * @param {object} grid Add grid options?
 * @param {boolean} base64 Is base64?
 * @param {boolean} grayscale Is grayscale?
 * @param {{ clientID: string, requestID: string, interval: number, event: string }} ws WS object
 * @param {{name: string, content: string, bbox: [number, number, number, number]}[]} overlays Overlays
 * @returns {Promise<{image: Buffer|string, resolution: [number, number]}>} Response
 */
export async function renderStyleJSONToImage(
  styleJSON,
  bbox,
  zoom,
  format,
  maxRendererPoolSize,
  concurrency,
  tileScale,
  tileSize,
  scheme,
  frame,
  grid,
  base64,
  grayscale,
  ws,
  overlays
) {
  let styleJSONOrPool;

  const id = v4();
  const dirPath = `${process.env.DATA_DIR}/exports/style_renders/${format}s/${id}`;
  const xyzDirPath = `${dirPath}/xyz/${id}`;

  const driver = format.toUpperCase();

  try {
    const targetZoom = Math.round(zoom);
    const targetScale = tileScale * Math.pow(2, zoom - targetZoom);

    /* Calculate summary */
    const coverages = createCoveragesFromBBoxAndZooms(
      bbox,
      targetZoom,
      targetZoom
    );
    const { realBBox, total, tileBounds } = getTileBoundsFromCoverages(
      coverages,
      "xyz",
      tileSize
    );

    let log = `Rendering ${total} tiles of style JSON to ${driver} with:`;
    log += `\n\tTemporary dir path: ${dirPath}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tZoom: ${zoom} - Target zoom: ${targetZoom}`;
    log += `\n\tFormat: ${format}`;
    log += `\n\tTile scale: ${tileScale} - Target tile scale: ${targetScale} - Tile size: ${tileSize}`;
    log += `\n\tScheme: ${scheme}`;
    log += `\n\tFrame: ${JSON.stringify(frame)}`;
    log += `\n\tGrid: ${JSON.stringify(grid)}`;
    log += `\n\tIs base64: ${base64}`;
    log += `\n\tIs grayscale: ${grayscale}`;
    log += `\n\tWS: ${JSON.stringify(ws)}`;
    log += `\n\tOverlays: ${overlays ? true : false}`;
    log += `\n\tCoverages: ${JSON.stringify(coverages)}`;

    printLog("info", log);

    const targetOverlays = [];

    /* Process style JSON */
    if (typeof styleJSON === "object") {
      for (const sourceStyleName of Object.keys(styleJSON.sources)) {
        const sourceStyle = styleJSON.sources[sourceStyleName];

        if (sourceStyle.url?.startsWith("data:")) {
          delete styleJSON.sources[sourceStyleName];

          for (let i = styleJSON.layers.length - 1; i >= 0; i--) {
            const layerStyle = styleJSON.layers[i];

            if (layerStyle.source === sourceStyleName) {
              styleJSON.layers.splice(i, 1);
            }
          }

          const overlayBBox = [
            ...sourceStyle.coordinates[3],
            ...sourceStyle.coordinates[1],
          ];

          if (
            overlayBBox[2] <= bbox[0] ||
            overlayBBox[0] >= bbox[2] ||
            overlayBBox[3] <= bbox[1] ||
            overlayBBox[1] >= bbox[3]
          ) {
            printLog(
              "info",
              `Overlay ${sourceStyleName} is outside bbox. Skipping...`
            );

            continue;
          }

          targetOverlays.push({
            content: Buffer.from(
              sourceStyle.url.slice(sourceStyle.url.indexOf(",") + 1),
              "base64"
            ),
            bbox: overlayBBox,
          });
        }
      }
    } else {
      styleJSON = await getRenderedStyleJSON(config.styles[styleJSON].path);
    }

    /* Process SVG layers */
    if (overlays) {
      for (const overlay of overlays) {
        if (
          overlay.bbox[2] <= bbox[0] ||
          overlay.bbox[0] >= bbox[2] ||
          overlay.bbox[3] <= bbox[1] ||
          overlay.bbox[1] >= bbox[3]
        ) {
          printLog(
            "info",
            `SVG layer ${overlay.name} is outside bbox. Skipping...`
          );

          continue;
        }

        targetOverlays.push({
          content: Buffer.from(overlay.content),
          bbox: overlay.bbox,
          format: overlay.format,
        });
      }
    }

    /* Create renderer pool */
    styleJSONOrPool =
      maxRendererPoolSize > 0
        ? createPool(
            {
              create: () => createRenderer("tile", tileScale, styleJSON),
              destroy: (renderer) => renderer.release(),
            },
            {
              min: 1,
              max: maxRendererPoolSize,
            }
          )
        : styleJSON;

    /* Socket */
    const callbackAtTaskNum = Math.round(0.1 * total);
    function socketCallback(progress) {
      try {
        emitWSMessage(
          ws.event,
          JSON.stringify({
            requestID: ws.requestID,
            data: {
              progress: progress,
            },
          }),
          [ws.clientID]
        );
      } catch (error) {
        printLog(
          "info",
          `Failed to send WS message to client id ${ws.clientID}: ${error}`
        );
      }
    }

    async function renderTileData(z, x, y, tasks) {
      const tileName = `${z}/${x}/${y}`;

      const completeTasks = tasks.completeTasks;

      printLog(
        "info",
        `Rendering style JSON - Tile "${tileName}" - ${completeTasks}/${total}...`
      );

      try {
        await renderImageTileData(
          styleJSONOrPool,
          targetScale,
          tileSize,
          z,
          x,
          y,
          format,
          maxRendererPoolSize > 0,
          `${xyzDirPath}/${z}/${x}/${y}.${format}`
        );

        if (ws && completeTasks % callbackAtTaskNum === 0) {
          const progress = Math.round((completeTasks / total) * 80);

          socketCallback(progress);
        }
      } catch (error) {
        printLog(
          "error",
          `Failed to render style JSON - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
        );

        throw error;
      }
    }

    printLog("info", "Rendering tiles to XYZ...");

    await handleTilesConcurrency(concurrency, renderTileData, tileBounds);

    /* Create image */
    printLog("info", "Merge tiles to image...");

    const mergedImage = await mergeTilesToImage(
      {
        dirPath: xyzDirPath,
        format: format,
        tileSize: tileSize,
        scheme: scheme,
        bbox: realBBox,
        z: targetZoom,
        xMin: tileBounds[0].x[0],
        xMax: tileBounds[0].x[1],
        yMin: tileBounds[0].y[0],
        yMax: tileBounds[0].y[1],
      },
      {
        bbox: bbox,
        format: format,
        grayscale: grayscale,
      }
    );

    if (ws) {
      socketCallback(90);
    }

    /* Add frame */
    printLog("info", "Adding frame to image...");

    const image = await addFrameToImage(
      {
        filePath: mergedImage,
        bbox: bbox,
        format: format,
      },
      targetOverlays,
      frame,
      grid,
      {
        format: format,
      }
    );

    /* Calculate resolution */
    const resolution = await calculateResolution(
      {
        filePath: image,
        bbox: bbox,
      },
      "mm"
    );

    if (ws) {
      socketCallback(100);
    }

    /* Response */
    return {
      image: base64 ? createBase64(image, format) : image,
      resolution: resolution,
    };
  } catch (error) {
    throw error;
  } finally {
    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0 && styleJSONOrPool) {
      styleJSONOrPool.drain().then(() => styleJSONOrPool.clear());
    }

    /* Remove tmp */
    await rm(dirPath, {
      recursive: true,
      force: true,
    });
  }
}

/**
 * Render SVG to Image
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @param {{ content: string, width: number, height: number, format: "jpeg"|"jpg"|"png"|"webp"|"gif" }[]} overlays SVG overlays
 * @param {number} concurrency Concurrency
 * @param {boolean} base64 Is base64?
 * @returns {Promise<string[]>}
 */
export async function renderSVGToImage(format, overlays, concurrency, base64) {
  const targetOverlays = Array(overlays.length);

  async function renderSVGToImageData(idx, overlays) {
    targetOverlays[idx] = await convertImage(
      Buffer.from(overlays[idx].content),
      {
        format: overlays[idx].format || format,
        width: overlays[idx].width,
        height: overlays[idx].height,
        base64: base64,
      }
    );
  }

  await handleConcurrency(concurrency, renderSVGToImageData, overlays);

  return targetOverlays;
}

/**
 * Render Image to PDF
 * @param {object} input Input object
 * @param {object} preview Preview object
 * @param {object} output Output object
 * @returns {Promise<Buffer|string>}
 */
export async function renderImageToPDF(input, preview, output) {
  if (input.ws) {
    emitWSMessage(
      input.ws.event,
      JSON.stringify({
        requestID: input.ws.requestID,
        data: {
          progress: 50,
        },
      }),
      [input.ws.clientID]
    );
  }

  const image = await splitImage(
    {
      image: Buffer.from(
        input.image.slice(input.image.indexOf(",") + 1),
        "base64"
      ),
      resolution: input.resolution,
    },
    preview,
    output
  );

  if (input.ws) {
    emitWSMessage(
      input.ws.event,
      JSON.stringify({
        requestID: input.ws.requestID,
        data: {
          progress: 100,
        },
      }),
      [input.ws.clientID]
    );
  }

  return image;
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
    const coverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { realBBox, total, tileBounds } = getTileBoundsFromCoverages(
      coverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to mbtiles with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
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

      log += `\n\tRefresh before: check MD5`;
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
          coverages,
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
        bbox: realBBox,
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

    printLog("info", "Rendering tiles to MBTiles...");

    await handleTilesConcurrency(
      concurrency,
      renderMBTilesTileData,
      tileBounds,
      item
    );

    /* Create overviews */
    if (createOverview) {
      const command = `gdaladdo -r lanczos -oo ZLEVEL=9 ${filePath} 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304`;

      printLog("info", `Creating overviews with gdal command: ${command}`);

      const commandOutput = await runCommand(command);

      printLog("info", `Gdal command output: ${commandOutput}`);

      // printLog("info", `Creating overviews...`);

      // await addMBTilesOverviews(source, 5, concurrency, tileSize);

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
    const coverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { realBBox, total, tileBounds } = getTileBoundsFromCoverages(
      coverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tSource path: ${sourcePath}`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
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

      log += `\n\tRefresh before: check MD5`;
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
          coverages,
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
        bbox: realBBox,
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
    const coverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { realBBox, total, tileBounds } = getTileBoundsFromCoverages(
      coverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to postgresql with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize} - Concurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale} - Tile size: ${tileSize}`;
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

      log += `\n\tRefresh before: check MD5`;
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
          coverages,
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

    await updatePostgreSQLMetadata(
      source,
      {
        ...metadata,
        bbox: realBBox,
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

    printLog("info", "Rendering tiles to PostgreSQL...");

    await handleTilesConcurrency(
      concurrency,
      renderPostgreSQLTileData,
      tileBounds,
      item
    );

    /* Create overviews */
    if (createOverview) {
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
