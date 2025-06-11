"use strict";

import { getPMTilesTile } from "./tile_pmtiles.js";
import { getRenderedStyleJSON } from "./style.js";
import mlgl from "@maplibre/maplibre-gl-native";
import { createPool } from "generic-pool";
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
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  createCoveragesFromBBoxAndZooms,
  getTileBoundsFromCoverages,
  detectFormatAndHeaders,
  createFallbackTileData,
  removeFilesOrFolders,
  calculateResolution,
  createTileMetadata,
  removeEmptyFolders,
  convertSVGToImage,
  processImageData,
  getLonLatFromXYZ,
  addFrameToImage,
  getDataFromURL,
  calculateSizes,
  createFolders,
  calculateMD5,
  mergeImages,
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
              headers["content-encoding"] !== undefined
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
              headers["content-encoding"] !== undefined
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
              headers["content-encoding"] !== undefined
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
 * @param {object} styleJSON StyleJSON
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageTileData(
  styleJSON,
  tileScale,
  tileSize,
  z,
  x,
  y,
  format
) {
  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

  const renderer = createRenderer("tile", tileScale, styleJSON);

  const data = await new Promise((resolve, reject) => {
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

  const originTileSize = hackTileSize * tileScale;
  const targetTileSize =
    isNeedHack === true ? (hackTileSize / 2) * tileScale : undefined;

  return await processImageData(
    data,
    originTileSize,
    originTileSize,
    targetTileSize,
    targetTileSize,
    format
  );
}

/**
 * Render image data
 * @param {object} styleJSON StyleJSON
 * @param {number} tileScale Tile scale
 * @param {number} z Zoom level
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @returns {Promise<Buffer>}
 */
export async function renderImageData(styleJSON, tileScale, z, bbox, format) {
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

  return await processImageData(
    data,
    sizes.width,
    sizes.height,
    undefined,
    undefined,
    format
  );
}

/**
 * Render image tile data with pool
 * @param {object} pool Pool object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Tile format
 * @returns {Promise<Buffer>}
 */
export async function renderImageTileDataWithPool(
  pool,
  tileScale,
  tileSize,
  z,
  x,
  y,
  format
) {
  const isNeedHack = z === 0 && tileSize === 256;
  const hackTileSize = isNeedHack === false ? tileSize : tileSize * 2;

  const renderer = await pool.acquire();

  const data = await new Promise((resolve, reject) => {
    renderer.render(
      {
        zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: hackTileSize,
        height: hackTileSize,
      },
      (error, data) => {
        if (renderer !== undefined) {
          pool.release(renderer);
        }

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  const originTileSize = hackTileSize * tileScale;
  const targetTileSize =
    isNeedHack === true ? (hackTileSize / 2) * tileScale : undefined;

  return await processImageData(
    data,
    originTileSize,
    originTileSize,
    targetTileSize,
    targetTileSize,
    format
  );
}

/**
 * Render styleJSON to Image
 * @param {object} styleJSON Style JSON
 * @param {[number, number, number, number]} bbox Bounding box in EPSG:4326
 * @param {number} zoom Zoom level
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @param {string} id Image id
 * @param {string} dirPath Exported image dir path
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {object} frame Add frame options?
 * @param {object} grid Add grid options?
 * @param {{name: string, content: string, bbox: [number, number, number, number]}[]} overlays Overlays
 * @returns {Promise<{filePath: number, res: [number, number]}>} Response
 */
export async function renderStyleJSONToImage(
  styleJSON,
  bbox,
  zoom,
  format,
  id,
  dirPath,
  maxRendererPoolSize,
  concurrency,
  storeTransparent,
  tileScale,
  tileSize,
  frame,
  grid,
  overlays
) {
  const startTime = Date.now();

  let source;
  let pool;

  const outputFilePath = `${dirPath}/${id}.${format}`;
  const outputDirPath = `${dirPath}/output`;
  const mergedDirPath = `${dirPath}/merged`;
  const mbtilesDirPath = `${dirPath}/mbtiles`;
  const baselayerDirPath = `${dirPath}/baselayer`;

  const driver = format.toUpperCase();

  try {
    const targetZoom = Math.round(zoom);

    /* Calculate summary */
    const targetCoverages = createCoveragesFromBBoxAndZooms(
      bbox,
      targetZoom,
      targetZoom
    );
    const { realBBox, total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style JSON to ${driver} with:`;
    log += `\n\tDir path: ${dirPath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tZoom: ${zoom}`;
    log += `\n\tFormat: ${format}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tTile size: ${tileSize}`;
    log += `\n\tFrame: ${JSON.stringify(frame === undefined ? {} : frame)}`;
    log += `\n\tGrid: ${JSON.stringify(grid === undefined ? {} : grid)}`;
    log += `\n\tOverlays: ${overlays === undefined ? false : true}`;
    log += `\n\tTarget zoom: ${targetZoom}`;
    log += `\n\tTarget coverages: ${JSON.stringify(targetCoverages)}`;

    printLog("info", log);

    const mbtilesFilePath = `${mbtilesDirPath}/${id}.mbtiles`;

    /* Open MBTiles SQLite database */
    printLog("info", "Creating MBTiles...");

    source = await openMBTilesDB(
      mbtilesFilePath,
      true,
      30000 // 30 secs
    );

    /* Update metadata */
    printLog("info", "Updating metadata...");

    await updateMBTilesMetadata(
      source,
      createTileMetadata({
        name: id,
        format: format,
        bounds: realBBox,
        minzoom: targetZoom,
        maxzoom: targetZoom,
      }),
      30000 // 30 secs
    );

    /* Create tmp folders */
    await createFolders([
      outputDirPath,
      mergedDirPath,
      mbtilesDirPath,
      baselayerDirPath,
    ]);

    const targetOverlays = [];

    /* Process style JSON */
    for (const sourceStyleName of Object.keys(styleJSON.sources)) {
      const sourceStyle = styleJSON.sources[sourceStyleName];

      if (sourceStyle.url?.startsWith("data:") === true) {
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

    /* Process SVG layers */
    if (overlays !== undefined) {
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

    /* Render tiles */
    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    let renderMBTilesTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createRenderer("tile", tileScale, styleJSON),
          destroy: (renderer) => renderer.release(),
        },
        {
          min: 1,
          max: maxRendererPoolSize,
        }
      );

      renderMBTilesTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Rendering style JSON - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Rendered data
          const data = await renderImageTileDataWithPool(
            pool,
            tileScale,
            tileSize,
            z,
            x,
            y,
            format
          );

          // Store data
          await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
        } catch (error) {
          printLog(
            "error",
            `Failed to render style JSON - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      };
    } else {
      renderMBTilesTileData = async (z, x, y, tasks) => {
        const tileName = `${z}/${x}/${y}`;

        const completeTasks = tasks.completeTasks;

        printLog(
          "info",
          `Rendering style JSON - Tile "${tileName}" - ${completeTasks}/${total}...`
        );

        try {
          // Rendered data
          const data = await renderImageTileData(
            styleJSON,
            tileScale,
            tileSize,
            z,
            x,
            y,
            format
          );

          // Store data
          await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
        } catch (error) {
          printLog(
            "error",
            `Failed to render style JSON - Tile "${tileName}" - ${completeTasks}/${total}: ${error}`
          );
        }
      };
    }

    printLog("info", "Rendering tiles to MBTiles...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(25);
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
      await delay(25);
    }

    const baselayerFilePath = `${baselayerDirPath}/${id}.${format}`;
    const imageFilePath = `${outputDirPath}/${id}.${format}`;

    /* Create image */
    const command = `gdal_translate -if MBTiles -of ${driver} -r lanczos -outsize ${
      Math.pow(2, zoom - targetZoom) * 100
    }% ${Math.pow(2, zoom - targetZoom) * 100}% -a_srs EPSG:4326 -a_ullr ${
      realBBox[0]
    } ${realBBox[3]} ${realBBox[2]} ${
      realBBox[1]
    } ${mbtilesFilePath} ${baselayerFilePath}`;

    printLog("info", `Creating ${id} baselayer with gdal command: ${command}`);

    const commandOutput = await runCommand(command);

    printLog("info", `Gdal command output: ${commandOutput}`);

    if (targetOverlays.length > 0) {
      /* Merge overlays to image */
      const mergedFilePath = `${mergedDirPath}/${id}.${format}`;

      printLog("info", "Merging overlays to image...");

      await mergeImages(
        {
          content: baselayerFilePath,
          bbox: bbox,
        },
        targetOverlays,
        {
          filePath: mergedFilePath,
          format: format,
        }
      );

      /* Add SRID */
      const command = `gdal_translate -if ${driver} -of ${driver}${
        format === "png" ? " -co ZLEVEL=9" : ""
      } -a_srs EPSG:4326 -a_ullr ${bbox[0]} ${bbox[3]} ${bbox[2]} ${
        bbox[1]
      } ${mergedFilePath} ${imageFilePath}`;

      printLog("info", `Adding SRID for image with gdal command: ${command}`);

      const commandOutput = await runCommand(command);

      printLog("info", `Gdal command output: ${commandOutput}`);
    } else {
      /* Crop image */
      const command = `gdal_translate -if ${driver} -of ${driver}${
        format === "png" ? " -co ZLEVEL=9" : ""
      } -r lanczos -projwin_srs EPSG:4326 -projwin ${bbox[0]} ${bbox[3]} ${
        bbox[2]
      } ${bbox[1]} -a_srs EPSG:4326 -a_ullr ${bbox[0]} ${bbox[3]} ${bbox[2]} ${
        bbox[1]
      } ${baselayerFilePath} ${imageFilePath}`;

      printLog("info", `Creating image with gdal command: ${command}`);

      const commandOutput = await runCommand(command);

      printLog("info", `Gdal command output: ${commandOutput}`);
    }

    /* Add frame */
    printLog("info", "Adding frame to image...");

    await addFrameToImage(
      {
        filePath: imageFilePath,
        bbox: bbox,
        format: format,
      },
      frame,
      grid,
      {
        filePath: outputFilePath,
        format: format,
      }
    );

    printLog(
      "info",
      `Completed render ${total} tiles of style JSON to ${driver} after ${
        (Date.now() - startTime) / 1000
      }s!`
    );

    return {
      filePath: outputFilePath,
      res: await calculateResolution({
        filePath: imageFilePath,
        bbox: bbox,
      }),
    };
  } catch (error) {
    printLog(
      "error",
      `Failed to render style JSON to ${driver} after ${
        (Date.now() - startTime) / 1000
      }s: ${error}`
    );

    throw error;
  } finally {
    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0 && pool !== undefined) {
      pool.drain().then(() => pool.clear());
    }

    // Close MBTiles SQLite database
    if (source !== undefined) {
      closeMBTilesDB(source);
    }

    /* Remove tmp */
    removeFilesOrFolders([
      mbtilesDirPath,
      baselayerDirPath,
      mergedDirPath,
      outputDirPath,
    ]);
  }
}

/**
 * Render SVG to Image
 * @param {"jpeg"|"jpg"|"png"|"webp"|"gif"} format Image format
 * @param {string[]} overlays SVG overlays
 * @param {number} concurrency Concurrency to download
 * @returns {Promise<{name: string, content: Buffer}[]>}
 */
export async function renderSVGToImage(format, overlays, concurrency) {
  const total = overlays.length;
  const targetOverlays = Array(total);
  const driver = format.toUpperCase();

  let log = `Rendering ${total} SVGs to ${driver}s with:`;
  log += `\n\tConcurrency: ${concurrency}`;

  printLog("info", log);

  /* Render SVGs */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  async function renderImageData(idx, tasks) {
    const completeTasks = tasks.completeTasks;

    printLog(
      "info",
      `Rendering SVG - Name "${overlays[idx].name}" - ${completeTasks}/${total}...`
    );

    try {
      // Rendered data
      targetOverlays[idx] = {
        name: overlays[idx].name,
        content: await convertSVGToImage(Buffer.from(overlays[idx].content), {
          format: overlays[idx].format || format,
          width: overlays[idx].width,
          height: overlays[idx].height,
        }),
      };
    } catch (error) {
      printLog(
        "error",
        `Failed to render SVG - Name "${overlays[idx].name}" - ${completeTasks}/${total}: ${error}`
      );

      throw error;
    }
  }

  printLog("info", `Rendering SVGs to ${driver}s...`);

  for (let idx = 0; idx < total; idx++) {
    /* Wait slot for a task */
    while (tasks.activeTasks >= concurrency) {
      await delay(25);
    }

    await tasks.mutex.runExclusive(() => {
      tasks.activeTasks++;
      tasks.completeTasks++;
    });

    /* Run a task */
    renderImageData(idx, tasks).finally(() =>
      tasks.mutex.runExclusive(() => {
        tasks.activeTasks--;
      })
    );
  }

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(25);
  }

  return targetOverlays;
}

/**
 * Render Image to PDF
 * @param {object} input Input object
 * @param {object} preview Preview object
 * @param {object} output Output object
 * @returns {Promise<Buffer>}
 */
export async function renderImageToPDF(input, preview, output) {}

/**
 * Render MBTiles tiles
 * @param {string} id Style ID
 * @param {string} filePath Exported file path
 * @param {object} metadata Metadata object
 * @param {number} maxRendererPoolSize Max renderer pool size
 * @param {number} concurrency Concurrency to download
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
  let pool;

  try {
    /* Calculate summary */
    const targetCoverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to mbtiles with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tTile size: ${tileSize}`;
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
    printLog("info", "Creating MBTiles...");

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
    const item = config.styles[id];
    const renderedStyleJSON = await getRenderedStyleJSON(item.path);

    const tasks = {
      mutex: new Mutex(),
      activeTasks: 0,
      completeTasks: 0,
    };

    /* Create renderer pool */
    let renderMBTilesTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createRenderer("tile", tileScale, renderedStyleJSON),
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
            const data = await renderImageTileDataWithPool(
              pool,
              tileScale,
              tileSize,
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
            const data = await renderImageTileData(
              renderedStyleJSON,
              tileScale,
              tileSize,
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

    printLog("info", "Rendering tiles to MBTiles...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(25);
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
      await delay(25);
    }

    /* Create overviews */
    if (createOverview === true) {
      const command = `gdaladdo -r lanczos -oo ZLEVEL=9 ${filePath} 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304`;

      printLog("info", `Creating overviews with gdal command: ${command}`);

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
    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0 && pool !== undefined) {
      pool.drain().then(() => pool.clear());
    }

    // Close MBTiles SQLite database
    if (source !== undefined) {
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
 * @param {number} concurrency Concurrency to download
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
  let pool;

  try {
    /* Calculate summary */
    const targetCoverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to xyz with:`;
    log += `\n\tSource path: ${sourcePath}`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tTile size: ${tileSize}`;
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
    printLog("info", "Creating MBTiles...");

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
    let renderXYZTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createRenderer("tile", tileScale, renderedStyleJSON),
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
            const data = await renderImageTileDataWithPool(
              pool,
              tileScale,
              tileSize,
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
            const data = await renderImageTileData(
              renderedStyleJSON,
              tileScale,
              tileSize,
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

    printLog("info", "Rendering tiles to XYZ...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(25);
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
      await delay(25);
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
    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0 && pool !== undefined) {
      pool.drain().then(() => pool.clear());
    }

    /* Close MD5 SQLite database */
    if (source !== undefined) {
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
 * @param {number} concurrency Concurrency to download
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
  let pool;

  try {
    /* Calculate summary */
    const targetCoverages = createCoveragesFromBBoxAndZooms(
      metadata.bounds,
      metadata.minzoom,
      metadata.maxzoom
    );
    const { total, tileBounds } = getTileBoundsFromCoverages(
      targetCoverages,
      "xyz"
    );

    let log = `Rendering ${total} tiles of style "${id}" to postgresql with:`;
    log += `\n\tFile path: ${filePath}`;
    log += `\n\tStore transparent: ${storeTransparent}`;
    log += `\n\tMax renderer pool size: ${maxRendererPoolSize}`;
    log += `\n\tConcurrency: ${concurrency}`;
    log += `\n\tCreate overview: ${createOverview}`;
    log += `\n\tTile scale: ${tileScale}`;
    log += `\n\tTile size: ${tileSize}`;
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
    printLog("info", "Creating PostgreSQL...");

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
    let renderPostgreSQLTileData;

    if (maxRendererPoolSize > 0) {
      pool = createPool(
        {
          create: () => createRenderer("tile", tileScale, renderedStyleJSON),
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
            const data = await renderImageTileDataWithPool(
              pool,
              tileScale,
              tileSize,
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
            const data = await renderImageTileData(
              renderedStyleJSON,
              tileScale,
              tileSize,
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

    printLog("info", "Rendering tiles to PostgreSQL...");

    for (const { z, x, y } of tileBounds) {
      for (let xCount = x[0]; xCount <= x[1]; xCount++) {
        for (let yCount = y[0]; yCount <= y[1]; yCount++) {
          if (item.export === true) {
            return;
          }

          /* Wait slot for a task */
          while (tasks.activeTasks >= concurrency) {
            await delay(25);
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
      await delay(25);
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
    /* Destroy renderer pool */
    if (maxRendererPoolSize > 0 && pool !== undefined) {
      pool.drain().then(() => pool.clear());
    }

    /* Close PostgreSQL database */
    if (source !== undefined) {
      closePostgreSQLDB(source);
    }
  }
}
