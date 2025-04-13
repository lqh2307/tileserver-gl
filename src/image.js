"use strict";

import { getPMTilesTile } from "./tile_pmtiles.js";
import { getSprite } from "./sprite.js";
import { printLog } from "./logger.js";
import { getFonts } from "./font.js";
import { config } from "./config.js";
import { Mutex } from "async-mutex";
import cluster from "cluster";
import sharp from "sharp";
import {
  updatePostgreSQLMetadata,
  getPostgreSQLTileFromURL,
  getPostgreSQLTileCreated,
  cachePostgreSQLTileData,
  getPostgreSQLTileMD5,
  getPostgreSQLTile,
  closePostgreSQLDB,
  openPostgreSQLDB,
} from "./tile_postgresql.js";
import {
  updateMBTilesMetadata,
  getMBTilesTileFromURL,
  getMBTilesTileCreated,
  cacheMBtilesTileData,
  getMBTilesTileMD5,
  getMBTilesTile,
  closeMBTilesDB,
  openMBTilesDB,
} from "./tile_mbtiles.js";
import {
  getTileBoundFromCoverage,
  detectFormatAndHeaders,
  removeEmptyFolders,
  getLonLatFromXYZ,
  getDataFromURL,
  calculateMD5,
  unzipAsync,
  runCommand,
  deepClone,
  delay,
} from "./utils.js";
import {
  updateXYZMetadata,
  getXYZTileCreated,
  getXYZTileFromURL,
  cacheXYZTileFile,
  getXYZTileMD5,
  closeXYZMD5DB,
  openXYZMD5DB,
  getXYZTile,
} from "./tile_xyz.js";

let mlgl;

if (cluster.isPrimary !== true) {
  import("@maplibre/maplibre-gl-native")
    .then((module) => {
      mlgl = module.default;

      printLog(
        "info",
        `Success to import "@maplibre/maplibre-gl-native". Enable backend render`
      );

      config.enableBackendRender = true;
    })
    .catch((error) => {
      printLog(
        "error",
        `Failed to import "@maplibre/maplibre-gl-native": ${error}. Disable backend render`
      );
    });
}

sharp.cache(false);

/**
 * Render tile callback
 * @param {Object} req
 * @param {Function} callback
 * @returns {Promise<void>}
 */
async function renderTileCallback(req, callback) {
  const url = decodeURIComponent(req.url);
  const parts = url.split("/");
  let tileFormat = "other";

  switch (parts[0]) {
    case "sprites:": {
      try {
        /* Get sprite */
        const data = await getSprite(parts[2], parts[3]);

        callback(null, {
          data: data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get sprite "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty sprite...`
        );

        callback(error, {
          data: null,
        });
      }

      break;
    }

    case "fonts:": {
      try {
        /* Get font */
        let data = await getFonts(parts[2], parts[3]);

        /* Unzip pbf font */
        const headers = detectFormatAndHeaders(data).headers;

        if (
          headers["content-type"] === "application/x-protobuf" &&
          headers["content-encoding"] !== undefined
        ) {
          data = await unzipAsync(data);
        }

        callback(null, {
          data: data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get font "${parts[2]}" - File "${parts[3]}": ${error}. Serving empty font...`
        );

        callback(error, {
          data: null,
        });
      }

      break;
    }

    case "pmtiles:": {
      const z = Number(parts[3]);
      const x = Number(parts[4]);
      const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
      const tileName = `${z}/${x}/${y}`;
      const item = config.datas[parts[2]];

      try {
        /* Get rendered tile */
        const dataTile = await getPMTilesTile(item.source, z, x, y);

        /* Unzip pbf rendered tile */
        if (
          dataTile.headers["content-type"] === "application/x-protobuf" &&
          dataTile.headers["content-encoding"] !== undefined
        ) {
          dataTile.data = await unzipAsync(dataTile.data);
        }

        callback(null, {
          data: dataTile.data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
        );

        tileFormat = item.tileJSON.format;
      }

      break;
    }

    case "mbtiles:": {
      const z = Number(parts[3]);
      const x = Number(parts[4]);
      const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
      const tileName = `${z}/${x}/${y}`;
      const item = config.datas[parts[2]];

      try {
        /* Get rendered tile */
        let dataTile;

        try {
          dataTile = getMBTilesTile(item.source, z, x, y);
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getMBTilesTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog(
                "info",
                `Caching data "${parts[2]}" - Tile "${tileName}"...`
              );

              cacheMBtilesTileData(
                item.source,
                z,
                x,
                tmpY,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
        }

        /* Unzip pbf rendered tile */
        if (
          dataTile.headers["content-type"] === "application/x-protobuf" &&
          dataTile.headers["content-encoding"] !== undefined
        ) {
          dataTile.data = await unzipAsync(dataTile.data);
        }

        callback(null, {
          data: dataTile.data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
        );

        tileFormat = item.tileJSON.format;
      }

      break;
    }

    case "xyz:": {
      const z = Number(parts[3]);
      const x = Number(parts[4]);
      const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
      const tileName = `${z}/${x}/${y}`;
      const item = config.datas[parts[2]];

      try {
        /* Get rendered tile */
        let dataTile;

        try {
          dataTile = await getXYZTile(
            item.source,
            z,
            x,
            y,
            item.tileJSON.format
          );
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getXYZTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog(
                "info",
                `Caching data "${parts[2]}" - Tile "${tileName}"...`
              );

              cacheXYZTileFile(
                item.source,
                item.md5Source,
                z,
                x,
                tmpY,
                item.tileJSON.format,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
        }

        /* Unzip pbf rendered tile */
        if (
          dataTile.headers["content-type"] === "application/x-protobuf" &&
          dataTile.headers["content-encoding"] !== undefined
        ) {
          dataTile.data = await unzipAsync(dataTile.data);
        }

        callback(null, {
          data: dataTile.data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
        );

        tileFormat = item.tileJSON.format;
      }

      break;
    }

    case "pg:": {
      const z = Number(parts[3]);
      const x = Number(parts[4]);
      const y = Number(parts[5].slice(0, parts[5].indexOf(".")));
      const tileName = `${z}/${x}/${y}`;
      const item = config.datas[parts[2]];

      try {
        /* Get rendered tile */
        let dataTile;

        try {
          dataTile = await getPostgreSQLTile(item.source, z, x, y);
        } catch (error) {
          if (
            item.sourceURL !== undefined &&
            error.message === "Tile does not exist"
          ) {
            const tmpY = item.scheme === "tms" ? (1 << z) - 1 - y : y;

            const targetURL = item.sourceURL
              .replace("{z}", `${z}`)
              .replace("{x}", `${x}`)
              .replace("{y}", `${tmpY}`);

            printLog(
              "info",
              `Forwarding data "${parts[2]}" - Tile "${tileName}" - To "${targetURL}"...`
            );

            /* Get data */
            dataTile = await getPostgreSQLTileFromURL(
              targetURL,
              60000 // 1 mins
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog(
                "info",
                `Caching data "${parts[2]}" - Tile "${tileName}"...`
              );

              cachePostgreSQLTileData(
                item.source,
                z,
                x,
                tmpY,
                dataTile.data,
                item.storeTransparent
              ).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache data "${parts[2]}" - Tile "${tileName}": ${error}`
                )
              );
            }
          } else {
            throw error;
          }
        }

        /* Unzip pbf rendered tile */
        if (
          dataTile.headers["content-type"] === "application/x-protobuf" &&
          dataTile.headers["content-encoding"] !== undefined
        ) {
          dataTile.data = await unzipAsync(dataTile.data);
        }

        callback(null, {
          data: dataTile.data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get data "${parts[2]}" - Tile "${tileName}": ${error}. Serving empty tile...`
        );

        tileFormat = item.tileJSON.format;
      }

      break;
    }

    case "http:":
    case "https:": {
      try {
        printLog("info", `Getting data tile from "${url}"...`);

        const dataTile = await getDataFromURL(
          url,
          60000, // 1 mins,
          "arraybuffer"
        );

        /* Unzip pbf data */
        const headers = detectFormatAndHeaders(dataTile.data).headers;

        if (
          headers["content-type"] === "application/x-protobuf" &&
          headers["content-encoding"] !== undefined
        ) {
          dataTile.data = await unzipAsync(dataTile.data);
        }

        callback(null, {
          data: dataTile.data,
        });
      } catch (error) {
        printLog(
          "warn",
          `Failed to get data tile from "${url}": ${error}. Serving empty tile...`
        );

        tileFormat = url.slice(url.lastIndexOf(".") + 1);
      }

      break;
    }

    default: {
      printLog("warn", `Unknown scheme: "${parts[0]}". Skipping...`);

      break;
    }
  }

  switch (tileFormat) {
    case "other": {
      break;
    }

    case "gif": {
      callback(null, {
        data: Buffer.from([
          0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80,
          0x00, 0x00, 0x4c, 0x69, 0x71, 0x00, 0x00, 0x00, 0x21, 0xff, 0x0b,
          0x4e, 0x45, 0x54, 0x53, 0x43, 0x41, 0x50, 0x45, 0x32, 0x2e, 0x30,
          0x03, 0x01, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x05, 0x00, 0x00,
          0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
          0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
        ]),
      });

      break;
    }

    case "png": {
      callback(null, {
        data: Buffer.from([
          0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00,
          0x0d, 0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
          0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89,
          0x00, 0x00, 0x00, 0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x03,
          0xe8, 0x00, 0x00, 0x03, 0xe8, 0x01, 0xb5, 0x7b, 0x52, 0x6b, 0x00,
          0x00, 0x00, 0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x60,
          0x60, 0x60, 0x60, 0x00, 0x00, 0x00, 0x05, 0x00, 0x01, 0xa5, 0xf6,
          0x45, 0x40, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
          0x42, 0x60, 0x82,
        ]),
      });

      break;
    }

    case "jpg": {
      callback(null, {
        data: Buffer.from([
          0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00, 0x06, 0x04, 0x05, 0x06,
          0x05, 0x04, 0x06, 0x06, 0x05, 0x06, 0x07, 0x07, 0x06, 0x08, 0x0a,
          0x10, 0x0a, 0x0a, 0x09, 0x09, 0x0a, 0x14, 0x0e, 0x0f, 0x0c, 0x10,
          0x17, 0x14, 0x18, 0x18, 0x17, 0x14, 0x16, 0x16, 0x1a, 0x1d, 0x25,
          0x1f, 0x1a, 0x1b, 0x23, 0x1c, 0x16, 0x16, 0x20, 0x2c, 0x20, 0x23,
          0x26, 0x27, 0x29, 0x2a, 0x29, 0x19, 0x1f, 0x2d, 0x30, 0x2d, 0x28,
          0x30, 0x25, 0x28, 0x29, 0x28, 0xff, 0xdb, 0x00, 0x43, 0x01, 0x07,
          0x07, 0x07, 0x0a, 0x08, 0x0a, 0x13, 0x0a, 0x0a, 0x13, 0x28, 0x1a,
          0x16, 0x1a, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0xff, 0xc0, 0x00,
          0x11, 0x08, 0x00, 0x01, 0x00, 0x01, 0x03, 0x01, 0x22, 0x00, 0x02,
          0x11, 0x01, 0x03, 0x11, 0x01, 0xff, 0xc4, 0x00, 0x15, 0x00, 0x01,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0xff, 0xc4, 0x00, 0x14, 0x10,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4, 0x00, 0x14, 0x01,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4, 0x00, 0x14, 0x11,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xda, 0x00, 0x0c, 0x03,
          0x01, 0x00, 0x02, 0x11, 0x03, 0x11, 0x00, 0x3f, 0x00, 0x95, 0x00,
          0x07, 0xff, 0xd9,
        ]),
      });

      break;
    }

    case "jpeg": {
      callback(null, {
        data: Buffer.from([
          0xff, 0xd8, 0xff, 0xdb, 0x00, 0x43, 0x00, 0x06, 0x04, 0x05, 0x06,
          0x05, 0x04, 0x06, 0x06, 0x05, 0x06, 0x07, 0x07, 0x06, 0x08, 0x0a,
          0x10, 0x0a, 0x0a, 0x09, 0x09, 0x0a, 0x14, 0x0e, 0x0f, 0x0c, 0x10,
          0x17, 0x14, 0x18, 0x18, 0x17, 0x14, 0x16, 0x16, 0x1a, 0x1d, 0x25,
          0x1f, 0x1a, 0x1b, 0x23, 0x1c, 0x16, 0x16, 0x20, 0x2c, 0x20, 0x23,
          0x26, 0x27, 0x29, 0x2a, 0x29, 0x19, 0x1f, 0x2d, 0x30, 0x2d, 0x28,
          0x30, 0x25, 0x28, 0x29, 0x28, 0xff, 0xdb, 0x00, 0x43, 0x01, 0x07,
          0x07, 0x07, 0x0a, 0x08, 0x0a, 0x13, 0x0a, 0x0a, 0x13, 0x28, 0x1a,
          0x16, 0x1a, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28,
          0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0x28, 0xff, 0xc0, 0x00,
          0x11, 0x08, 0x00, 0x01, 0x00, 0x01, 0x03, 0x01, 0x22, 0x00, 0x02,
          0x11, 0x01, 0x03, 0x11, 0x01, 0xff, 0xc4, 0x00, 0x15, 0x00, 0x01,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0xff, 0xc4, 0x00, 0x14, 0x10,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4, 0x00, 0x14, 0x01,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xc4, 0x00, 0x14, 0x11,
          0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xda, 0x00, 0x0c, 0x03,
          0x01, 0x00, 0x02, 0x11, 0x03, 0x11, 0x00, 0x3f, 0x00, 0x95, 0x00,
          0x07, 0xff, 0xd9,
        ]),
      });

      break;
    }

    case "webp": {
      callback(null, {
        data: Buffer.from([
          0x52, 0x49, 0x46, 0x46, 0x40, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42,
          0x50, 0x56, 0x50, 0x38, 0x58, 0x0a, 0x00, 0x00, 0x00, 0x10, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x4c, 0x50,
          0x48, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x56, 0x50, 0x38, 0x20,
          0x18, 0x00, 0x00, 0x00, 0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x01,
          0x00, 0x01, 0x00, 0x01, 0x40, 0x26, 0x25, 0xa4, 0x00, 0x03, 0x70,
          0x00, 0xfe, 0xfd, 0x36, 0x68, 0x00,
        ]),
      });

      break;
    }

    case "pbf": {
      callback(null, {
        data: Buffer.from([]),
      });

      break;
    }

    default: {
      printLog("warn", `Unknown tile format: "${tileFormat}". Skipping...`);

      callback(null, {
        data: Buffer.from([]),
      });

      break;
    }
  }
}

/**
 * Render image
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number} compressionLevel Compression level
 * @param {Object} styleJSON StyleJSON
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<Buffer>}
 */
export async function renderImage(
  tileScale,
  tileSize,
  compressionLevel,
  styleJSON,
  z,
  x,
  y
) {
  const renderer = new mlgl.Map({
    mode: "tile",
    ratio: tileScale,
    request: renderTileCallback,
  });

  renderer.load(styleJSON);

  const data = await new Promise((resolve, reject) => {
    renderer.render(
      {
        zoom: z !== 0 && tileSize === 256 ? z - 1 : z,
        center: getLonLatFromXYZ(x, y, z, "center", "xyz"),
        width: z === 0 && tileSize === 256 ? 512 : tileSize,
        height: z === 0 && tileSize === 256 ? 512 : tileSize,
      },
      (error, data) => {
        renderer.release();

        if (error) {
          return reject(error);
        }

        resolve(data);
      }
    );
  });

  if (z === 0 && tileSize === 256) {
    // HACK2: This hack allows tile-server to support zoom level 0 - 256px tiles, which would actually be zoom -1 in maplibre-gl-native
    return await sharp(data, {
      raw: {
        premultiplied: true,
        width: 512 * tileScale,
        height: 512 * tileScale,
        channels: 4,
      },
    })
      .resize({
        width: 256 * tileScale,
        height: 256 * tileScale,
      })
      .png({
        compressionLevel: compressionLevel,
      })
      .toBuffer();
    // END HACK2
  } else {
    return await sharp(data, {
      raw: {
        premultiplied: true,
        width: tileSize * tileScale,
        height: tileSize * tileScale,
        channels: 4,
      },
    })
      .png({
        compressionLevel: compressionLevel,
      })
      .toBuffer();
  }
}

/**
 * Render MBTiles tiles
 * @param {string} id Style ID
 * @param {Object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {[number, number, number, number]} bbox Bounding box in format [lonMin, latMin, lonMax, latMax] in EPSG:4326
 * @param {number} maxzoom Max zoom level
 * @param {number} concurrency Concurrency download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderMBTilesTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  bbox,
  maxzoom,
  concurrency,
  storeTransparent,
  createOverview,
  refreshBefore
) {
  const startTime = Date.now();

  /* Calculate summary */
  const tileBound = getTileBoundFromCoverage(
    {
      bbox: bbox,
      zoom: maxzoom,
    },
    "xyz"
  );

  let log = `Rendering ${
    tileBound.total
  } tiles of style "${id}" to mbtiles with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax zoom: ${maxzoom}\n\tBBox: ${JSON.stringify(
    bbox
  )}\n\tTile size: ${tileSize}\n\tTile scale: ${tileScale}\n\tCreate overview: ${createOverview}`;

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

  printLog("info", log);

  /* Open MBTiles SQLite database */
  const source = await openMBTilesDB(
    `${process.env.DATA_DIR}/exports/mbtiles/${id}/${id}.mbtiles`,
    true
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updateMBTilesMetadata(
    source,
    {
      ...metadata,
      maxzoom: maxzoom,
      minzoom: maxzoom,
    },
    300000 // 5 mins
  );

  /* Render tiles */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  const rendered = config.styles[id].rendered;

  async function renderMBTilesTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needRender = false;

      if (refreshTimestamp === true) {
        try {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
          );

          // Rendered data
          const data = await renderImage(
            tileScale,
            tileSize,
            rendered.compressionLevel,
            rendered.styleJSON,
            z,
            x,
            y
          );

          if (calculateMD5(data) !== getMBTilesTileMD5(source, z, x, y)) {
            // Store data
            await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
          }
        } catch (error) {
          if (error.message === "Tile MD5 does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = getMBTilesTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needRender = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else {
        needRender = true;
      }

      if (needRender === true) {
        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
        );

        // Rendered data
        const data = await renderImage(
          tileScale,
          tileSize,
          rendered.compressionLevel,
          rendered.styleJSON,
          z,
          x,
          y
        );

        // Store data
        await cacheMBtilesTileData(source, z, x, y, data, storeTransparent);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}: ${error}`
      );
    }
  }

  printLog("info", "Rendering datas...");

  const { z, x, y } = tileBound;
  for (let xCount = x[0]; xCount <= x[1]; xCount++) {
    for (let yCount = y[0]; yCount <= y[1]; yCount++) {
      if (rendered.export === true) {
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

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(50);
  }

  // Close MBTiles SQLite database
  await closeMBTilesDB(source);

  /* Create overviews */
  if (createOverview === true) {
    printLog("info", "Creating overviews...");

    const command = `gdaladdo -r lanczos -oo ZLEVEL=9 ${process.env.DATA_DIR}/exports/mbtiles/${id}/${id}.mbtiles`;

    printLog("info", `Gdal command: ${command}`);

    const commandOutput = await runCommand(command);

    printLog("info", `Gdal command output: ${commandOutput}`);
  }

  printLog(
    "info",
    `Completed render ${
      tileBound.total
    } tiles of style "${id}" to mbtiles after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Render XYZ tiles
 * @param {string} id Style ID
 * @param {Object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number[]} bbox Bounding box in format [lonMin, latMin, lonMax, latMax] in EPSG:4326
 * @param {number} maxzoom Max zoom level
 * @param {number} concurrency Concurrency to download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderXYZTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  bbox,
  maxzoom,
  concurrency,
  storeTransparent,
  createOverview,
  refreshBefore
) {
  const startTime = Date.now();

  const tileBound = getTileBoundFromCoverage(
    {
      bbox: bbox,
      zoom: maxzoom,
    },
    "xyz"
  );

  let log = `Rendering ${
    tileBound.total
  } tiles of style "${id}" to xyz with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax zoom: ${maxzoom}\n\tBBox: ${JSON.stringify(
    bbox
  )}\n\tTile size: ${tileSize}\n\tTile scale: ${tileScale}\n\tCreate overview: ${createOverview}`;

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

  printLog("info", log);

  /* Open MD5 SQLite database */
  const source = await openXYZMD5DB(
    `${process.env.DATA_DIR}/exports/xyzs/${id}/${id}.sqlite`,
    true
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updateXYZMetadata(
    source,
    {
      ...metadata,
      maxzoom: maxzoom,
      minzoom: maxzoom,
    },
    300000 // 5 mins
  );

  /* Render tile files */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  const rendered = config.styles[id].rendered;

  async function renderXYZTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needRender = false;

      if (refreshTimestamp === true) {
        try {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
          );

          // Rendered data
          const data = await renderImage(
            tileScale,
            tileSize,
            rendered.compressionLevel,
            rendered.styleJSON,
            z,
            x,
            y
          );

          if (calculateMD5(data) !== getXYZTileMD5(source, z, x, y)) {
            // Store data
            await cacheXYZTileFile(
              `${process.env.DATA_DIR}/exports/xyzs/${id}`,
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
          if (error.message === "Tile MD5 does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = getXYZTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needRender = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else {
        needRender = true;
      }

      if (needRender === true) {
        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
        );

        // Rendered data
        const data = await renderImage(
          tileScale,
          tileSize,
          rendered.compressionLevel,
          rendered.styleJSON,
          z,
          x,
          y
        );

        // Store data
        await cacheXYZTileFile(
          `${process.env.DATA_DIR}/exports/xyzs/${id}`,
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
        `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}: ${error}`
      );
    }
  }

  printLog("info", "Rendering datas...");

  const { z, x, y } = tileBound;
  for (let xCount = x[0]; xCount <= x[1]; xCount++) {
    for (let yCount = y[0]; yCount <= y[1]; yCount++) {
      if (rendered.export === true) {
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

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(50);
  }

  /* Close MD5 SQLite database */
  await closeXYZMD5DB(source);

  /* Remove parent folders if empty */
  await removeEmptyFolders(
    `${process.env.DATA_DIR}/caches/xyzs/${id}`,
    /^.*\.(sqlite|gif|png|jpg|jpeg|webp|pbf)$/
  );

  /* Create overviews */
  if (createOverview === true) {
  }

  printLog(
    "info",
    `Completed render ${tileBound.total} tiles of style "${id}" to xyz after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Render PostgreSQL tiles
 * @param {string} id Style ID
 * @param {Object} metadata Metadata object
 * @param {number} tileScale Tile scale
 * @param {256|512} tileSize Tile size
 * @param {number[]} bbox Bounding box in format [lonMin, latMin, lonMax, latMax] in EPSG:4326
 * @param {number} maxzoom Max zoom level
 * @param {number} concurrency Concurrency download
 * @param {boolean} storeTransparent Is store transparent tile?
 * @param {boolean} createOverview Is create overview?
 * @param {string|number|boolean} refreshBefore Date string in format "YYYY-MM-DDTHH:mm:ss"/Number of days before which files should be refreshed/Compare MD5
 * @returns {Promise<void>}
 */
export async function renderPostgreSQLTiles(
  id,
  metadata,
  tileScale,
  tileSize,
  bbox,
  maxzoom,
  concurrency,
  storeTransparent,
  createOverview,
  refreshBefore
) {
  const startTime = Date.now();

  const tileBound = getTileBoundFromCoverage(
    {
      bbox: bbox,
      zoom: maxzoom,
    },
    "xyz"
  );

  let log = `Rendering ${
    tileBound.total
  } tiles of style "${id}" to postgresql with:\n\tStore transparent: ${storeTransparent}\n\tConcurrency: ${concurrency}\n\tMax zoom: ${maxzoom}\n\tBBox: ${JSON.stringify(
    bbox
  )}\n\tTile size: ${tileSize}\n\tTile scale: ${tileScale}\n\tCreate overview: ${createOverview}`;

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

  printLog("info", log);

  /* Open PostgreSQL database */
  const source = await openPostgreSQLDB(
    `${process.env.POSTGRESQL_BASE_URI}/${id}`,
    true
  );

  /* Update metadata */
  printLog("info", "Updating metadata...");

  await updatePostgreSQLMetadata(
    source,
    {
      ...metadata,
      maxzoom: maxzoom,
      minzoom: maxzoom,
    },
    300000 // 5 mins
  );

  /* Render tiles */
  const tasks = {
    mutex: new Mutex(),
    activeTasks: 0,
    completeTasks: 0,
  };

  const rendered = config.styles[id].rendered;

  async function renderPostgreSQLTileData(z, x, y, tasks) {
    const tileName = `${z}/${x}/${y}`;

    const completeTasks = tasks.completeTasks;

    try {
      let needRender = false;

      if (refreshTimestamp === true) {
        try {
          printLog(
            "info",
            `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
          );

          // Rendered data
          const [data, md5] = await Promise.all([
            renderImage(
              tileScale,
              tileSize,
              rendered.compressionLevel,
              rendered.styleJSON,
              z,
              x,
              y
            ),
            getPostgreSQLTileMD5(source, z, x, y),
          ]);

          if (calculateMD5(data) !== md5) {
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
          if (error.message === "Tile MD5 does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else if (refreshTimestamp !== undefined) {
        try {
          const created = await getPostgreSQLTileCreated(source, z, x, y);

          if (!created || created < refreshTimestamp) {
            needRender = true;
          }
        } catch (error) {
          if (error.message === "Tile created does not exist") {
            needRender = true;
          } else {
            throw error;
          }
        }
      } else {
        needRender = true;
      }

      if (needRender === true) {
        printLog(
          "info",
          `Rendering style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}...`
        );

        // Rendered data
        const data = await renderImage(
          tileScale,
          tileSize,
          rendered.compressionLevel,
          rendered.styleJSON,
          z,
          x,
          y
        );

        // Store data
        await cachePostgreSQLTileData(source, z, x, y, data, storeTransparent);
      }
    } catch (error) {
      printLog(
        "error",
        `Failed to render style "${id}" - Tile "${tileName}" - ${completeTasks}/${tileBound.total}: ${error}`
      );
    }
  }

  printLog("info", "Rendering datas...");

  const { z, x, y } = tileBound;
  for (let xCount = x[0]; xCount <= x[1]; xCount++) {
    for (let yCount = y[0]; yCount <= y[1]; yCount++) {
      if (rendered.export === true) {
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

  /* Wait all tasks done */
  while (tasks.activeTasks > 0) {
    await delay(50);
  }

  /* Close PostgreSQL database */
  await closePostgreSQLDB(source);

  /* Create overviews */
  if (createOverview === true) {
  }

  printLog(
    "info",
    `Completed render ${
      tileBound.total
    } tiles of style "${id}" to postgresql after ${
      (Date.now() - startTime) / 1000
    }s!`
  );
}

/**
 * Create Rendered metadata
 * @param {Object} metadata Metadata object
 * @returns {Object}
 */
export function createRenderedMetadata(metadata) {
  const data = {};

  if (metadata.name !== undefined) {
    data.name = metadata.name;
  } else {
    data.name = "Unknown";
  }

  if (metadata.description !== undefined) {
    data.description = metadata.description;
  } else {
    data.description = "Unknown";
  }

  if (metadata.attribution !== undefined) {
    data.attribution = metadata.attribution;
  } else {
    data.attribution = "<b>Viettel HighTech</b>";
  }

  if (metadata.version !== undefined) {
    data.version = metadata.version;
  } else {
    data.version = "1.0.0";
  }

  if (metadata.type !== undefined) {
    data.type = metadata.type;
  } else {
    data.type = "overlay";
  }

  if (metadata.format !== undefined) {
    data.format = metadata.format;
  } else {
    data.format = "png";
  }

  if (metadata.minzoom !== undefined) {
    data.minzoom = metadata.minzoom;
  } else {
    data.minzoom = 0;
  }

  if (metadata.maxzoom !== undefined) {
    data.maxzoom = metadata.maxzoom;
  } else {
    data.maxzoom = 22;
  }

  if (metadata.bounds !== undefined) {
    data.bounds = deepClone(metadata.bounds);
  } else {
    data.bounds = [-180, -85.051129, 180, 85.051129];
  }

  if (metadata.center !== undefined) {
    data.center = [
      (data.bounds[0] + data.bounds[2]) / 2,
      (data.bounds[1] + data.bounds[3]) / 2,
      Math.floor((data.minzoom + data.maxzoom) / 2),
    ];
  }

  return data;
}
