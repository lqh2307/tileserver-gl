"use strict";

import { cacheMBtilesTileData, getMBTilesTile } from "./tile_mbtiles.js";
import { getDataTileFromURL, getDataFileFromURL } from "./utils.js";
import { cacheXYZTileFile, getXYZTile } from "./tile_xyz.js";
import { cacheGeoJSONFile, getGeoJSON } from "./geojson.js";
import { cacheSpriteFile, getSprite } from "./sprite.js";
import { cacheStyleFile, getStyle } from "./style.js";
import { printLog } from "./logger.js";
import { config } from "./config.js";
import {
  cachePostgreSQLTileData,
  getPostgreSQLTile,
} from "./tile_postgresql.js";
import {
  getFallbackFont,
  mergeFontDatas,
  cacheFontFile,
  getFont,
} from "./font.js";

/**
 * Get and cache MBTiles data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCacheMBTilesDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return getMBTilesTile(item.source, z, x, y);
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
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        30000 // 30 secs
      );

      /* Cache */
      if (item.storeCache === true) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

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
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
          )
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache XYZ data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCacheXYZDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return await getXYZTile(item.source, z, x, y, item.tileJSON.format);
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
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        30000 // 30 secs
      );

      /* Cache */
      if (item.storeCache === true) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

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
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
          )
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache PostgreSQL data tile
 * @param {string} id Data id
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getAndCachePostgreSQLDataTile(id, z, x, y) {
  const item = config.datas[id];
  const tileName = `${z}/${x}/${y}`;

  try {
    return await getPostgreSQLTile(item.source, z, x, y);
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
        `Forwarding data "${id}" - Tile "${tileName}" - To "${targetURL}"...`
      );

      /* Get data */
      const dataTile = await getDataTileFromURL(
        targetURL,
        30000 // 30 secs
      );

      /* Cache */
      if (item.storeCache === true) {
        printLog("info", `Caching data "${id}" - Tile "${tileName}"...`);

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
            `Failed to cache data "${id}" - Tile "${tileName}": ${error}`
          )
        );
      }

      return dataTile;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache data StyleJSON
 * @param {string} id StyleJSON id
 * @returns {Promise<object>}
 */
export async function getAndCacheDataStyleJSON(id) {
  const item = config.styles[id];

  try {
    return await getStyle(item.path);
  } catch (error) {
    if (
      item.sourceURL !== undefined &&
      error.message === "JSON does not exist"
    ) {
      printLog("info", `Forwarding style "${id}" - To "${item.sourceURL}"...`);

      const styleJSON = await getDataFileFromURL(
        item.sourceURL,
        30000 // 30 secs
      );

      if (item.storeCache === true) {
        printLog("info", `Caching style "${id}" - File "${item.path}"...`);

        cacheStyleFile(item.path, styleJSON).catch((error) =>
          printLog(
            "error",
            `Failed to cache style "${id}" - File "${item.path}": ${error}`
          )
        );
      }

      return styleJSON;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache data GeoJSON
 * @param {string} id GeoJSON group id
 * @param {string} layer GeoJSON group layer
 * @returns {Promise<object>}
 */
export async function getAndCacheDataGeoJSON(id, layer) {
  const geoJSONLayer = config.geojsons[id][layer];

  try {
    return await getGeoJSON(geoJSONLayer.path);
  } catch (error) {
    if (
      geoJSONLayer.sourceURL !== undefined &&
      error.message === "JSON does not exist"
    ) {
      printLog(
        "info",
        `Forwarding GeoJSON "${id}" - To "${geoJSONLayer.sourceURL}"...`
      );

      const geoJSON = await getDataFileFromURL(
        geoJSONLayer.sourceURL,
        30000 // 30 secs
      );

      if (geoJSONLayer.storeCache === true) {
        printLog(
          "info",
          `Caching GeoJSON "${id}" - File "${geoJSONLayer.path}"...`
        );

        cacheGeoJSONFile(geoJSONLayer.path, geoJSON).catch((error) =>
          printLog(
            "error",
            `Failed to cache GeoJSON "${id}" - File "${geoJSONLayer.path}": ${error}`
          )
        );
      }

      return geoJSON;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache data Sprite
 * @param {string} id Sprite id
 * @param {string} fileName Sprite file name
 * @returns {Promise<object>}
 */
export async function getAndCacheDataSprite(id, fileName) {
  const item = config.sprites[id];

  try {
    return await getSprite(item.path, fileName);
  } catch (error) {
    if (
      item.sourceURL !== undefined &&
      error.message === "Sprite does not exist"
    ) {
      const targetURL = item.sourceURL.replace("/sprite", `/${fileName}`);

      printLog(
        "info",
        `Forwarding sprite "${id}" - Filename "${fileName}" - To "${targetURL}"...`
      );

      /* Get sprite */
      const sprite = await getDataFileFromURL(
        targetURL,
        30000 // 30 secs
      );

      /* Cache */
      if (item.storeCache === true) {
        printLog("info", `Caching sprite "${id}" - Filename "${fileName}"...`);

        cacheSpriteFile(item.path, fileName, sprite).catch((error) =>
          printLog(
            "error",
            `Failed to cache sprite "${id}" - Filename "${fileName}": ${error}`
          )
        );
      }

      return sprite;
    } else {
      throw error;
    }
  }
}

/**
 * Get and cache data Fonts
 * @param {string} ids Font ids
 * @param {string} fileName Font file name
 * @returns {Promise<object>}
 */
export async function getAndCacheDataFonts(ids, fileName) {
  /* Get font datas */
  const buffers = await Promise.all(
    ids.split(",").map(async (id) => {
      const item = config.fonts[id];

      try {
        if (item === undefined) {
          throw new Error("Font does not exist");
        }

        return await getFont(item.path, fileName);
      } catch (error) {
        try {
          if (
            item !== undefined &&
            item.sourceURL !== undefined &&
            error.message === "Font does not exist"
          ) {
            const targetURL = item.sourceURL.replace("{range}.pbf", fileName);

            printLog(
              "info",
              `Forwarding font "${id}" - Filename "${fileName}" - To "${targetURL}"...`
            );

            /* Get font */
            const font = await getDataFileFromURL(
              targetURL,
              30000 // 30 secs
            );

            /* Cache */
            if (item.storeCache === true) {
              printLog(
                "info",
                `Caching font "${id}" - Filename "${fileName}"...`
              );

              cacheFontFile(item.path, fileName, font).catch((error) =>
                printLog(
                  "error",
                  `Failed to cache font "${id}" - Filename "${fileName}": ${error}`
                )
              );
            }

            return font;
          } else {
            throw error;
          }
        } catch (error) {
          printLog(
            "warn",
            `Failed to get font "${id}": ${error}. Using fallback font "Open Sans Regular"...`
          );

          return await getFallbackFont(fileName);
        }
      }
    })
  );

  /* Merge font datas */
  return mergeFontDatas(buffers);
}

/**
 * Validate tile metadata (no validate json field)
 * @param {object} metadata Metadata object
 * @returns {void}
 */
export function validateTileMetadata(metadata) {
  /* Validate name */
  if (metadata.name === undefined) {
    throw new Error(`"name" property is invalid`);
  }

  /* Validate type */
  if (metadata.type !== undefined) {
    if (["baselayer", "overlay"].includes(metadata.type) === false) {
      throw new Error(`"type" property is invalid`);
    }
  }

  /* Validate format */
  if (
    ["jpeg", "jpg", "pbf", "png", "webp", "gif"].includes(metadata.format) ===
    false
  ) {
    throw new Error(`"format" property is invalid`);
  }

  /* Validate json */
  /*
  if (metadata.format === "pbf" && metadata.json === undefined) {
    throw new Error(`"json" property is invalid`);
  }
  */

  /* Validate minzoom */
  if (metadata.minzoom < 0 || metadata.minzoom > 25) {
    throw new Error(`"minzoom" property is invalid`);
  }

  /* Validate maxzoom */
  if (metadata.maxzoom < 0 || metadata.maxzoom > 25) {
    throw new Error(`"maxzoom" property is invalid`);
  }

  /* Validate minzoom & maxzoom */
  if (metadata.minzoom > metadata.maxzoom) {
    throw new Error(`"zoom" property is invalid`);
  }

  /* Validate bounds */
  if (metadata.bounds !== undefined) {
    if (
      metadata.bounds.length !== 4 ||
      Math.abs(metadata.bounds[0]) > 180 ||
      Math.abs(metadata.bounds[2]) > 180 ||
      Math.abs(metadata.bounds[1]) > 90 ||
      Math.abs(metadata.bounds[3]) > 90 ||
      metadata.bounds[0] >= metadata.bounds[2] ||
      metadata.bounds[1] >= metadata.bounds[3]
    ) {
      throw new Error(`"bounds" property is invalid`);
    }
  }

  /* Validate center */
  if (metadata.center !== undefined) {
    if (
      metadata.center.length !== 3 ||
      Math.abs(metadata.center[0]) > 180 ||
      Math.abs(metadata.center[1]) > 90 ||
      metadata.center[2] < 0 ||
      metadata.center[2] > 25
    ) {
      throw new Error(`"center" property is invalid`);
    }
  }
}
