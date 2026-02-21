"use strict";

import { PMTiles, FetchSource } from "pmtiles";
import { openSync, readSync } from "node:fs";
import {
  FALLBACK_VECTOR_LAYERS,
  detectFormatAndHeaders,
  getCenterFromBBox,
  FALLBACK_BBOX,
  getFileSize,
} from "../utils/index.js";

/*********************************** PMTiles *************************************/

/**
 * Private class for PMTiles
 */
class PMTilesFileSource {
  constructor(fd) {
    this.fd = fd;
  }

  getKey() {
    return this.fd;
  }

  getBytes(offset, length) {
    const buffer = Buffer.alloc(length);

    readSync(this.fd, buffer, 0, buffer.length, offset);

    return {
      data: buffer.buffer.slice(
        buffer.byteOffset,
        buffer.byteOffset + buffer.byteLength,
      ),
    };
  }
}

/**
 * Open PMTiles
 * @param {string} filePath PMTiles filepath
 * @returns {object}
 */
export function openPMTiles(filePath) {
  let source;

  if (["https://", "http://"].some((scheme) => filePath.startsWith(scheme))) {
    source = new FetchSource(filePath);
  } else {
    source = new PMTilesFileSource(openSync(filePath, "r"));
  }

  return new PMTiles(source);
}

/**
 * Get PMTiles metadata
 * @param {object} pmtilesSource
 * @returns {Promise<object>}
 */
export async function getPMTilesMetadata(pmtilesSource) {
  /* Get metadatas */
  const [pmtilesHeader, pmtilesMetadata] = await Promise.all([
    pmtilesSource.getHeader(),
    pmtilesSource.getMetadata(),
  ]);

  /* Default metadata */
  const metadata = {};

  metadata.name = pmtilesMetadata.name ?? "Unknown";
  metadata.description = pmtilesMetadata.description ?? metadata.name;
  metadata.attribution =
    pmtilesMetadata.attribution ?? "<b>Viettel HighTech</b>";
  metadata.version = pmtilesMetadata.version ?? "1.0.0";

  switch (pmtilesHeader.tileType) {
    case 1: {
      metadata.format = "pbf";

      break;
    }

    case 2: {
      metadata.format = "png";

      break;
    }

    case 3: {
      metadata.format = "jpeg";

      break;
    }

    case 4: {
      metadata.format = "webp";

      break;
    }

    case 5: {
      metadata.format = "avif";

      break;
    }

    default: {
      metadata.format = "png";

      break;
    }
  }

  metadata.minzoom = pmtilesHeader.minZoom ?? 0;
  metadata.maxzoom = pmtilesHeader.maxZoom ?? 22;

  if (
    pmtilesHeader.minLon !== undefined &&
    pmtilesHeader.minLat !== undefined &&
    pmtilesHeader.maxLon !== undefined &&
    pmtilesHeader.maxLat !== undefined
  ) {
    metadata.bounds = [
      pmtilesHeader.minLon,
      pmtilesHeader.minLat,
      pmtilesHeader.maxLon,
      pmtilesHeader.maxLat,
    ];
  } else {
    metadata.bounds = FALLBACK_BBOX;
  }

  if (
    pmtilesHeader.centerLon !== undefined &&
    pmtilesHeader.centerLat !== undefined &&
    pmtilesHeader.centerZoom !== undefined
  ) {
    metadata.center = [
      pmtilesHeader.centerLon,
      pmtilesHeader.centerLat,
      pmtilesHeader.centerZoom,
    ];
  } else {
    metadata.center = getCenterFromBBox(
      metadata.bounds,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    );
  }

  if (metadata.format === "pbf") {
    metadata.vector_layers =
      pmtilesMetadata.vector_layers ?? FALLBACK_VECTOR_LAYERS;
  }

  return metadata;
}

/**
 * Get PMTiles tile
 * @param {object} pmtilesSource
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<object>}
 */
export async function getPMTilesTile(pmtilesSource, z, x, y) {
  const zxyTile = await pmtilesSource.getZxy(z, x, y);
  if (!zxyTile?.data) {
    throw new Error("Not Found");
  }

  return {
    data: zxyTile.data,
    headers: detectFormatAndHeaders(zxyTile.data).headers,
  };
}

/**
 * Get the size of PMTiles
 * @param {string} filePath PMTiles filepath
 * @returns {Promise<number>}
 */
export async function getPMTilesSize(filePath) {
  return await getFileSize(filePath);
}
