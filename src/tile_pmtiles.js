"use strict";

import { deepClone, detectFormatAndHeaders } from "./utils.js";
import { PMTiles, FetchSource } from "pmtiles";
import { openSync, readSync } from "node:fs";
import { stat } from "node:fs/promises";

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
        buffer.byteOffset + buffer.byteLength
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

  if (
    ["https://", "http://"].some(
      (scheme) => filePath.startsWith(scheme)
    )
  ) {
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
  /* Default metadata */
  const metadata = {};

  /* Get metadatas */
  const [pmtilesHeader, pmtilesMetadata] = await Promise.all([
    pmtilesSource.getHeader(),
    pmtilesSource.getMetadata(),
  ]);

  if (pmtilesMetadata.name !== undefined) {
    metadata.name = pmtilesMetadata.name;
  } else {
    metadata.name = "Unknown";
  }

  if (pmtilesMetadata.description !== undefined) {
    metadata.description = pmtilesMetadata.description;
  } else {
    metadata.description = metadata.name;
  }

  if (pmtilesMetadata.attribution !== undefined) {
    metadata.attribution = pmtilesMetadata.attribution;
  } else {
    metadata.attribution = "<b>Viettel HighTech</b>";
  }

  if (pmtilesMetadata.version !== undefined) {
    metadata.version = pmtilesMetadata.version;
  } else {
    metadata.version = "1.0.0";
  }

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

  if (pmtilesHeader.minZoom !== undefined) {
    metadata.minzoom = pmtilesHeader.minZoom;
  } else {
    metadata.minzoom = 0;
  }

  if (pmtilesHeader.maxZoom !== undefined) {
    metadata.maxzoom = pmtilesHeader.maxZoom;
  } else {
    metadata.maxzoom = 22;
  }

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
    metadata.bounds = [-180, -85.051129, 180, 85.051129];
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
    metadata.center = [
      (metadata.bounds[0] + metadata.bounds[2]) / 2,
      (metadata.bounds[1] + metadata.bounds[3]) / 2,
      Math.floor((metadata.minzoom + metadata.maxzoom) / 2),
    ];
  }

  if (pmtilesMetadata.vector_layers !== undefined) {
    metadata.vector_layers = deepClone(pmtilesMetadata.vector_layers);
  } else {
    if (metadata.format === "pbf") {
      metadata.vector_layers = [];
    }
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
    throw new Error("Tile does not exist");
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
  const stats = await stat(filePath);

  return stats.size;
}
