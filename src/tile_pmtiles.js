"use strict";

import { deepClone, detectFormatAndHeaders } from "./utils.js";
import { PMTiles, FetchSource } from "pmtiles";
import fsPromise from "node:fs/promises";
import fs from "node:fs";

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

    fs.readSync(this.fd, buffer, 0, buffer.length, offset);

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
 * @returns {Object}
 */
export function openPMTiles(filePath) {
  let source;

  if (
    filePath.startsWith("https://") === true ||
    filePath.startsWith("http://") === true
  ) {
    source = new FetchSource(filePath);
  } else {
    source = new PMTilesFileSource(fs.openSync(filePath, "r"));
  }

  return new PMTiles(source);
}

/**
 * Get PMTiles metadata
 * @param {Object} pmtilesSource
 * @returns {Promise<Object>}
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
    metadata.description = "Unknown";
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

  if (pmtilesHeader.tileType === 1) {
    metadata.format = "pbf";
  } else if (pmtilesHeader.tileType === 2) {
    metadata.format = "png";
  } else if (pmtilesHeader.tileType === 3) {
    metadata.format = "jpeg";
  } else if (pmtilesHeader.tileType === 4) {
    metadata.format = "webp";
  } else if (pmtilesHeader.tileType === 5) {
    metadata.format = "avif";
  } else {
    metadata.format = "png";
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
 * Create PMTiles metadata
 * @param {Object} metadata Metadata object
 * @returns {Object}
 */
export function createPMTilesMetadata(metadata) {
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

  if (metadata.vector_layers !== undefined) {
    data.vector_layers = deepClone(metadata.vector_layers);
  } else {
    if (data.format === "pbf") {
      data.vector_layers = [];
    }
  }

  if (metadata.cacheCoverages !== undefined) {
    data.cacheCoverages = deepClone(metadata.cacheCoverages);
  }

  return data;
}

/**
 * Get PMTiles tile
 * @param {Object} pmtilesSource
 * @param {number} z Zoom level
 * @param {number} x X tile index
 * @param {number} y Y tile index
 * @returns {Promise<Object>}
 */
export async function getPMTilesTile(pmtilesSource, z, x, y) {
  const zxyTile = await pmtilesSource.getZxy(z, x, y);
  if (!zxyTile?.data) {
    throw new Error("Tile does not exist");
  }

  const data = Buffer.from(zxyTile.data);

  return {
    data: data,
    headers: detectFormatAndHeaders(data).headers,
  };
}

/**
 * Get the size of PMTiles
 * @param {string} filePath PMTiles filepath
 * @returns {Promise<number>}
 */
export async function getPMTilesSize(filePath) {
  const stat = await fsPromise.stat(filePath);

  return stat.size;
}

/**
 * Validate PMTiles metadata (no validate json field)
 * @param {Object} metadata PMTiles metadata
 * @returns {void}
 */
export function validatePMTiles(metadata) {
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
  if (metadata.minzoom < 0 || metadata.minzoom > 22) {
    throw new Error(`"minzoom" property is invalid`);
  }

  /* Validate maxzoom */
  if (metadata.maxzoom < 0 || metadata.maxzoom > 22) {
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
      metadata.center[2] > 22
    ) {
      throw new Error(`"center" property is invalid`);
    }
  }
}
