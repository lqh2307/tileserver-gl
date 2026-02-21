"use strict";

import { getCenterFromBBox, MAX_LAT, MAX_LON } from "./spatial.js";
import { BACKGROUND_COLOR, createImageOutput } from "./image.js";

const FALLBACK_BBOX = [-MAX_LON, -MAX_LAT, MAX_LON, MAX_LAT];

const FALLBACK_VECTOR_LAYERS = [];

const RASTER_TILE_FORMATS = new Set(["jpeg", "jpg", "png", "webp"]);
const VECTOR_TILE_FORMATS = new Set(["pbf"]);
const ALL_TILE_FORMATS = new Set([
  ...RASTER_TILE_FORMATS,
  ...VECTOR_TILE_FORMATS,
]);

const TILE_SIZES = new Set(["256", "512"]);

const LAYER_TYPES = new Set(["baselayer", "overlay"]);

/* Create fallback tile data */
const FALLBACK_TILE_DATA = {
  pbf: Buffer.from([]),
};

(async () =>
  await Promise.all(
    [...RASTER_TILE_FORMATS].map(async (format) => {
      FALLBACK_TILE_DATA[format] = await createImageOutput({
        createOption: {
          width: 1,
          height: 1,
          channels: 4,
          background: BACKGROUND_COLOR,
        },
        format: format,
      });
    }),
  ))();

/**
 * Create tile metadata
 * @param {object} metadata Metadata object
 * @returns {object}
 */
function createTileMetadata(metadata = {}) {
  const data = {};

  data.name = metadata.name ?? "Unknown";
  data.description = metadata.description ?? data.name;
  data.attribution = metadata.attribution ?? "<b>Viettel HighTech</b>";
  data.version = metadata.version ?? "1.0.0";
  data.type = metadata.type ?? "overlay";
  data.format = metadata.format ?? "png";
  data.minzoom = metadata.minzoom ?? 0;
  data.maxzoom = metadata.maxzoom ?? 22;
  data.bounds = metadata.bounds ?? FALLBACK_BBOX;

  if (metadata.center !== undefined) {
    data.center = metadata.center;
  } else {
    data.center = getCenterFromBBox(
      data.bounds,
      Math.floor((data.minzoom + data.maxzoom) / 2),
    );
  }

  if (data.format === "pbf") {
    data.vector_layers = metadata.vector_layers ?? FALLBACK_VECTOR_LAYERS;
  }

  if (metadata.cacheCoverages !== undefined) {
    data.cacheCoverages = metadata.cacheCoverages;
  }

  return data;
}

/**
 * Validate tile metadata (no validate json field)
 * @param {object} metadata Metadata object
 * @returns {void}
 */
function validateTileMetadata(metadata) {
  /* Validate name */
  if (metadata.name === undefined) {
    throw new Error(`"name" property is invalid`);
  }

  /* Validate type */
  if (metadata.type !== undefined) {
    if (!LAYER_TYPES.has(metadata.type)) {
      throw new Error(`"type" property is invalid`);
    }
  }

  /* Validate format */
  if (!ALL_TILE_FORMATS.has(metadata.format)) {
    throw new Error(`"format" property is invalid`);
  }

  /* Validate json */
  if (metadata.format === "pbf" && metadata.vector_layers === undefined) {
    throw new Error(`"vector_layers" property is invalid`);
  }
}

export {
  FALLBACK_VECTOR_LAYERS,
  RASTER_TILE_FORMATS,
  VECTOR_TILE_FORMATS,
  FALLBACK_TILE_DATA,
  ALL_TILE_FORMATS,
  FALLBACK_BBOX,
  LAYER_TYPES,
  TILE_SIZES,
  validateTileMetadata,
  createTileMetadata,
};
