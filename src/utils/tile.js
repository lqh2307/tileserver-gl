"use strict";

import { getCenterFromBBox, MAX_LAT, MAX_LON } from "./spatial.js";
import { BACKGROUND_COLOR, createImageOutput } from "./image.js";
import { DEFAULT_TILE_BATCH_SIZE } from "../defaults/index.js";
import { min } from "./number.js";

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

(async () => {
  await Promise.all(
    [...RASTER_TILE_FORMATS].map(async (format) => {
      FALLBACK_TILE_DATA[format] = await createImageOutput({
        createOption: {
          width: 1,
          height: 1,
          channels: 4,
          background: BACKGROUND_COLOR,
        },
        format,
      });
    }),
  );
})();

/**
 * Create tile metadata
 * @param {{ [key: string]: any }} metadata Metadata object
 * @returns {{ [key: string]: any }}
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
 * @param {{ [key: string]: any }} metadata Metadata object
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

/**
 * Split tile bounds into batches without expanding them into individual tiles.
 * Every yielded batch contains at most `batchSize` tiles.
 * @param {{ z: number, x: [number, number], y: [number, number], total?: number }[]} tileBounds Tile bounds
 * @param {number} batchSize Maximum tiles in a batch
 * @returns {Generator<{ z: number, x: [number, number], y: [number, number], total: number }[]>} Tile bound batches
 */
function* getTileBoundsBatches(
  tileBounds,
  batchSize = DEFAULT_TILE_BATCH_SIZE,
) {
  let batch = [];
  let batchTotal = 0;

  const baseChunkHeight = Math.floor(Math.sqrt(batchSize));

  function* splitTileBound(tileBound) {
    const width = tileBound.x[1] - tileBound.x[0] + 1;
    const height = tileBound.y[1] - tileBound.y[0] + 1;

    let chunkHeight = min(height, baseChunkHeight);
    let chunkWidth = min(width, Math.floor(batchSize / chunkHeight));

    chunkHeight = min(height, Math.floor(batchSize / chunkWidth));

    for (
      let xMin = tileBound.x[0];
      xMin <= tileBound.x[1];
      xMin += chunkWidth
    ) {
      const xMax = min(xMin + chunkWidth - 1, tileBound.x[1]);

      for (
        let yMin = tileBound.y[0];
        yMin <= tileBound.y[1];
        yMin += chunkHeight
      ) {
        const yMax = min(yMin + chunkHeight - 1, tileBound.y[1]);

        yield {
          z: tileBound.z,
          x: [xMin, xMax],
          y: [yMin, yMax],
          total: (xMax - xMin + 1) * (yMax - yMin + 1),
        };
      }
    }
  }

  for (const tileBound of tileBounds) {
    for (const part of splitTileBound(tileBound)) {
      if (batchTotal + part.total > batchSize) {
        yield batch;

        batch = [];
        batchTotal = 0;
      }

      batch.push(part);

      batchTotal += part.total;
    }
  }

  if (batch.length) {
    yield batch;
  }
}

export {
  FALLBACK_VECTOR_LAYERS,
  getTileBoundsBatches,
  validateTileMetadata,
  createTileMetadata,
  RASTER_TILE_FORMATS,
  VECTOR_TILE_FORMATS,
  FALLBACK_TILE_DATA,
  ALL_TILE_FORMATS,
  FALLBACK_BBOX,
  LAYER_TYPES,
  TILE_SIZES,
};
