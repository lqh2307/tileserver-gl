"use strict";

import { FALLBACK_BBOX, FALLBACK_VECTOR_LAYERS } from "../resources/index.js";
import { BACKGROUND_COLOR, createImageOutput } from "./image.js";
import { getCenterFromBBox } from "./spatial.js";

/* Create fallback tile data */
const FALLBACK_TILE_DATA = {
  pbf: Buffer.from([]),
};

(async () =>
  await Promise.all(
    ["gif", "png", "jpg", "jpeg", "webp"].map(async (format) => {
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

export { FALLBACK_TILE_DATA, createTileMetadata };
