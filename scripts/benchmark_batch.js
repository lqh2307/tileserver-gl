"use strict";

import { DEFAULT_TILE_BATCH_SIZE } from "../src/defaults/index.js";
import { getTileBoundsBatches, min } from "../src/utils/index.js";
import { performance } from "node:perf_hooks";

const totalTiles = Number.parseInt(process.argv[2] ?? "1000000", 10);
const batchSize = Number.parseInt(
  process.argv[3] ?? `${DEFAULT_TILE_BATCH_SIZE}`,
  10,
);

if (!Number.isInteger(totalTiles) || totalTiles < 1) {
  throw new RangeError("Total tiles must be a positive integer");
}

const maxAxis = 2 ** 25;
if (totalTiles > maxAxis ** 2) {
  throw new RangeError(`Total tiles must not exceed ${maxAxis ** 2}`);
}

const width = min(totalTiles, maxAxis);
const fullRows = Math.floor(totalTiles / width);
const remainder = totalTiles % width;
const tileBounds = [];

if (fullRows) {
  tileBounds.push({
    z: 25,
    x: [0, width - 1],
    y: [0, fullRows - 1],
  });
}
if (remainder) {
  tileBounds.push({
    z: 25,
    x: [0, remainder - 1],
    y: [fullRows, fullRows],
  });
}

const heapBefore = process.memoryUsage().heapUsed;
const started = performance.now();
let batches = 0;
let generatedTiles = 0;

for (const batch of getTileBoundsBatches(tileBounds, batchSize)) {
  batches += 1;
  generatedTiles += batch.reduce((sum, tileBound) => {
    return sum + tileBound.total;
  }, 0);
}

const elapsed = performance.now() - started;
const heapDelta = process.memoryUsage().heapUsed - heapBefore;

console.log(
  JSON.stringify(
    {
      requestedTiles: totalTiles,
      generatedTiles,
      batchSize,
      batches,
      elapsedMs: +elapsed.toFixed(3),
      heapDeltaMB: +(heapDelta / 1024 / 1024).toFixed(3),
    },
    null,
    2,
  ),
);
