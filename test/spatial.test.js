"use strict";

import assert from "node:assert/strict";
import test from "node:test";
import { transformBBoxSRS } from "../src/utils/spatial.js";

test("transformBBoxSRS preserves minX,minY,maxX,maxY order", () => {
  const bounds = [100, 20, 110, 25];
  assert.deepEqual(
    transformBBoxSRS({
      srcSRS: "EPSG:4326",
      dstSRS: "EPSG:4326",
      bounds,
    }),
    bounds,
  );

  const transformed = transformBBoxSRS({
    srcSRS: "EPSG:4326",
    dstSRS: "EPSG:3857",
    bounds,
  });
  assert.ok(transformed.every(Number.isFinite));
  assert.ok(transformed[0] < transformed[2]);
  assert.ok(transformed[1] < transformed[3]);
});
