"use strict";

import os from "node:os";

export const DEFAULT_CONCURRENCY = Math.max(
  1,
  Math.min(os.availableParallelism(), 16),
);
export const DEFAULT_MAX_TRY = 5;
export const DEFAULT_QUERY_TIMEOUT = 60000;
export const DEFAULT_INFO_TIMEOUT = 1800000;
export const DEFAULT_STORE_TRANSPARENT = true;
export const DEFAULT_TILE_BATCH_SIZE = 10000;
export const DEFAULT_CACHE_TIMEOUT = 300000;
