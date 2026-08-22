"use strict";

import os from "node:os";

/** Default upper-bounded parallelism used by batch operations. @type {number} */
export const DEFAULT_CONCURRENCY = Math.max(
  1,
  Math.min(os.availableParallelism(), 16),
);
/** Default number of attempts for retryable operations. @type {number} */
export const DEFAULT_MAX_TRY = 5;
/** Default lock and database query timeout in milliseconds. @type {number} */
export const DEFAULT_QUERY_TIMEOUT = 60000;
/** Default timeout for metadata and information requests in milliseconds. @type {number} */
export const DEFAULT_INFO_TIMEOUT = 1800000;
/** Whether transparent raster tiles are stored by default. @type {boolean} */
export const DEFAULT_STORE_TRANSPARENT = true;
/** Maximum number of tiles processed in one default batch. @type {number} */
export const DEFAULT_TILE_BATCH_SIZE = 10000;
/** Default in-memory cache lifetime in milliseconds. @type {number} */
export const DEFAULT_CACHE_TIMEOUT = 300000;
/** Idle time before an unused renderer is released from a persistent pool. @type {number} */
export const DEFAULT_RENDERER_IDLE_TIMEOUT = 60000;
