"use strict";

import { readFile } from "node:fs/promises";
import protobuf from "protocol-buffers";
import path from "node:path";

let vectorTileProtoPromise;

/**
 * Load and compile the vector-tile protobuf schema once per worker.
 * @returns {Promise<object>} Compiled protocol-buffers schema
 */
export function getVectorTileProto() {
  if (!vectorTileProtoPromise) {
    vectorTileProtoPromise = readFile(
      path.join("public/protos", "vector_tile.proto"),
    )
      .then(protobuf)
      .catch((error) => {
        vectorTileProtoPromise = undefined;
        throw error;
      });
  }

  return vectorTileProtoPromise;
}
