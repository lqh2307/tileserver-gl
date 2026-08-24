"use strict";

import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import express from "express";
import {
  buildCapabilities,
  normalizeVersion,
  parseWMSBBox,
  serve_wms,
} from "../src/serves/serve_wms.js";

test("WMS 1.3.0 applies the EPSG:4326 latitude/longitude axis order", () => {
  assert.deepEqual(parseWMSBBox("10,20,30,40", "EPSG:4326"), [20, 10, 40, 30]);
});

test("WMS capabilities expose the supported operations and XML formats", async () => {
  const capabilities = await buildCapabilities({
    version: "1.3.0",
    baseURL: "https://example.test/wms",
    layers: [],
  });

  assert.match(capabilities, /<WMS_Capabilities version="1\.3\.0"/);
  assert.match(capabilities, /<GetCapabilities>/);
  assert.match(capabilities, /<GetMap>/);
  assert.match(capabilities, /<GetFeatureInfo>/);
  assert.match(capabilities, /image\/png/);
});

test("WMS returns an OGC exception for unsupported operations", async () => {
  const app = express();
  serve_wms.init(app);
  const server = http.createServer(app);

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();
    const response = await fetch(
      `http://127.0.0.1:${address.port}/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetTile`,
    );
    const body = await response.text();

    assert.equal(response.status, 400);
    assert.match(response.headers.get("content-type"), /text\/xml/);
    assert.match(body, /OperationNotSupported/);

    const post = await fetch(
      `http://127.0.0.1:${address.port}/wms?SERVICE=WMS&REQUEST=GetCapabilities`,
      {
        method: "POST",
      },
    );
    assert.notEqual(post.status, 200);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
  }
});

test("WMS rejects versions other than 1.3.0", () => {
  assert.throws(() => {
    return normalizeVersion("1.1.1");
  }, /Unsupported WMS version/);
});
