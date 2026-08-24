"use strict";

import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import express from "express";
import { config } from "../src/configs/index.js";
import {
  buildCapabilities,
  getTileMatrixSet,
  normalizeFormat,
  serve_wmts,
} from "../src/serves/serve_wmts.js";

test("WMTS builds capabilities with TileMatrixSets and REST templates", async () => {
  const capabilities = await buildCapabilities({
    baseURL: "https://example.test",
    layers: [
      {
        id: "roads",
        kind: "style",
        item: {},
        tileJSON: {},
        title: "Roads",
        abstract: "Road map",
        bbox: [-180, -85, 180, 85],
        minZoom: 0,
        maxZoom: 22,
        formats: [normalizeFormat("image/png")],
        matrixSets: [getTileMatrixSet("GoogleMapsCompatible_256")],
      },
    ],
  });

  assert.match(capabilities, /version="1\.0\.0"/);
  assert.match(capabilities, /name="GetCapabilities"/);
  assert.match(capabilities, /name="GetTile"/);
  assert.match(capabilities, /GoogleMapsCompatible_256/);
  assert.match(
    capabilities,
    /https:\/\/example\.test\/wmts\/roads\/default\/\{TileMatrixSet\}/,
  );
});

test("WMTS serves KVP and RESTful vector tiles", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "tileserver-wmts-test-"));
  const previous = config.datas;
  const previousStyles = config.styles;
  await mkdir(path.join(root, "0", "0"), {
    recursive: true,
  });
  await writeFile(path.join(root, "0", "0", "0.pbf"), Buffer.from("tile"));
  config.datas = {
    raw: {
      sourceType: "xyz",
      source: root,
      tileJSON: {
        name: "Raw vector",
        description: "Raw vector tile",
        format: "pbf",
        minzoom: 0,
        maxzoom: 0,
        bounds: [-180, -85, 180, 85],
      },
    },
  };
  config.styles = {
    roads: {
      tileJSON: {
        name: "Roads",
        description: "Road map",
        bounds: [-180, -85, 180, 85],
        minzoom: 0,
        maxzoom: 0,
      },
      wmts: {
        formats: ["image/png"],
      },
    },
  };

  const app = express();
  serve_wmts.init(app);
  const server = http.createServer(app);
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const baseURL = `http://127.0.0.1:${server.address().port}`;
    const kvp = await fetch(
      `${baseURL}/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetTile&LAYER=raw&STYLE=default&FORMAT=application%2Fx-protobuf&TILEMATRIXSET=GoogleMapsCompatible_256&TILEMATRIX=0&TILEROW=0&TILECOL=0`,
    );
    assert.equal(kvp.status, 200);
    assert.equal(kvp.headers.get("content-type"), "application/x-protobuf");
    assert.equal(await kvp.text(), "tile");

    const rest = await fetch(
      `${baseURL}/wmts/raw/default/GoogleMapsCompatible_256/0/0/0.pbf`,
    );
    assert.equal(rest.status, 200);
    assert.equal(await rest.text(), "tile");

    const capabilities = await fetch(
      `${baseURL}/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetCapabilities`,
    );
    const capabilitiesText = await capabilities.text();
    assert.equal(capabilities.status, 200);
    assert.match(capabilitiesText, /<ows:Identifier>roads<\/ows:Identifier>/);
    assert.match(capabilitiesText, /href="http:\/\/127\.0\.0\.1:[0-9]+\/wmts"/);

    const styleCapabilities = await fetch(`${baseURL}/styles/roads/wmts.xml`);
    assert.equal(styleCapabilities.status, 404);

    const styleTile = await fetch(
      `${baseURL}/styles/roads/GoogleMapsCompatible_256/0/0/0.png`,
    );
    assert.equal(styleTile.status, 404);
  } finally {
    config.datas = previous;
    config.styles = previousStyles;
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
    await rm(root, {
      recursive: true,
      force: true,
    });
  }
});
