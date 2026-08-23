"use strict";

import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import express from "express";
import { config } from "../src/configs/index.js";
import { serve_wfs } from "../src/serves/serve_wfs.js";

async function withWFSApp(callback) {
  const root = await mkdtemp(path.join(os.tmpdir(), "tileserver-wfs-test-"));
  const filePath = path.join(root, "points.geojson");
  const previous = config.geojsons;
  await writeFile(
    filePath,
    JSON.stringify({
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          id: "p1",
          properties: {
            name: "Hanoi",
            rank: 1,
          },
          geometry: {
            type: "Point",
            coordinates: [105.8, 21.0],
          },
        },
      ],
    }),
  );
  config.geojsons = {
    test: {
      points: {
        path: filePath,
      },
    },
  };

  const app = express();
  serve_wfs.init(app);
  const server = http.createServer(app);
  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    await callback(`http://127.0.0.1:${server.address().port}`);
  } finally {
    config.geojsons = previous;
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
}

test("WFS capabilities and DescribeFeatureType publish GeoJSON layers", async () => {
  await withWFSApp(async (baseURL) => {
    const capabilities = await fetch(
      `${baseURL}/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities`,
    );
    const capabilitiesText = await capabilities.text();
    assert.equal(capabilities.status, 200);
    assert.match(capabilitiesText, /<wfs:Name>test:points<\/wfs:Name>/);

    const schema = await fetch(
      `${baseURL}/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=DescribeFeatureType&TYPENAMES=test%3Apoints`,
    );
    const schemaText = await schema.text();
    assert.equal(schema.status, 200);
    assert.match(schemaText, /name="name"/);
    assert.match(schemaText, /name="rank"/);
  });
});

test("WFS GetFeature supports GeoJSON output, BBOX and pagination", async () => {
  await withWFSApp(async (baseURL) => {
    const params = new URLSearchParams({
      SERVICE: "WFS",
      VERSION: "2.0.0",
      REQUEST: "GetFeature",
      TYPENAMES: "test:points",
      OUTPUTFORMAT: "application/json",
      BBOX: "105,20,106,22",
      COUNT: "1",
    });
    const response = await fetch(`${baseURL}/wfs?${params}`);
    const body = await response.json();
    assert.equal(response.status, 200);
    assert.equal(body.numberMatched, 1);
    assert.equal(body.numberReturned, 1);
    assert.equal(body.features[0].properties.name, "Hanoi");

    const gmlParams = new URLSearchParams({
      SERVICE: "WFS",
      VERSION: "2.0.0",
      REQUEST: "GetFeature",
      TYPENAMES: "test:points",
      OUTPUTFORMAT: "GML2",
    });
    const gml = await fetch(`${baseURL}/wfs?${gmlParams}`);
    const gmlBody = await gml.text();
    assert.equal(gml.status, 200);
    assert.match(gmlBody, /FeatureCollection/);
    assert.match(gmlBody, /Hanoi/);
  });
});

test("WFS is read-only and rejects POST requests", async () => {
  await withWFSApp(async (baseURL) => {
    const response = await fetch(`${baseURL}/wfs`, {
      method: "POST",
    });
    assert.equal(response.status, 404);
  });
});
