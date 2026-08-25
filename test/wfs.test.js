"use strict";

import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import express from "express";
import { config } from "../src/configs/index.js";
import { normalizeWFSVersion, serve_wfs } from "../src/serves/serve_wfs.js";

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

test("WFS rejects versions other than 2.0.0", () => {
  assert.throws(() => {
    return normalizeWFSVersion("1.1.0");
  }, /Unsupported WFS version/);
});

test("WFS capabilities and DescribeFeatureType publish GeoJSON layers", async () => {
  await withWFSApp(async (baseURL) => {
    const capabilities = await fetch(
      `${baseURL}/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities`,
    );
    const capabilitiesText = await capabilities.text();
    assert.equal(capabilities.status, 200);
    assert.match(capabilitiesText, /<wfs:Name>test:points<\/wfs:Name>/);

    const cachedCapabilities = await fetch(
      `${baseURL}/wfs?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetCapabilities`,
    );
    assert.equal(await cachedCapabilities.text(), capabilitiesText);

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

    const invalidParams = new URLSearchParams(params);
    invalidParams.set("BBOX", "106,20,105,22");
    const invalid = await fetch(`${baseURL}/wfs?${invalidParams}`);
    const invalidBody = await invalid.text();
    assert.equal(invalid.status, 400);
    assert.match(invalidBody, /BBOX coordinates are not ordered/);

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

    const projectedParams = new URLSearchParams(params);
    projectedParams.delete("BBOX");
    projectedParams.set("SRSNAME", "EPSG:3857");
    const projectedResponse = await fetch(
      `${baseURL}/wfs?${projectedParams}`,
    );
    const projectedBody = await projectedResponse.json();
    const projectedPoint = projectedBody.features[0].geometry.coordinates;
    assert.ok(projectedPoint[0] > 10_000_000);
    assert.ok(projectedPoint[1] > 2_000_000);

    projectedParams.set("OUTPUTFORMAT", "GML2");
    const projectedGML = await fetch(`${baseURL}/wfs?${projectedParams}`);
    const projectedGMLBody = await projectedGML.text();
    const position = projectedGMLBody.match(/<gml:pos>([^<]+)<\/gml:pos>/);
    assert.ok(position);
    const [gmlX, gmlY] = position[1].split(" ").map(Number);
    assert.ok(Math.abs(gmlX - projectedPoint[0]) < 1e-6);
    assert.ok(Math.abs(gmlY - projectedPoint[1]) < 1e-6);
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
