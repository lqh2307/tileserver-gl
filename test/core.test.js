"use strict";

import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import http from "node:http";
import test from "node:test";
import express from "express";
import { StatusCodes } from "http-status-codes";
import { deflateSync } from "node:zlib";
import { responseCompressionMiddleware } from "../src/middlewares/index.js";
import { serve_swagger } from "../src/serves/serve_swagger.js";
import { serve_task } from "../src/serves/serve_task.js";
import {
  findFiles,
  calculateMD5,
  getDataFromURL,
  getJSONSchema,
  getRequestHost,
  getTaskTargets,
  getTileBoundsBatches,
  isTaskTargetMatched,
  isExistFile,
  isLocalURL,
  isURL,
  closeSQLite,
  openSQLite,
  removeEmptyFolders,
  runAllWithLimit,
  runSingleFlight,
  normalizeResponseEncoding,
  shouldCompressResponse,
  walkFiles,
  max,
} from "../src/utils/index.js";
import { DEFAULT_TILE_BATCH_SIZE } from "../src/defaults/index.js";

test("tile bounds use the 10000 tile default batch", () => {
  assert.equal(DEFAULT_TILE_BATCH_SIZE, 10000);

  const batches = Array.from(
    getTileBoundsBatches([
      {
        z: 15,
        x: [0, 500],
        y: [0, 50],
      },
    ]),
  );
  const totals = batches.map((batch) => {
    return batch.reduce((sum, tileBound) => {
      return sum + tileBound.total;
    }, 0);
  });

  assert.ok(totals.length > 1);
  assert.ok(
    totals.every((total) => {
      return total <= DEFAULT_TILE_BATCH_SIZE;
    }),
  );
  assert.equal(
    totals.reduce((sum, total) => {
      return sum + total;
    }, 0),
    501 * 51,
  );
});

test("response compression uses the environment default and query override", () => {
  const previous = process.env.COMPRESS_RESPONSE;

  try {
    process.env.COMPRESS_RESPONSE = "true";

    assert.equal(shouldCompressResponse({ query: {} }), true);
    assert.equal(shouldCompressResponse({ query: { compression: "false" } }), false);
    assert.equal(shouldCompressResponse({ query: { compression: "true" } }), true);

    process.env.COMPRESS_RESPONSE = "false";

    assert.equal(shouldCompressResponse({ query: {} }), false);
    assert.equal(shouldCompressResponse({ query: { compression: "true" } }), true);
  } finally {
    if (previous === undefined) {
      delete process.env.COMPRESS_RESPONSE;
    } else {
      process.env.COMPRESS_RESPONSE = previous;
    }
  }
});

test("response compression middleware negotiates gzip for eligible responses", async () => {
  const previous = process.env.COMPRESS_RESPONSE;
  process.env.COMPRESS_RESPONSE = "true";

  const app = express()
    .use(responseCompressionMiddleware())
    .get("/json", (_req, res) => {
      res.json({ payload: "x".repeat(2000) });
    })
    .get("/small", (_req, res) => {
      res.json({ payload: "small" });
    })
    .get("/vendor-pbf", (_req, res) => {
      res
        .type("application/vnd.mapbox-vector-tile")
        .send(Buffer.alloc(2000, 97));
    })
    .get("/no-transform", (_req, res) => {
      res.set("cache-control", "no-transform");
      res.json({ payload: "x".repeat(2000) });
    })
    .use("/assets", express.static(path.resolve("public", "resources")));
  const server = http.createServer(app);

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const port = server.address().port;
    const response = await fetch(`http://127.0.0.1:${port}/json`, {
      headers: {
        "accept-encoding": "gzip",
      },
    });

    assert.equal(response.headers.get("content-encoding"), "gzip");
    assert.equal(response.headers.get("content-length"), null);
    assert.equal(response.headers.get("vary"), "Accept-Encoding");
    assert.equal((await response.json()).payload.length, 2000);

    const identityResponse = await fetch(`http://127.0.0.1:${port}/json`, {
      headers: {
        "accept-encoding": "identity",
      },
    });

    assert.equal(identityResponse.headers.get("content-encoding"), null);
    assert.equal(identityResponse.headers.get("vary"), "Accept-Encoding");
    assert.equal((await identityResponse.json()).payload.length, 2000);

    const small = await fetch(`http://127.0.0.1:${port}/small`, {
      headers: {
        "accept-encoding": "gzip",
      },
    });
    assert.equal(small.headers.get("content-encoding"), "gzip");

    const vendorPbf = await fetch(
      `http://127.0.0.1:${port}/vendor-pbf`,
      {
        headers: {
          "accept-encoding": "gzip",
        },
      },
    );
    assert.equal(vendorPbf.headers.get("content-encoding"), "gzip");
    assert.equal((await vendorPbf.arrayBuffer()).byteLength, 2000);

    const disabled = await fetch(
      `http://127.0.0.1:${port}/small?compression=false`,
      {
        headers: {
          "accept-encoding": "gzip",
        },
      },
    );
    assert.equal(disabled.headers.get("content-encoding"), null);

    const noTransform = await fetch(
      `http://127.0.0.1:${port}/no-transform`,
      {
        headers: {
          "accept-encoding": "gzip",
        },
      },
    );
    assert.equal(noTransform.headers.get("content-encoding"), null);

    const staticAsset = await fetch(
      `http://127.0.0.1:${port}/assets/maplibre-gl.js`,
      {
        headers: {
          "accept-encoding": "gzip",
        },
        signal: AbortSignal.timeout(10_000),
      },
    );
    assert.equal(staticAsset.headers.get("content-encoding"), "gzip");
    assert.ok((await staticAsset.arrayBuffer()).byteLength > 1_000_000);

    const rangedAsset = await fetch(
      `http://127.0.0.1:${port}/assets/maplibre-gl.js`,
      {
        headers: {
          "accept-encoding": "gzip",
          range: "bytes=0-99",
        },
      },
    );
    assert.equal(rangedAsset.status, 206);
    assert.equal(rangedAsset.headers.get("content-encoding"), null);
    assert.equal((await rangedAsset.arrayBuffer()).byteLength, 100);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });

    if (previous === undefined) {
      delete process.env.COMPRESS_RESPONSE;
    } else {
      process.env.COMPRESS_RESPONSE = previous;
    }
  }
});

test("response encoding normalization mutates disposable headers", async () => {
  const data = deflateSync("vector tile");
  const headers = {
    "content-type": "application/x-protobuf",
    "content-encoding": "deflate",
    "content-length": String(data.length),
  };
  const normalized = await normalizeResponseEncoding(data, headers, {
    query: {
      compression: "false",
    },
    acceptsEncodings: () => {
      return false;
    },
  });

  assert.equal(normalized.toString(), "vector tile");
  assert.equal(headers["content-encoding"], undefined);
  assert.equal(headers["content-length"], undefined);
  assert.equal(data[0], 0x78);
});

test("task selectors expand and match individual sync resources", () => {
  const seedConfig = {
    datas: {
      osm: {},
      satellite: {},
    },
    styles: {
      basic: {},
    },
  };
  const cleanUpConfig = {
    datas: {
      osm: {},
    },
    geojsons: {
      boundaries: {},
    },
  };

  assert.deepEqual(
    getTaskTargets(
      {
        type: "data",
        id: "osm",
      },
      seedConfig,
      cleanUpConfig,
    ),
    [
      {
        type: "data",
        id: "osm",
      },
    ],
  );
  assert.deepEqual(
    getTaskTargets(
      {
        type: "data",
      },
      seedConfig,
      cleanUpConfig,
    ),
    [
      {
        type: "data",
        id: "osm",
      },
      {
        type: "data",
        id: "satellite",
      },
    ],
  );

  const allTargets = getTaskTargets({}, seedConfig, cleanUpConfig);
  assert.equal(allTargets.length, 4);
  assert.equal(
    isTaskTargetMatched(
      {
        type: "data",
        id: "osm",
      },
      {
        type: "data",
        id: "osm",
      },
    ),
    true,
  );
  assert.equal(
    isTaskTargetMatched(
      {
        type: "style",
        id: "basic",
      },
      {
        type: "data",
      },
    ),
    false,
  );
  assert.deepEqual(
    getTaskTargets(
      {
        id: "osm",
      },
      seedConfig,
      cleanUpConfig,
    ),
    [],
  );
});

test("task API validates selectors and accepts empty type groups", async () => {
  const app = express();
  serve_task.init(app);
  const server = app.listen(0, "127.0.0.1");

  await new Promise((resolve) => {
    server.once("listening", resolve);
  });

  try {
    const { port } = server.address();
    const [
      legacyResponse,
      incompleteResponse,
      invalidRestartResponse,
      emptyTypeResponse,
      missingIdResponse,
    ] = await Promise.all([
      fetch(`http://127.0.0.1:${port}/tasks/start?seedDatas=true`),
      fetch(`http://127.0.0.1:${port}/tasks/start?id=osm`),
      fetch(`http://127.0.0.1:${port}/tasks/start?restart=yes`),
      fetch(`http://127.0.0.1:${port}/tasks/start?type=style`),
      fetch(`http://127.0.0.1:${port}/tasks/start?type=style&id=__missing__`),
    ]);

    assert.equal(legacyResponse.status, StatusCodes.BAD_REQUEST);
    assert.equal(incompleteResponse.status, StatusCodes.BAD_REQUEST);
    assert.equal(invalidRestartResponse.status, StatusCodes.BAD_REQUEST);
    assert.equal(emptyTypeResponse.status, StatusCodes.OK);
    assert.equal(missingIdResponse.status, StatusCodes.NOT_FOUND);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
  }
});

test("SQLite writers use NFS-compatible pragmas", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "tile-server-sqlite-"));
  let source;

  try {
    source = await openSQLite(path.join(root, "shared.sqlite"), true, 1000);

    assert.equal(
      source.pragma("journal_mode", {
        simple: true,
      }),
      "truncate",
    );
    assert.equal(
      source.pragma("synchronous", {
        simple: true,
      }),
      1,
    );
    assert.equal(
      source.pragma("mmap_size", {
        simple: true,
      }),
      0,
    );
  } finally {
    if (source) {
      closeSQLite(source);
    }

    await rm(root, {
      recursive: true,
      force: true,
    });
  }
});

test("runAllWithLimit respects concurrency and reports all failures", async () => {
  let active = 0;
  let maximumActive = 0;

  function* tasks() {
    for (let index = 0; index < 8; index++) {
      yield async () => {
        active += 1;
        maximumActive = max(maximumActive, active);
        await new Promise((resolve) => {
          setImmediate(resolve);
        });
        active -= 1;

        if (index === 2 || index === 6) {
          throw new Error(`failure-${index}`);
        }
      };
    }
  }

  await assert.rejects(runAllWithLimit(tasks(), 3), (error) => {
    assert.ok(error instanceof AggregateError);
    assert.equal(error.errors.length, 2);

    return true;
  });
  assert.ok(maximumActive <= 3);
});

test("single-flight shares only concurrent work", async () => {
  let calls = 0;
  const task = async () => {
    calls += 1;
    await new Promise((resolve) => {
      setImmediate(resolve);
    });

    return calls;
  };

  const values = await Promise.all([
    runSingleFlight("same", task),
    runSingleFlight("same", task),
    runSingleFlight("same", task),
  ]);

  assert.deepEqual(values, [1, 1, 1]);
  assert.equal(await runSingleFlight("same", task), 2);
});

test("recursive lookup returns correct paths and supports streaming", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "tileserver-file-test-"));

  try {
    await mkdir(path.join(root, "1", "2"), {
      recursive: true,
    });
    await writeFile(path.join(root, "1", "2", "3.pbf"), "tile");

    assert.deepEqual(await findFiles(root, /^\d+\.pbf$/, true, true), [
      path.join(root, "1", "2", "3.pbf"),
    ]);

    const streamed = [];
    for await (const filePath of walkFiles(root, /^\d+\.pbf$/)) {
      streamed.push(filePath);
    }
    assert.deepEqual(streamed, [path.join(root, "1", "2", "3.pbf")]);
  } finally {
    await rm(root, {
      recursive: true,
      force: true,
    });
  }
});

test("empty folder cleanup preserves matching branches", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "tileserver-cleanup-test-"));
  const keepPath = path.join(root, "keep", "tile.pbf");
  const removePath = path.join(root, "remove", "ignored.txt");

  try {
    await Promise.all([
      mkdir(path.dirname(keepPath), {
        recursive: true,
      }),
      mkdir(path.dirname(removePath), {
        recursive: true,
      }),
    ]);
    await Promise.all([
      writeFile(keepPath, "tile"),
      writeFile(removePath, "ignored"),
    ]);

    assert.equal(await removeEmptyFolders(root, /^.*\.pbf$/), true);
    assert.equal(await isExistFile(path.dirname(keepPath), true), true);
    assert.equal(await isExistFile(path.dirname(removePath), true), false);

    await unlink(keepPath);
    assert.equal(await removeEmptyFolders(root, /^.*\.pbf$/), false);
    assert.equal(await isExistFile(root, true), false);
  } finally {
    await rm(root, {
      recursive: true,
      force: true,
    });
  }
});

test("MBTiles reuses its prepared tile statement", async () => {
  const { getMBTilesTile } = await import("../src/resources/tile_mbtiles.js");
  const tileData = Buffer.from([1, 2, 3]);
  let prepareCount = 0;
  const source = {
    prepare: () => {
      prepareCount += 1;

      return {
        get: () => {
          return {
            tile_data: tileData,
          };
        },
      };
    },
  };

  assert.equal(getMBTilesTile(source, 0, 0, 0).data, tileData);
  assert.equal(getMBTilesTile(source, 0, 0, 0).data, tileData);
  assert.equal(prepareCount, 1);
});

test("PostgreSQL extra-info uses one query over the tile bounds batch", async () => {
  const calls = [];
  const source = {
    query: async (text, values) => {
      calls.push({ text, values });

      return {
        rows: [
          {
            zoom_level: 2,
            tile_column: 1,
            tile_row: 0,
            hash: "hash-1",
          },
        ],
      };
    },
  };

  const { getPostgreSQLTileExtraInfo } =
    await import("../src/resources/tile_postgresql.js");
  const result = await getPostgreSQLTileExtraInfo({
    source,
    tileBounds: [
      { z: 2, x: [0, 1], y: [0, 1] },
      { z: 3, x: [2, 3], y: [4, 5] },
    ],
  });

  assert.deepEqual(result, {
    "2/1/0": "hash-1",
  });
  assert.equal(calls.length, 1);
  assert.match(calls[0].text, /FROM tiles/);
  assert.doesNotMatch(calls[0].text, /FROM md5s/);
  assert.equal(calls[0].values.length, 10);
});

test("XYZ extra-info is built from tile files in bounded batches", async () => {
  const root = await mkdtemp(path.join(tmpdir(), "tileserver-xyz-test-"));
  const sourcePath = path.join(root, "tiles");
  const tilePath = path.join(sourcePath, "2", "1", "0.pbf");
  const databasePath = path.join(sourcePath, "tiles.sqlite");
  const previousDataDir = process.env.DATA_DIR;
  let source;

  try {
    await mkdir(path.dirname(tilePath), {
      recursive: true,
    });
    await Promise.all(
      ["config", "seed", "cleanup"].map((name) => {
        return writeFile(path.join(root, `${name}.json`), "{}");
      }),
    );
    const tileData = Buffer.from("vector-tile");
    await writeFile(tilePath, tileData);
    process.env.DATA_DIR = root;

    const {
      calculateXYZTileExtraInfo,
      closeXYZMD5DB,
      getXYZTileExtraInfo,
      openXYZMD5DB,
    } = await import("../src/resources/tile_xyz.js");

    source = await openXYZMD5DB(databasePath, true);
    await calculateXYZTileExtraInfo(sourcePath, source);
    assert.deepEqual(
      getXYZTileExtraInfo({
        source,
        tileBounds: [
          {
            z: 2,
            x: [1, 1],
            y: [0, 0],
          },
        ],
      }),
      {
        "2/1/0": calculateMD5(tileData),
      },
    );

    await unlink(tilePath);
    await calculateXYZTileExtraInfo(sourcePath, source);
    assert.deepEqual(
      getXYZTileExtraInfo({
        source,
        tileBounds: [
          {
            z: 2,
            x: [1, 1],
            y: [0, 0],
          },
        ],
      }),
      {},
    );

    closeXYZMD5DB(source);
    source = undefined;
  } finally {
    if (source) {
      const { closeXYZMD5DB } = await import("../src/resources/tile_xyz.js");
      closeXYZMD5DB(source);
    }

    if (previousDataDir === undefined) {
      delete process.env.DATA_DIR;
    } else {
      process.env.DATA_DIR = previousDataDir;
    }
    await rm(root, {
      recursive: true,
      force: true,
    });
  }
});

test("request host prefers referer and supports a reverse-proxy prefix", () => {
  assert.equal(
    getRequestHost({
      query: {
        referer: "https://public.example/tile-server",
      },
      headers: {},
    }),
    "https://public.example/tile-server",
  );
  assert.equal(
    getRequestHost({
      query: {},
      protocol: "http",
      headers: {
        host: "internal:8080",
        "x-forwarded-proto": "https",
        "x-forwarded-host": "ms.c4i.vn",
        "x-prefix": "/ms-tile-server",
      },
    }),
    "https://ms.c4i.vn/ms-tile-server",
  );
  assert.equal(isLocalURL("xyz://tiles"), true);
  assert.equal(isURL("http-not-a-url"), false);
});

test("HTTP 400 includes response detail and is not retried", async () => {
  let requests = 0;
  const server = http.createServer((_req, res) => {
    requests += 1;
    res.writeHead(400, {
      "content-type": "application/json",
    });
    res.end(
      JSON.stringify({
        error: "invalid tile bounds",
      }),
    );
  });

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();
    await assert.rejects(
      getDataFromURL(`http://127.0.0.1:${address.port}/extra-info`, {
        method: "POST",
        timeout: 1000,
        body: {},
        responseType: "json",
        keepAlive: true,
        headers: {},
        decompress: true,
        maxTry: 3,
      }),
      (error) => {
        assert.equal(error.statusCode, 400);
        assert.match(error.message, /invalid tile bounds/);

        return true;
      },
    );
    assert.equal(requests, 1);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
  }
});

test("HTTP 204 and 404 are not retried", async () => {
  const requests = new Map();
  const server = http.createServer((req, res) => {
    requests.set(req.url, (requests.get(req.url) ?? 0) + 1);
    res.writeHead(req.url === "/no-content" ? 204 : 404);
    res.end();
  });

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();

    for (const endpoint of ["/no-content", "/not-found"]) {
      await assert.rejects(
        getDataFromURL(`http://127.0.0.1:${address.port}${endpoint}`, {
          method: "GET",
          timeout: 1000,
          responseType: "arraybuffer",
          headers: {},
          decompress: false,
          maxTry: 3,
        }),
      );
    }

    assert.deepEqual(Object.fromEntries(requests), {
      "/no-content": 1,
      "/not-found": 1,
    });
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
  }
});

test("JSON schemas are read once and reused", async () => {
  const [first, second] = await Promise.all([
    getJSONSchema("tile_bounds"),
    getJSONSchema("tile_bounds"),
  ]);

  assert.equal(first, second);
});

test("Swagger keeps the public prefix and host", async () => {
  const app = express().enable("trust proxy");
  serve_swagger.init(app);
  const server = http.createServer(app);

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  try {
    const address = server.address();
    const baseURL = `http://127.0.0.1:${address.port}`;
    const headers = {
      "x-forwarded-host": "ms.c4i.vn",
      "x-forwarded-proto": "https",
      "x-prefix": "/ms-tile-server",
    };
    const redirect = await fetch(`${baseURL}/swagger`, {
      headers,
      redirect: "manual",
    });

    assert.equal(redirect.status, 308);
    assert.equal(redirect.headers.get("location"), "/ms-tile-server/swagger/");

    const page = await fetch(`${baseURL}/swagger/`, {
      headers,
    });
    assert.equal(page.status, 200);
    assert.match(await page.text(), /\.\/swagger-ui-init\.js/);

    const initializer = await fetch(`${baseURL}/swagger/swagger-ui-init.js`, {
      headers,
    });
    assert.equal(initializer.status, 200);
    assert.match(
      await initializer.text(),
      /https:\/\/ms\.c4i\.vn\/ms-tile-server/,
    );

    const refererURL = "https://release.c4i.vn/tile-server";
    const refererRedirect = await fetch(
      `${baseURL}/swagger?referer=${encodeURIComponent(refererURL)}`,
      {
        redirect: "manual",
      },
    );
    assert.equal(
      refererRedirect.headers.get("location"),
      `/swagger/?referer=${encodeURIComponent(refererURL)}`,
    );

    const refererPage = await fetch(
      `${baseURL}/swagger/?referer=${encodeURIComponent(refererURL)}`,
    );
    assert.match(
      await refererPage.text(),
      new RegExp(
        `swagger-ui-init\\.js\\?referer=${encodeURIComponent(refererURL)}`,
      ),
    );

    const refererInitializer = await fetch(
      `${baseURL}/swagger/swagger-ui-init.js?referer=${encodeURIComponent(refererURL)}`,
    );
    assert.match(
      await refererInitializer.text(),
      /https:\/\/release\.c4i\.vn\/tile-server/,
    );
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => {
        return error ? reject(error) : resolve();
      });
    });
  }
});
