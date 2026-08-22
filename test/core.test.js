"use strict";

import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import http from "node:http";
import test from "node:test";
import express from "express";
import { serve_swagger } from "../src/serves/serve_swagger.js";
import {
  findFiles,
  calculateMD5,
  getDataFromURL,
  getJSONSchema,
  getRequestHost,
  getTileBoundsBatches,
  isExistFile,
  isLocalURL,
  isURL,
  closeSQLite,
  openSQLite,
  removeEmptyFolders,
  runAllWithLimit,
  runSingleFlight,
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
