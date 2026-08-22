"use strict";

import { DEFAULT_CONCURRENCY } from "../defaults/index.js";
import { config } from "../configs/index.js";
import sharp from "sharp";
import {
  getAndCacheDataStyleJSON,
  getAndCacheDataGeoJSON,
  getRenderedStyleJSON,
} from "../resources/index.js";
import {
  calculateMaxZoom,
  transformBBoxSRS,
  getRequestHost,
  splitParameter,
  getParameter,
  xmlEscape,
} from "../utils/index.js";
import {
  getStaticRendererPool,
  renderImageStaticData,
} from "../render_style.js";

const WMS_1_1_1 = "1.1.1";
const WMS_1_3_0 = "1.3.0";
const WGS84_BBOX = [-180, -85.051129, 180, 85.051129];
const DEFAULT_MAX_SIZE = 4096;
const CAPABILITIES_UPDATE_SEQUENCE = String(Date.now());
const SUPPORTED_IMAGE_FORMATS = new Map([
  ["image/png", "png"],
  ["image/jpeg", "jpeg"],
  ["image/jpg", "jpeg"],
  ["image/webp", "webp"],
]);
const SUPPORTED_INFO_FORMATS = new Set([
  "application/json",
  "application/geo+json",
  "text/plain",
  "text/html",
]);

class WMSException extends Error {
  constructor(code, message) {
    super(message);
    this.name = "WMSException";
    this.code = code;
  }
}

function getParameters(req) {
  const parameters = {
    ...(req.body && typeof req.body === "object" ? req.body : {}),
    ...(req.query ?? {}),
  };

  return parameters;
}

function normalizeVersion(value) {
  const version = String(value ?? WMS_1_3_0);

  if (version !== WMS_1_1_1 && version !== WMS_1_3_0) {
    throw new WMSException(
      "InvalidVersion",
      `Unsupported WMS version "${version}".`,
    );
  }

  return version;
}

function normalizeCRS(value) {
  const crs = String(value ?? "")
    .trim()
    .toUpperCase();

  if (!crs) {
    throw new WMSException("MissingParameterValue", "CRS/SRS is required.");
  }

  if (
    crs === "URN:OGC:DEF:CRS:OGC:1.3:CRS84" ||
    crs === "URN:OGC:DEF:CRS:OGC::CRS84" ||
    crs === "CRS84"
  ) {
    return "CRS:84";
  }

  const epsgURN = crs.match(/^URN:OGC:DEF:CRS:EPSG:(?::|\d+:)(\d+)$/);
  if (epsgURN) {
    return `EPSG:${epsgURN[1]}`;
  }

  return crs;
}

function projectionCRS(crs) {
  return crs === "CRS:84" ? "EPSG:4326" : crs;
}

function hasInvertedAxis(version, crs) {
  return version === WMS_1_3_0 && crs === "EPSG:4326";
}

/** Parse a WMS BBOX and return WGS84 [west, south, east, north]. */
export function parseWMSBBox(value, version, crsValue) {
  const crs = normalizeCRS(crsValue);
  const values = String(value ?? "")
    .split(",")
    .map(Number);

  if (
    values.length !== 4 ||
    values.some((item) => {
      return !Number.isFinite(item);
    })
  ) {
    throw new WMSException(
      "InvalidParameterValue",
      "BBOX must contain four numbers.",
    );
  }

  let bounds = values;
  if (hasInvertedAxis(version, crs)) {
    bounds = [values[1], values[0], values[3], values[2]];
  }

  if (bounds[0] >= bounds[2] || bounds[1] >= bounds[3]) {
    throw new WMSException(
      "InvalidParameterValue",
      "BBOX coordinates are not ordered.",
    );
  }

  try {
    const transformed = transformBBoxSRS({
      srcSRS: projectionCRS(crs),
      dstSRS: "EPSG:4326",
      bounds,
    });

    if (
      transformed[0] >= transformed[2] ||
      transformed[1] >= transformed[3] ||
      transformed.some((item) => {
        return !Number.isFinite(item);
      })
    ) {
      throw new Error("invalid transformed bounds");
    }

    return transformed;
  } catch (error) {
    throw new WMSException(
      "InvalidCRS",
      `CRS "${crs}" is not supported: ${error.message}`,
    );
  }
}

let wmsLayerCache;

function getLayer(id) {
  const styles = config.styles ?? {};
  if (!wmsLayerCache || wmsLayerCache.styles !== styles) {
    wmsLayerCache = {
      styles,
      layers: new Map(),
    };
  }

  const item = styles[id];

  if (!item) {
    throw new WMSException("LayerNotDefined", `Layer "${id}" does not exist.`);
  }

  const cached = wmsLayerCache.layers.get(id);
  if (cached?.item === item) {
    return cached.layer;
  }

  const metadata = item.tileJSON ?? {};
  const wms = item.wms ?? {};

  const layer = {
    id,
    item,
    title: wms.title ?? metadata.name ?? item.name ?? id,
    abstract: wms.abstract ?? metadata.description ?? item.name ?? id,
    bbox: wms.bbox ?? item.bbox ?? metadata.bounds ?? WGS84_BBOX,
    queryable: Boolean(wms.queryable),
    dimensions: wms.dimensions ?? {},
  };

  wmsLayerCache.layers.set(id, {
    item,
    layer,
  });
  return layer;
}

function resolveLayers(pathId, parameters) {
  const layerIds = pathId
    ? [pathId]
    : splitParameter(getParameter(parameters, "LAYERS"));

  if (!layerIds.length) {
    throw new WMSException("LayerNotDefined", "LAYERS is required.");
  }

  return layerIds.map(getLayer);
}

function getServiceMetadata() {
  const options = config.options?.wms ?? {};

  return {
    title: options.title ?? "Tile Server WMS",
    abstract:
      options.abstract ?? "Web Map Service generated from MapLibre styles.",
    keywords: options.keywords ?? ["WMS", "OGC", "MapLibre"],
    fees: options.fees ?? "none",
    accessConstraints: options.accessConstraints ?? "none",
    contact: options.contact ?? {},
  };
}

function operationURL(url, operation, formats) {
  const formatXML = formats
    .map((format) => {
      return `<Format>${xmlEscape(format)}</Format>`;
    })
    .join("");

  return `<${operation}>${formatXML}<DCPType><HTTP><Get><OnlineResource xlink:href="${xmlEscape(url)}"/></Get></HTTP></DCPType></${operation}>`;
}

function dimensionXML(layer) {
  return Object.entries(layer.dimensions)
    .map(([name, value]) => {
      const dimension =
        typeof value === "string"
          ? {
              values: value,
            }
          : value;
      return `<Dimension name="${xmlEscape(name)}" units="${xmlEscape(dimension.units ?? "ISO8601")}"${dimension.unitSymbol ? ` unitSymbol="${xmlEscape(dimension.unitSymbol)}"` : ""}>${xmlEscape(dimension.values ?? "")}</Dimension>`;
    })
    .join("");
}

function capabilitiesLayer(layer, version, baseURL) {
  const bbox = layer.bbox;
  const style = `<Style><Name>default</Name><Title>Default</Title><LegendURL width="24" height="24"><Format>image/png</Format><OnlineResource xlink:href="${xmlEscape(`${baseURL}?SERVICE=WMS&VERSION=${version}&REQUEST=GetLegendGraphic&FORMAT=image/png&LAYER=${encodeURIComponent(layer.id)}`)}"/></LegendURL></Style>`;

  if (version === WMS_1_1_1) {
    return `<Layer queryable="${layer.queryable ? 1 : 0}"><Name>${xmlEscape(layer.id)}</Name><Title>${xmlEscape(layer.title)}</Title><Abstract>${xmlEscape(layer.abstract)}</Abstract><SRS>CRS:84</SRS><SRS>EPSG:4326</SRS><SRS>EPSG:3857</SRS><LatLonBoundingBox minx="${bbox[0]}" miny="${bbox[1]}" maxx="${bbox[2]}" maxy="${bbox[3]}"/>${style}${dimensionXML(layer)}</Layer>`;
  }

  return `<Layer queryable="${layer.queryable ? 1 : 0}"><Name>${xmlEscape(layer.id)}</Name><Title>${xmlEscape(layer.title)}</Title><Abstract>${xmlEscape(layer.abstract)}</Abstract><CRS>CRS:84</CRS><CRS>EPSG:4326</CRS><CRS>EPSG:3857</CRS><EX_GeographicBoundingBox><westBoundLongitude>${bbox[0]}</westBoundLongitude><eastBoundLongitude>${bbox[2]}</eastBoundLongitude><southBoundLatitude>${bbox[1]}</southBoundLatitude><northBoundLatitude>${bbox[3]}</northBoundLatitude></EX_GeographicBoundingBox><BoundingBox CRS="EPSG:3857" minx="-20037508.34" miny="-20037508.34" maxx="20037508.34" maxy="20037508.34"/>${style}${dimensionXML(layer)}</Layer>`;
}

/** Build a WMS capabilities document for the supplied layer set. */
export function buildCapabilities({ version, baseURL, layers }) {
  const service = getServiceMetadata();
  const layerXML = layers
    .map((layer) => {
      return capabilitiesLayer(layer, version, baseURL);
    })
    .join("");
  const formats = ["image/png", "image/jpeg", "image/webp"];
  const infoFormats = Array.from(SUPPORTED_INFO_FORMATS);
  const commonRequest = [
    operationURL(baseURL, "GetCapabilities", [
      "text/xml",
      "application/vnd.ogc.wms_xml",
    ]),
    operationURL(baseURL, "GetMap", formats),
    operationURL(baseURL, "GetFeatureInfo", infoFormats),
    operationURL(baseURL, "GetLegendGraphic", formats.concat("image/svg+xml")),
    operationURL(baseURL, "GetStyles", ["application/vnd.ogc.sld+xml"]),
    operationURL(baseURL, "DescribeLayer", [
      "text/xml",
      "application/vnd.ogc.wms_xml",
    ]),
  ].join("");

  if (version === WMS_1_1_1) {
    return `<?xml version="1.0" encoding="UTF-8"?><WMT_MS_Capabilities version="1.1.1" updateSequence="${xmlEscape(CAPABILITIES_UPDATE_SEQUENCE)}" xmlns="http://www.opengis.net/wms" xmlns:xlink="http://www.w3.org/1999/xlink"><Service><Name>WMS</Name><Title>${xmlEscape(service.title)}</Title><Abstract>${xmlEscape(service.abstract)}</Abstract><KeywordList>${service.keywords
      .map((keyword) => {
        return `<Keyword>${xmlEscape(keyword)}</Keyword>`;
      })
      .join(
        "",
      )}</KeywordList><OnlineResource xlink:href="${xmlEscape(baseURL)}"/><Fees>${xmlEscape(service.fees)}</Fees><AccessConstraints>${xmlEscape(service.accessConstraints)}</AccessConstraints></Service><Capability><Request>${commonRequest}</Request><Exception><Format>application/vnd.ogc.se_xml</Format><Format>application/vnd.ogc.se_inimage</Format><Format>application/vnd.ogc.se_blank</Format></Exception><Layer><Title>${xmlEscape(service.title)}</Title><SRS>CRS:84</SRS><SRS>EPSG:4326</SRS><SRS>EPSG:3857</SRS>${layerXML}</Layer></Capability></WMT_MS_Capabilities>`;
  }

  return `<?xml version="1.0" encoding="UTF-8"?><WMS_Capabilities version="1.3.0" updateSequence="${xmlEscape(CAPABILITIES_UPDATE_SEQUENCE)}" xmlns="http://www.opengis.net/wms" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd"><Service><Name>WMS</Name><Title>${xmlEscape(service.title)}</Title><Abstract>${xmlEscape(service.abstract)}</Abstract><KeywordList>${service.keywords
    .map((keyword) => {
      return `<Keyword>${xmlEscape(keyword)}</Keyword>`;
    })
    .join(
      "",
    )}</KeywordList><OnlineResource xlink:href="${xmlEscape(baseURL)}"/><Fees>${xmlEscape(service.fees)}</Fees><AccessConstraints>${xmlEscape(service.accessConstraints)}</AccessConstraints></Service><Capability><Request>${commonRequest}</Request><Exception><Format>XML</Format><Format>INIMAGE</Format><Format>BLANK</Format></Exception><Layer><Title>${xmlEscape(service.title)}</Title><CRS>CRS:84</CRS><CRS>EPSG:4326</CRS><CRS>EPSG:3857</CRS>${layerXML}</Layer></Capability></WMS_Capabilities>`;
}

function parseImageFormat(value) {
  const format = String(value ?? "image/png").toLowerCase();
  const output = SUPPORTED_IMAGE_FORMATS.get(format);

  if (!output) {
    throw new WMSException(
      "InvalidFormat",
      `Image format "${format}" is not supported.`,
    );
  }

  return {
    format,
    output,
  };
}

function parseInteger(value, name, min, max) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new WMSException(
      "InvalidParameterValue",
      `${name} must be an integer between ${min} and ${max}.`,
    );
  }

  return parsed;
}

function parseColor(value) {
  const text = String(value ?? "0xFFFFFF").replace(/^0x/i, "#");
  if (!/^#[0-9a-f]{6}$/i.test(text)) {
    throw new WMSException(
      "InvalidParameterValue",
      "BGCOLOR must be a hexadecimal RGB color.",
    );
  }

  return text;
}

function colorToRGBA(color) {
  const value = String(color ?? "#808080");
  const match = value.match(/^#([0-9a-f]{6})([0-9a-f]{2})?$/i);
  if (match) {
    return `#${match[1]}${match[2] ?? ""}`;
  }

  return /^rgba?\(/i.test(value) ? value : "#808080";
}

function literalValue(value, fallback) {
  if (typeof value === "string" || typeof value === "number") {
    return value;
  }

  if (Array.isArray(value)) {
    for (const child of value.slice(1)) {
      const result = literalValue(child, undefined);
      if (result !== undefined) {
        return result;
      }
    }
  }

  return fallback;
}

function styleColor(layer, property, fallback) {
  return colorToRGBA(literalValue(layer.paint?.[property], fallback));
}

function legendSVG(styleJSON, title, width, height) {
  const layers = (styleJSON.layers ?? []).filter((layer) => {
    return layer.layout?.visibility !== "none";
  });
  const rowHeight = 26;
  const totalHeight = Math.max(height, 32 + layers.length * rowHeight);
  const rows = layers
    .map((layer, index) => {
      const y = 28 + index * rowHeight;
      const type = layer.type;
      let symbol = `<rect x="8" y="${y - 15}" width="20" height="14" fill="${styleColor(layer, "fill-color", "#808080")}" stroke="${styleColor(layer, "fill-outline-color", "#404040")}"/>`;
      if (type === "line") {
        symbol = `<line x1="8" y1="${y - 8}" x2="28" y2="${y - 8}" stroke="${styleColor(layer, "line-color", "#404040")}" stroke-width="${Number(literalValue(layer.paint?.["line-width"], 2)) || 2}"/>`;
      } else if (type === "circle" || type === "symbol") {
        symbol = `<circle cx="18" cy="${y - 8}" r="6" fill="${styleColor(layer, "circle-color", "#808080")}" stroke="${styleColor(layer, "circle-stroke-color", "#404040")}"/>`;
      }

      return `${symbol}<text x="38" y="${y - 3}" font-family="sans-serif" font-size="12" fill="#222">${xmlEscape(layer.metadata?.wmsTitle ?? layer.id)}</text>`;
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?><svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${totalHeight}" viewBox="0 0 ${width} ${totalHeight}"><rect width="100%" height="100%" fill="white"/><text x="8" y="18" font-family="sans-serif" font-size="14" font-weight="bold" fill="#111">${xmlEscape(title)}</text>${rows}</svg>`;
}

function sldColor(layer, property, fallback) {
  const value = literalValue(layer.paint?.[property], fallback);
  return typeof value === "string" && value.startsWith("#") ? value : fallback;
}

function styleToSLD(styleJSON, layerName, title) {
  const rules = (styleJSON.layers ?? [])
    .map((layer) => {
      let symbolizer;
      if (layer.type === "line") {
        symbolizer = `<LineSymbolizer><Stroke><CssParameter name="stroke">${xmlEscape(sldColor(layer, "line-color", "#808080"))}</CssParameter><CssParameter name="stroke-width">${xmlEscape(literalValue(layer.paint?.["line-width"], 1))}</CssParameter></Stroke></LineSymbolizer>`;
      } else if (layer.type === "circle") {
        symbolizer = `<PointSymbolizer><Graphic><Mark><WellKnownName>circle</WellKnownName><Fill><CssParameter name="fill">${xmlEscape(sldColor(layer, "circle-color", "#808080"))}</CssParameter></Fill></Mark><Size>${xmlEscape(literalValue(layer.paint?.["circle-radius"], 5) * 2)}</Size></Graphic></PointSymbolizer>`;
      } else if (layer.type === "fill") {
        symbolizer = `<PolygonSymbolizer><Fill><CssParameter name="fill">${xmlEscape(sldColor(layer, "fill-color", "#808080"))}</CssParameter><CssParameter name="fill-opacity">${xmlEscape(literalValue(layer.paint?.["fill-opacity"], 1))}</CssParameter></Fill></PolygonSymbolizer>`;
      } else {
        return "";
      }

      return `<Rule><Name>${xmlEscape(layer.id)}</Name><Title>${xmlEscape(layer.id)}</Title>${symbolizer}</Rule>`;
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?><StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><NamedLayer><Name>${xmlEscape(layerName)}</Name><UserStyle><Name>default</Name><Title>${xmlEscape(title)}</Title><FeatureTypeStyle>${rules}</FeatureTypeStyle></UserStyle></NamedLayer></StyledLayerDescriptor>`;
}

function geometryBBox(geometry) {
  const points = [];
  const collect = (value) => {
    if (!Array.isArray(value)) {
      return;
    }
    if (
      value.length >= 2 &&
      typeof value[0] === "number" &&
      typeof value[1] === "number"
    ) {
      points.push(value);
      return;
    }
    value.forEach(collect);
  };
  collect(geometry?.coordinates);
  if (!points.length) {
    return undefined;
  }
  return [
    Math.min(
      ...points.map((point) => {
        return point[0];
      }),
    ),
    Math.min(
      ...points.map((point) => {
        return point[1];
      }),
    ),
    Math.max(
      ...points.map((point) => {
        return point[0];
      }),
    ),
    Math.max(
      ...points.map((point) => {
        return point[1];
      }),
    ),
  ];
}

function distanceToSegment(point, start, end) {
  const dx = end[0] - start[0];
  const dy = end[1] - start[1];
  if (!dx && !dy) {
    return Math.hypot(point[0] - start[0], point[1] - start[1]);
  }
  const t = Math.max(
    0,
    Math.min(
      1,
      ((point[0] - start[0]) * dx + (point[1] - start[1]) * dy) /
        (dx * dx + dy * dy),
    ),
  );
  return Math.hypot(
    point[0] - (start[0] + t * dx),
    point[1] - (start[1] + t * dy),
  );
}

function pointInRing(point, ring) {
  let inside = false;
  for (
    let index = 0, previous = ring.length - 1;
    index < ring.length;
    previous = index++
  ) {
    const currentPoint = ring[index];
    const previousPoint = ring[previous];
    const intersects =
      currentPoint[1] > point[1] !== previousPoint[1] > point[1] &&
      point[0] <
        ((previousPoint[0] - currentPoint[0]) * (point[1] - currentPoint[1])) /
          (previousPoint[1] - currentPoint[1]) +
          currentPoint[0];
    if (intersects) {
      inside = !inside;
    }
  }
  return inside;
}

function pointHitsGeometry(point, geometry, tolerance) {
  if (!geometry) {
    return false;
  }
  const bbox = geometryBBox(geometry);
  if (
    bbox &&
    (point[0] < bbox[0] - tolerance ||
      point[0] > bbox[2] + tolerance ||
      point[1] < bbox[1] - tolerance ||
      point[1] > bbox[3] + tolerance)
  ) {
    return false;
  }

  const hitLine = (line) => {
    return line.some((coordinate, index) => {
      return index === 0
        ? Math.hypot(point[0] - coordinate[0], point[1] - coordinate[1]) <=
            tolerance
        : distanceToSegment(point, line[index - 1], coordinate) <= tolerance;
    });
  };

  switch (geometry.type) {
    case "Point":
      return (
        Math.hypot(
          point[0] - geometry.coordinates[0],
          point[1] - geometry.coordinates[1],
        ) <= tolerance
      );
    case "MultiPoint":
      return geometry.coordinates.some((coordinate) => {
        return (
          Math.hypot(point[0] - coordinate[0], point[1] - coordinate[1]) <=
          tolerance
        );
      });
    case "LineString":
      return hitLine(geometry.coordinates);
    case "MultiLineString":
      return geometry.coordinates.some(hitLine);
    case "Polygon":
      return (
        pointInRing(point, geometry.coordinates[0]) &&
        !geometry.coordinates.slice(1).some((ring) => {
          return pointInRing(point, ring);
        })
      );
    case "MultiPolygon":
      return geometry.coordinates.some((polygon) => {
        return (
          pointInRing(point, polygon[0]) &&
          !polygon.slice(1).some((ring) => {
            return pointInRing(point, ring);
          })
        );
      });
    case "GeometryCollection":
      return geometry.geometries.some((child) => {
        return pointHitsGeometry(point, child, tolerance);
      });
    default:
      return false;
  }
}

function asFeatureArray(data) {
  if (!data) {
    return [];
  }
  if (data.type === "FeatureCollection") {
    return data.features ?? [];
  }
  if (data.type === "Feature") {
    return [data];
  }
  return [
    {
      type: "Feature",
      properties: {},
      geometry: data,
    },
  ];
}

async function getGeoJSONForSource(source) {
  if (source?.data && typeof source.data === "object") {
    return source.data;
  }
  if (
    typeof source?.data !== "string" ||
    !source.data.startsWith("geojson://")
  ) {
    return undefined;
  }
  const parts = source.data.split("/");
  if (!parts[2] || !parts[3]) {
    return undefined;
  }
  return JSON.parse(await getAndCacheDataGeoJSON(parts[2], parts[3]));
}

async function queryGeoJSONLayers(layerIds, point, tolerance, featureCount) {
  const result = [];
  for (const layer of layerIds) {
    let styleJSON;
    try {
      styleJSON = JSON.parse(await getAndCacheDataStyleJSON(layer.item.path));
    } catch {
      continue;
    }

    const sources = new Map();
    for (const [sourceId, source] of Object.entries(styleJSON.sources ?? {})) {
      const data = await getGeoJSONForSource(source).catch(() => {
        return undefined;
      });
      if (data) {
        sources.set(sourceId, data);
      }
    }

    for (const styleLayer of styleJSON.layers ?? []) {
      const data = sources.get(styleLayer.source);
      if (!data) {
        continue;
      }
      for (const feature of asFeatureArray(data)) {
        if (!pointHitsGeometry(point, feature.geometry, tolerance)) {
          continue;
        }
        result.push({
          type: "Feature",
          id: feature.id,
          geometry: feature.geometry,
          properties: feature.properties ?? {},
          layer: styleLayer.id,
        });
        if (result.length >= featureCount) {
          return result;
        }
      }
    }
  }
  return result;
}

function infoResponse(features, format) {
  if (format === "application/json" || format === "application/geo+json") {
    return {
      contentType: format,
      body: JSON.stringify({
        type: "FeatureCollection",
        features,
      }),
    };
  }
  if (format === "text/html") {
    const rows = features
      .flatMap((feature) => {
        return Object.entries(feature.properties ?? {}).map(([key, value]) => {
          return `<tr><th>${xmlEscape(key)}</th><td>${xmlEscape(typeof value === "object" ? JSON.stringify(value) : value)}</td></tr>`;
        });
      })
      .join("");
    return {
      contentType: "text/html",
      body: `<table><caption>${features.length} feature(s)</caption>${rows}</table>`,
    };
  }
  return {
    contentType: "text/plain",
    body: features.length
      ? features
          .map((feature) => {
            return `Layer: ${feature.layer}\n${Object.entries(
              feature.properties ?? {},
            )
              .map(([key, value]) => {
                return `${key}=${typeof value === "object" ? JSON.stringify(value) : value}`;
              })
              .join("\n")}`;
          })
          .join("\n\n")
      : "No features found.",
  };
}

function parseMapRequest(layers, parameters, version) {
  const crs = normalizeCRS(
    getParameter(parameters, version === WMS_1_3_0 ? "CRS" : "SRS"),
  );
  const bbox = parseWMSBBox(getParameter(parameters, "BBOX"), version, crs);
  const width = parseInteger(
    getParameter(parameters, "WIDTH"),
    "WIDTH",
    1,
    getWMSOptions().maxWidth ?? DEFAULT_MAX_SIZE,
  );
  const height = parseInteger(
    getParameter(parameters, "HEIGHT"),
    "HEIGHT",
    1,
    getWMSOptions().maxHeight ?? DEFAULT_MAX_SIZE,
  );
  const image = parseImageFormat(getParameter(parameters, "FORMAT"));
  const styles = splitParameter(getParameter(parameters, "STYLES"));
  if (
    styles.length &&
    (styles.length !== layers.length ||
      styles.some((style) => {
        return style !== "default";
      }))
  ) {
    throw new WMSException(
      "StyleNotDefined",
      "Only the default style is available for WMS layers.",
    );
  }

  const transparent =
    String(getParameter(parameters, "TRANSPARENT", "FALSE")).toUpperCase() ===
    "TRUE";
  const background = parseColor(
    getParameter(parameters, "BGCOLOR", "0xFFFFFF"),
  );
  const dpi = Number(getParameter(parameters, "DPI", 96));
  const tileScale =
    Number.isFinite(dpi) && dpi > 0 ? Math.min(Math.max(dpi / 96, 1), 4) : 1;
  const zoom = calculateMaxZoom(bbox, width, height, 256);
  return {
    bbox,
    width,
    height,
    image,
    transparent,
    background,
    tileScale,
    zoom,
  };
}

async function renderMap(layers, parameters, version) {
  const {
    bbox,
    width,
    height,
    image,
    transparent,
    background,
    tileScale,
    zoom,
  } = parseMapRequest(layers, parameters, version);
  const rendered = await Promise.all(
    layers.map(async (layer) => {
      const styleJSON = await getRenderedStyleJSON(layer.item.path);
      return renderImageStaticData({
        styleJSON,
        pool: getStaticRendererPool({
          key: `wms:${layer.id}:${tileScale}`,
          styleJSON,
          tileScale,
          max: DEFAULT_CONCURRENCY,
        }),
        tileScale,
        tileSize: 256,
        zoom,
        bbox,
        width,
        height,
        format: "png",
        resizeOption: {
          fit: "fill",
        },
      });
    }),
  );

  let output = rendered[0];
  if (rendered.length > 1) {
    output = await sharp({
      create: {
        width,
        height,
        channels: 4,
        background: {
          r: 0,
          g: 0,
          b: 0,
          alpha: 0,
        },
      },
    })
      .composite(
        rendered.map((input) => {
          return {
            input,
          };
        }),
      )
      .png()
      .toBuffer();
  }

  let outputImage = sharp(output);
  if (!transparent || image.output !== "png") {
    outputImage = outputImage.flatten({
      background,
    });
  }
  outputImage = outputImage.toFormat(image.output);
  return {
    data: await outputImage.toBuffer(),
    contentType: image.format,
    bbox,
    width,
    height,
  };
}

async function sendException(res, error, parameters) {
  const code = error.code ?? "InternalError";
  const message =
    error instanceof WMSException
      ? error.message
      : "The WMS request could not be completed.";
  const exceptionFormat = String(
    getParameter(parameters, "EXCEPTIONS", "application/vnd.ogc.se_xml"),
  ).toLowerCase();

  if (exceptionFormat.includes("se_inimage") || exceptionFormat === "inimage") {
    const width = Math.min(
      Math.max(Number(getParameter(parameters, "WIDTH", 256)) || 256, 1),
      DEFAULT_MAX_SIZE,
    );
    const height = Math.min(
      Math.max(Number(getParameter(parameters, "HEIGHT", 256)) || 256, 1),
      DEFAULT_MAX_SIZE,
    );
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}"><rect width="100%" height="100%" fill="white"/><text x="8" y="20" font-family="sans-serif" font-size="14" fill="red">${xmlEscape(`${code}: ${message}`)}</text></svg>`;
    res
      .status(400)
      .set("content-type", "image/png")
      .send(await sharp(Buffer.from(svg)).png().toBuffer());
    return;
  }

  if (exceptionFormat.includes("se_blank") || exceptionFormat === "blank") {
    res
      .status(400)
      .set("content-type", "image/png")
      .send(
        await sharp({
          create: {
            width: 1,
            height: 1,
            channels: 4,
            background: {
              r: 0,
              g: 0,
              b: 0,
              alpha: 0,
            },
          },
        })
          .png()
          .toBuffer(),
      );
    return;
  }

  const version = String(getParameter(parameters, "VERSION", WMS_1_3_0));
  const root =
    version === WMS_1_1_1 ? "ServiceExceptionReport" : "ServiceExceptionReport";
  res
    .status(400)
    .set("content-type", "text/xml; charset=utf-8")
    .send(
      `<?xml version="1.0" encoding="UTF-8"?><${root} version="${xmlEscape(version)}" xmlns="http://www.opengis.net/ogc"><ServiceException code="${xmlEscape(code)}">${xmlEscape(message)}</ServiceException></${root}>`,
    );
}

async function handleWMS(req, res, pathId) {
  const parameters = getParameters(req);
  let version;
  try {
    version = normalizeVersion(getParameter(parameters, "VERSION", WMS_1_3_0));
    const service = String(
      getParameter(parameters, "SERVICE", "WMS"),
    ).toUpperCase();
    if (service !== "WMS") {
      throw new WMSException("InvalidParameterValue", "SERVICE must be WMS.");
    }
    const request = String(
      getParameter(parameters, "REQUEST", "GetCapabilities"),
    ).toLowerCase();
    const host = getRequestHost(req);
    const baseURL = pathId
      ? `${host}/styles/${encodeURIComponent(pathId)}/wms`
      : `${host}/wms`;

    if (request === "getcapabilities") {
      const requestedSequence = getParameter(parameters, "UPDATESEQUENCE");
      if (requestedSequence !== undefined) {
        if (String(requestedSequence) === CAPABILITIES_UPDATE_SEQUENCE) {
          throw new WMSException(
            "CurrentUpdateSequence",
            "The requested capabilities document is current.",
          );
        }
        if (
          /^\d+$/.test(String(requestedSequence)) &&
          Number(requestedSequence) > Number(CAPABILITIES_UPDATE_SEQUENCE)
        ) {
          throw new WMSException(
            "InvalidUpdateSequence",
            "The requested update sequence is newer than this service.",
          );
        }
      }
      const layers = pathId
        ? [getLayer(pathId)]
        : Object.keys(config.styles ?? {}).map(getLayer);
      res.status(200).set("content-type", "text/xml; charset=utf-8").send(
        buildCapabilities({
          version,
          baseURL,
          layers,
        }),
      );
      return;
    }

    if (request === "getmap") {
      const result = await renderMap(
        resolveLayers(pathId, parameters),
        parameters,
        version,
      );
      res.status(200).set("content-type", result.contentType).send(result.data);
      return;
    }

    if (request === "getfeatureinfo") {
      const layers = resolveLayers(pathId, {
        ...parameters,
        LAYERS: getParameter(
          parameters,
          "QUERY_LAYERS",
          getParameter(parameters, "LAYERS"),
        ),
      });
      const infoFormat = String(
        getParameter(parameters, "INFO_FORMAT", "text/plain"),
      ).toLowerCase();
      if (!SUPPORTED_INFO_FORMATS.has(infoFormat)) {
        throw new WMSException(
          "InvalidFormat",
          `INFO_FORMAT "${infoFormat}" is not supported.`,
        );
      }
      const mapResult = parseMapRequest(layers, parameters, version);
      const x = parseInteger(
        getParameter(parameters, version === WMS_1_3_0 ? "I" : "X"),
        version === WMS_1_3_0 ? "I" : "X",
        0,
        mapResult.width - 1,
      );
      const y = parseInteger(
        getParameter(parameters, version === WMS_1_3_0 ? "J" : "Y"),
        version === WMS_1_3_0 ? "J" : "Y",
        0,
        mapResult.height - 1,
      );
      const tolerance =
        (Math.max(
          mapResult.bbox[2] - mapResult.bbox[0],
          mapResult.bbox[3] - mapResult.bbox[1],
        ) /
          Math.min(mapResult.width, mapResult.height)) *
        5;
      const point = [
        mapResult.bbox[0] +
          ((x + 0.5) / mapResult.width) *
            (mapResult.bbox[2] - mapResult.bbox[0]),
        mapResult.bbox[3] -
          ((y + 0.5) / mapResult.height) *
            (mapResult.bbox[3] - mapResult.bbox[1]),
      ];
      const features = await queryGeoJSONLayers(
        layers,
        point,
        tolerance,
        parseInteger(
          getParameter(parameters, "FEATURE_COUNT", 1),
          "FEATURE_COUNT",
          1,
          100,
        ),
      );
      const result = infoResponse(features, infoFormat);
      res.status(200).set("content-type", result.contentType).send(result.body);
      return;
    }

    if (request === "getlegendgraphic") {
      const id = pathId ?? getParameter(parameters, "LAYER");
      if (!id) {
        throw new WMSException("LayerNotDefined", "LAYER is required.");
      }
      const layer = getLayer(id);
      const format = String(
        getParameter(parameters, "FORMAT", "image/png"),
      ).toLowerCase();
      const width = parseInteger(
        getParameter(parameters, "WIDTH", 240),
        "WIDTH",
        1,
        DEFAULT_MAX_SIZE,
      );
      const height = parseInteger(
        getParameter(parameters, "HEIGHT", 32),
        "HEIGHT",
        1,
        DEFAULT_MAX_SIZE,
      );
      const styleJSON = await getRenderedStyleJSON(layer.item.path);
      const svg = Buffer.from(legendSVG(styleJSON, layer.title, width, height));
      if (format === "image/svg+xml") {
        res.status(200).set("content-type", format).send(svg);
        return;
      }
      const image = SUPPORTED_IMAGE_FORMATS.get(format);
      if (!image) {
        throw new WMSException(
          "InvalidFormat",
          `Legend format "${format}" is not supported.`,
        );
      }
      res
        .status(200)
        .set("content-type", format)
        .send(await sharp(svg).toFormat(image).toBuffer());
      return;
    }

    if (request === "getstyles") {
      const layer = getLayer(pathId ?? getParameter(parameters, "LAYER"));
      const styleJSON = JSON.parse(
        await getAndCacheDataStyleJSON(layer.item.path),
      );
      res
        .status(200)
        .set("content-type", "application/vnd.ogc.sld+xml")
        .send(styleToSLD(styleJSON, layer.id, layer.title));
      return;
    }

    if (request === "describelayer") {
      const layer = getLayer(pathId ?? getParameter(parameters, "LAYERS"));
      const url = `${host}/geojsons`;
      res
        .status(200)
        .set("content-type", "text/xml; charset=utf-8")
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><DescribeLayerResponse xmlns="http://www.opengis.net/sld" xmlns:ows="http://www.opengis.net/ows" version="${version}"><LayerDescription name="${xmlEscape(layer.id)}" owsType="WFS" owsURL="${xmlEscape(url)}" typeName="${xmlEscape(layer.id)}"/></DescribeLayerResponse>`,
        );
      return;
    }

    throw new WMSException(
      "OperationNotSupported",
      `Request "${request}" is not supported.`,
    );
  } catch (error) {
    await sendException(res, error, parameters);
  }
}

export const serve_wms = {
  init: (app) => {
    const register = (route) => {
      app.get(route, (req, res) => {
        return handleWMS(req, res, req.params.id);
      });
    };

    register("/wms");
    register("/wms/:id");
    register("/styles/:id/wms");
  },
};

export { WMSException, normalizeCRS, normalizeVersion };
