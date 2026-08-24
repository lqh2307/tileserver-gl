"use strict";

import { getAndCacheParsedDataGeoJSON } from "../resources/index.js";
import { config } from "../configs/index.js";
import {
  compileHandleBarsTemplate,
  transformPointSRS,
  transformBBoxSRS,
  getGeometryBBox,
  splitParameter,
  getRequestHost,
  getParameter,
  xmlEscape,
  min,
  max,
} from "../utils/index.js";

const WFS_VERSION = "2.0.0";
const WFS_NAMESPACE = "http://www.opengis.net/wfs";
const WFS_FEATURE_NAMESPACE = "http://www.example.com/wfs";
const GML32_NAMESPACE = "http://www.opengis.net/gml/3.2";
const MAX_FEATURES = 10000;
const DEFAULT_COUNT = 1000;

class WFSException extends Error {
  constructor(code, message) {
    super(message);
    this.name = "WFSException";
    this.code = code;
  }
}

function xmlName(value) {
  const name = String(value ?? "value").replace(/[^a-zA-Z0-9_.-]/g, "_");
  return /^[a-zA-Z_]/.test(name) ? name : `_${name}`;
}

function localName(value) {
  return String(value ?? "")
    .split(":")
    .pop();
}

function normalizeVersion(value) {
  const version = String(value ?? WFS_VERSION);
  if (version !== WFS_VERSION) {
    throw new WFSException(
      "VersionNegotiationFailed",
      `Unsupported WFS version "${version}".`,
    );
  }
  return version;
}

function normalizeCRS(value) {
  const crs = String(value ?? "EPSG:4326")
    .trim()
    .toUpperCase();
  if (crs === "CRS84" || crs === "CRS:84" || crs.includes("CRS84")) {
    return "EPSG:4326";
  }
  const urn = crs.match(/^URN:OGC:DEF:CRS:EPSG:(?::|\d+:)(\d+)$/);
  return urn ? `EPSG:${urn[1]}` : crs;
}

let featureTypesCache;

function getFeatureTypes() {
  const geojsons = config.geojsons ?? {};
  if (featureTypesCache?.source === geojsons) {
    return featureTypesCache.types;
  }

  const result = [];
  for (const [group, layers] of Object.entries(geojsons)) {
    for (const [layer, item] of Object.entries(layers ?? {})) {
      result.push({
        name: `${group}:${layer}`,
        group,
        layer,
        item,
        title: item.title ?? `${group} - ${layer}`,
      });
    }
  }
  const byName = new Map();
  const byPath = new Map();
  const byLayer = new Map();
  for (const type of result) {
    byName.set(type.name, type);
    byPath.set(`${type.group}/${type.layer}`, type);
    const aliases = byLayer.get(type.layer) ?? [];
    aliases.push(type);
    byLayer.set(type.layer, aliases);
  }
  featureTypesCache = {
    source: geojsons,
    types: result,
    byName,
    byPath,
    byLayer,
  };
  return result;
}

function resolveFeatureType(name) {
  const value = String(name ?? "");
  getFeatureTypes();
  const type =
    featureTypesCache.byName.get(value) ?? featureTypesCache.byPath.get(value);
  if (type) {
    return type;
  }

  const aliases = featureTypesCache.byLayer.get(value) ?? [];
  if (aliases.length === 1) {
    return aliases[0];
  }
  throw new WFSException(
    "InvalidParameterValue",
    `Feature type "${value}" does not exist.`,
  );
}

function resolveFeatureTypes(parameters, pathName) {
  const values = pathName
    ? [pathName]
    : splitParameter(
        getParameter(
          parameters,
          "TYPENAMES",
          getParameter(parameters, "TYPENAME"),
        ),
      );
  if (!values.length) {
    throw new WFSException("MissingParameterValue", "TYPENAMES is required.");
  }
  return values.map(resolveFeatureType);
}

async function readFeatures(type) {
  const data = await getAndCacheParsedDataGeoJSON(type.group, type.layer);
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

function featureId(feature, type, index) {
  return String(feature.id ?? `${type.group}.${type.layer}.${index + 1}`);
}

function bboxIntersects(first, second) {
  return !(
    first[2] < second[0] ||
    first[0] > second[2] ||
    first[3] < second[1] ||
    first[1] > second[3]
  );
}

function parseBBox(value) {
  if (!value) {
    return;
  }
  const parts = String(value).split(",");
  const values = parts.slice(0, 4).map(Number);
  if (
    parts.length < 4 ||
    values.some((item) => {
      return !Number.isFinite(item);
    })
  ) {
    throw new WFSException(
      "InvalidParameterValue",
      "BBOX must contain four numbers.",
    );
  }
  const crs = normalizeCRS(parts[4] ?? "EPSG:4326");
  try {
    return transformBBoxSRS({
      srcSRS: crs,
      dstSRS: "EPSG:4326",
      bounds: values.slice(0, 4),
    });
  } catch (error) {
    throw new WFSException(
      "InvalidParameterValue",
      `BBOX CRS is not supported: ${error.message}`,
    );
  }
}

function propertyValue(feature, name) {
  if (name === "id" || name === "fid") {
    return feature.id;
  }
  return feature.properties?.[name];
}

function compareValue(actual, expected, operator) {
  const left = actual instanceof Date ? actual.getTime() : actual;
  const right = expected instanceof Date ? expected.getTime() : expected;
  switch (operator) {
    case "eq":
      return String(left ?? "") === String(right ?? "");
    case "neq":
      return String(left ?? "") !== String(right ?? "");
    case "lt":
      return left < right;
    case "lte":
      return left <= right;
    case "gt":
      return left > right;
    case "gte":
      return left >= right;
    default:
      return false;
  }
}

function tagValue(xml, tag) {
  const match = String(xml ?? "").match(
    new RegExp(`<[^>]*${tag}[^>]*>([\\s\\S]*?)</[^>]*${tag}>`, "i"),
  );
  return match ? match[1].replace(/<[^>]+>/g, "").trim() : undefined;
}

function filterFromXML(xml) {
  if (!xml) {
    return;
  }
  const source = String(xml).replace(/\s+/g, " ");
  const resource = source.match(/ResourceId[^>]+(?:rid|fid)=["']([^"']+)["']/i);
  if (resource) {
    return {
      type: "id",
      value: resource[1],
    };
  }

  const bbox = source.match(
    /<(?:[^:>]+:)?BBOX[\s\S]*?>([\s\S]*?)<\/(?:[^:>]+:)?BBOX>/i,
  );
  if (bbox) {
    const lower =
      tagValue(bbox[1], "lowerCorner") ?? tagValue(bbox[1], "lowercorner");
    const upper =
      tagValue(bbox[1], "upperCorner") ?? tagValue(bbox[1], "uppercorner");
    if (lower && upper) {
      return {
        type: "bbox",
        value: parseBBox(
          `${lower.split(/\s+/).join(",")},${upper.split(/\s+/).join(",")}`,
        ),
      };
    }
  }

  const logical = source.match(
    /<(?:[^:>]+:)?(And|Or|Not)[^>]*>([\s\S]*?)<\/(?:[^:>]+:)?\1>/i,
  );
  if (logical) {
    const children = [
      ...logical[2].matchAll(
        /<(?:[^:>]+:)?(?:PropertyIs\w+|BBOX|ResourceId|And|Or|Not)[^>]*>[\s\S]*?<\/(?:[^:>]+:)?(?:PropertyIs\w+|BBOX|ResourceId|And|Or|Not)>/gi,
      ),
    ]
      .map((item) => {
        return filterFromXML(item[0]);
      })
      .filter(Boolean);
    return {
      type: logical[1].toLowerCase(),
      children,
    };
  }

  const comparison = source.match(
    /<(?:[^:>]+:)?(PropertyIsEqualTo|PropertyIsNotEqualTo|PropertyIsLessThan|PropertyIsLessThanOrEqualTo|PropertyIsGreaterThan|PropertyIsGreaterThanOrEqualTo)[^>]*>([\s\S]*?)<\/(?:[^:>]+:)?\1>/i,
  );
  if (comparison) {
    const property =
      tagValue(comparison[2], "PropertyName") ??
      tagValue(comparison[2], "ValueReference");
    const literal = tagValue(comparison[2], "Literal");
    const operator = {
      PropertyIsEqualTo: "eq",
      PropertyIsNotEqualTo: "neq",
      PropertyIsLessThan: "lt",
      PropertyIsLessThanOrEqualTo: "lte",
      PropertyIsGreaterThan: "gt",
      PropertyIsGreaterThanOrEqualTo: "gte",
    }[comparison[1]];
    return {
      type: "comparison",
      property,
      value: literal,
      operator,
    };
  }

  const like = source.match(
    /<(?:[^:>]+:)?PropertyIsLike[^>]*wildCard=["']([^"']+)["'][^>]*>([\s\S]*?)<\/(?:[^:>]+:)?PropertyIsLike>/i,
  );
  if (like) {
    return {
      type: "like",
      property: tagValue(like[2], "PropertyName"),
      value: tagValue(like[2], "Literal"),
      wildcard: like[1],
    };
  }
  return;
}

function filterFromCQL(value) {
  if (!value) {
    return;
  }
  const match = String(value).match(
    /^\s*([\w.-]+)\s*(=|<>|<=|>=|<|>)\s*['"]?([^'"]+)['"]?\s*$/i,
  );
  if (!match) {
    return;
  }
  return {
    type: "comparison",
    property: match[1],
    value: match[3],
    operator: {
      "=": "eq",
      "<>": "neq",
      "<": "lt",
      "<=": "lte",
      ">": "gt",
      ">=": "gte",
    }[match[2]],
  };
}

function matchesFilter(feature, filter, type, index) {
  if (!filter) {
    return true;
  }
  switch (filter.type) {
    case "id":
      return featureId(feature, type, index) === filter.value;
    case "bbox":
      return Boolean(
        getGeometryBBox(feature.geometry) &&
        bboxIntersects(getGeometryBBox(feature.geometry), filter.value),
      );
    case "comparison":
      return compareValue(
        propertyValue(feature, filter.property),
        filter.value,
        filter.operator,
      );
    case "like": {
      const expression = String(filter.value)
        .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
        .replaceAll(filter.wildcard, ".*");
      return new RegExp(`^${expression}$`, "i").test(
        String(propertyValue(feature, filter.property) ?? ""),
      );
    }
    case "and":
      return filter.children.every((child) => {
        return matchesFilter(feature, child, type, index);
      });
    case "or":
      return filter.children.some((child) => {
        return matchesFilter(feature, child, type, index);
      });
    case "not":
      return !filter.children.some((child) => {
        return matchesFilter(feature, child, type, index);
      });
    default:
      return true;
  }
}

function geometryTransform(geometry, srcCRS, dstCRS) {
  if (!geometry || srcCRS === dstCRS) {
    return geometry;
  }
  const coordinates = (value) => {
    if (Array.isArray(value) && typeof value[0] === "number") {
      return transformPointSRS({
        srcSRS: srcCRS,
        dstSRS: dstCRS,
        point: value.slice(0, 2),
      }).concat(value.slice(2));
    }
    return value?.map(coordinates);
  };
  if (geometry.type === "GeometryCollection") {
    return {
      ...geometry,
      geometries: geometry.geometries.map((item) => {
        return geometryTransform(item, srcCRS, dstCRS);
      }),
    };
  }
  return {
    ...geometry,
    coordinates: coordinates(geometry.coordinates),
  };
}

function projectFeature(feature, type, index, srsName) {
  return {
    ...feature,
    id: featureId(feature, type, index),
    geometry: geometryTransform(feature.geometry, "EPSG:4326", srsName),
  };
}

function fieldSchema(features) {
  const fields = new Map();
  for (const feature of features) {
    for (const [name, value] of Object.entries(feature.properties ?? {})) {
      const type =
        typeof value === "number"
          ? Number.isInteger(value)
            ? "integer"
            : "double"
          : typeof value === "boolean"
            ? "boolean"
            : "string";
      fields.set(
        name,
        fields.get(name) === "string" ? type : (fields.get(name) ?? type),
      );
    }
  }
  return fields;
}

function positionText(coordinate) {
  return coordinate.slice(0, 2).join(" ");
}

function gmlGeometry(geometry, gmlNS, srsName) {
  if (!geometry) {
    return "";
  }
  const attr = ` srsName="${xmlEscape(srsName)}"`;
  const line = (coordinates) => {
    return `<gml:LineString${attr}><gml:posList>${coordinates.map(positionText).join(" ")}</gml:posList></gml:LineString>`;
  };
  switch (geometry.type) {
    case "Point":
      return `<gml:Point${attr}><gml:pos>${positionText(geometry.coordinates)}</gml:pos></gml:Point>`;
    case "LineString":
      return line(geometry.coordinates);
    case "MultiLineString":
      return `<gml:MultiCurve${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:curveMember>${line(item)}</gml:curveMember>`;
        })
        .join("")}</gml:MultiCurve>`;
    case "Polygon":
      return `<gml:Polygon${attr}><gml:exterior><gml:LinearRing><gml:posList>${geometry.coordinates[0].map(positionText).join(" ")}</gml:posList></gml:LinearRing></gml:exterior>${geometry.coordinates
        .slice(1)
        .map((ring) => {
          return `<gml:interior><gml:LinearRing><gml:posList>${ring.map(positionText).join(" ")}</gml:posList></gml:LinearRing></gml:interior>`;
        })
        .join("")}</gml:Polygon>`;
    case "MultiPolygon":
      return `<gml:MultiSurface${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:surfaceMember>${gmlGeometry(
            {
              type: "Polygon",
              coordinates: item,
            },
            gmlNS,
            srsName,
          )}</gml:surfaceMember>`;
        })
        .join("")}</gml:MultiSurface>`;
    case "MultiPoint":
      return `<gml:MultiPoint${attr}>${geometry.coordinates
        .map((item) => {
          return `<gml:pointMember><gml:Point${attr}><gml:pos>${positionText(item)}</gml:pos></gml:Point></gml:pointMember>`;
        })
        .join("")}</gml:MultiPoint>`;
    case "GeometryCollection":
      return `<gml:MultiGeometry${attr}>${geometry.geometries
        .map((item) => {
          return `<gml:geometryMember>${gmlGeometry(item, gmlNS, srsName)}</gml:geometryMember>`;
        })
        .join("")}</gml:MultiGeometry>`;
    default:
      return "";
  }
}

function featureToGML(feature, type, index, srsName) {
  const featureName = xmlName(type.layer);
  const properties = Object.entries(feature.properties ?? {})
    .map(([name, value]) => {
      return `<${xmlName(name)}>${xmlEscape(value)}</${xmlName(name)}>`;
    })
    .join("");
  const geometry = feature.geometry
    ? `<geometry>${gmlGeometry(geometryTransform(feature.geometry, "EPSG:4326", srsName), GML32_NAMESPACE, srsName)}</geometry>`
    : "";
  return `<feature:${featureName} gml:id="${xmlEscape(featureId(feature, type, index))}" xmlns:feature="${WFS_FEATURE_NAMESPACE}">${properties}${geometry}</feature:${featureName}>`;
}

function featureCollectionGML(items, srsName, matched, lockId) {
  const memberTag = "wfs:member";
  const members = items
    .map(({ feature, type, index }) => {
      return `<${memberTag}>${featureToGML(feature, type, index, srsName)}</${memberTag}>`;
    })
    .join("");
  return `<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection xmlns:wfs="${WFS_NAMESPACE}" xmlns:gml="${GML32_NAMESPACE}" xmlns:feature="${WFS_FEATURE_NAMESPACE}" timeStamp="${new Date().toISOString()}" numberMatched="${matched}" numberReturned="${items.length}"${lockId ? ` lockId="${xmlEscape(lockId)}"` : ""}>${members}</wfs:FeatureCollection>`;
}

function featureCollectionJSON(features, matched, startIndex) {
  return JSON.stringify({
    type: "FeatureCollection",
    numberMatched: matched,
    numberReturned: features.length,
    totalFeatures: matched,
    startIndex,
    features,
  });
}

async function descriptor(type) {
  const features = await readFeatures(type);
  let bbox;

  for (const feature of features) {
    const geometryBBox = getGeometryBBox(feature.geometry);
    if (!geometryBBox) {
      continue;
    }

    if (!bbox) {
      bbox = geometryBBox.slice();
      continue;
    }

    bbox[0] = min(bbox[0], geometryBBox[0]);
    bbox[1] = min(bbox[1], geometryBBox[1]);
    bbox[2] = max(bbox[2], geometryBBox[2]);
    bbox[3] = max(bbox[3], geometryBBox[3]);
  }

  bbox ??= [-180, -90, 180, 90];
  return {
    ...type,
    features,
    bbox,
    fields: fieldSchema(features),
  };
}

async function capabilities(baseURL) {
  const types = await Promise.all(getFeatureTypes().map(descriptor));
  return await compileHandleBarsTemplate("wfs", {
    baseURL: xmlEscape(baseURL),
    types: types.map((type) => {
      return {
        name: xmlEscape(type.name),
        title: xmlEscape(type.title),
        bbox: type.bbox,
      };
    }),
  });
}

function getRequestData(req) {
  return {
    operation: getParameter(req.query, "REQUEST", "GetCapabilities"),
    parameters: req.query ?? {},
    xml: "",
  };
}

function applyQuery(features, type, parameters) {
  const filter =
    filterFromXML(getParameter(parameters, "FILTER")) ??
    filterFromCQL(getParameter(parameters, "CQL_FILTER")) ??
    (getParameter(
      parameters,
      "FEATUREID",
      getParameter(parameters, "RESOURCEID"),
    )
      ? {
          type: "id",
          value: getParameter(
            parameters,
            "FEATUREID",
            getParameter(parameters, "RESOURCEID"),
          ),
        }
      : undefined);
  const bbox = parseBBox(getParameter(parameters, "BBOX"));
  let result = features.filter((feature, index) => {
    if (!matchesFilter(feature, filter, type, index)) {
      return false;
    }
    if (!bbox) {
      return true;
    }
    const geometry = getGeometryBBox(feature.geometry);

    return Boolean(geometry && bboxIntersects(geometry, bbox));
  });
  const sort = getParameter(parameters, "SORTBY");
  if (sort) {
    const parts = String(sort).trim().split(/\s+/);
    const field = localName(parts[0]);
    const descending =
      String(parts[1] ?? "").toUpperCase() === "D" ||
      String(parts[1] ?? "").toUpperCase() === "DESC";
    result = result.sort((first, second) => {
      const direction = descending ? -1 : 1;
      return (
        (propertyValue(first, field) > propertyValue(second, field) ? 1 : -1) *
        direction
      );
    });
  }
  return result;
}

function outputFormat(parameters) {
  const format = String(
    getParameter(parameters, "OUTPUTFORMAT", "application/json"),
  ).toLowerCase();
  if (format.includes("json") || format === "geojson") {
    return "json";
  }
  if (format.includes("gml") || format === "text/xml") {
    return "gml";
  }
  throw new WFSException(
    "InvalidParameterValue",
    `OUTPUTFORMAT "${format}" is not supported.`,
  );
}

function describeFeatureType(type) {
  const fields = [...type.fields.entries()]
    .map(([name, fieldType]) => {
      return `<xsd:element name="${xmlEscape(xmlName(name))}" type="xsd:${fieldType}" minOccurs="0"/>`;
    })
    .join("");
  return `<?xml version="1.0" encoding="UTF-8"?><xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:gml="${GML32_NAMESPACE}" xmlns:feature="${WFS_FEATURE_NAMESPACE}" targetNamespace="${WFS_FEATURE_NAMESPACE}" elementFormDefault="qualified"><xsd:element name="${xmlEscape(xmlName(type.layer))}" type="feature:${xmlName(type.layer)}Type" substitutionGroup="gml:AbstractFeature"/><xsd:complexType name="${xmlEscape(xmlName(type.layer))}Type"><xsd:complexContent><xsd:extension base="gml:AbstractFeatureType"><xsd:sequence>${fields}<xsd:element name="geometry" type="gml:GeometryPropertyType" minOccurs="0"/></xsd:sequence></xsd:extension></xsd:complexContent></xsd:complexType></xsd:schema>`;
}

async function handleWFS(req, res, pathName) {
  const request = getRequestData(req);
  const parameters = request.parameters;
  try {
    normalizeVersion(getParameter(parameters, "VERSION", WFS_VERSION));
    if (
      String(getParameter(parameters, "SERVICE", "WFS")).toUpperCase() !== "WFS"
    ) {
      throw new WFSException("InvalidParameterValue", "SERVICE must be WFS.");
    }
    let operation = String(
      request.operation ??
        getParameter(parameters, "REQUEST", "GetCapabilities"),
    ).toLowerCase();
    if (
      ![
        "getcapabilities",
        "describefeaturetype",
        "getpropertyvalue",
        "getfeature",
      ].includes(operation)
    ) {
      throw new WFSException(
        "OperationNotSupported",
        `Request "${operation}" is not supported for GET-only WFS.`,
      );
    }
    const baseURL = `${getRequestHost(req)}/wfs`;

    if (operation === "getcapabilities") {
      res
        .type("text/xml")
        .status(200)
        .send(await capabilities(baseURL));
      return;
    }
    if (operation === "describefeaturetype") {
      const type = await descriptor(
        resolveFeatureTypes(parameters, pathName)[0],
      );
      res.type("text/xml").status(200).send(describeFeatureType(type));
      return;
    }
    if (operation === "getpropertyvalue") {
      const types = await Promise.all(
        resolveFeatureTypes(parameters, pathName).map(descriptor),
      );
      const property = getParameter(
        parameters,
        "VALUEREFERENCE",
        getParameter(parameters, "PROPERTYNAME"),
      );
      const values = types
        .flatMap((type) => {
          return type.features.map((feature) => {
            return propertyValue(feature, property);
          });
        })
        .filter((value) => {
          return value !== undefined;
        });
      res
        .type("text/xml")
        .status(200)
        .send(
          `<?xml version="1.0" encoding="UTF-8"?><wfs:ValueCollection xmlns:wfs="${WFS_NAMESPACE}">${values
            .map((value) => {
              return `<wfs:member>${xmlEscape(value)}</wfs:member>`;
            })
            .join("")}</wfs:ValueCollection>`,
        );
      return;
    }
    if (operation === "getfeature") {
      const types = await Promise.all(
        resolveFeatureTypes(parameters, pathName).map(descriptor),
      );
      const srsName = normalizeCRS(
        getParameter(parameters, "SRSNAME", "EPSG:4326"),
      );
      const all = types.flatMap((type) => {
        return applyQuery(type.features, type, parameters).map(
          (feature, index) => {
            return {
              feature,
              type,
              index,
            };
          },
        );
      });
      const matched = all.length;
      const startIndex = max(
        0,
        Number(getParameter(parameters, "STARTINDEX", 0)) || 0,
      );
      const count = min(
        MAX_FEATURES,
        max(
          0,
          Number(
            getParameter(
              parameters,
              "COUNT",
              getParameter(parameters, "MAXFEATURES", DEFAULT_COUNT),
            ),
          ) || DEFAULT_COUNT,
        ),
      );
      const selected =
        String(
          getParameter(parameters, "RESULTTYPE", "results"),
        ).toLowerCase() === "hits"
          ? []
          : all.slice(startIndex, startIndex + count);
      const propertyNames = splitParameter(
        getParameter(
          parameters,
          "PROPERTYNAME",
          getParameter(parameters, "PROPERTYNAMES"),
        ),
      );
      const output = outputFormat(parameters);
      const projected = selected.map(({ feature, type, index }) => {
        const result = projectFeature(feature, type, index, srsName);
        if (!propertyNames.length) {
          return {
            feature: result,
            type,
            index,
          };
        }
        return {
          feature: {
            ...result,
            properties: Object.fromEntries(
              propertyNames
                .map((name) => {
                  return [name, result.properties?.[name]];
                })
                .filter(([, value]) => {
                  return value !== undefined;
                }),
            ),
          },
          type,
          index,
        };
      });
      if (output === "json") {
        res
          .type("application/json")
          .status(200)
          .send(
            featureCollectionJSON(
              projected.map((item) => {
                return item.feature;
              }),
              matched,
              startIndex,
            ),
          );
      } else {
        res
          .type("text/xml")
          .status(200)
          .send(featureCollectionGML(projected, srsName, matched, undefined));
      }
      return;
    }
    throw new WFSException(
      "OperationNotSupported",
      `Request "${operation}" is not supported.`,
    );
  } catch (error) {
    const code = error.code ?? "NoApplicableCode";
    const message =
      error instanceof WFSException
        ? error.message
        : "The WFS request could not be completed.";
    res
      .status(400)
      .type("text/xml")
      .send(
        await compileHandleBarsTemplate("ows_exception", {
          version: xmlEscape(getParameter(parameters, "VERSION", WFS_VERSION)),
          code: xmlEscape(code),
          message: xmlEscape(message),
        }),
      );
  }
}

export const serve_wfs = {
  init: (app) => {
    /**
     * @swagger
     * tags:
     *   - name: WFS
     *     description: OGC Web Feature Service endpoints
     * /wfs:
     *   get:
     *     tags: [WFS]
     *     summary: Execute a WFS request
     *     description: Read-only WFS 2.0.0 endpoint supporting GetCapabilities, DescribeFeatureType, GetFeature, and GetPropertyValue.
     *     parameters:
     *       - { in: query, name: SERVICE, schema: { type: string, enum: [WFS], default: WFS } }
     *       - { in: query, name: REQUEST, required: true, schema: { type: string, enum: [GetCapabilities, DescribeFeatureType, GetPropertyValue, GetFeature], example: GetFeature } }
     *       - { in: query, name: VERSION, schema: { type: string, enum: ['2.0.0'], default: '2.0.0' } }
     *       - { in: query, name: TYPENAMES, schema: { type: string }, description: Comma-separated feature type names. }
     *       - { in: query, name: OUTPUTFORMAT, schema: { type: string, enum: [application/json, application/gml+xml; version=3.2], example: application/json } }
     *       - { in: query, name: BBOX, schema: { type: string }, description: Four coordinates with optional CRS. }
     *       - { in: query, name: SRSNAME, schema: { type: string, example: EPSG:4326 } }
     *       - { in: query, name: PROPERTYNAME, schema: { type: string }, description: Comma-separated properties to return. }
     *       - { in: query, name: RESULTTYPE, schema: { type: string, enum: [results, hits], default: results } }
     *       - { in: query, name: COUNT, schema: { type: integer, minimum: 0 } }
     *       - { in: query, name: STARTINDEX, schema: { type: integer, minimum: 0 } }
     *     responses:
     *       200:
     *         description: WFS XML or GeoJSON response.
     *         content:
     *           application/json: { schema: { type: object } }
     *           application/xml: { schema: { type: string } }
     *       400:
     *         description: OGC exception report.
     */
    /**
     * @swagger
     * /geojsons/{group}/{layer}/wfs:
     *   get:
     *     tags: [WFS]
     *     summary: Execute WFS for one GeoJSON layer
     *     parameters:
     *       - { in: path, name: group, required: true, schema: { type: string }, description: GeoJSON group. }
     *       - { in: path, name: layer, required: true, schema: { type: string }, description: GeoJSON layer. }
     *       - { in: query, name: REQUEST, required: true, schema: { type: string, enum: [GetCapabilities, DescribeFeatureType, GetPropertyValue, GetFeature] } }
     *       - { in: query, name: VERSION, schema: { type: string, enum: ['2.0.0'], default: '2.0.0' } }
     *       - { in: query, name: TYPENAMES, schema: { type: string }, description: Optional feature type name. }
     *       - { in: query, name: OUTPUTFORMAT, schema: { type: string, enum: [application/json, application/gml+xml; version=3.2] } }
     *       - { in: query, name: BBOX, schema: { type: string } }
     *       - { in: query, name: COUNT, schema: { type: integer, minimum: 0 } }
     *       - { in: query, name: STARTINDEX, schema: { type: integer, minimum: 0 } }
     *     responses:
     *       200:
     *         description: WFS XML or GeoJSON response for the selected layer.
     *       400:
     *         description: OGC exception report.
     */
    const register = (route) => {
      app.get(route, (req, res) => {
        return handleWFS(
          req,
          res,
          req.params.group && req.params.layer
            ? `${req.params.group}:${req.params.layer}`
            : undefined,
        );
      });
    };
    register("/wfs");
    register("/geojsons/:group/:layer/wfs");
  },
};

export { WFSException, normalizeVersion as normalizeWFSVersion };
