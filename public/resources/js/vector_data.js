const baseURL = document.body.dataset.baseUrl;
const id = document.body.dataset.id;
const layerList = document.getElementById("layerList");
const inspector = document.getElementById("featureInspector");
let pinned = false;
function renderCoordinate(point, lngLat) {
  inspector.hidden = false;
  inspector.innerHTML = `<button class="inspector-close" type="button" aria-label="Close">&times;</button>
    <strong>Map position</strong>
    <dl><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>${escapeHTML(`[${lngLat.lng}, ${lngLat.lat}]`)}</dd></div></dl>`;
  const mapContainer = document.getElementById("map");
  const width = Math.min(inspector.offsetWidth || 320, mapContainer.clientWidth - 16);
  const height = inspector.offsetHeight || 120;
  inspector.style.left = `${Math.max(8, Math.min(point.x + 14, mapContainer.clientWidth - width - 8))}px`;
  inspector.style.top = `${Math.max(8, Math.min(point.y + 14, mapContainer.clientHeight - height - 8))}px`;
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

function escapeHTML(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function renderProperties(feature, point, lngLat) {
  const properties = feature.properties || {};
  const rows = Object.entries(properties).map(([key, value]) => {
    const formatted = typeof value === "object" && value !== null ? JSON.stringify(value) : String(value);
    return `<div class="property-row"><dt>${escapeHTML(key)}</dt><dd>${escapeHTML(formatted)}</dd></div>`;
  }).join("");

  inspector.hidden = false;
  const mapContainer = document.getElementById("map");
  const width = Math.min(inspector.offsetWidth || 320, mapContainer.clientWidth - 16);
  const height = inspector.offsetHeight || 180;
  inspector.style.left = `${Math.max(8, Math.min(point.x + 14, mapContainer.clientWidth - width - 8))}px`;
  inspector.style.top = `${Math.max(8, Math.min(point.y + 14, mapContainer.clientHeight - height - 8))}px`;
  inspector.style.bottom = "auto";
  inspector.innerHTML = `<button class="inspector-close" type="button" aria-label="Close">×</button>
    <strong>${escapeHTML(feature.layer?.["source-layer"] || "Feature")}</strong>
    <dl><div class="property-row"><dt>Type</dt><dd>${escapeHTML(feature.geometry?.type || "Unknown")}</dd></div><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>${escapeHTML(lngLat ? `[${lngLat.lng}, ${lngLat.lat}]` : "")}</dd></div>${rows || '<div class="empty-properties">Feature has no properties</div>'}</dl>`;
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

function addVectorLayers(style, vectorLayers) {
  vectorLayers.forEach((vectorLayer) => {
    const sourceLayer = vectorLayer.id;
    style.layers.push(
      {
        id: `source_${sourceLayer}_polygon`,
        source: "source",
        "source-layer": sourceLayer,
        type: "fill",
        filter: ["==", "$type", "Polygon"],
        paint: { "fill-color": assignInspectLayerColor(sourceLayer, 0.3), "fill-antialias": true, "fill-outline-color": assignInspectLayerColor(sourceLayer, 0.6) },
      },
      {
        id: `source_${sourceLayer}_line`,
        source: "source",
        "source-layer": sourceLayer,
        type: "line",
        filter: ["==", "$type", "LineString"],
        layout: { "line-join": "round", "line-cap": "round" },
        paint: { "line-color": assignInspectLayerColor(sourceLayer, 0.6) },
      },
      {
        id: `source_${sourceLayer}_circle`,
        source: "source",
        "source-layer": sourceLayer,
        type: "circle",
        filter: ["==", "$type", "Point"],
        paint: { "circle-color": assignInspectLayerColor(sourceLayer, 0.8), "circle-radius": 2 },
      },
    );
  });
}

function addLayerList(map, vectorLayers) {
  vectorLayers.forEach((vectorLayer, index) => {
    const item = document.createElement("button");
    item.className = "layer-item";
    item.type = "button";
    item.innerHTML = `<span class="layer-swatch" style="background:${assignInspectLayerColor(vectorLayer.id)}"></span>${escapeHTML(vectorLayer.id)}`;
    item.onclick = () => {
      const prefix = `source_${vectorLayer.id}_`;
      const visible = map.getLayoutProperty(`${prefix}polygon`, "visibility") !== "none";
      ["polygon", "line", "circle"].forEach((type) => {
        map.setLayoutProperty(`${prefix}${type}`, "visibility", visible ? "none" : "visible");
      });
      item.classList.toggle("is-hidden", visible);
    };
    layerList.appendChild(item);
  });
}

function addCoverageLayers(style, jsonResponse) {
  const coverageLayers = {};
  (jsonResponse.cacheCoverages || []).forEach(({ zoom, bbox }) => {
    coverageLayers[zoom] ??= { type: "FeatureCollection", features: [] };
    coverageLayers[zoom].features.push({
      type: "Feature",
      geometry: { type: "LineString", coordinates: [[bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]] },
    });
  });
  Object.entries(coverageLayers).forEach(([zoom, data]) => {
    style.sources[`cache_${zoom}`] = { type: "geojson", data };
    style.layers.push({ id: `cache_${zoom}_line`, type: "line", source: `cache_${zoom}`, paint: { "line-color": "rgba(255, 0, 0, 0.6)", "line-width": 1 } });
  });
}

function setupCoverageControl(map, style, jsonResponse) {
  if (!jsonResponse.cacheCoverages) return;
  let isVisible = true;
  let currentLayer;
  const update = () => {
    if (currentLayer) map.setLayoutProperty(currentLayer, "visibility", "none");
    currentLayer = undefined;
    if (isVisible) {
      const candidate = `cache_${Math.floor(map.getZoom())}_line`;
      if (style.layers.some((item) => item.id === candidate)) {
        map.setLayoutProperty(candidate, "visibility", "visible");
        currentLayer = candidate;
      }
    }
  };
  map.on("zoomend", update);
  const button = document.createElement("button");
  button.textContent = "Hide cache Coverages";
  button.className = "coverage-toggle";
  button.onclick = () => { isVisible = !isVisible; button.textContent = `${isVisible ? "Hide" : "Show"} cache Coverages`; update(); };
  document.body.appendChild(button);
  update();
}

fetch(`${baseURL}/datas/${id}.json`, { headers: { "content-type": "application/json" } })
  .then((response) => {
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return response.json();
  })
  .then((jsonResponse) => {
    const style = {
      version: 8,
      sources: { source: { type: "vector", url: `${baseURL}/datas/${id}.json` } },
      layers: [],
    };
    const vectorLayers = jsonResponse.vector_layers || [];
    addVectorLayers(style, vectorLayers);
    addCoverageLayers(style, jsonResponse);

    const map = new maplibregl.Map({ container: "map", hash: true, style });
    addMapLibreGlobalControls(map);
    addLayerList(map, vectorLayers);
    setupCoverageControl(map, style, jsonResponse);
    let currentMarker;

    function updateMarker(lngLat) {
      if (currentMarker) currentMarker.remove();
      currentMarker = new maplibregl.Marker({ draggable: true, color: "#0065ff" })
        .setLngLat(lngLat)
        .addTo(map);
      currentMarker.on("dragend", () => {
        const coordinate = inspector.querySelector(".coordinate-row dd");
        if (coordinate) coordinate.textContent = `[${currentMarker.getLngLat().lng}, ${currentMarker.getLngLat().lat}]`;
      });
    }

    map.on("mousemove", (event) => {
      const feature = map.queryRenderedFeatures(event.point).find((item) => item.source === "source");
      map.getCanvas().style.cursor = feature ? "pointer" : "";
      if (pinned) return;
      if (feature) renderProperties(feature, event.point, event.lngLat);
      else renderCoordinate(event.point, event.lngLat);
    });
    map.on("mouseleave", () => {
      map.getCanvas().style.cursor = "";
      if (!pinned) inspector.hidden = true;
    });
    map.on("click", (event) => {
      updateMarker(event.lngLat);
      const features = map.queryRenderedFeatures(event.point).filter((feature) => feature.source === "source");
      if (features.length) {
        pinned = true;
        renderProperties(features[0], event.point, event.lngLat);
      } else {
        pinned = true;
        renderCoordinate(event.point, event.lngLat);
      }
    });
    map.on("contextmenu", () => {
      pinned = false;
      inspector.hidden = true;
      if (currentMarker) currentMarker.remove();
    });
  })
  .catch((error) => alert(`Error: ${error instanceof Error ? error.message : String(error)}`));
