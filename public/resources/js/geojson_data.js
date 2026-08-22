const baseURL = document.body.dataset.baseUrl;
const group = document.body.dataset.group;
const layer = document.body.dataset.layer;
const layerList = document.getElementById("layerList");
const inspector = document.getElementById("featureInspector");
let pinned = false;

function handleError(error) {
  alert(`Error: ${error instanceof Error ? error.message : String(error)}`);
}

function geometryLayer(source, type) {
  const id = `${source}_${type}`;
  const common = { id, source, type: type === "polygon" ? "fill" : type === "line" ? "line" : "circle" };

  if (type === "polygon") {
    return { ...common, filter: ["==", "$type", "Polygon"], paint: { "fill-color": assignInspectLayerColor(source, 0.3), "fill-antialias": true, "fill-outline-color": assignInspectLayerColor(source, 0.6) } };
  }
  if (type === "line") {
    return { ...common, filter: ["==", "$type", "LineString"], layout: { "line-join": "round", "line-cap": "round" }, paint: { "line-color": assignInspectLayerColor(source, 0.6) } };
  }
  return { ...common, filter: ["==", "$type", "Point"], paint: { "circle-color": assignInspectLayerColor(source, 0.8), "circle-radius": 2 } };
}

function createStyle(sources) {
  const style = { version: 8, sources: {}, layers: [] };
  Object.entries(sources).forEach(([name, source]) => {
    style.sources[name] = { type: "geojson", data: source.url };
    const types = source.geometryTypes || ["polygon", "line", "circle"];
    types.forEach((type) => style.layers.push(geometryLayer(name, type)));
  });
  return style;
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
    <strong>${escapeHTML(feature.layer?.id || "Feature")}</strong>
    <dl><div class="property-row"><dt>Type</dt><dd>${escapeHTML(feature.geometry?.type || "Unknown")}</dd></div><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>${escapeHTML(lngLat ? `[${lngLat.lng}, ${lngLat.lat}]` : "")}</dd></div>${rows || '<div class="empty-properties">Feature has no properties</div>'}</dl>`;
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

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

function addLayerList(map, sources) {
  Object.keys(sources).forEach((name, index) => {
    const item = document.createElement("button");
    item.className = "layer-item";
    item.type = "button";
    item.innerHTML = `<span class="layer-swatch" style="background:${assignInspectLayerColor(name)}"></span>${escapeHTML(name)}`;
    item.onclick = () => {
      const visible = map.getLayoutProperty(`${name}_${sources[name].geometryTypes?.[0] || "polygon"}`, "visibility") !== "none";
      (sources[name].geometryTypes || ["polygon", "line", "circle"]).forEach((type) => {
        map.setLayoutProperty(`${name}_${type}`, "visibility", visible ? "none" : "visible");
      });
      item.classList.toggle("is-hidden", visible);
    };
    layerList.appendChild(item);
  });
}

function showMap(sources) {
  const map = new maplibregl.Map({ container: "map", hash: true, style: createStyle(sources) });
  addMapLibreGlobalControls(map);
  addLayerList(map, sources);
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
    const feature = map.queryRenderedFeatures(event.point)[0];
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
    const features = map.queryRenderedFeatures(event.point);
    if (features.length) {
      pinned = true;
      renderProperties(features[0], event.point, event.lngLat);
      return;
    }
    pinned = true;
    renderCoordinate(event.point, event.lngLat);
  });
  map.on("contextmenu", () => {
    pinned = false;
    inspector.hidden = true;
    if (currentMarker) currentMarker.remove();
  });
}

const infoURL = layer ? `${baseURL}/geojsons/${group}/${layer}.json` : `${baseURL}/geojsons/${group}.json`;
fetch(infoURL, { headers: { "content-type": "application/json" } })
  .then((response) => {
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return response.json();
  })
  .then((info) => {
    const sources = layer ? { [layer]: info } : info.geojsons;
    showMap(sources);
  })
  .catch(handleError);
