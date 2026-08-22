const baseURL = document.body.dataset.baseUrl;
const id = document.body.dataset.id;
const inspector = document.getElementById("featureInspector");
let pinned = false;

function escapeHTML(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function positionInspector(point) {
  const mapContainer = document.getElementById("map");
  const width = Math.min(inspector.offsetWidth || 320, mapContainer.clientWidth - 16);
  const height = inspector.offsetHeight || 140;
  inspector.style.left = `${Math.max(8, Math.min(point.x + 14, mapContainer.clientWidth - width - 8))}px`;
  inspector.style.top = `${Math.max(8, Math.min(point.y + 14, mapContainer.clientHeight - height - 8))}px`;
}

function renderCoordinate(point, lngLat) {
  inspector.hidden = false;
  inspector.innerHTML = `<button class="inspector-close" type="button" aria-label="Close">&times;</button>
    <strong>Map position</strong>
    <dl><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>${escapeHTML(`[${lngLat.lng}, ${lngLat.lat}]`)}</dd></div></dl>`;
  positionInspector(point);
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

function renderFeature(feature, point, lngLat) {
  const rows = Object.entries(feature.properties || {}).map(([key, value]) => {
    const formatted = typeof value === "object" && value !== null ? JSON.stringify(value) : String(value);
    return `<div class="property-row"><dt>${escapeHTML(key)}</dt><dd>${escapeHTML(formatted)}</dd></div>`;
  }).join("");
  inspector.hidden = false;
  inspector.innerHTML = `<button class="inspector-close" type="button" aria-label="Close">&times;</button>
    <strong>${escapeHTML(feature.layer?.["source-layer"] || feature.layer?.id || feature.source || "Feature")}</strong>
    <dl><div class="property-row"><dt>Type</dt><dd>${escapeHTML(feature.geometry?.type || "Unknown")}</dd></div><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>${escapeHTML(`[${lngLat.lng}, ${lngLat.lat}]`)}</dd></div>${rows || '<div class="empty-properties">Feature has no properties</div>'}</dl>`;
  positionInspector(point);
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

function addMarker(map, lngLat, toMapPoint, removeMarker) {
  removeMarker();
  const marker = new maplibregl.Marker({ draggable: true, color: "#0065ff" })
    .setLngLat(lngLat)
    .addTo(map);
  marker.on("dragend", () => {
    const position = marker.getLngLat();
    const coordinate = inspector.querySelector(".coordinate-row dd");
    if (coordinate) coordinate.textContent = `[${position.lng}, ${position.lat}]`;
  });
  return marker;
}

function showVectorViewer() {
  const map = new maplibregl.Map({ container: "map", style: `${baseURL}/styles/${id}/style.json`, hash: true });
  addMapLibreGlobalControls(map);
  let marker;
  const removeMarker = () => { if (marker) marker.remove(); marker = undefined; };

  map.on("mousemove", (event) => {
    const feature = map.queryRenderedFeatures(event.point)[0];
    map.getCanvas().style.cursor = feature ? "pointer" : "";
    if (pinned) return;
    if (feature) renderFeature(feature, event.point, event.lngLat);
    else renderCoordinate(event.point, event.lngLat);
  });
  map.on("mouseleave", () => { map.getCanvas().style.cursor = ""; if (!pinned) inspector.hidden = true; });
  map.on("click", (event) => {
    marker = addMarker(map, event.lngLat, null, removeMarker);
    const feature = map.queryRenderedFeatures(event.point)[0];
    pinned = true;
    if (feature) renderFeature(feature, event.point, event.lngLat);
    else renderCoordinate(event.point, event.lngLat);
  });
  map.on("contextmenu", () => { pinned = false; inspector.hidden = true; removeMarker(); });
}

function showRasterViewer(jsonResponse) {
  const map = L.map("map", { zoomControl: false, maxBounds: [[-90, -180], [90, 180]], maxBoundsViscosity: 1 });
  for (const tile of jsonResponse.tiles || []) {
    L.tileLayer(tile, { minZoom: jsonResponse.minzoom, maxZoom: jsonResponse.maxzoom, maxNativeZoom: 22, attribution: jsonResponse.attribution }).addTo(map);
  }
  addLeafletGlobalControls(map);
  new L.Hash(map);
  let marker;
  const removeMarker = () => { if (marker) marker.remove(); marker = undefined; };
  map.on("mousemove", (event) => { if (!pinned) renderCoordinate({ x: event.containerPoint.x, y: event.containerPoint.y }, event.latlng); });
  map.on("mouseout", () => { if (!pinned) inspector.hidden = true; });
  map.on("click", (event) => {
    removeMarker();
    marker = L.marker(event.latlng, { draggable: true }).addTo(map);
    pinned = true;
    renderCoordinate({ x: event.containerPoint.x, y: event.containerPoint.y }, event.latlng);
    marker.on("dragend", () => {
      const position = marker.getLatLng();
      const coordinate = inspector.querySelector(".coordinate-row dd");
      if (coordinate) coordinate.textContent = `[${position.lng}, ${position.lat}]`;
    });
  });
  map.on("contextmenu", () => { pinned = false; inspector.hidden = true; removeMarker(); });
}

function handleError(error) { alert(`Error: ${error instanceof Error ? error.message : String(error)}`); }

if (location.search.indexOf("vector") >= 0) {
  showVectorViewer();
} else {
  fetch(`${baseURL}/styles/${id}.json`, { headers: { "content-type": "application/json" } })
    .then((response) => { if (!response.ok) throw new Error(`${response.status} ${response.statusText}`); return response.json(); })
    .then(showRasterViewer)
    .catch(handleError);
}
