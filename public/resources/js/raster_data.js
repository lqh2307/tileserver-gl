const baseURL = document.body.dataset.baseUrl;
const id = document.body.dataset.id;
const inspector = document.getElementById("featureInspector");
let pinned = false;

function renderCoordinate(point, latlng) {
  inspector.hidden = false;
  inspector.innerHTML = `<button class="inspector-close" type="button" aria-label="Close">&times;</button>
    <strong>Map position</strong>
    <dl><div class="property-row coordinate-row"><dt>Coordinates</dt><dd>[${latlng.lng}, ${latlng.lat}]</dd></div></dl>`;
  const mapContainer = document.getElementById("map");
  const width = Math.min(inspector.offsetWidth || 320, mapContainer.clientWidth - 16);
  const height = inspector.offsetHeight || 120;
  inspector.style.left = `${Math.max(8, Math.min(point.x + 14, mapContainer.clientWidth - width - 8))}px`;
  inspector.style.top = `${Math.max(8, Math.min(point.y + 14, mapContainer.clientHeight - height - 8))}px`;
  inspector.querySelector(".inspector-close").onclick = () => { pinned = false; inspector.hidden = true; };
}

function handleError(error) {
  if (error.response) {
    alert(
      `Error: Status code: ${error.response.status} - ${error.response.statusText}`,
    );
  } else if (error.request) {
    alert(`No response received: ${error.message}`);
  } else {
    alert(`Error: ${error.message}`);
  }
}

fetch(`${baseURL}/datas/${id}.json`, {
  method: "GET",
  headers: {
    "content-type": "application/json",
  },
})
  .then((response) => response.json())
  .then((jsonResponse) => {
    const map = L.map("map", {
      zoomControl: false,
      maxBounds: [
        [-90, -180],
        [90, 180],
      ],
      maxBoundsViscosity: 1,
    });

    for (const tile of jsonResponse.tiles || []) {
      L.tileLayer(tile, {
        minZoom: jsonResponse.minzoom,
        maxZoom: jsonResponse.maxzoom,
        maxNativeZoom: 22,
        attribution: jsonResponse.attribution,
      }).addTo(map);
    }

    if (jsonResponse.cacheCoverages) {
      const cacheCoverageLayers = {};
      const cacheCoverageGeoJSONLayers = {};

      for (const { zoom, bbox } of jsonResponse.cacheCoverages) {
        if (!cacheCoverageLayers[zoom]) {
          cacheCoverageLayers[zoom] = {
            type: "MultiLineString",
            coordinates: [],
          };
        }

        cacheCoverageLayers[zoom].coordinates.push([
          [bbox[0], bbox[1]],
          [bbox[2], bbox[1]],
          [bbox[2], bbox[3]],
          [bbox[0], bbox[3]],
          [bbox[0], bbox[1]],
        ]);
      }

      let isCoverageVisible = true;
      let currentCoverageLayer = undefined;

      function updateVisibleCoverages() {
        if (currentCoverageLayer) {
          currentCoverageLayer.remove();

          currentCoverageLayer = undefined;
        }

        if (isCoverageVisible) {
          const currentZoom = map.getZoom();

          if (cacheCoverageLayers[currentZoom]) {
            currentCoverageLayer = L.geoJSON(cacheCoverageLayers[currentZoom], {
              weight: 1,
              color: "rgba(255, 0, 0, 0.6)",
            });

            currentCoverageLayer.addTo(map);
          }
        }
      }

      map.on("zoomend", updateVisibleCoverages);

      const cacheCoveragesButton = document.createElement("div");
      cacheCoveragesButton.innerHTML = "Hide cache Coverages";
      cacheCoveragesButton.style.cssText =
        "position:absolute;bottom:50px;left:10px;padding:5px 10px;font-size:14px;cursor:pointer;background:rgba(0, 150, 255, 0.5);color:white;border:none;border-radius:5px;display:inline-block;z-index:1000;";

      cacheCoveragesButton.onclick = () => {
        if (isCoverageVisible) {
          cacheCoveragesButton.innerHTML = "Show cache Coverages";

          isCoverageVisible = false;
        } else {
          cacheCoveragesButton.innerHTML = "Hide cache Coverages";

          isCoverageVisible = true;
        }

        updateVisibleCoverages();
      };

      document.body.appendChild(cacheCoveragesButton);

      updateVisibleCoverages();
    }

    addLeafletGlobalControls(map);

    new L.Hash(map);

    let currentMarker;

    map.on("mousemove", (event) => {
      if (!pinned) renderCoordinate(event.containerPoint, event.latlng);
    });
    map.on("mouseout", () => { if (!pinned) inspector.hidden = true; });

    map.on("click", (event) => {
      if (currentMarker) {
        currentMarker.remove();
      }

      currentMarker = L.marker([event.latlng.lat, event.latlng.lng], {
        draggable: true,
      }).addTo(map);
      pinned = true;
      renderCoordinate(event.containerPoint, event.latlng);

      currentMarker.on("dragend", () => {
        const position = currentMarker.getLatLng();
        const coordinate = inspector.querySelector(".coordinate-row dd");
        if (coordinate) coordinate.textContent = `[${position.lng}, ${position.lat}]`;
      });
    });

    map.on("contextmenu", () => {
      pinned = false;
      inspector.hidden = true;
      if (currentMarker) {
        currentMarker.remove();

        currentMarker = undefined;
      }
    });
  })
  .catch(handleError);
