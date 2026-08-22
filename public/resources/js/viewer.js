const baseURL = document.body.dataset.baseUrl;
const id = document.body.dataset.id;

function handleError(error) {
  if (error.response) {
    alert(
      `Error: Status code: ${error.response.status} - ${error.response.statusText}`,
    );
  } else if (error.request) {
    alert("No response received");
  } else {
    alert(`Error: ${error.message}`);
  }
}

if (location.search.indexOf("vector") >= 0) {
  const map = new maplibregl.Map({
    container: "map",
    style: `${baseURL}/styles/${id}/style.json`,
    hash: true,
  });

  addMapLibreGlobalControls(map);

  let currentMarker;

  map.on("click", (event) => {
    if (currentMarker) {
      currentMarker.remove();
    }

    currentMarker = new maplibregl.Marker({
      draggable: true,
    })
      .setLngLat(event.lngLat)
      .addTo(map);

    alert(`Position: [${event.lngLat.lng}, ${event.lngLat.lat}]`);

    currentMarker.on("dragend", () => {
      const lngLat = currentMarker.getLngLat();

      alert(`Position: [${lngLat.lng}, ${lngLat.lat}]`);
    });
  });

  map.on("contextmenu", (event) => {
    if (currentMarker) {
      currentMarker.remove();

      currentMarker = undefined;
    }
  });
} else {
  fetch(`${baseURL}/styles/${id}.json`, {
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

      addLeafletGlobalControls(map);

      new L.Hash(map);

      let currentMarker;

      map.on("click", (event) => {
        if (currentMarker) {
          currentMarker.remove();
        }

        currentMarker = L.marker([event.latlng.lat, event.latlng.lng], {
          draggable: true,
        }).addTo(map);

        alert(`Position: [${event.latlng.lng}, ${event.latlng.lat}]`);

        currentMarker.on("dragend", () => {
          const position = currentMarker.getLatLng();

          alert(`Position: [${position.lng}, ${position.lat}]`);
        });
      });

      map.on("contextmenu", (event) => {
        if (currentMarker) {
          currentMarker.remove();

          currentMarker = undefined;
        }
      });
    })
    .catch(handleError);
}
