(function (window) {
  "use strict";

  window.addMapLibreGlobalControls = function (map) {
    map.addControl(
      new maplibregl.NavigationControl({
        visualizePitch: true,
      }),
      "top-right",
    );
    map.addControl(new maplibregl.FullscreenControl(), "top-right");
    map.addControl(new maplibregl.GlobeControl(), "top-right");
    map.addControl(
      new maplibregl.GeolocateControl({
        positionOptions: {
          enableHighAccuracy: true,
        },
        trackUserLocation: true,
        showUserHeading: true,
      }),
      "top-right",
    );
    map.addControl(
      new maplibregl.ScaleControl({
        unit: "metric",
      }),
      "bottom-left",
    );
  };

  window.addLeafletGlobalControls = function (map) {
    new L.Control.Zoom({
      position: "topright",
    }).addTo(map);

    const LocateControl = L.Control.extend({
      options: {
        position: "topright",
      },
      onAdd: function () {
        const button = L.DomUtil.create("button", "leaflet-control-locate");
        button.type = "button";
        button.title = "Show my location";
        button.setAttribute("aria-label", "Show my location");
        button.innerHTML = "⌖";
        L.DomEvent.on(button, "click", function (event) {
          L.DomEvent.stopPropagation(event);
          map.locate({
            setView: true,
            maxZoom: 16,
            enableHighAccuracy: true,
          });
        });
        return button;
      },
    });
    new LocateControl().addTo(map);

    L.control.scale({ position: "bottomleft", imperial: false }).addTo(map);
  };
})(window);
