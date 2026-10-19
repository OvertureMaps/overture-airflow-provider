// Shared map utilities for MapLibre GL components.

// OpenFreeMap Positron: free, keyless vector basemap. Attribution
// (OpenFreeMap / OpenMapTiles / OpenStreetMap) comes from the style's TileJSON.
const BASEMAP_STYLE = 'https://tiles.openfreemap.org/styles/positron';

// Blank stand-in (Positron's background colour) for when BASEMAP_STYLE can't be
// fetched. MapLibre only fires 'load' once a style has loaded, so without it
// the overlays components add on 'load' would never render.
const BASEMAP_FALLBACK_STYLE = {
    version: 8,
    sources: {},
    layers: [{ id: 'background', type: 'background', paint: { 'background-color': 'rgb(242,243,240)' } }],
};

// Create a MapLibre map on the shared basemap. Overlays added on 'load' via
// map.addLayer() (no beforeId) render above every basemap layer.
function createBaseMap(options) {
    const map = new maplibregl.Map({ ...options, style: BASEMAP_STYLE });

    // Before the style loads, the only error source is the style itself.
    // Stop listening afterwards so later errors (e.g. tile failures) still
    // reach MapLibre's default console logging.
    const onError = (e) => {
        stopWatching();
        console.warn('Basemap style failed to load; using a blank background.', e.error);
        map.setStyle(BASEMAP_FALLBACK_STYLE, { diff: false });
    };
    const stopWatching = () => {
        map.off('error', onError);
        map.off('style.load', stopWatching);
    };
    map.on('error', onError);
    map.on('style.load', stopWatching);
    return map;
}

// Extend a maplibregl.LngLatBounds with all coordinates in a geometry.
function collectBounds(geometry, bounds) {
    if (!geometry) return;
    const type = geometry.type;
    if (type === 'Point') {
        bounds.extend(geometry.coordinates);
    } else if (type === 'MultiPoint' || type === 'LineString') {
        for (const coord of geometry.coordinates) bounds.extend(coord);
    } else if (type === 'MultiLineString' || type === 'Polygon') {
        for (const ring of geometry.coordinates)
            for (const coord of ring) bounds.extend(coord);
    } else if (type === 'MultiPolygon') {
        for (const polygon of geometry.coordinates)
            for (const ring of polygon)
                for (const coord of ring) bounds.extend(coord);
    } else if (type === 'GeometryCollection') {
        for (const geom of geometry.geometries) collectBounds(geom, bounds);
    }
}

// Fit a map to the bounds of a GeoJSON FeatureCollection.
function fitMapToFeatures(map, features) {
    if (features.length === 0) return;
    const bounds = new maplibregl.LngLatBounds();
    for (const f of features) collectBounds(f.geometry, bounds);
    map.fitBounds(bounds, { padding: 40 });
}
