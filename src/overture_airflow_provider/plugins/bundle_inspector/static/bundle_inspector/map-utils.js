// Shared map utilities for MapLibre GL components.

// OpenFreeMap Positron: free, keyless vector basemap. Attribution
// (OpenFreeMap / OpenMapTiles / OpenStreetMap) comes from the style's TileJSON.
// Overlays added later via map.addLayer() (no beforeId) render above it.
const BASEMAP_STYLE = 'https://tiles.openfreemap.org/styles/positron';

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
