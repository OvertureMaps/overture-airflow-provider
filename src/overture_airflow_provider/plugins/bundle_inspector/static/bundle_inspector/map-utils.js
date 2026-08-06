// Shared map utilities for MapLibre GL components.

const CARTO_STYLE = {
    version: 8,
    sources: {
        'carto': {
            type: 'raster',
            tiles: ['https://basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '&copy; OpenStreetMap &copy; CARTO',
        },
    },
    layers: [{
        id: 'carto-tiles',
        type: 'raster',
        source: 'carto',
    }],
};

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
