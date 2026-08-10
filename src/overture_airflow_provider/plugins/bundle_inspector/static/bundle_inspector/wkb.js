// Minimal WKB hex to GeoJSON decoder.
// Handles Point, LineString, Polygon, and their Multi* variants,
// plus GeometryCollection. Both byte orders (big/little endian).

function parseWkbHex(raw) {
    try {
        const hex = raw.replace(/\s+/g, '');
        const bytes = new Uint8Array(hex.length / 2);
        for (let i = 0; i < bytes.length; i++) {
            bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
        }
        const view = new DataView(bytes.buffer);
        const result = readGeometry(view, 0);
        return result.geometry;
    } catch {
        return null;
    }
}

function readGeometry(view, offset) {
    const byteOrder = view.getUint8(offset);
    const le = byteOrder === 1;
    offset += 1;

    let wkbType = view.getUint32(offset, le);
    offset += 4;

    // Detect Z/M dimensions from EWKB flags or ISO WKB type ranges
    const hasSRID = (wkbType & 0x20000000) !== 0;
    const hasZ = (wkbType & 0x80000000) !== 0 || (Math.floor(wkbType / 1000) % 10 === 1) || (Math.floor(wkbType / 1000) % 10 === 3);
    const hasM = (wkbType & 0x40000000) !== 0 || (Math.floor(wkbType / 1000) % 10 === 2) || (Math.floor(wkbType / 1000) % 10 === 3);
    // Coordinate stride: 2 (XY) + Z? + M?
    const coordSize = (2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0)) * 8;
    // Normalize to base type (1-7)
    const baseType = (wkbType & 0xFF) || (wkbType % 1000);
    if (hasSRID) offset += 4;

    switch (baseType) {
        case 1: return readPoint(view, offset, le, coordSize);
        case 2: return readLineString(view, offset, le, coordSize);
        case 3: return readPolygon(view, offset, le, coordSize);
        case 4: return readMulti(view, offset, le, 'MultiPoint');
        case 5: return readMulti(view, offset, le, 'MultiLineString');
        case 6: return readMulti(view, offset, le, 'MultiPolygon');
        case 7: return readGeometryCollection(view, offset, le);
        default: throw new Error('Unknown WKB type: ' + wkbType);
    }
}

function readCoord(view, offset, le, coordSize) {
    const x = view.getFloat64(offset, le);
    const y = view.getFloat64(offset + 8, le);
    return { coord: [x, y], offset: offset + (coordSize || 16) };
}

function readPoint(view, offset, le, coordSize) {
    const { coord, offset: next } = readCoord(view, offset, le, coordSize);
    return { geometry: { type: 'Point', coordinates: coord }, offset: next };
}

function readCoordArray(view, offset, le, coordSize) {
    const count = view.getUint32(offset, le);
    offset += 4;
    const coords = [];
    for (let i = 0; i < count; i++) {
        const { coord, offset: next } = readCoord(view, offset, le, coordSize);
        coords.push(coord);
        offset = next;
    }
    return { coords, offset };
}

function readLineString(view, offset, le, coordSize) {
    const { coords, offset: next } = readCoordArray(view, offset, le, coordSize);
    return { geometry: { type: 'LineString', coordinates: coords }, offset: next };
}

function readPolygon(view, offset, le, coordSize) {
    const ringCount = view.getUint32(offset, le);
    offset += 4;
    const rings = [];
    for (let i = 0; i < ringCount; i++) {
        const { coords, offset: next } = readCoordArray(view, offset, le, coordSize);
        rings.push(coords);
        offset = next;
    }
    return { geometry: { type: 'Polygon', coordinates: rings }, offset };
}

function readMulti(view, offset, le, type) {
    const count = view.getUint32(offset, le);
    offset += 4;
    const parts = [];
    for (let i = 0; i < count; i++) {
        const result = readGeometry(view, offset);
        parts.push(result.geometry.coordinates);
        offset = result.offset;
    }
    return { geometry: { type, coordinates: parts }, offset };
}

function readGeometryCollection(view, offset, le) {
    const count = view.getUint32(offset, le);
    offset += 4;
    const geometries = [];
    for (let i = 0; i < count; i++) {
        const result = readGeometry(view, offset);
        geometries.push(result.geometry);
        offset = result.offset;
    }
    return { geometry: { type: 'GeometryCollection', geometries }, offset };
}

const WKB_HEX_RE = /^[0-9a-fA-F\s]+$/;
const VALID_WKB_TYPES = new Set([1, 2, 3, 4, 5, 6, 7]);

// Detect which column (if any) contains WKB hex geometry.
// Returns the column index, or -1 if none found.
function detectGeometryColumn(columns, firstRow) {
    for (let i = 0; i < firstRow.length; i++) {
        const raw = firstRow[i];
        if (!raw || raw.length < 10) continue;
        if (!WKB_HEX_RE.test(raw)) continue;

        const val = raw.replace(/\s+/g, '');
        if (val.length % 2 !== 0) continue;

        // Check the WKB type byte
        const endian = parseInt(val.slice(0, 2), 16);
        let rawType;
        if (endian === 1) { // little-endian: type is bytes 1-4, LSB first
            rawType = parseInt(val.slice(2, 4), 16);
        } else { // big-endian: type is bytes 1-4, MSB first
            rawType = parseInt(val.slice(8, 10), 16);
        }
        if (!VALID_WKB_TYPES.has(rawType & 0xFF)) continue;

        // Confirm by attempting a full decode
        if (parseWkbHex(raw)) return i;
    }
    return -1;
}

// Average of all vertices in a geometry -- used as a marker position, not a true centroid.
function vertexAverage(geometry) {
    let sx = 0, sy = 0, n = 0;
    (function walk(coords) {
        if (typeof coords[0] === 'number') { sx += coords[0]; sy += coords[1]; n++; }
        else for (const c of coords) walk(c);
    })(geometry.coordinates || []);
    return n ? [sx / n, sy / n] : [0, 0];
}

