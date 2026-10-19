// Guards the shared basemap wiring for the bundle_inspector map components.
// The components are plain browser scripts sharing globals, so a renamed or
// missing basemap constant would only surface as a runtime ReferenceError.
// Run with: node --test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const STATIC_DIR = path.join(
    __dirname,
    '..',
    '..',
    'src',
    'overture_airflow_provider',
    'plugins',
    'bundle_inspector',
    'static',
    'bundle_inspector',
);
const MAP_COMPONENTS = ['pmtiles-viewer.js', 'bbox-map.js', 'athena-preview.js'];

function read(...parts) {
    return fs.readFileSync(path.join(STATIC_DIR, ...parts), 'utf8');
}

test('map-utils.js exposes BASEMAP_STYLE as the keyless OpenFreeMap Positron style', () => {
    const sandbox = {};
    vm.createContext(sandbox);
    vm.runInContext(`${read('map-utils.js')}\nthis.BASEMAP_STYLE = BASEMAP_STYLE;`, sandbox);

    assert.equal(sandbox.BASEMAP_STYLE, 'https://tiles.openfreemap.org/styles/positron');
});

for (const component of MAP_COMPONENTS) {
    test(`${component} uses BASEMAP_STYLE for its map style`, () => {
        const source = read('components', component);
        const styles = [...source.matchAll(/\bstyle:\s*([A-Za-z_$][\w$]*)/g)].map(m => m[1]);

        assert.ok(styles.length > 0, `no maplibregl.Map style found in ${component}`);
        assert.deepEqual(new Set(styles), new Set(['BASEMAP_STYLE']));
    });
}
