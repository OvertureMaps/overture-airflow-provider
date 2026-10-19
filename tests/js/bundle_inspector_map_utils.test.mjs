// Tests for the shared basemap helper (createBaseMap) in map-utils.js, using a
// stub maplibregl.Map. The map components are plain browser scripts sharing
// globals, so they're also checked to build maps through the helper.
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

class StubMap {
    constructor(options) {
        this.options = options;
        this.listeners = {};
        this.setStyleCalls = [];
    }
    on(type, fn) {
        (this.listeners[type] ??= []).push(fn);
        return this;
    }
    off(type, fn) {
        this.listeners[type] = (this.listeners[type] || []).filter(f => f !== fn);
        return this;
    }
    fire(type, event = {}) {
        for (const fn of [...(this.listeners[type] || [])]) fn(event);
    }
    listens(type) {
        return (this.listeners[type] || []).length > 0;
    }
    setStyle(style, opts) {
        this.setStyleCalls.push({ style, opts });
    }
}

function loadMapUtils() {
    const warnings = [];
    const sandbox = {
        maplibregl: { Map: StubMap },
        console: { warn: (...args) => warnings.push(args) },
    };
    vm.createContext(sandbox);
    vm.runInContext(
        `${read('map-utils.js')}
        this.BASEMAP_STYLE = BASEMAP_STYLE;
        this.BASEMAP_FALLBACK_STYLE = BASEMAP_FALLBACK_STYLE;`,
        sandbox,
    );
    return { sandbox, warnings };
}

test('createBaseMap uses the keyless OpenFreeMap basemap and passes caller options through', () => {
    const { sandbox } = loadMapUtils();

    const map = sandbox.createBaseMap({ container: 'el', zoom: 3, style: 'ignored' });

    assert.equal(sandbox.BASEMAP_STYLE, 'https://tiles.openfreemap.org/styles/positron');
    assert.equal(map.options.style, sandbox.BASEMAP_STYLE);
    assert.equal(map.options.container, 'el');
    assert.equal(map.options.zoom, 3);
});

test('createBaseMap falls back to a blank style once if the basemap style fails to load', () => {
    const { sandbox, warnings } = loadMapUtils();
    const map = sandbox.createBaseMap({ container: 'el' });

    map.fire('error', { error: new Error('Failed to fetch') });
    map.fire('error', { error: new Error('second failure') });

    assert.equal(map.setStyleCalls.length, 1);
    assert.equal(map.setStyleCalls[0].style, sandbox.BASEMAP_FALLBACK_STYLE);
    assert.equal(map.setStyleCalls[0].opts.diff, false);
    assert.equal(warnings.length, 1);
    assert.equal(map.listens('error'), false);
    assert.equal(map.listens('style.load'), false);
});

test('createBaseMap leaves later errors to MapLibre once the basemap style has loaded', () => {
    const { sandbox, warnings } = loadMapUtils();
    const map = sandbox.createBaseMap({ container: 'el' });

    map.fire('style.load');
    map.fire('error', { error: new Error('tile failed') });

    assert.equal(map.setStyleCalls.length, 0);
    assert.equal(warnings.length, 0);
    // No lingering listener, so MapLibre's default console.error logging still applies.
    assert.equal(map.listens('error'), false);
});

for (const component of MAP_COMPONENTS) {
    test(`${component} builds its map with createBaseMap`, () => {
        const source = read('components', component);

        assert.match(source, /\bcreateBaseMap\(/);
        assert.doesNotMatch(source, /new\s+maplibregl\.Map\(/);
    });
}
