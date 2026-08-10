// Lightweight unit tests for the bundle_inspector static JS, using Node's
// built-in test runner and `vm` module instead of pulling in a JS test
// framework/dependency. Run with: node --test
//
// core.js is a plain browser script (no module exports), so it's loaded into
// a vm context with the minimal DOM/fetch surface it touches at call time.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CORE_JS_PATH = path.join(
    __dirname,
    '..',
    '..',
    'src',
    'overture_airflow_provider',
    'plugins',
    'bundle_inspector',
    'static',
    'bundle_inspector',
    'core.js',
);
const source = fs.readFileSync(CORE_JS_PATH, 'utf8');

/**
 * Loads core.js into a fresh vm context, stubbing `fetch` to return each
 * response in `responses` in order (one per call).
 */
function loadCore(responses) {
    let call = 0;
    const sandbox = {
        fetch: async () => {
            const body = responses[call];
            call += 1;
            return {
                ok: !body.error,
                status: body.error ? 400 : 200,
                json: async () => body,
            };
        },
        document: { createElement: () => ({}) },
        window: { location: { pathname: '/' } },
        history: { replaceState: () => {} },
        btoa: (s) => Buffer.from(s).toString('base64'),
        CSS: { escape: (s) => s },
        URLSearchParams,
    };
    vm.createContext(sandbox);
    vm.runInContext(source, sandbox);
    return sandbox;
}

test('fetchAthenaQuery returns the error immediately when a poll response has no state (regression for #70)', async () => {
    const sandbox = loadCore([
        { query_id: 'abc123' }, // athena-query start
        { error: "TABLE_NOT_FOUND: line 1:15: Table 'awsdatacatalog.db.t' does not exist" }, // athena-status poll
    ]);

    const result = await sandbox.fetchAthenaQuery('select 1');

    assert.deepEqual(result, {
        error: "TABLE_NOT_FOUND: line 1:15: Table 'awsdatacatalog.db.t' does not exist",
    });
});

test('fetchAthenaQuery returns immediately when athena-query start itself errors', async () => {
    const sandbox = loadCore([{ error: 'INVALID_SQL: syntax error' }]);

    const result = await sandbox.fetchAthenaQuery('not sql');

    assert.deepEqual(result, { error: 'INVALID_SQL: syntax error' });
});

test('fetchAthenaQuery returns the SUCCEEDED status on the happy path', async () => {
    const sandbox = loadCore([
        { query_id: 'abc123' },
        { state: 'SUCCEEDED', rows: [[1]] },
    ]);

    const result = await sandbox.fetchAthenaQuery('select 1');

    assert.deepEqual(result, { state: 'SUCCEEDED', rows: [[1]] });
});
