// Initialization and top-level actions.
// Depends on: core.js, tree.js, detail.js, components/registry.js

async function loadRoot() {
    const root = document.getElementById('treeRoot');
    root.innerHTML = '<div class="tree-loading"><span class="spinner-inline"></span> Loading...</div>';

    try {
        const data = await fetchChildren('');
        root.innerHTML = '';
        renderChildren(data.children, '', 0, root);
    } catch (err) {
        root.innerHTML = '<div class="tree-loading" style="color:#dc3545;">Error loading tree.</div>';
        console.error('loadRoot error:', err);
    }
}

async function refresh() {
    state.tree = {};
    state.selectedPrefix = null;
    state.stageInfo = {};

    document.getElementById('treeRoot').innerHTML = '';
    document.getElementById('detailPanel').innerHTML = '<div class="detail-empty">Select a node to inspect.</div>';

    await fetchConfig();
    await loadRoot();
}

async function initialize() {
    try {
        await fetchConfig();

        // Restore user override from URL path before loading the tree
        const urlPath = prefixFromUrl();
        if (urlPath && state.config && state.config.environment === 'dev') {
            const firstSlash = urlPath.indexOf('/');
            if (firstSlash > 0) {
                const maybeUser = urlPath.slice(0, firstSlash);
                // Stage names contain underscores; user namespaces don't start with known stages
                const KNOWN_STAGES = ['theme_promote', 'theme_stage', 'release_candidate'];
                if (!KNOWN_STAGES.includes(maybeUser)) {
                    state.user = maybeUser;
                }
            }
        }

        await loadUserSelector();
        await loadRoot();

        // Navigate to path from URL if present
        if (urlPath) {
            let prefix = urlPath;
            if (state.user && prefix.startsWith(state.user + '/')) {
                prefix = prefix.slice(state.user.length + 1);
            }
            if (prefix) await expandToPath(prefix);
        }
    } catch (err) {
        console.error('initialize error:', err);
        document.getElementById('treeRoot').innerHTML =
            '<div class="tree-loading" style="color:#dc3545;">Failed to initialize. Reload the page.</div>';
    }
}

document.addEventListener('DOMContentLoaded', initialize);
