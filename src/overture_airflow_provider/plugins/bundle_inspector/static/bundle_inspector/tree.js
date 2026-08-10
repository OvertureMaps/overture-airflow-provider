// Tree rendering and navigation.
// Depends on: core.js, components/registry.js

function depthForPrefix(prefix) {
    if (!prefix) return 0;
    return prefix.replace(/\/$/, '').split('/').length;
}

function stageForPrefix(prefix) {
    if (!prefix) return null;
    return prefix.split('/')[0];
}

function runDepthForStage(stage) {
    const info = state.stageInfo[stage];
    if (info) return info.run_depth;
    if (stage === 'release_candidate') return 3;
    return 4;
}

function displayName(rawName) {
    const stripped = rawName.replace(/\/$/, '');
    const eqIdx = stripped.indexOf('=');
    if (eqIdx !== -1) return stripped.substring(eqIdx + 1);
    return stripped;
}

function hierarchyLabel(stage, nodeDepth) {
    const info = state.stageInfo[stage];
    if (!info || !info.hierarchy) return '';
    const idx = nodeDepth - 2;
    if (idx < 0 || idx >= info.hierarchy.length) return '';
    const label = info.hierarchy[idx];
    return label.startsWith('!') ? label.slice(1) : label;
}

function labelForNode(stage, nodeDepth) {
    if (nodeDepth === 1) return 'stage';
    return hierarchyLabel(stage, nodeDepth);
}

function isComponentNode(prefix) {
    const stage = stageForPrefix(prefix);
    if (!stage) return false;
    return depthForPrefix(prefix) === runDepthForStage(stage) + 1;
}

function isRunLevelNode(prefix) {
    const stage = stageForPrefix(prefix);
    if (!stage) return false;
    return depthForPrefix(prefix) === runDepthForStage(stage);
}

// -- Rendering ------------------------------------------------

function renderNode(item, parentPrefix, parentDepth, parentEl) {
    const fullPrefix = parentPrefix + item.name;
    const isDir = item.type === 'directory';
    const nodeDepth = depthForPrefix(fullPrefix);
    const stage = stageForPrefix(fullPrefix);
    const component = isDir && isComponentNode(fullPrefix);

    const row = document.createElement('div');
    row.className = 'tree-item';
    row.style.paddingLeft = `${12 + parentDepth * 16}px`;
    row.dataset.prefix = fullPrefix;

    const toggle = document.createElement('span');
    toggle.className = 'tree-toggle';

    const label = document.createElement('span');

    if (component) {
        toggle.textContent = ' ';
        label.className = 'tree-label component';
        label.textContent = displayName(item.name);
        row.addEventListener('click', () => selectComponent(fullPrefix));
    } else if (isDir) {
        const cached = state.tree[fullPrefix];
        toggle.textContent = cached && cached.expanded ? 'v' : '>';
        label.className = 'tree-label dir';

        const hint = labelForNode(stage, nodeDepth);
        const display = displayName(item.name);
        label.textContent = hint ? `${hint}: ${display}` : display;

        row.addEventListener('click', () => toggleNode(fullPrefix));
    } else {
        toggle.textContent = ' ';
        label.className = 'tree-label file';
        label.textContent = item.name;
    }

    row.appendChild(toggle);
    row.appendChild(label);

    if (isDir && isRunLevelNode(fullPrefix)) {
        const statusSpan = document.createElement('span');
        statusSpan.className = 'tree-status';
        row.appendChild(statusSpan);
    }

    parentEl.appendChild(row);

    if (isDir && !component) {
        const childContainer = document.createElement('div');
        childContainer.className = 'tree-children';
        childContainer.id = domIdForPrefix(fullPrefix);
        parentEl.appendChild(childContainer);

        const cached = state.tree[fullPrefix];
        if (cached && cached.expanded && cached.children) {
            childContainer.classList.add('expanded');
            renderChildren(cached.children, fullPrefix, depthForPrefix(fullPrefix), childContainer);
        }
    }
}

function renderChildren(items, parentPrefix, depth, containerEl) {
    containerEl.innerHTML = '';
    for (const item of items) {
        renderNode(item, parentPrefix, depth, containerEl);
    }
}

// -- Toggle / expand ------------------------------------------

async function toggleNode(prefix) {
    const cached = state.tree[prefix];
    const container = document.getElementById(domIdForPrefix(prefix));
    if (!container) return;

    if (cached && cached.expanded) {
        cached.expanded = false;
        container.classList.remove('expanded');
        updateToggleIcon(prefix, false);
        return;
    }

    if (cached && cached.children) {
        cached.expanded = true;
        container.classList.add('expanded');
        updateToggleIcon(prefix, true);
        selectIfRunLevel(prefix, cached.data);
        return;
    }

    const loadingEl = document.createElement('div');
    loadingEl.className = 'tree-loading';
    loadingEl.innerHTML = '<span class="spinner-inline"></span> Loading...';
    container.innerHTML = '';
    container.appendChild(loadingEl);
    container.classList.add('expanded');
    updateToggleIcon(prefix, true);

    try {
        const data = await fetchChildren(prefix);

        if (data.stage) {
            state.stageInfo[data.stage] = {
                run_depth: data.run_depth,
                hierarchy: data.hierarchy,
            };
        }

        state.tree[prefix] = {
            expanded: true,
            children: data.children,
            data: data,
        };

        renderChildren(data.children, prefix, depthForPrefix(prefix), container);
        updateRunStatusIcon(prefix, data);
        selectIfRunLevel(prefix, data);
    } catch (err) {
        container.innerHTML = '<div class="tree-loading" style="color:#dc3545;">Error loading</div>';
        console.error('toggleNode error:', err);
    }
}

function updateToggleIcon(prefix, expanded) {
    const row = document.querySelector(`.tree-item[data-prefix="${CSS.escape(prefix)}"]`);
    if (!row) return;
    const toggle = row.querySelector('.tree-toggle');
    if (toggle) toggle.textContent = expanded ? 'v' : '>';
}

function updateRunStatusIcon(prefix, data) {
    if (!isRunLevelNode(prefix)) return;
    const row = document.querySelector(`.tree-item[data-prefix="${CSS.escape(prefix)}"]`);
    if (!row) return;
    const statusSpan = row.querySelector('.tree-status');
    if (!statusSpan) return;
    statusSpan.textContent = data.has_success ? '\u2714' : '\u2718';
    statusSpan.style.color = data.has_success ? '#96C93D' : '#dc3545';
}

function selectIfRunLevel(prefix, data) {
    if (!isRunLevelNode(prefix)) return;
    selectNode(prefix, data);
}

function selectNode(prefix, data) {
    for (const fn of activeCleanups) fn();
    activeCleanups = [];
    setSelectedTreeItem(prefix);
    renderDetail(prefix, data);
}

// -- Expand to path -------------------------------------------

async function expandToPath(prefix) {
    const parts = prefix.replace(/\/$/, '').split('/');
    const progressivePrefixes = [];
    for (let i = 0; i < parts.length; i++) {
        progressivePrefixes.push(parts.slice(0, i + 1).join('/') + '/');
    }

    if (document.getElementById('treeRoot').children.length === 0) {
        await loadRoot();
    }

    for (const p of progressivePrefixes) {
        const cached = state.tree[p];
        if (!cached || !cached.expanded) {
            await toggleNode(p);
        }
    }

    const finalPrefix = progressivePrefixes[progressivePrefixes.length - 1];
    const finalCached = state.tree[finalPrefix];
    if (finalCached) {
        selectNode(finalPrefix, finalCached.data);
    }

    const row = document.querySelector(`.tree-item[data-prefix="${CSS.escape(finalPrefix)}"]`);
    if (row) row.scrollIntoView({ block: 'center', behavior: 'smooth' });
}
