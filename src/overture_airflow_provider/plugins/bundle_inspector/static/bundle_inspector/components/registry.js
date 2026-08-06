// Display component registry.
// Each component file calls registerComponent() to self-register.
// Multiple components can register for the same name; they render stacked.
// Depends on: core.js

const DISPLAY_COMPONENTS = {};
let activeCleanups = [];

function registerComponent(name, component) {
    if (!DISPLAY_COMPONENTS[name]) {
        DISPLAY_COMPONENTS[name] = [];
    }
    DISPLAY_COMPONENTS[name].push(component);
}

function componentNameFromPrefix(prefix) {
    const parts = prefix.replace(/\/$/, '').split('/');
    return parts[parts.length - 1];
}

async function renderComponentContent(contentDiv, prefix, componentName) {
    const displays = DISPLAY_COMPONENTS[componentName];

    if (displays && displays.length > 0) {
        for (const display of displays) {
            const wrapper = document.createElement('div');
            wrapper.className = 'component-section';
            contentDiv.appendChild(wrapper);
            const cleanup = await display.render(wrapper, prefix);
            if (cleanup) activeCleanups.push(cleanup);
        }
    } else {
        const wrapper = document.createElement('div');
        wrapper.className = 'component-section';
        contentDiv.appendChild(wrapper);
        const cleanup = await renderFolderTree(wrapper, prefix);
        if (cleanup) activeCleanups.push(cleanup);
    }
}

function buildNarrowedPrefix(basePrefix, themeDir, typeDir) {
    let p = basePrefix;
    if (themeDir) p += `${themeDir}/`;
    if (typeDir) p += `${typeDir}/`;
    return p;
}

async function selectComponent(prefix) {
    for (const fn of activeCleanups) fn();
    activeCleanups = [];

    setSelectedTreeItem(prefix);

    const panel = document.getElementById('detailPanel');
    const name = componentNameFromPrefix(prefix);
    const path = s3PathForPrefix(prefix);

    panel.innerHTML = `
        <div class="detail-heading">${escapeHtml(name)}</div>
        <div class="detail-path"><a href="${escapeHtml(s3ConsoleUrl(prefix))}" target="_blank" rel="noopener">${escapeHtml(path)}</a></div>
    `;

    const contentDiv = document.createElement('div');
    contentDiv.className = 'component-content';
    panel.appendChild(contentDiv);

    let themeTypes;
    try {
        themeTypes = await fetchThemeTypes(prefix);
    } catch (err) {
        console.error('fetchThemeTypes error:', err);
        themeTypes = { themes: [], types_by_theme: {} };
    }

    const { themes, theme_dirs, types_by_theme } = themeTypes;

    // No Hive partitions: render directly with original prefix
    if (themes.length === 0) {
        await renderComponentContent(contentDiv, prefix, name);
        return;
    }

    // Render selector (even for single combo, so user sees what's selected)
    const selectorDiv = document.createElement('div');
    selectorDiv.className = 'theme-type-selector';
    panel.insertBefore(selectorDiv, contentDiv);

    // Theme dropdown: display labels as text, raw dir names as values
    const themeSelect = document.createElement('select');
    for (let i = 0; i < themes.length; i++) {
        const opt = document.createElement('option');
        opt.value = theme_dirs[i];
        opt.textContent = themes[i];
        themeSelect.appendChild(opt);
    }

    const typeSelect = document.createElement('select');

    function populateTypes(themeLabel) {
        typeSelect.innerHTML = '';
        const typeDirs = types_by_theme[themeLabel] || [];
        if (typeDirs.length === 0) {
            typeSelect.style.display = 'none';
            typeLabel.style.display = 'none';
        } else {
            typeSelect.style.display = '';
            typeLabel.style.display = '';
            for (const dir of typeDirs) {
                const opt = document.createElement('option');
                opt.value = dir;
                // Strip "type=" prefix for display if present
                opt.textContent = dir.includes('=') ? dir.split('=')[1] : dir;
                typeSelect.appendChild(opt);
            }
        }
    }

    function selectedThemeLabel() {
        const idx = themeSelect.selectedIndex;
        return idx >= 0 ? themes[idx] : themes[0];
    }

    async function renderSelected() {
        for (const fn of activeCleanups) fn();
        activeCleanups = [];
        contentDiv.innerHTML = '';
        const themeDir = themeSelect.value;
        const typeDir = typeSelect.value || null;
        const narrowed = buildNarrowedPrefix(prefix, themeDir, typeDir);
        await renderComponentContent(contentDiv, narrowed, name);
    }

    const themeLabel = document.createElement('label');
    themeLabel.textContent = 'Theme';
    const typeLabel = document.createElement('label');
    typeLabel.textContent = 'Type';

    selectorDiv.appendChild(themeLabel);
    selectorDiv.appendChild(themeSelect);
    selectorDiv.appendChild(typeLabel);
    selectorDiv.appendChild(typeSelect);

    populateTypes(themes[0]);

    themeSelect.addEventListener('change', () => {
        populateTypes(selectedThemeLabel());
        renderSelected();
    });
    typeSelect.addEventListener('change', () => renderSelected());

    await renderSelected();
}
