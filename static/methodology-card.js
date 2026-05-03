function $(id) {
    return document.getElementById(id);
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function renderPrimitive(label, value) {
    return `
        <div class="kv-item">
            <span class="kv-label">${escapeHtml(label)}</span>
            <div>${escapeHtml(value)}</div>
        </div>
    `;
}

function renderArrayItem(item) {
    if (typeof item === "string") {
        return `<div class="stack-item">${escapeHtml(item)}</div>`;
    }
    const title = item.category || item.name || item.source_id || item.metric_id || "Item";
    const body = Object.entries(item)
        .filter(([key]) => !["category", "name", "source_id", "metric_id"].includes(key))
        .map(([key, value]) => `<div><strong>${escapeHtml(key)}</strong> ${renderInlineValue(value)}</div>`)
        .join("");
    return `<div class="stack-item"><strong>${escapeHtml(title)}</strong>${body}</div>`;
}

function renderInlineValue(value) {
    if (Array.isArray(value)) {
        return value.map((entry) => typeof entry === "string" ? escapeHtml(entry) : escapeHtml(JSON.stringify(entry))).join("; ");
    }
    if (value && typeof value === "object") {
        return escapeHtml(JSON.stringify(value));
    }
    return escapeHtml(String(value));
}

function renderMetricCard(metric) {
    return `
        <article class="metric-card">
            <strong>${escapeHtml(metric.name)}</strong>
            <div class="pill">weight ${escapeHtml(metric.weight)}</div>
            <div class="pill">${escapeHtml(metric.signal_class)}</div>
            <div class="metric-meta"><strong>Source</strong> ${escapeHtml(metric.source)}</div>
            <div class="metric-meta"><strong>Normalization</strong> ${escapeHtml(metric.normalization)}</div>
            <div class="metric-meta"><strong>Thresholds</strong> <code>${escapeHtml(JSON.stringify(metric.thresholds))}</code></div>
            <div class="metric-meta"><strong>Missingness</strong> ${escapeHtml(metric.missingness_rule)}</div>
        </article>
    `;
}

function renderFields(fields) {
    const parts = [];
    const primitiveCards = [];

    for (const [key, value] of Object.entries(fields || {})) {
        if (Array.isArray(value)) {
            if (key === "metrics") {
                parts.push(`<div class="metric-grid">${value.map(renderMetricCard).join("")}</div>`);
            } else {
                parts.push(`
                    <div class="stack">
                        <div class="kv-label">${escapeHtml(key)}</div>
                        ${value.map(renderArrayItem).join("")}
                    </div>
                `);
            }
        } else if (value && typeof value === "object") {
            primitiveCards.push(renderPrimitive(key, JSON.stringify(value)));
        } else {
            primitiveCards.push(renderPrimitive(key, value));
        }
    }

    if (primitiveCards.length) {
        parts.unshift(`<div class="kv-grid">${primitiveCards.join("")}</div>`);
    }

    return parts.join("");
}

function renderSection(section) {
    return `
        <section class="card">
            <div class="section-header">
                <p class="eyebrow">${escapeHtml(section.id.replaceAll("_", " "))}</p>
                <h2>${escapeHtml(section.title)}</h2>
                <p class="section-summary">${escapeHtml(section.summary)}</p>
            </div>
            ${renderFields(section.fields)}
        </section>
    `;
}

async function loadCard() {
    try {
        const resp = await fetch("/api/methodology-card");
        if (!resp.ok) throw new Error(`API returned ${resp.status}`);
        const data = await resp.json();
        $("sections").innerHTML = data.sections.map(renderSection).join("");
        $("loading").hidden = true;
        $("sections").hidden = false;
    } catch (err) {
        $("loading").hidden = true;
        $("error").hidden = false;
        $("error").textContent = `Unable to load methodology card: ${err.message}`;
    }
}

loadCard();
