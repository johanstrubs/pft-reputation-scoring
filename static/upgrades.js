function $(id) {
    return document.getElementById(id);
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function truncateKey(key) {
    if (!key) return "";
    return `${key.slice(0, 12)}...${key.slice(-6)}`;
}

function showError(message) {
    const banner = $("error-banner");
    banner.textContent = message;
    banner.style.display = "block";
}

function setLoading(loading) {
    $("loading-card").style.display = loading ? "block" : "none";
    if (loading) {
        $("upgrades-app").style.display = "none";
    }
}

function renderSummary(report) {
    $("latest-version").textContent = report.latest_version || "Unknown";
    $("upgrade-summary").textContent = `${report.upgraded_count} of ${report.total_validators} validators on latest version (${report.upgraded_pct.toFixed(1)}%)`;
    $("progress-bar").style.width = `${report.upgraded_pct}%`;
    $("json-link").href = report.json_report_url;
    $("distribution-note").textContent = `${report.version_distribution.length} distinct version buckets in the current cohort`;
    $("lagging-note").textContent = report.lagging_validators.length
        ? `${report.lagging_validators.length} validators are not yet on ${report.latest_version}`
        : "No lagging validators in the current round";
}

function renderDistribution(report) {
    $("distribution-list").innerHTML = report.version_distribution.map((entry) => `
        <div class="distribution-row ${entry.version === report.latest_version ? "latest" : ""}">
            <div class="distribution-label">${escapeHtml(entry.version)}</div>
            <div class="distribution-bar-shell"><div class="distribution-bar" style="width:${Math.max(entry.percentage, 2)}%"></div></div>
            <div class="history-meta">${entry.count} validators • ${entry.percentage.toFixed(1)}%</div>
        </div>
    `).join("");
}

function renderLagging(report) {
    $("current-version-card").innerHTML = `
        <div class="table-row-header">
            <strong>Current-version validators</strong>
            <span>${report.upgraded_count} validators on ${escapeHtml(report.latest_version || "unknown")}</span>
        </div>
        <div class="table-meta">This section stays collapsed by design so the table below can focus on lagging validators and rollout follow-up.</div>
    `;

    if (!report.lagging_validators.length) {
        $("lagging-table").innerHTML = `<div class="table-row"><strong>Everyone is current.</strong><div class="table-meta">No non-latest validators were present in the latest round.</div></div>`;
        return;
    }

    const byVersion = new Map();
    for (const validator of report.lagging_validators) {
        if (!byVersion.has(validator.current_version)) {
            byVersion.set(validator.current_version, []);
        }
        byVersion.get(validator.current_version).push(validator);
    }

    $("lagging-table").innerHTML = Array.from(byVersion.entries()).map(([version, validators]) => `
        <section class="version-group">
            <div class="version-group-title">${escapeHtml(version)} • ${validators.length} validator${validators.length === 1 ? "" : "s"}</div>
            ${validators.map((validator) => `
                <article class="table-row">
                    <div class="table-row-header">
                        <div>
                            <strong>${escapeHtml(validator.domain || truncateKey(validator.public_key))}</strong>
                            <div class="table-meta">${escapeHtml(validator.public_key)}</div>
                        </div>
                        <span class="status-chip">Lagging</span>
                    </div>
                    <div class="table-meta">Current version: <span class="lagging-version">${escapeHtml(validator.current_version)}</span></div>
                    <div class="table-meta">Days behind latest release appearance: ${validator.days_behind}</div>
                </article>
            `).join("")}
        </section>
    `).join("");
}

function renderChart(report) {
    const svg = $("adoption-chart");
    const empty = $("chart-empty");
    const history = report.adoption_history || [];
    if (!history.length) {
        svg.style.display = "none";
        empty.style.display = "block";
        return;
    }

    empty.style.display = "none";
    svg.style.display = "block";

    const width = 700;
    const height = 220;
    const left = 42;
    const right = 16;
    const top = 18;
    const bottom = 30;
    const innerWidth = width - left - right;
    const innerHeight = height - top - bottom;

    const points = history.map((entry, index) => {
        const x = left + (history.length === 1 ? innerWidth / 2 : (innerWidth * index) / (history.length - 1));
        const y = top + innerHeight - (entry.percentage / 100) * innerHeight;
        return { x, y, label: entry.date, percentage: entry.percentage };
    });

    const path = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
    const guides = [0, 25, 50, 75, 100].map((pct) => {
        const y = top + innerHeight - (pct / 100) * innerHeight;
        return `<line x1="${left}" y1="${y}" x2="${width - right}" y2="${y}" stroke="rgba(156,176,205,0.15)" stroke-width="1" />
                <text x="8" y="${y + 4}" fill="#9cb0cd" font-size="11">${pct}%</text>`;
    }).join("");

    const dots = points.map((point) => `
        <circle cx="${point.x}" cy="${point.y}" r="4" fill="#76d6a3">
            <title>${point.label}: ${point.percentage.toFixed(1)}%</title>
        </circle>
    `).join("");

    const firstLabel = points[0];
    const lastLabel = points[points.length - 1];
    svg.innerHTML = `
        ${guides}
        <path d="${path}" fill="none" stroke="#69a7ff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></path>
        ${dots}
        <text x="${left}" y="${height - 8}" fill="#9cb0cd" font-size="11">${firstLabel.label}</text>
        <text x="${width - right - 72}" y="${height - 8}" fill="#9cb0cd" font-size="11">${lastLabel.label}</text>
    `;
}

async function loadUpgrades() {
    $("error-banner").style.display = "none";
    setLoading(true);
    try {
        const resp = await fetch("/api/upgrades");
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load upgrade tracker");
        }
        const report = await resp.json();
        renderSummary(report);
        renderDistribution(report);
        renderLagging(report);
        renderChart(report);
        $("upgrades-app").style.display = "block";
    } catch (error) {
        showError(error.message || "Failed to load upgrade tracker");
    } finally {
        setLoading(false);
    }
}

loadUpgrades();
