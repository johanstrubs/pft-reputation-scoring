const state = { report: null };

function $(id) { return document.getElementById(id); }

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function truncateKey(key) {
    if (!key) return "unknown";
    return `${key.slice(0, 10)}...${key.slice(-6)}`;
}

function showError(message) {
    const banner = $("error-banner");
    banner.textContent = message;
    banner.style.display = "block";
}

function clearError() {
    const banner = $("error-banner");
    banner.textContent = "";
    banner.style.display = "none";
}

function setLoading(loading) {
    $("loading-card").style.display = loading ? "block" : "none";
    if (loading) $("peer-app").style.display = "none";
}

function updateShareLink(publicKey) {
    const url = new URL(window.location.href);
    url.searchParams.set("validator", publicKey);
    window.history.replaceState({}, "", url);
}

function renderSummary(report) {
    $("mode-banner").textContent = report.mode_banner;
    $("validator-name").textContent = report.domain || truncateKey(report.public_key);
    $("validator-node").textContent = report.observable_node?.node_public_key || "No observable node record mapped";
    $("peer-count").textContent = report.mode === "adjacency"
        ? `${report.summary.current_peer_count} current peers`
        : `${report.summary.total_nodes_analyzed} nodes observed`;
    $("quality-summary").textContent = `${report.summary.good_count} good, ${report.summary.acceptable_count} acceptable, ${report.summary.risky_count} risky`;
    const deltaText = report.summary.projected_rank_delta > 0 ? `up ${report.summary.projected_rank_delta}` : "no projected rank gain";
    $("projected-rank").textContent = `Rank ${report.summary.projected_rank}`;
    $("projected-score").textContent = `Best-case incremental upside: ${deltaText} with healthier peer picks`;
    $("json-link").href = report.json_report_url;
    $("disclaimer-text").textContent = report.disclaimer;
    $("table-title").textContent = report.table_title;
}

function renderFindings(findings) {
    const container = $("risk-findings");
    if (!findings.length) {
        container.innerHTML = `<div class="finding-card"><div class="finding-title">No major concentration flags detected</div><div class="finding-detail">The current observable peer landscape does not show an obvious provider, geography, version, or overlap issue from the data available.</div></div>`;
        return;
    }
    container.innerHTML = findings.map((finding) => `
        <article class="finding-card ${escapeHtml(finding.severity)}">
            <div class="status-chip ${escapeHtml(finding.severity)}">${escapeHtml(finding.severity)}</div>
            <div class="finding-title">${escapeHtml(finding.title)}</div>
            <div class="finding-detail">${escapeHtml(finding.detail)}</div>
        </article>
    `).join("");
}

function renderTable(rows) {
    const tbody = $("node-rows");
    tbody.innerHTML = rows.map((row) => `
        <tr>
            <td>
                <div class="row-title">${escapeHtml(row.domain || truncateKey(row.validator_public_key || row.node_public_key))}</div>
                <div class="mono">${escapeHtml(row.node_public_key || "unknown")}</div>
            </td>
            <td>${row.non_validating ? "Non-validating node" : "Validator node"}</td>
            <td><span class="status-chip ${escapeHtml(row.quality_rating)}">${escapeHtml(row.quality_rating)}</span></td>
            <td>${escapeHtml(row.provider || "Unknown")} ${row.asn ? `<span class="mono">AS${escapeHtml(row.asn)}</span>` : ""}</td>
            <td>${escapeHtml(row.country || "Unknown")}</td>
            <td>${escapeHtml(row.server_version || "unknown")}</td>
            <td>${row.latency_ms == null ? "n/a" : `${escapeHtml(row.latency_ms)}ms`}</td>
            <td>${escapeHtml(row.quality_reason)}</td>
        </tr>
    `).join("");
}

function renderRecommendations(targetId, recommendations, emptyMessage) {
    const container = $(targetId);
    if (!recommendations.length) {
        container.innerHTML = `<div class="recommendation-card"><div class="rec-title">${escapeHtml(emptyMessage)}</div></div>`;
        return;
    }
    container.innerHTML = recommendations.map((recommendation) => `
        <article class="recommendation-card ${escapeHtml(recommendation.quality_rating)}">
            <div class="status-chip ${escapeHtml(recommendation.quality_rating)}">${escapeHtml(recommendation.quality_rating)}</div>
            <div class="rec-title">${escapeHtml(truncateKey(recommendation.validator_public_key || recommendation.node_public_key))}</div>
            <div class="rec-meta">
                ${escapeHtml(recommendation.provider || "Unknown")} ${recommendation.asn ? `| AS${escapeHtml(recommendation.asn)}` : ""} | ${escapeHtml(recommendation.country || "Unknown")}
                <br>
                ${escapeHtml(recommendation.ip || "unknown")} : ${escapeHtml(recommendation.port || 2559)}
            </div>
            <div class="finding-detail">${escapeHtml(recommendation.reason)}</div>
        </article>
    `).join("");
}

async function loadReport(publicKey) {
    clearError();
    setLoading(true);
    try {
        const resp = await fetch(`/api/peers/${encodeURIComponent(publicKey)}`);
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load peer analysis");
        }
        const report = await resp.json();
        state.report = report;
        updateShareLink(publicKey);
        renderSummary(report);
        renderFindings(report.risk_findings);
        renderTable(report.node_rows);
        renderRecommendations("add-recommendations", report.add_recommendations, "No strong add recommendations were identified from the current data.");
        renderRecommendations("drop-recommendations", report.drop_recommendations, "No current peers stand out as obvious drop candidates.");
        $("peer-app").style.display = "block";
    } catch (err) {
        $("peer-app").style.display = "none";
        state.report = null;
        showError(err.message || "Failed to load peer analysis");
    } finally {
        setLoading(false);
    }
}

function copyShareLink() {
    navigator.clipboard.writeText(window.location.href);
}

$("load-button").addEventListener("click", () => {
    const publicKey = $("validator-key").value.trim();
    if (!publicKey) {
        showError("Enter a validator public key first.");
        return;
    }
    loadReport(publicKey);
});

$("share-button").addEventListener("click", copyShareLink);

const params = new URLSearchParams(window.location.search);
const prefilled = params.get("validator");
if (prefilled) {
    $("validator-key").value = prefilled;
    loadReport(prefilled);
}
