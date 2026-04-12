function $(id) { return document.getElementById(id); }

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function escapeAttr(value) {
    return escapeHtml(value)
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
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
    if (loading) $("remediation-app").style.display = "none";
}

function updateShareLink(publicKey) {
    const url = new URL(window.location.href);
    url.searchParams.set("validator", publicKey);
    window.history.replaceState({}, "", url);
}

function renderSummary(report) {
    $("validator-name").textContent = report.domain || truncateKey(report.public_key);
    $("validator-key-text").textContent = report.public_key;
    $("summary-status").textContent = report.status_summary;
    $("summary-impact").textContent = `Estimated total upside if all actionable items are fixed: ${report.total_estimated_score_improvement.toFixed(1)} points`;
    $("severity-mix").textContent = `${report.summary_counts.critical} critical / ${report.summary_counts.warning} warning / ${report.summary_counts.advisory} advisory`;
    $("report-timestamp").textContent = new Date(report.timestamp).toLocaleString();
    $("json-link").href = report.json_report_url;
}

function renderSourceStatus(sourceStatus) {
    $("source-status").innerHTML = Object.entries(sourceStatus).map(([name, entry]) => `
        <article class="source-card">
            <div class="finding-title">${escapeHtml(name.replaceAll("_", " "))}</div>
            <div class="source-meta">Status: <strong>${escapeHtml(entry.status)}</strong></div>
            <div class="source-meta">Last updated: ${escapeHtml(new Date(entry.timestamp).toLocaleString())}</div>
            ${entry.count != null ? `<div class="source-meta">Count: ${escapeHtml(entry.count)}</div>` : ""}
            ${entry.json_report_url ? `<div class="source-meta"><a href="${escapeHtml(entry.json_report_url)}" target="_blank" rel="noreferrer">Open source JSON</a></div>` : ""}
        </article>
    `).join("");
}

function renderFinding(item) {
    const commandText = item.commands.join("\n");
    return `
        <article class="finding-card ${escapeHtml(item.severity)}">
            <div class="status-chip ${escapeHtml(item.severity)}">${escapeHtml(item.severity)}</div>
            <div class="finding-title">${escapeHtml(item.title)}</div>
            <div class="finding-summary">${escapeHtml(item.summary)}</div>
            <div class="meta-grid">
                <div class="meta-line">Source: <strong>${escapeHtml(item.sources.join(", "))}</strong></div>
                <div class="meta-line">Category: <strong>${escapeHtml(item.category)}</strong></div>
                <div class="meta-line">Detected: <strong>${escapeHtml(item.detected_value)}</strong></div>
                <div class="meta-line">Expected: <strong>${escapeHtml(item.expected_value)}</strong></div>
                <div class="meta-line">Estimated score impact: <strong>${escapeHtml(item.estimated_score_impact)}</strong> (${escapeHtml(item.impact_confidence)})</div>
                <div class="meta-line">Last updated: <strong>${escapeHtml(new Date(item.source_timestamp).toLocaleString())}</strong></div>
            </div>
            <div class="command-block">
                <div class="command-head">
                    <strong>Illustrative commands</strong>
                    <button class="copy-button" type="button" data-copy="${escapeAttr(commandText)}">Copy</button>
                </div>
                <pre>${escapeHtml(commandText)}</pre>
            </div>
            ${item.rollback_note ? `<div class="meta-line">Rollback note: ${escapeHtml(item.rollback_note)}</div>` : ""}
        </article>
    `;
}

function attachCopyButtons() {
    document.querySelectorAll("[data-copy]").forEach((button) => {
        button.addEventListener("click", async () => {
            await navigator.clipboard.writeText(button.dataset.copy || "");
            button.textContent = "Copied";
            setTimeout(() => { button.textContent = "Copy"; }, 1200);
        });
    });
}

function renderFindings(report) {
    $("actionable-findings").innerHTML = report.actionable_findings.length
        ? report.actionable_findings.map(renderFinding).join("")
        : `<article class="finding-card advisory"><div class="finding-title">No action needed</div><div class="finding-summary">All upstream surfaces currently report a clean bill of health for this validator.</div></article>`;
    $("advisories").innerHTML = report.advisories.length
        ? report.advisories.map(renderFinding).join("")
        : `<article class="finding-card advisory"><div class="finding-summary">No advisories are currently open.</div></article>`;
    attachCopyButtons();
}

async function loadReport(publicKey) {
    clearError();
    setLoading(true);
    try {
        const resp = await fetch(`/api/remediate/${encodeURIComponent(publicKey)}`);
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load remediation plan");
        }
        const report = await resp.json();
        updateShareLink(publicKey);
        renderSummary(report);
        renderSourceStatus(report.source_status);
        renderFindings(report);
        $("remediation-app").style.display = "block";
    } catch (err) {
        $("remediation-app").style.display = "none";
        showError(err.message || "Failed to load remediation plan");
    } finally {
        setLoading(false);
    }
}

$("load-button").addEventListener("click", () => {
    const publicKey = $("validator-key").value.trim();
    if (!publicKey) {
        showError("Enter a validator public key first.");
        return;
    }
    loadReport(publicKey);
});

$("share-button").addEventListener("click", () => navigator.clipboard.writeText(window.location.href));

const params = new URLSearchParams(window.location.search);
const prefilled = params.get("validator");
if (prefilled) {
    $("validator-key").value = prefilled;
    loadReport(prefilled);
}
