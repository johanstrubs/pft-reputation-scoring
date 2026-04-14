const $ = (id) => document.getElementById(id);

const state = {
    report: null,
};

function fmtNumber(value) {
    return typeof value === "number" ? value.toFixed(2) : "-";
}

function fmtDelta(value) {
    if (value == null) return "-";
    const sign = value > 0 ? "+" : "";
    return `${sign}${typeof value === "number" ? value.toFixed(2) : value}`;
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}

function setLoading(loading) {
    $("loading").hidden = !loading;
    $("improvements-app").hidden = loading || !state.report;
}

function showError(message) {
    $("error").hidden = false;
    $("error").textContent = message;
    $("empty").hidden = true;
    $("status-card").hidden = true;
    $("improvements-app").hidden = true;
    $("loading").hidden = true;
}

function clearError() {
    $("error").hidden = true;
    $("error").textContent = "";
}

function renderSummary(report) {
    $("status-card").hidden = false;
    $("status-title").textContent = report.demo_mode
        ? "Improvement timeline loaded in demo verification mode"
        : "Improvement timeline loaded";
    const tracking = report.tracking_since ? `Tracking since ${report.tracking_since}` : "Tracking baseline available";
    $("status-subtitle").textContent = `${tracking} | ${report.domain || report.public_key}`;
    $("json-link").href = report.json_report_url;

    $("resolved-count").textContent = String(report.total_findings_resolved);
    $("score-improvement").textContent = fmtNumber(report.total_score_improvement);
    $("rank-delta").textContent = report.rank_delta_since_tracking == null
        ? "-"
        : `${report.current_rank} now (${report.rank_delta_since_tracking > 0 ? "+" : ""}${report.rank_delta_since_tracking} vs start)`;
}

function renderBiggestWins(report) {
    const root = $("biggest-wins");
    if (!report.biggest_wins.length) {
        root.innerHTML = `<div class="item"><strong>No resolved findings yet.</strong><div class="subtle">This section will light up after the tracker confirms fixes.</div></div>`;
        return;
    }
    root.innerHTML = report.biggest_wins.map((item) => `
        <div class="item">
            <h3>${escapeHtml(item.title)}</h3>
            <div class="meta">
                <span class="chip ${escapeHtml(item.impact_confidence)}">${escapeHtml(item.impact_confidence)}</span>
                ${item.synthetic ? `<span class="chip synthetic">demo</span>` : ""}
            </div>
            <div>Resolved ${escapeHtml(item.resolved_date)} after ${escapeHtml(item.days_to_resolution)} day(s).</div>
            <div class="delta ${item.score_delta > 0 ? "positive" : "neutral"}">Score delta: ${fmtDelta(item.score_delta)}</div>
        </div>
    `).join("");
}

function renderNetworkSummary(report) {
    const summary = report.network_summary;
    $("network-summary").innerHTML = `
        <div class="item">
            <div><strong>Resolved this week:</strong> ${summary.total_resolved_this_week}</div>
            <div><strong>Average days to resolution:</strong> ${summary.average_days_to_resolution}</div>
            <div><strong>Most commonly resolved:</strong> ${escapeHtml(summary.most_common_resolved_finding_type || "None yet")}</div>
            <div><strong>Most commonly ignored:</strong> ${escapeHtml(summary.most_common_ignored_finding_type || "None yet")}</div>
        </div>
    `;
}

function renderResolved(report) {
    const root = $("resolved-timeline");
    if (!report.resolved_findings.length) {
        root.innerHTML = `<div class="item"><strong>No confirmed resolutions yet.</strong><div class="subtle">Use the demo seed fallback until enough daily snapshots accumulate.</div></div>`;
        return;
    }
    root.innerHTML = report.resolved_findings.map((item) => `
        <div class="item">
            <h3>${escapeHtml(item.title)}</h3>
            <div class="meta">
                <span class="chip ${escapeHtml(item.impact_confidence)}">${escapeHtml(item.impact_confidence)}</span>
                ${item.synthetic ? `<span class="chip synthetic">demo seeded</span>` : ""}
                <span class="chip">${escapeHtml(item.severity)}</span>
            </div>
            <div><strong>Opened:</strong> ${escapeHtml(item.opened_date)} | <strong>Resolved:</strong> ${escapeHtml(item.resolved_date)} | <strong>Days:</strong> ${escapeHtml(item.days_to_resolution)}</div>
            <div><strong>Before:</strong> score ${fmtNumber(item.score_before)}, rank ${item.rank_before ?? "-"} | <strong>After:</strong> score ${fmtNumber(item.score_after)}, rank ${item.rank_after ?? "-"}</div>
            <div class="delta ${item.score_delta > 0 ? "positive" : "neutral"}"><strong>Score delta:</strong> ${fmtDelta(item.score_delta)} | <strong>Estimated impact:</strong> ${fmtDelta(item.estimated_impact)}</div>
            <div class="subtle"><strong>Detected:</strong> ${escapeHtml(item.detected_value)} | <strong>Expected:</strong> ${escapeHtml(item.expected_value)}</div>
        </div>
    `).join("");
}

function renderOpen(report) {
    const root = $("open-findings");
    if (!report.open_findings.length) {
        root.innerHTML = `<div class="item"><strong>No open findings right now.</strong><div class="subtle">This validator does not currently have outstanding readiness or diagnostic issues in the tracker.</div></div>`;
        return;
    }
    root.innerHTML = report.open_findings.map((item) => `
        <div class="item">
            <h3>${escapeHtml(item.title)}</h3>
            <div class="meta">
                <span class="chip">${escapeHtml(item.severity)}</span>
                <span class="chip">${escapeHtml(item.days_open)} day(s) open</span>
            </div>
            <div><strong>First seen:</strong> ${escapeHtml(item.first_seen_date)}</div>
            <div class="subtle"><strong>Detected:</strong> ${escapeHtml(item.detected_value)} | <strong>Expected:</strong> ${escapeHtml(item.expected_value)}</div>
            <div><a href="${item.remediation_url}" target="_blank" rel="noreferrer">Open remediation plan</a></div>
        </div>
    `).join("");
}

function renderReport(report) {
    state.report = report;
    renderSummary(report);
    renderBiggestWins(report);
    renderNetworkSummary(report);
    renderResolved(report);
    renderOpen(report);
    $("empty").hidden = true;
    $("improvements-app").hidden = false;
}

async function loadReport(publicKey) {
    clearError();
    if (!publicKey) {
        state.report = null;
        $("empty").hidden = false;
        $("status-card").hidden = true;
        $("improvements-app").hidden = true;
        $("loading").hidden = true;
        return;
    }

    setLoading(true);
    $("empty").hidden = true;
    try {
        const resp = await fetch(`/api/improvements/${encodeURIComponent(publicKey)}`);
        const data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.detail || "Failed to load improvement timeline");
        }
        renderReport(data);
        setLoading(false);
        const nextUrl = new URL(window.location.href);
        nextUrl.searchParams.set("validator", publicKey);
        window.history.replaceState({}, "", nextUrl);
    } catch (err) {
        showError(err.message || "Failed to load improvement timeline");
    }
}

$("load-button").addEventListener("click", () => loadReport($("validator-input").value.trim()));
$("validator-input").addEventListener("keydown", (event) => {
    if (event.key === "Enter") loadReport($("validator-input").value.trim());
});

const params = new URLSearchParams(window.location.search);
const initialValidator = params.get("validator");
if (initialValidator) {
    $("validator-input").value = initialValidator;
    loadReport(initialValidator);
} else {
    $("loading").hidden = true;
    $("empty").hidden = false;
}
