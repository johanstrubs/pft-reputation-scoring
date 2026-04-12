const state = {
    report: null,
};

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
    if (loading) {
        $("diversity-app").style.display = "none";
    }
}

function updateShareLink(publicKey) {
    const url = new URL(window.location.href);
    url.searchParams.set("validator", publicKey);
    window.history.replaceState({}, "", url);
}

function fmtSigned(value, decimals = 2) {
    const n = Number(value || 0);
    return `${n >= 0 ? "+" : ""}${n.toFixed(decimals)}`;
}

function fmtPct(value) {
    return `${Number(value || 0).toFixed(1)}%`;
}

function renderSummary(report) {
    const current = report.current_context;
    $("validator-name").textContent = current.domain || truncateKey(current.public_key);
    $("validator-key-text").textContent = current.public_key;
    $("diversity-score").textContent = current.diversity_score.toFixed(3);
    $("rank-text").textContent = `Composite ${current.composite_score.toFixed(2)} • Rank #${current.rank} of ${current.validator_count}`;
    $("bundle-label").textContent = current.bundle_label;
    $("health-message").textContent = current.clean_bill_of_health
        ? "This validator is already in underrepresented groups."
        : "This validator is contributing to one or more overrepresented groups.";
    $("json-link").href = report.json_report_url;
    $("disclaimer").textContent = report.disclaimer;
}

function renderGroupings(report) {
    const entries = [
        ["Provider", report.current_context.provider_group],
        ["ASN", report.current_context.asn_group],
        ["Country", report.current_context.country_group],
        ["Bundle", report.current_context.bundle_group],
    ];
    $("grouping-grid").innerHTML = entries.map(([label, grouping]) => {
        if (!grouping) {
            return `
                <article class="group-card">
                    <div class="group-title">${label}</div>
                    <div class="meta-text">No live topology value available for this validator yet.</div>
                </article>
            `;
        }
        return `
            <article class="group-card ${grouping.above_threshold ? "over" : ""}">
                <div class="status-chip ${grouping.above_threshold ? "" : "healthy"}">${grouping.above_threshold ? "Over threshold" : "Healthy"}</div>
                <div class="group-title">${label}: ${escapeHtml(grouping.value)}</div>
                <div class="metric-line">${grouping.shared_count} validators share this grouping</div>
                <div class="metric-line">Concentration: ${fmtPct(grouping.concentration_pct)}</div>
                <div class="metric-line">${grouping.above_threshold ? `${fmtPct(grouping.threshold_over_pct)} above the 33% threshold` : "Below the 33% threshold"}</div>
            </article>
        `;
    }).join("");
}

function renderConcentration(report) {
    $("concentration-list").innerHTML = report.concentration_summary.map((entry) => `
        <article class="bundle-card">
            <div class="bundle-title">${escapeHtml(entry.bundle.label)}</div>
            <div class="metric-line">${entry.validator_count} validators • ${fmtPct(entry.concentration_pct)}</div>
        </article>
    `).join("");
}

function renderRecommendations(report) {
    const cleanBill = $("clean-bill");
    const list = $("recommendations-list");
    if (!report.recommendations.length) {
        cleanBill.style.display = "block";
        cleanBill.innerHTML = `
            <strong>Clean bill of health.</strong>
            <div>This validator is already in underrepresented provider, ASN, and country groupings, so no migration recommendation is needed.</div>
        `;
        list.innerHTML = "";
        return;
    }
    cleanBill.style.display = "none";
    list.innerHTML = report.recommendations.map((rec) => `
        <article class="recommendation-card">
            <div class="recommendation-title">${escapeHtml(rec.target_bundle.label)}</div>
            <div class="metric-line">Diversity score: ${rec.projected_diversity_score.toFixed(3)} (${fmtSigned(rec.diversity_score_delta, 3)})</div>
            <div class="metric-line">Composite score delta: ${fmtSigned(rec.composite_score_delta)}</div>
            <div class="metric-line">Rank delta: ${fmtSigned(rec.rank_delta, 0)}</div>
            <div class="metric-line">Source bundle concentration: ${fmtPct(rec.source_bundle_pct_before)} -> ${fmtPct(rec.source_bundle_pct_after)}</div>
            <div class="metric-line">Target bundle concentration: ${fmtPct(rec.target_bundle_pct_before)} -> ${fmtPct(rec.target_bundle_pct_after)}</div>
        </article>
    `).join("");
}

function renderSimulatorOptions(report) {
    const select = $("bundle-select");
    select.innerHTML = report.available_target_bundles.map((projection, index) => `
        <option value="${index}">${escapeHtml(projection.target_bundle.label)}${projection.target_bundle.source === "preset" ? " (preset)" : ""}</option>
    `).join("");
    renderSimulatorProjection(report.available_target_bundles[0] || null);
}

function renderSimulatorProjection(projection) {
    const output = $("simulator-output");
    if (!projection) {
        output.innerHTML = `<div class="meta-text">No target bundles are available for simulation yet.</div>`;
        return;
    }
    output.innerHTML = `
        <div class="status-chip ${projection.target_bundle_would_exceed_threshold ? "" : "healthy"}">${projection.target_bundle_would_exceed_threshold ? "Would exceed threshold" : "Allowed target"}</div>
        <div class="recommendation-title">${escapeHtml(projection.target_bundle.label)}</div>
        <div class="metric-line">Projected diversity score: ${projection.projected_diversity_score.toFixed(3)} (${fmtSigned(projection.diversity_score_delta, 3)})</div>
        <div class="metric-line">Projected composite score: ${projection.projected_composite_score.toFixed(2)} (${fmtSigned(projection.composite_score_delta)})</div>
        <div class="metric-line">Projected rank: #${projection.projected_rank} (${fmtSigned(projection.rank_delta, 0)})</div>
        <div class="metric-line">Source bundle concentration: ${fmtPct(projection.source_bundle_pct_before)} -> ${fmtPct(projection.source_bundle_pct_after)}</div>
        <div class="metric-line">Target bundle concentration: ${fmtPct(projection.target_bundle_pct_before)} -> ${fmtPct(projection.target_bundle_pct_after)}</div>
    `;
}

async function loadReport(publicKey) {
    clearError();
    setLoading(true);
    try {
        const resp = await fetch(`/api/diversity/${encodeURIComponent(publicKey)}`);
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load diversity report");
        }
        const report = await resp.json();
        state.report = report;
        updateShareLink(publicKey);
        renderSummary(report);
        renderGroupings(report);
        renderConcentration(report);
        renderRecommendations(report);
        renderSimulatorOptions(report);
        $("diversity-app").style.display = "block";
    } catch (error) {
        $("diversity-app").style.display = "none";
        state.report = null;
        showError(error.message || "Failed to load diversity report");
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
$("bundle-select").addEventListener("change", (event) => {
    if (!state.report) return;
    const projection = state.report.available_target_bundles[Number(event.target.value)];
    renderSimulatorProjection(projection);
});

const params = new URLSearchParams(window.location.search);
const prefilled = params.get("validator");
if (prefilled) {
    $("validator-key").value = prefilled;
    loadReport(prefilled);
}
