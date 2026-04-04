function $(id) {
    return document.getElementById(id);
}

function truncateKey(key) {
    if (!key) return "";
    return `${key.slice(0, 10)}...${key.slice(-6)}`;
}

function severityLabel(status) {
    if (!status) return "-";
    return status.charAt(0).toUpperCase() + status.slice(1);
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
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
    $("diagnose-app").style.display = loading ? "none" : $("diagnose-app").style.display;
}

function updateShareLink(publicKey) {
    const url = new URL(window.location.href);
    url.searchParams.set("validator", publicKey);
    window.history.replaceState({}, "", url);
}

function renderSummary(report) {
    $("overall-status").textContent = severityLabel(report.overall_status);
    $("overall-status").className = `headline ${report.overall_status}`;
    $("status-summary").textContent = report.status_summary;
    $("composite-score").textContent = report.composite_score.toFixed(2);
    $("validator-rank").textContent = `Rank #${report.rank} of ${report.validator_count}`;
    $("validator-name").textContent = report.domain || truncateKey(report.public_key);
    $("validator-key-text").textContent = report.public_key;
    $("json-link").href = report.json_report_url;
}

function renderFindings(report) {
    const list = $("findings-list");
    const cleanBill = $("clean-bill");
    $("finding-count").textContent = `${report.findings.length} finding${report.findings.length === 1 ? "" : "s"}`;

    if (!report.findings.length) {
        cleanBill.style.display = "block";
        cleanBill.innerHTML = `
            <strong>Clean bill of health.</strong>
            <div>${escapeHtml(report.status_summary)}</div>
        `;
        list.innerHTML = "";
        return;
    }

    cleanBill.style.display = report.findings.every((finding) => finding.severity === "advisory") ? "block" : "none";
    cleanBill.innerHTML = report.findings.every((finding) => finding.severity === "advisory")
        ? `<strong>No fault conditions detected.</strong><div>This validator is mostly healthy. The items below are advisory improvements, not active faults.</div>`
        : "";

    list.innerHTML = report.findings.map((finding) => `
        <article class="finding-card ${escapeHtml(finding.severity)}">
            <div class="severity-chip ${escapeHtml(finding.severity)}">${escapeHtml(finding.severity)}</div>
            <div class="finding-title">${escapeHtml(finding.title)}</div>
            <div class="finding-meta">${escapeHtml(finding.category)} finding on <strong>${escapeHtml(finding.metric)}</strong></div>
            <div class="metric-context">Current value: <strong>${escapeHtml(finding.current_value)}</strong> | Trigger threshold: <strong>${escapeHtml(finding.threshold_value)}</strong></div>
            <p><strong>Likely cause:</strong> ${escapeHtml(finding.likely_cause)}</p>
            <p><strong>Recommended action:</strong> ${escapeHtml(finding.recommended_action)}</p>
        </article>
    `).join("");
}

function renderStrengths(report) {
    const list = $("strengths-list");
    if (!report.strengths.length) {
        list.innerHTML = `
            <article class="strength-card">
                <div class="strength-title">No standout strengths yet</div>
                <div class="finding-meta">This usually means the validator is still building consistency or its metrics are clustered around the cohort middle.</div>
            </article>
        `;
        return;
    }

    list.innerHTML = report.strengths.map((strength) => `
        <article class="strength-card">
            <div class="strength-title">${escapeHtml(strength.title)}</div>
            <div class="metric-context">Current value: <strong>${escapeHtml(strength.current_value)}</strong> | Benchmark: <strong>${escapeHtml(strength.benchmark)}</strong></div>
            <div class="finding-meta">${escapeHtml(strength.why_it_matters)}</div>
        </article>
    `).join("");
}

async function loadReport(publicKey) {
    clearError();
    setLoading(true);
    try {
        const resp = await fetch(`/api/diagnose/${encodeURIComponent(publicKey)}`);
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load diagnostic report");
        }
        const report = await resp.json();
        updateShareLink(publicKey);
        renderSummary(report);
        renderFindings(report);
        renderStrengths(report);
        $("diagnose-app").style.display = "block";
    } catch (err) {
        $("diagnose-app").style.display = "none";
        showError(err.message || "Failed to load diagnostic report");
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
