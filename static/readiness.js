const state = { report: null };

function $(id) { return document.getElementById(id); }

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
    if (loading) $("readiness-app").style.display = "none";
}

function updateShareLink(publicKey) {
    const url = new URL(window.location.href);
    url.searchParams.set("validator", publicKey);
    window.history.replaceState({}, "", url);
}

function renderSummary(report) {
    $("overall-status").textContent = report.status_summary;
    $("overall-status").className = `headline ${report.overall_status}`;
    $("status-summary").textContent = `${report.checks.filter((check) => check.status === "pass").length} passing checks across configuration, operational, and attestation categories`;
    $("validator-name").textContent = report.domain || truncateKey(report.public_key);
    $("validator-key-text").textContent = report.public_key;
    $("round-id").textContent = `#${report.round_id}`;
    $("round-ts").textContent = new Date(report.timestamp).toLocaleString();
    $("json-link").href = report.json_report_url;
}

function renderChecks(report) {
    const categories = ["configuration", "operational", "attestation"];
    $("category-sections").innerHTML = categories.map((category) => {
        const checks = report.checks.filter((check) => check.category === category);
        return `
            <section class="category-card">
                <h2 class="category-title">${category.charAt(0).toUpperCase() + category.slice(1)}</h2>
                <div class="section-note">${checks.length} check${checks.length === 1 ? "" : "s"}</div>
                <div class="check-list">
                    ${checks.map((check) => `
                        <article class="check-card ${escapeHtml(check.status)}">
                            <div class="status-chip ${escapeHtml(check.status)}">${escapeHtml(check.status)}</div>
                            <div class="check-name">${escapeHtml(check.name)}</div>
                            <div class="check-meta">Detected: <strong>${escapeHtml(check.detected_value)}</strong> | Expected: <strong>${escapeHtml(check.expected_value)}</strong></div>
                            <div class="check-meta">Source timestamp: ${escapeHtml(check.source_timestamp)}</div>
                            ${check.status !== "pass" && check.remediation ? `<div class="check-meta">Remediation</div>` : ""}
                            ${check.remediation ? `<pre>${escapeHtml(check.remediation)}</pre>` : ""}
                        </article>
                    `).join("")}
                </div>
            </section>
        `;
    }).join("");
}

async function loadReport(publicKey) {
    clearError();
    setLoading(true);
    try {
        const resp = await fetch(`/api/readiness/${encodeURIComponent(publicKey)}`);
        if (!resp.ok) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.detail || "Failed to load readiness report");
        }
        const report = await resp.json();
        state.report = report;
        updateShareLink(publicKey);
        renderSummary(report);
        renderChecks(report);
        $("readiness-app").style.display = "block";
    } catch (err) {
        $("readiness-app").style.display = "none";
        state.report = null;
        showError(err.message || "Failed to load readiness report");
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
