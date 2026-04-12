from datetime import datetime
from pydantic import BaseModel


class ValidatorMetrics(BaseModel):
    agreement_1h: float | None = None
    agreement_1h_total: int | None = None
    agreement_24h: float | None = None
    agreement_24h_total: int | None = None
    agreement_30d: float | None = None
    agreement_30d_total: int | None = None
    poll_success_pct: float | None = None  # % of successful polls (our own tracking)
    uptime_seconds: int | None = None
    uptime_pct: float | None = None
    latency_ms: float | None = None
    peer_count: int | None = None
    avg_ledger_interval: float | None = None  # seconds per ledger
    validated_ledger_age: float | None = None
    server_version: str | None = None
    server_state: str | None = None
    asn: int | None = None
    isp: str | None = None
    country: str | None = None
    node_ip: str | None = None


class ValidatorSubScores(BaseModel):
    agreement_1h: float = 0.0
    agreement_24h: float = 0.0
    agreement_30d: float = 0.0
    uptime: float = 0.0
    poll_success: float = 0.0
    latency: float = 0.0
    peer_count: float = 0.0
    version: float = 0.0
    diversity: float = 0.0


class ValidatorSnapshot(BaseModel):
    public_key: str
    domain: str | None = None
    unl: bool = False
    metrics: ValidatorMetrics = ValidatorMetrics()


class ValidatorScore(BaseModel):
    public_key: str
    domain: str | None = None
    composite_score: float
    metrics: ValidatorMetrics
    sub_scores: ValidatorSubScores
    last_updated: str


class ScoresResponse(BaseModel):
    round_id: int
    timestamp: str
    methodology_version: str
    validator_count: int
    enrichment_coverage: dict | None = None
    validators: list[ValidatorScore]


class RoundSummary(BaseModel):
    round_id: int
    timestamp: str
    validator_count: int
    avg_score: float | None
    min_score: float | None
    max_score: float | None


class HistoryResponse(BaseModel):
    rounds: list[RoundSummary]


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    last_scoring_round: str | None


class MethodologyResponse(BaseModel):
    version: str
    description: str
    weights: dict[str, float]
    thresholds: dict[str, dict]


class WeeklyDigestResponse(BaseModel):
    id: int
    created_at: str
    latest_round_id: int
    comparison_round_id: int
    delivery_status: str
    posted_at: str | None = None
    message_id: str | None = None
    payload: dict


class WeeklyDigestHistoryResponse(BaseModel):
    digests: list[WeeklyDigestResponse]


class IncidentEventResponse(BaseModel):
    id: int
    incident_id: int
    round_id: int | None = None
    validator_key: str
    event_type: str
    severity: str
    event_phase: str
    synthetic: bool = False
    correlated: bool = False
    created_at: str
    current_values: dict
    previous_values: dict | None = None


class IncidentResponse(BaseModel):
    id: int
    validator_key: str
    severity: str
    status: str
    synthetic: bool = False
    correlated: bool = False
    summary: str
    start_time: str
    end_time: str | None = None
    duration_seconds: int | None = None
    event_types: list[str]
    active_event_types: list[str]
    latest_round_id: int | None = None
    latest_event_time: str
    before_values: dict | None = None
    during_values: dict | None = None
    after_values: dict | None = None
    events: list[IncidentEventResponse] | None = None


class IncidentListResponse(BaseModel):
    incidents: list[IncidentResponse]


class DiagnosticFindingResponse(BaseModel):
    category: str
    metric: str
    severity: str
    title: str
    current_value: str
    threshold_value: str
    likely_cause: str
    recommended_action: str


class DiagnosticStrengthResponse(BaseModel):
    metric: str
    title: str
    current_value: str
    benchmark: str
    why_it_matters: str


class DiagnosticReportResponse(BaseModel):
    public_key: str
    domain: str | None = None
    round_id: int
    timestamp: str
    composite_score: float
    rank: int
    validator_count: int
    overall_status: str
    status_summary: str
    json_report_url: str
    findings: list[DiagnosticFindingResponse]
    strengths: list[DiagnosticStrengthResponse]


class AIDiagnosticResponse(BaseModel):
    ai_summary: str | None = None
    model: str | None = None
    generated_at: str | None = None
    cached: bool = False
    message: str | None = None


class ReadinessCheckResponse(BaseModel):
    name: str
    category: str
    status: str
    detected_value: str
    expected_value: str
    remediation: str | None = None
    source_timestamp: str


class ReadinessReportResponse(BaseModel):
    public_key: str
    domain: str | None = None
    round_id: int
    timestamp: str
    overall_status: str
    status_summary: str
    json_report_url: str
    checks: list[ReadinessCheckResponse]


class UpgradeDistributionEntryResponse(BaseModel):
    version: str
    count: int
    percentage: float


class LaggingValidatorResponse(BaseModel):
    public_key: str
    domain: str | None = None
    current_version: str
    days_behind: int


class UpgradeHistoryEntryResponse(BaseModel):
    date: str
    percentage: float
    upgraded_count: int
    total_validators: int


class UpgradesResponse(BaseModel):
    latest_version: str | None = None
    total_validators: int
    upgraded_count: int
    upgraded_pct: float
    version_distribution: list[UpgradeDistributionEntryResponse]
    lagging_validators: list[LaggingValidatorResponse]
    adoption_history: list[UpgradeHistoryEntryResponse]
    json_report_url: str


class DiversityGroupingResponse(BaseModel):
    value: str
    shared_count: int
    concentration_pct: float
    above_threshold: bool
    threshold_over_pct: float


class DiversityBundleResponse(BaseModel):
    provider: str
    asn: int
    country: str
    label: str
    source: str


class DiversityProjectionResponse(BaseModel):
    target_bundle: DiversityBundleResponse
    projected_diversity_score: float
    diversity_score_delta: float
    projected_composite_score: float
    composite_score_delta: float
    projected_rank: int
    rank_delta: int
    source_bundle_pct_before: float
    source_bundle_pct_after: float
    target_bundle_pct_before: float
    target_bundle_pct_after: float
    target_bundle_would_exceed_threshold: bool


class DiversityCurrentContextResponse(BaseModel):
    public_key: str
    domain: str | None = None
    provider: str | None = None
    asn: int | None = None
    country: str | None = None
    bundle_label: str
    diversity_score: float
    composite_score: float
    rank: int
    validator_count: int
    provider_group: DiversityGroupingResponse | None = None
    asn_group: DiversityGroupingResponse | None = None
    country_group: DiversityGroupingResponse | None = None
    bundle_group: DiversityGroupingResponse | None = None
    clean_bill_of_health: bool


class DiversityConcentrationEntryResponse(BaseModel):
    bundle: DiversityBundleResponse
    validator_count: int
    concentration_pct: float


class DiversityReportResponse(BaseModel):
    current_context: DiversityCurrentContextResponse
    concentration_summary: list[DiversityConcentrationEntryResponse]
    available_target_bundles: list[DiversityProjectionResponse]
    recommendations: list[DiversityProjectionResponse]
    disclaimer: str
    json_report_url: str


class PeerNodeResponse(BaseModel):
    node_public_key: str | None = None
    validator_public_key: str | None = None
    domain: str | None = None
    ip: str | None = None
    port: int | None = None
    provider: str | None = None
    asn: int | None = None
    country: str | None = None
    server_version: str | None = None
    latency_ms: float | None = None
    agreement_24h: float | None = None
    quality_rating: str
    quality_reason: str
    non_validating: bool = False


class PeerRiskFindingResponse(BaseModel):
    title: str
    severity: str
    detail: str


class PeerRecommendationResponse(BaseModel):
    node_public_key: str | None = None
    validator_public_key: str | None = None
    ip: str | None = None
    port: int | None = None
    provider: str | None = None
    asn: int | None = None
    country: str | None = None
    quality_rating: str
    reason: str


class PeerSummaryResponse(BaseModel):
    total_nodes_analyzed: int
    current_peer_count: int
    good_count: int
    acceptable_count: int
    risky_count: int
    projected_composite_score: float
    projected_rank: int
    projected_rank_delta: int


class PeerReportResponse(BaseModel):
    public_key: str
    domain: str | None = None
    mode: str
    mode_banner: str
    json_report_url: str
    disclaimer: str
    observable_node: PeerNodeResponse | None = None
    summary: PeerSummaryResponse
    risk_findings: list[PeerRiskFindingResponse]
    table_title: str
    node_rows: list[PeerNodeResponse]
    add_recommendations: list[PeerRecommendationResponse]
    drop_recommendations: list[PeerRecommendationResponse]
