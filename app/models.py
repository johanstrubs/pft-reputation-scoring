from datetime import datetime
from pydantic import BaseModel


class ValidatorMetrics(BaseModel):
    agreement_1h: float | None = None
    agreement_24h: float | None = None
    agreement_30d: float | None = None
    uptime_seconds: int | None = None
    uptime_pct: float | None = None
    latency_ms: float | None = None
    peer_count: int | None = None
    avg_ledger_interval: float | None = None  # seconds per ledger
    server_version: str | None = None
    server_state: str | None = None
    asn: int | None = None
    isp: str | None = None
    country: str | None = None


class ValidatorSubScores(BaseModel):
    agreement_1h: float = 0.0
    agreement_24h: float = 0.0
    agreement_30d: float = 0.0
    uptime: float = 0.0
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
