"""Stable response contracts for the public JSON API.

The top-level composition of each response is part of the published API.  Row
columns inside view-driven lists deliberately remain open dictionaries: those
columns are already governed by the SQL-view contracts and should not require a
Pydantic release every time a non-breaking column is added.

All composed models allow extra top-level fields.  This is intentional response
validation, not a closed projection: FastAPI must never silently discard a new
provenance or coverage section returned by the core query layer.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JsonObject = dict[str, Any]
JsonRows = list[JsonObject]


class OpenResponse(BaseModel):
    """Base for composed responses whose known fields are a stable minimum."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# Error responses -----------------------------------------------------------------


class BadRequestErrorResponse(BaseModel):
    """A malformed query parameter or an otherwise invalid client request."""

    detail: str | JsonRows
    kind: Literal["bad_request"]


class NotFoundErrorResponse(BaseModel):
    """A valid request for a resource that does not exist."""

    detail: str
    kind: Literal["not_found"]


class UnavailableErrorResponse(BaseModel):
    """A required source or registered view that is temporarily unavailable."""

    detail: str
    kind: Literal["unavailable"]


# Meta and catalog -----------------------------------------------------------------


class RootMetadataResponse(OpenResponse):
    """Stable discovery document served at the unversioned API root."""

    name: str
    version: str
    docs: str
    openapi: str
    licence: str
    attribution: str
    catalog: str
    resources: list[str]


class HealthResponse(OpenResponse):
    """Readiness state after successfully probing the registered DuckDB views."""

    status: Literal["ok"]
    views_registered: int


class CatalogResource(OpenResponse):
    """One curated public resource in the API manifest."""

    resource: str
    list_url: str = Field(alias="list")
    item_url: str | None = Field(alias="item")
    description: str
    filters: list[str]
    count: int | None


class CatalogResponse(OpenResponse):
    """The curated resource manifest and its licensing metadata."""

    licence: str
    attribution: str
    source: str
    resources: list[CatalogResource]


class CoverageCaveats(OpenResponse):
    """Cross-domain scope and aggregation qualifications."""

    register_of_interests: str
    ted_award_winners: str
    money_grains: str


class CoverageResponse(OpenResponse):
    """Live corpus coverage summaries for the major published domains."""

    procurement_awards: JsonObject | None
    ted_awards: JsonObject | None
    public_body_payments: JsonObject | None
    sipo_donations: JsonObject | None
    sipo_election_expenses: JsonObject | None
    charities_latest_year: JsonObject | None
    caveats: CoverageCaveats


class AttendanceYearsResponse(OpenResponse):
    """Available attendance reporting years for one chamber."""

    house: str
    years: list[int]


# Domain compositions --------------------------------------------------------------


class CharitySeriesResponse(OpenResponse):
    """One charity's filed financial series."""

    rcn: int
    by_year: JsonRows


class CharitySectorResponse(OpenResponse):
    """Register-wide charity totals, with the latest filing year."""

    latest_year: JsonObject | None
    sector_totals_by_year: JsonRows
    note: str


CharityFinancialsResponse = CharitySeriesResponse | CharitySectorResponse


class CommitteeResponse(OpenResponse):
    """One committee rollup and its long-format party-seat breakdown."""

    detail: JsonObject
    party_seats: JsonRows


class ConstituencyDossierResponse(OpenResponse):
    """One constituency's representation, work, housing and council context."""

    constituency: str
    header: JsonObject
    members: JsonRows
    party_breakdown: JsonRows
    house_work: JsonObject | None
    housing_context: JsonRows
    council_context: JsonRows
    caveat: str


class CorporateNoticesResponse(OpenResponse):
    """Filtered corporate-register notices and their interpretation caveat."""

    count: int
    notices: JsonRows
    caveat: str


class CorporateRepeatDistressResponse(OpenResponse):
    """Experimental repeat-notice firms list."""

    firms: JsonRows
    caveat: str


class CorporateReceiversResponse(OpenResponse):
    """Whole-corpus receivership summary and rankings."""

    summary: JsonObject | None
    appointers: JsonRows
    firms: JsonRows
    appointer_type_mix: JsonRows
    notices_by_year: JsonRows
    caveat: str


class CouncillorCouncilsResponse(OpenResponse):
    """Councils for which a councillor roster is published."""

    councils: list[str]


class CouncillorVotesResponse(OpenResponse):
    """One councillor's recorded votes and the source-coverage denominator."""

    council: str
    member: str
    votes: JsonRows
    provenance: JsonObject | None


class CouncilDecisionsResponse(OpenResponse):
    """Extracted council motion events, coverage and topic counts."""

    council: str
    decisions: JsonRows
    coverage: JsonObject | None
    topics: JsonRows


class CouncilPowersResponse(OpenResponse):
    """Reserved-versus-executive power split for council documents."""

    council: str
    split: JsonObject | None
    classes: JsonRows


class CouncillorRosterResponse(OpenResponse):
    """A council roster plus coverage state and Chief Executive context."""

    council: str
    lea: str | None
    councillors: JsonRows
    coverage: JsonObject | None
    chief_executive: JsonObject | None


class HousingSupplyResponse(OpenResponse):
    """National housing supply, HAP and completions context."""

    supply: JsonObject | None
    hap: JsonObject | None
    completions: JsonRows


class HousingAccommodationSpendResponse(OpenResponse):
    """Accommodation spend by year and provider at the realised-spend grain."""

    by_year: JsonRows
    providers: JsonRows
    caveat: str


class JudicialAppointmentsResponse(OpenResponse):
    """Appointment events, elevation paths and the sitting-bench roster."""

    appointments: JsonRows
    elevation_ladder: JsonRows
    roster: JsonRows


class CourtsHealthResponse(OpenResponse):
    """Court-system capacity measures that do not name or rank judges."""

    clearance: JsonRows
    waiting_times: JsonRows
    courthouses: JsonRows


class DpoProfileResponse(OpenResponse):
    """One former office-holder's registered lobbying footprint."""

    individual: str
    summary: JsonObject
    firms: JsonRows
    client_breakdown: JsonRows
    politicians_targeted: JsonRows
    caveat: str


class CouncilsResponse(OpenResponse):
    """National council headline, council index and map layers."""

    national_summary: JsonObject | None
    councils: JsonRows
    map_layers: JsonRows


class CouncilDossierResponse(OpenResponse):
    """One local authority's accountability sections."""

    local_authority: str
    chief_executive: JsonObject
    noac_scorecard: JsonRows
    noac_scorecard_history: JsonRows
    cash_signals: JsonObject | None
    collection_rates: JsonObject | None
    planning_overturn: JsonObject | None
    derelict_sites_levy: JsonObject | None
    housing_performance: JsonObject | None
    council_money: JsonObject | None
    caveat: str


class CouncilNoacIndicatorsResponse(OpenResponse):
    """Full raw NOAC indicator set for one local authority."""

    local_authority: str
    indicators: JsonRows
    caveat: str


class MemberFeedIdentity(OpenResponse):
    """Resolved member identity used by questions and speech feeds."""

    unique_member_code: str
    member_name: str
    house: str


class MemberQuestionsResponse(OpenResponse):
    """A filtered parliamentary-question feed for one member."""

    member: MemberFeedIdentity
    total_matched: int
    returned: int
    questions: JsonRows


class MemberInterestIdentity(OpenResponse):
    """Member identity used by the interests register."""

    member_name: str
    house: str


class MemberInterestsResponse(OpenResponse):
    """One member's annual summaries and individual interest declarations."""

    member: MemberInterestIdentity
    by_year: JsonRows
    declarations: JsonRows


class MemberSpeechesResponse(OpenResponse):
    """A filtered floor-contribution feed and its all-time summary."""

    member: MemberFeedIdentity
    summary: JsonObject | None
    total_matched: int
    returned: int
    speeches: JsonRows


class MinisterHolderResponse(OpenResponse):
    """The holder of one department on a requested date."""

    department: str
    on_date: str
    minister: JsonObject | None


class MinisterDisambiguationResponse(OpenResponse):
    """Several department labels matched the user's query."""

    disambiguation: JsonRows
    note: str


class MinisterPickerResponse(OpenResponse):
    """No unique department matched; available departments may be supplied."""

    error: str
    departments: JsonRows | None = None


MinisterLookupResponse = MinisterHolderResponse | MinisterDisambiguationResponse | MinisterPickerResponse


class CabinetResponse(OpenResponse):
    """The current ministerial line-up and department registry."""

    current_ministers: JsonRows
    departments: JsonRows


class DiaryOrganisationsResponse(OpenResponse):
    """Organisations ranked by logged ministerial diary meetings."""

    organisations: JsonRows
    caveat: str


class DiaryOrganisationResponse(OpenResponse):
    """One organisation's ministerial-access summary and meetings."""

    organisation: str
    summary: JsonObject
    meetings: JsonRows
    caveat: str


class DiaryMeetingsResponse(OpenResponse):
    """Filtered external meetings from published ministerial diaries."""

    meetings: JsonRows
    caveat: str


class PartyDonationsDetailResponse(OpenResponse):
    """Individual disclosed receipts for one party."""

    party: str
    donations: JsonRows


class PartyDonationsSummaryResponse(OpenResponse):
    """All-party donation headline and ranking."""

    summary: JsonObject | None
    by_party: JsonRows
    note: str


PartyDonationsResponse = PartyDonationsDetailResponse | PartyDonationsSummaryResponse


class PartyElectionSpendDetailResponse(OpenResponse):
    """Per-candidate election expenses for one party."""

    party: str
    candidates: JsonRows


class PartyElectionSpendSummaryResponse(OpenResponse):
    """All-party election-expense headline and ranking."""

    summary: JsonObject | None
    by_party: JsonRows
    note: str


PartyElectionSpendResponse = PartyElectionSpendDetailResponse | PartyElectionSpendSummaryResponse


class SupplierDossierResponse(OpenResponse):
    """One supplier's summary and award records."""

    summary: JsonObject | None
    awards: JsonRows
    caveat: str


class ProcurementCompetitionResponse(OpenResponse):
    """Buyer-level bid-competition signals and query summary."""

    summary: JsonObject
    buyers: JsonRows
    caveat: str


class ProcurementLobbyingOverlapResponse(OpenResponse):
    """Procurement/lobbying co-occurrence grouped once per supplier."""

    summary: JsonObject
    suppliers: JsonRows
    caveat: str


class PublicBodyPaymentsResponse(OpenResponse):
    """Public-body payments ranking at one selected side and money grain."""

    side: Literal["supplier", "publisher"]
    coverage: JsonObject | None
    ranking: JsonRows
    caveat: str


class DivisionInterestBreakdownResponse(OpenResponse):
    """One division's vote tally split by declared-interest flags."""

    division: JsonObject
    interest_breakdown: JsonRows
    caveat: str


class TopicVoteSearchResponse(OpenResponse):
    """Debates and individual votes matching requested topic keywords."""

    topics: list[str]
    house: str
    debates: JsonRows
    votes: JsonRows


class VotesInterestsCrossReferenceResponse(OpenResponse):
    """Members matching a division vote and declared-interest query."""

    query: JsonObject
    match_count: int
    distinct_members: int
    matches: JsonRows
    caveat: str


# Bulk-data manifest ---------------------------------------------------------------


class DataCurrencyResponse(OpenResponse):
    """Record-date and source-fetch clocks for one bulk export."""

    latest_record: str | None
    source_fetched_at: str
    note: str


class ExportResourceResponse(OpenResponse):
    """One allow-listed bulk export.

    Availability-dependent fields are optional.  The route uses
    ``response_model_exclude_unset`` so absent values remain absent rather than
    being invented as nulls.
    """

    name: str
    download: str
    formats: list[Literal["parquet", "csv"]]
    description: str
    licence: str
    attribution: str
    caveat: str
    available: bool
    privacy: str | None = None
    n_rows: int | None = None
    data_currency: DataCurrencyResponse | None = None


class ExportManifestResponse(OpenResponse):
    """Bulk-export manifest with privacy, provenance and currency metadata."""

    generated_at: str
    note: str
    references: dict[str, str]
    resources: list[ExportResourceResponse]
