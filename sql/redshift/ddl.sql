-- Amazon Redshift (Serverless or provisioned) schema for the curated claims mart.
-- Column order matches the Parquet files written by the pipeline, because
-- COPY ... FORMAT AS PARQUET maps columns by position.
--
-- Distribution: facts and member-level aggregates are distributed on
-- member_token so member-level joins stay node-local; small dimensions use
-- DISTSTYLE ALL. Sort keys follow the most common filter (service month).

CREATE SCHEMA IF NOT EXISTS claims_mart;

CREATE TABLE IF NOT EXISTS claims_mart.dim_member (
    member_sk        BIGINT        NOT NULL,
    member_token     VARCHAR(64)   NOT NULL,
    plan_id          VARCHAR(20),
    state            CHAR(2),
    zip3             CHAR(3),
    gender           CHAR(1),
    age_band         VARCHAR(10),
    birth_year       INTEGER,
    effective_from   TIMESTAMP     NOT NULL,
    effective_to     TIMESTAMP     NOT NULL,
    is_current       BOOLEAN       NOT NULL,
    PRIMARY KEY (member_sk)
)
DISTKEY (member_token)
SORTKEY (member_token, effective_from);

CREATE TABLE IF NOT EXISTS claims_mart.dim_provider (
    npi              VARCHAR(10)   NOT NULL,
    provider_name    VARCHAR(200),
    specialty        VARCHAR(100),
    taxonomy_code    VARCHAR(10),
    state            CHAR(2),
    PRIMARY KEY (npi)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS claims_mart.fact_claim_line (
    claim_id            VARCHAR(20)    NOT NULL,
    claim_family_id     VARCHAR(20)    NOT NULL,
    line_number         INTEGER        NOT NULL,
    claim_status        VARCHAR(10)    NOT NULL,
    member_token        VARCHAR(64)    NOT NULL,
    member_sk           BIGINT,
    plan_id_at_service  VARCHAR(20),
    billing_npi         VARCHAR(10),
    claim_type          VARCHAR(15),
    place_of_service    VARCHAR(2),
    service_from_date   DATE,
    primary_dx          VARCHAR(10),
    procedure_code      VARCHAR(5),
    units               INTEGER,
    billed_amount       DECIMAL(12,2),
    allowed_amount      DECIMAL(12,2),
    paid_amount         DECIMAL(12,2),
    is_eligible         BOOLEAN,
    received_ts         TIMESTAMP,
    service_month       DATE,
    PRIMARY KEY (claim_id, line_number)
)
DISTKEY (member_token)
COMPOUND SORTKEY (service_month, billing_npi);

CREATE TABLE IF NOT EXISTS claims_mart.fact_pharmacy (
    rx_claim_id      VARCHAR(25)    NOT NULL,
    ndc              VARCHAR(13),
    prescriber_npi   VARCHAR(10),
    fill_date        DATE,
    days_supply      INTEGER,
    quantity         DECIMAL(10,3),
    ingredient_cost  DECIMAL(12,2),
    dispensing_fee   DECIMAL(12,2),
    paid_amount      DECIMAL(12,2),
    member_token     VARCHAR(64)    NOT NULL,
    service_month    DATE,
    PRIMARY KEY (rx_claim_id)
)
DISTKEY (member_token)
SORTKEY (service_month);

CREATE TABLE IF NOT EXISTS claims_mart.agg_member_month (
    member_token     VARCHAR(64)    NOT NULL,
    service_month    DATE           NOT NULL,
    medical_paid     DECIMAL(18,2),
    medical_claims   BIGINT,
    rx_paid          DECIMAL(18,2),
    rx_fills         BIGINT,
    total_paid       DECIMAL(18,2)
)
DISTKEY (member_token)
SORTKEY (service_month);

CREATE TABLE IF NOT EXISTS claims_mart.agg_plan_month (
    plan_id           VARCHAR(20)   NOT NULL,
    service_month     DATE          NOT NULL,
    medical_paid      DECIMAL(18,2),
    rx_paid           DECIMAL(18,2),
    total_paid        DECIMAL(18,2),
    members_enrolled  BIGINT,
    pmpm              DECIMAL(18,2)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS claims_mart.high_cost_claimants (
    member_token     VARCHAR(64)    NOT NULL,
    total_paid       DECIMAL(18,2),
    medical_paid     DECIMAL(18,2),
    rx_paid          DECIMAL(18,2),
    cost_rank        INTEGER,
    percentile       DOUBLE PRECISION
)
DISTSTYLE ALL;

-- Analysts get the de-identified mart only. The PHI vault never reaches Redshift.
-- GRANT USAGE ON SCHEMA claims_mart TO GROUP analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA claims_mart TO GROUP analysts;
