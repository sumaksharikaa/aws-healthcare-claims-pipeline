-- Example analytics on the curated claims mart (Redshift SQL).

-- 1. PMPM trend by plan
SELECT plan_id, service_month, members_enrolled, total_paid, pmpm
FROM claims_mart.agg_plan_month
ORDER BY plan_id, service_month;

-- 2. Top diagnoses by paid amount, current (non-voided) claims only
SELECT primary_dx, COUNT(DISTINCT claim_id) AS claims, SUM(paid_amount) AS paid
FROM claims_mart.fact_claim_line
GROUP BY primary_dx
ORDER BY paid DESC
LIMIT 10;

-- 3. Paid amount on claims with no eligibility on the date of service (recovery opportunity)
SELECT service_month, COUNT(DISTINCT claim_id) AS claims, SUM(paid_amount) AS paid_without_coverage
FROM claims_mart.fact_claim_line
WHERE NOT is_eligible
GROUP BY service_month
ORDER BY service_month;

-- 4. Provider cost profile vs. specialty average (window function)
WITH by_provider AS (
    SELECT f.billing_npi, p.specialty, SUM(f.paid_amount) / COUNT(DISTINCT f.claim_id) AS paid_per_claim
    FROM claims_mart.fact_claim_line f
    JOIN claims_mart.dim_provider p ON p.npi = f.billing_npi
    GROUP BY f.billing_npi, p.specialty
)
SELECT billing_npi, specialty, paid_per_claim,
       AVG(paid_per_claim) OVER (PARTITION BY specialty) AS specialty_avg,
       paid_per_claim / NULLIF(AVG(paid_per_claim) OVER (PARTITION BY specialty), 0) AS cost_index
FROM by_provider
ORDER BY cost_index DESC
LIMIT 20;

-- 5. Spend by plan *at the time of service* (point-in-time via SCD2)
SELECT plan_id_at_service, SUM(paid_amount) AS paid
FROM claims_mart.fact_claim_line
GROUP BY plan_id_at_service;

-- 6. Members who changed plans, with their plan history
SELECT member_token, plan_id, effective_from, effective_to, is_current
FROM claims_mart.dim_member
WHERE member_token IN (
    SELECT member_token FROM claims_mart.dim_member GROUP BY member_token HAVING COUNT(DISTINCT plan_id) > 1
)
ORDER BY member_token, effective_from;
