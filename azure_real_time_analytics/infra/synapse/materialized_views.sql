-- Materialized view example for fast BI queries
CREATE MATERIALIZED VIEW IF NOT EXISTS dbo.mv_claims_daily AS
SELECT claim_id, policy_id, cast(event_time as date) as event_date, sum(amount) as total_amount
FROM dbo.claims_silver
GROUP BY claim_id, policy_id, cast(event_time as date);