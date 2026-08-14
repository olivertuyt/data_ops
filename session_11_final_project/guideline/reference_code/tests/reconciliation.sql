-- Run after three reruns of the same business date.
SELECT metric_date, sales_channel, revenue_basis, count(*) AS row_count
FROM shopvn.gold.fact_daily_revenue
GROUP BY metric_date, sales_channel, revenue_basis
HAVING count(*) <> 1;

-- Must return no rows.
SELECT run_id, check_name, actual_value, expected_value
FROM shopvn.audit.data_quality_results
WHERE severity = 'BLOCKING' AND passed = false;

-- Confirm every Gold publish has a prior DQ PASS audit.
SELECT p.run_id
FROM shopvn.audit.pipeline_runs p
LEFT JOIN shopvn.audit.pipeline_runs q
  ON q.run_id = p.run_id
 AND q.stage = 'candidate_dq'
 AND q.status = 'PASS'
WHERE p.stage = 'gold_publish'
  AND p.status = 'PASS'
  AND q.run_id IS NULL;
