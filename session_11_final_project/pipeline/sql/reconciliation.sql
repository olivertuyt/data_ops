-- Run against Trino after three successful reruns of the same logical date/window.

-- Every serving Gold model must remain unique at its documented grain.
SELECT metric_date, sales_channel, revenue_basis, count(*) AS row_count
FROM shopvn.serving.fact_daily_revenue
GROUP BY metric_date, sales_channel, revenue_basis
HAVING count(*) <> 1;

-- No blocking check may fail for a published run.
SELECT p.run_id, p.domain, q.dataset, q.check_name
FROM shopvn.audit.published_domains p
JOIN shopvn.audit.data_quality_results q
  ON q.run_id = p.run_id AND q.domain = p.domain
WHERE p.status = 'PASS'
  AND q.severity = 'BLOCKING'
  AND q.passed = false;

-- A PASS publication marker requires a PASS DQ audit.
SELECT p.run_id, p.domain
FROM shopvn.audit.published_domains p
LEFT JOIN shopvn.audit.pipeline_runs r
  ON r.run_id = p.run_id
 AND r.object_name = p.domain
 AND r.stage = 'candidate_dq'
 AND r.status = 'PASS'
WHERE p.status = 'PASS' AND r.run_id IS NULL;

-- Silver/Gold schemas must not contain direct PII names.
SELECT table_schema, table_name, column_name
FROM shopvn.information_schema.columns
WHERE table_schema IN ('silver', 'serving')
  AND column_name IN ('full_name', 'phone', 'email', 'ward', 'comment');
