SELECT
  domain,
  SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS status_1_count,
  SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS status_0_count
FROM (
  SELECT
    url,
    status,
    substr(
      url,
      instr(url, '//') + 2,
      instr(substr(url, instr(url, '//') + 2), '/') - 1
    ) AS domain
  FROM urls
)
GROUP BY domain
ORDER BY status_1_count DESC;