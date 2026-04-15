SELECT
  domain,

  SUM(CASE WHEN year = '2005' THEN 1 ELSE 0 END) AS y2005,
  SUM(CASE WHEN year = '2006' THEN 1 ELSE 0 END) AS y2006,
  SUM(CASE WHEN year = '2007' THEN 1 ELSE 0 END) AS y2007,
  SUM(CASE WHEN year = '2008' THEN 1 ELSE 0 END) AS y2008,
  SUM(CASE WHEN year = '2009' THEN 1 ELSE 0 END) AS y2009,
  SUM(CASE WHEN year = '2010' THEN 1 ELSE 0 END) AS y2010,
  SUM(CASE WHEN year = '2011' THEN 1 ELSE 0 END) AS y2011,
  SUM(CASE WHEN year = '2012' THEN 1 ELSE 0 END) AS y2012,
  SUM(CASE WHEN year = '2013' THEN 1 ELSE 0 END) AS y2013,
  SUM(CASE WHEN year = '2014' THEN 1 ELSE 0 END) AS y2014,
  SUM(CASE WHEN year = '2015' THEN 1 ELSE 0 END) AS y2015,
  SUM(CASE WHEN year = '2016' THEN 1 ELSE 0 END) AS y2016,
  SUM(CASE WHEN year = '2017' THEN 1 ELSE 0 END) AS y2017,
  SUM(CASE WHEN year = '2018' THEN 1 ELSE 0 END) AS y2018,
  SUM(CASE WHEN year = '2019' THEN 1 ELSE 0 END) AS y2019,
  SUM(CASE WHEN year = '2020' THEN 1 ELSE 0 END) AS y2020,
  SUM(CASE WHEN year = '2021' THEN 1 ELSE 0 END) AS y2021,
  SUM(CASE WHEN year = '2022' THEN 1 ELSE 0 END) AS y2022,
  SUM(CASE WHEN year = '2023' THEN 1 ELSE 0 END) AS y2023,
  SUM(CASE WHEN year = '2024' THEN 1 ELSE 0 END) AS y2024,
  SUM(CASE WHEN year = '2025' THEN 1 ELSE 0 END) AS y2025,
  SUM(CASE WHEN year = '2026' THEN 1 ELSE 0 END) AS y2026

FROM (
  SELECT
    -- Extract domain
    substr(
      u.url,
      instr(u.url, '//') + 2,
      instr(substr(u.url, instr(u.url, '//') + 2), '/') - 1
    ) AS domain,

    -- Extract year from messy text
    CASE
      WHEN t.date GLOB '*2005*' THEN '2005'
      WHEN t.date GLOB '*2006*' THEN '2006'
      WHEN t.date GLOB '*2007*' THEN '2007'
      WHEN t.date GLOB '*2008*' THEN '2008'
      WHEN t.date GLOB '*2009*' THEN '2009'
      WHEN t.date GLOB '*2010*' THEN '2010'
      WHEN t.date GLOB '*2011*' THEN '2011'
      WHEN t.date GLOB '*2012*' THEN '2012'
      WHEN t.date GLOB '*2013*' THEN '2013'
      WHEN t.date GLOB '*2014*' THEN '2014'
      WHEN t.date GLOB '*2015*' THEN '2015'
      WHEN t.date GLOB '*2016*' THEN '2016'
      WHEN t.date GLOB '*2017*' THEN '2017'
      WHEN t.date GLOB '*2018*' THEN '2018'
      WHEN t.date GLOB '*2019*' THEN '2019'
      WHEN t.date GLOB '*2020*' THEN '2020'
      WHEN t.date GLOB '*2021*' THEN '2021'
      WHEN t.date GLOB '*2022*' THEN '2022'
      WHEN t.date GLOB '*2023*' THEN '2023'
      WHEN t.date GLOB '*2024*' THEN '2024'
      WHEN t.date GLOB '*2025*' THEN '2025'
      WHEN t.date GLOB '*2026*' THEN '2026'
      ELSE NULL
    END AS year

  FROM texts t
  JOIN urls u ON t.url = u.url
)
WHERE year IS NOT NULL
GROUP BY domain
ORDER BY domain;