DELETE FROM texts
WHERE url IN (
    SELECT url FROM urls WHERE url LIKE '%wikipedia.org%'
);

DELETE FROM urls
WHERE url LIKE '%wikipedia.org%';