import duckdb
import os

# Define file paths
links_file = 'siaran_pers_general_links.json'
articles_file = 'siaran_pers_general.json'

# Initialize DuckDB
con = duckdb.connect(database=':memory:')

print("Checking counts by domain...")

# Query to group by domain and count
# We extract the domain from the link using regexp_extract
query = f"""
WITH links_data AS (
    SELECT 
        regexp_extract(link, 'https?://([^/]+)', 1) as domain,
        count(*) as total_links
    FROM read_json_auto('{links_file}')
    GROUP BY 1
),
articles_data AS (
    SELECT 
        regexp_extract(link, 'https?://([^/]+)', 1) as domain,
        count(*) as scraped_count
    FROM read_json_auto('{articles_file}')
    GROUP BY 1
)
SELECT 
    COALESCE(l.domain, a.domain) as domain,
    COALESCE(l.total_links, 0) as total_links,
    COALESCE(a.scraped_count, 0) as scraped_count,
    CASE 
        WHEN COALESCE(l.total_links, 0) > 0 
        THEN ROUND(CAST(COALESCE(a.scraped_count, 0) AS FLOAT) / l.total_links * 100, 2)
        ELSE 0.0
    END as progress_pct
FROM links_data l
FULL OUTER JOIN articles_data a ON l.domain = a.domain
ORDER BY total_links DESC;
"""

try:
    result = con.execute(query).df()
    print("\nSummary by Domain:")
    print(result.to_string(index=False))
    
    total_l = result['total_links'].sum()
    total_a = result['scraped_count'].sum()
    print(f"\nOVERALL TOTAL:")
    print(f"Total Links: {total_l}")
    print(f"Scraped Articles: {total_a}")
    print(f"Overall Progress: {total_a / total_l * 100:.2f}%" if total_l > 0 else "Overall Progress: 0.00%")

except Exception as e:
    print(f"Error running query: {e}")
