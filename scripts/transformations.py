import psycopg2
import pandas as pd
import os

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="my_db",
    user="my_user",
    password="my_password"
)

# Create output folder if it doesn't exist
output_folder = "output"
# This line is no longer necessary since we are overwriting files in the folder
os.makedirs(output_folder, exist_ok=True)

# --- Helper to run a query, print results, and save to CSV ---
def run_and_save(query, title, filename):
    print(f"\n--- {title} ---")
    df = pd.read_sql_query(query, conn)
    if df.empty:
        print("No results found.")
    else:
        print(df.to_string(index=False))
        output_path = os.path.join(output_folder, filename)
        df.to_csv(output_path, index=False)  # This will overwrite the file if it already exists
        print(f"✅ Saved to {output_path}")

# --- SQL Queries ---

# Q1: Longest and Shortest Runways per Country (with DISTINCT to avoid duplicates)
query_runways = """
WITH runway_details AS (
    SELECT 
        country_name,
        airport_name,
        runway_length,
        runway_width,
        RANK() OVER (PARTITION BY country_code ORDER BY runway_length DESC NULLS LAST) AS rank_longest,
        RANK() OVER (PARTITION BY country_code ORDER BY runway_length ASC NULLS LAST) AS rank_shortest
    FROM 
        (SELECT DISTINCT country_name, airport_name, runway_length, runway_width, country_code
         FROM gk1_master_airports
         WHERE runway_length IS NOT NULL AND airport_type != 'closed') AS distinct_runways
)
SELECT 
    country_name,
    airport_name,
    runway_length,
    runway_width,
    'Longest' AS runway_type
FROM runway_details
WHERE rank_longest = 1

UNION ALL

SELECT 
    country_name,
    airport_name,
    runway_length,
    runway_width,
    'Shortest' AS runway_type
FROM runway_details
WHERE rank_shortest = 1

ORDER BY country_name, runway_type;
"""

# Q2.1: Top 3 Countries by Number of Airports (handling duplicates, excluding closed airports)
query_top3 = """
SELECT 
    country_name,
    COUNT(DISTINCT airport_ident) AS airport_count
FROM 
    gk1_master_airports
WHERE 
    airport_type != 'closed'  -- Exclude closed airports
GROUP BY 
    country_name
ORDER BY 
    airport_count DESC
LIMIT 3;
"""

# Q2.2: Bottom 10 Countries by Number of Airports (handling duplicates, excluding closed airports)
query_bottom10 = """
SELECT 
    country_name,
    COUNT(DISTINCT airport_ident) AS airport_count
FROM 
    gk1_master_airports
WHERE 
    airport_type != 'closed'  -- Exclude closed airports
GROUP BY 
    country_name
ORDER BY 
    airport_count ASC
LIMIT 10;
"""

# --- Run and Save All Queries ---
run_and_save(query_runways, "Longest and Shortest Runways per Country", "longest_shortest_runways_master.csv")
run_and_save(query_top3, "Top 3 Countries by Number of Airports", "top3_countries_airports_master.csv")
run_and_save(query_bottom10, "Bottom 10 Countries by Number of Airports", "bottom10_countries_airports_master.csv")

# Close connection
conn.close()
