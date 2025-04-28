import psycopg2

# Database connection
from db import get_connection

conn = get_connection()
cur = conn.cursor()

# --- Helper Function to run check and print results ---
def run_check(description, sql):
    print(f"\n🔎 {description}")
    cur.execute(sql)
    result = cur.fetchall()
    if result:
        print(f"❌ Issues Found: {len(result)} rows")
        for row in result[:5]:  # Show only first 5 problematic rows
            print(row)
        if len(result) > 5:
            print(f"...and {len(result) - 5} more.")
    else:
        print("✅ Passed")

# --- DQ Checks ---

print("\n📋 Running Data Quality Checks...\n")

# --- gk1_airports checks ---

run_check("gk1_airports: Duplicate IDs", """
SELECT id, COUNT(*) FROM gk1_airports GROUP BY id HAVING COUNT(*) > 1;
""")

run_check("gk1_airports: NULL ident", """
SELECT * FROM gk1_airports WHERE ident IS NULL;
""")

run_check("gk1_airports: NULL latitude or longitude", """
SELECT * FROM gk1_airports WHERE latitude_deg IS NULL OR longitude_deg IS NULL;
""")

run_check("gk1_airports: Invalid latitude/longitude", """
SELECT * FROM gk1_airports WHERE latitude_deg NOT BETWEEN -90 AND 90 OR longitude_deg NOT BETWEEN -180 AND 180;
""")

run_check("gk1_airports: Invalid iso_country codes", """
SELECT DISTINCT iso_country FROM gk1_airports
WHERE iso_country IS NOT NULL AND iso_country NOT IN (SELECT code FROM gk1_countries);
""")

# --- gk1_countries checks ---

run_check("gk1_countries: Duplicate IDs", """
SELECT id, COUNT(*) FROM gk1_countries GROUP BY id HAVING COUNT(*) > 1;
""")

run_check("gk1_countries: NULL code or name", """
SELECT * FROM gk1_countries WHERE code IS NULL OR name IS NULL;
""")

run_check("gk1_countries: Invalid code length (should be 2)", """
SELECT * FROM gk1_countries WHERE LENGTH(code) != 2;
""")

# --- gk1_runways checks ---

run_check("gk1_runways: Duplicate IDs", """
SELECT id, COUNT(*) FROM gk1_runways GROUP BY id HAVING COUNT(*) > 1;
""")

run_check("gk1_runways: Invalid airport_ref (no matching airport)", """
SELECT DISTINCT airport_ref FROM gk1_runways
WHERE airport_ref IS NOT NULL AND airport_ref NOT IN (SELECT id FROM gk1_airports);
""")

run_check("gk1_runways: Invalid or negative length_ft", """
SELECT * FROM gk1_runways WHERE length_ft IS NOT NULL AND length_ft <= 0;
""")

run_check("gk1_runways: Invalid or negative width_ft", """
SELECT * FROM gk1_runways WHERE width_ft IS NOT NULL AND width_ft <= 0;
""")

run_check("gk1_runways: Invalid latitude/longitude", """
SELECT * FROM gk1_runways
WHERE (le_latitude_deg IS NOT NULL AND (le_latitude_deg NOT BETWEEN -90 AND 90))
   OR (le_longitude_deg IS NOT NULL AND (le_longitude_deg NOT BETWEEN -180 AND 180));
""")

# --- Finish ---

cur.close()
conn.close()

print("\n✅ Data Quality Checks Completed.\n")
