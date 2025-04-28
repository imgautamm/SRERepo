import psycopg2
import pandas as pd
import os

# Connect to PostgreSQL
from db import get_connection

conn = get_connection()
cur = conn.cursor()

# Master table creation query
create_master_query = """
DROP TABLE IF EXISTS master_airports;

CREATE TABLE master_airports AS
SELECT 
    a.id AS airport_id,
    a.ident AS airport_ident,
    a.type AS airport_type,
    a.name AS airport_name,
    a.latitude_deg AS airport_latitude,
    a.longitude_deg AS airport_longitude,
    a.elevation_ft AS airport_elevation,
    a.iso_country AS country_code,
    c.name AS country_name,
    r.id AS runway_id,
    r.length_ft AS runway_length,
    r.width_ft AS runway_width,
    r.surface AS runway_surface,
    r.lighted AS runway_lighted,
    r.closed AS runway_closed
FROM 
    gk1_airports a
LEFT JOIN 
    gk1_countries c ON a.iso_country = c.code
LEFT JOIN 
    gk1_runways r ON a.id = r.airport_ref;
"""

# Execute the query to create the master table
cur.execute(create_master_query)
conn.commit()

print("✅ Master table 'master_airports' created successfully!")

# Extract the data from the master table into a pandas DataFrame
query = "SELECT * FROM master_airports"
df = pd.read_sql_query(query, conn)

# Create the data folder if it doesn't exist
data_folder = "data"
os.makedirs(data_folder, exist_ok=True)

# Save the DataFrame to a CSV file in the data folder
output_path = os.path.join(data_folder, "master_airports.csv")
df.to_csv(output_path, index=False)

print(f"✅ Data extracted and saved to {output_path}")

# Close the cursor and connection
cur.close()
conn.close()
