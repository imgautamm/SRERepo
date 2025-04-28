import psycopg2
import requests
import io
import csv
from config import CSV_PUBLIC_URL

# Database connection details
from db import get_connection

conn = get_connection()
cur = conn.cursor()

# --- Step 1: Create Tables ---

create_airports_table = """
CREATE TABLE IF NOT EXISTS gk1_airports (
    id INTEGER PRIMARY KEY,
    ident TEXT,
    type TEXT,
    name TEXT,
    latitude_deg DOUBLE PRECISION,
    longitude_deg DOUBLE PRECISION,
    elevation_ft INTEGER,
    continent TEXT,
    iso_country TEXT,
    iso_region TEXT,
    municipality TEXT,
    scheduled_service TEXT,
    gps_code TEXT,
    iata_code TEXT,
    local_code TEXT,
    home_link TEXT,
    wikipedia_link TEXT,
    keywords TEXT
);
"""

create_countries_table = """
CREATE TABLE IF NOT EXISTS gk1_countries (
    id INTEGER PRIMARY KEY,
    code TEXT,
    name TEXT,
    continent TEXT,
    wikipedia_link TEXT,
    keywords TEXT
);
"""

create_runways_table = """
CREATE TABLE IF NOT EXISTS gk1_runways (
    id INTEGER PRIMARY KEY,
    airport_ref INTEGER,
    airport_ident TEXT,
    length_ft INTEGER,
    width_ft INTEGER,
    surface TEXT,
    lighted BOOLEAN,
    closed BOOLEAN,
    le_ident TEXT,
    le_latitude_deg DOUBLE PRECISION,
    le_longitude_deg DOUBLE PRECISION,
    le_elevation_ft INTEGER,
    le_heading_degT DOUBLE PRECISION,
    le_displaced_threshold_ft INTEGER,
    he_ident TEXT,
    he_latitude_deg DOUBLE PRECISION,
    he_longitude_deg DOUBLE PRECISION,
    he_elevation_ft INTEGER,
    he_heading_degT DOUBLE PRECISION,
    he_displaced_threshold_ft INTEGER
);
"""

cur.execute(create_airports_table)
cur.execute(create_countries_table)
cur.execute(create_runways_table)
conn.commit()

# --- Step 2: Insert Functions ---

def load_csv_to_table(csv_url, insert_query):
    response = requests.get(csv_url)
    response.raise_for_status()
    csv_file = io.StringIO(response.content.decode('utf-8'))
    reader = csv.reader(csv_file)
    next(reader)  # skip header
    for row in reader:
        row = [field if field != '' else None for field in row]
        cur.execute(insert_query, row)

# Insert queries

insert_airports = """
INSERT INTO gk1_airports (
    id, ident, type, name, latitude_deg, longitude_deg, elevation_ft,
    continent, iso_country, iso_region, municipality, scheduled_service,
    gps_code, iata_code, local_code, home_link, wikipedia_link, keywords
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (id) DO NOTHING;
"""

insert_countries = """
INSERT INTO gk1_countries (
    id, code, name, continent, wikipedia_link, keywords
) VALUES (
    %s, %s, %s, %s, %s, %s
)
ON CONFLICT (id) DO NOTHING;
"""

insert_runways = """
INSERT INTO gk1_runways (
    id, airport_ref, airport_ident, length_ft, width_ft, surface,
    lighted, closed, le_ident, le_latitude_deg, le_longitude_deg,
    le_elevation_ft, le_heading_degT, le_displaced_threshold_ft,
    he_ident, he_latitude_deg, he_longitude_deg, he_elevation_ft,
    he_heading_degT, he_displaced_threshold_ft
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
ON CONFLICT (id) DO NOTHING;
"""

# --- Step 3: URLs and Loading Data ---

airports_url = f"{CSV_PUBLIC_URL}/airports.csv"
countries_url = f"{CSV_PUBLIC_URL}/countries.csv"
runways_url = f"{CSV_PUBLIC_URL}/runways.csv"

load_csv_to_table(airports_url, insert_airports)
load_csv_to_table(countries_url, insert_countries)
load_csv_to_table(runways_url, insert_runways)

# --- Step 4: Finish up ---

conn.commit()
cur.close()
conn.close()

print("All tables created and data loaded successfully!")
