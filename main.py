import requests
import duckdb
import pandas as pd

def get_pokemon_data(pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    data = requests.get(url).json()

    return {
        "id": data["id"],
        "name": data["name"],
        "height": data["height"],
        "weight": data["weight"]
    }

# Fetch data
pokemon = get_pokemon_data(1)

# Convert to DataFrame
df = pd.DataFrame([pokemon])

# Connect to DuckDB
con = duckdb.connect("pokemon.db")

# Staging table
con.execute("CREATE TABLE IF NOT EXISTS staging_pokemon AS SELECT * FROM df")

# Clean table (transformation)
con.execute("""
CREATE TABLE IF NOT EXISTS clean_pokemon AS
SELECT 
    id,
    UPPER(name) AS name,
    height,
    weight,
    CASE 
        WHEN weight > 100 THEN 'Heavy'
        ELSE 'Light'
    END AS weight_category
FROM staging_pokemon
""")

# Query clean data
result = con.execute("SELECT * FROM clean_pokemon").fetchdf()

print("Clean Data:")
print(result)