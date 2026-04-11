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
pokemon_list = []

for i in range(1, 11):  # 10 pokemon
    pokemon = get_pokemon_data(i)
    pokemon_list.append(pokemon)

df = pd.DataFrame(pokemon_list)

# Connect to DuckDB
con = duckdb.connect("pokemon.db")

# Create table and insert data
con.execute("CREATE TABLE IF NOT EXISTS staging_pokemon AS SELECT * FROM df")
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


# Query data
result = con.execute("SELECT * FROM clean_pokemon").fetchdf()

print("Clean Data:")
print(result)

result.to_csv("pokemon_output.csv", index=False)
print("CSV file created!")