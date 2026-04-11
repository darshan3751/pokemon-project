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

# Create table and insert data
con.execute("CREATE TABLE IF NOT EXISTS pokemon AS SELECT * FROM df")

print("Data saved to database!")
