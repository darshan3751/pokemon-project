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
    "weight": data["weight"],
    "base_experience": data["base_experience"],
    "type": data["types"][0]["type"]["name"]
}

# Fetch data
pokemon_list = []

for i in range(1, 11):  # 10 pokemon
    pokemon = get_pokemon_data(i)
    pokemon_list.append(pokemon)

df = pd.DataFrame(pokemon_list)

# Connect to DuckDB
con = duckdb.connect("pokemon.db")

# Staging table
con.execute("CREATE TABLE IF NOT EXISTS staging_pokemon AS SELECT * FROM df")

# Clean table (transformation)
con.execute("DROP TABLE IF EXISTS clean_pokemon")

con.execute("""
CREATE TABLE clean_pokemon AS
SELECT 
    id,
    UPPER(name) AS name,
    height,
    weight,
    base_experience,
    type,
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

result.to_csv("pokemon_output.csv", index=False)
print("CSV file created!")

# avg experience by type
result1 = con.execute("""
SELECT 
    type,
    AVG(base_experience) as avg_exp
FROM clean_pokemon
GROUP BY type
ORDER BY avg_exp DESC
""").fetchdf()

print("\nAverage Experience by Type:")
print(result1)

# heviest pokemon
result2 = con.execute("""
SELECT 
    type,
    MAX(weight) as max_weight
FROM clean_pokemon
GROUP BY type
""").fetchdf()

print("\nHeaviest Pokemon per Type:")
print(result2)

# top 5 strong pokemon
result3 = con.execute("""
SELECT 
    name,
    base_experience
FROM clean_pokemon
ORDER BY base_experience DESC
LIMIT 5
""").fetchdf()

print("\nTop 5 Strongest Pokemon:")
print(result3)

result3.to_csv("top_pokemon.csv", index=False)