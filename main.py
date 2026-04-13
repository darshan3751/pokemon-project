from extract import get_pokemon_data
import duckdb
import pandas as pd

def main():
    print("\n🚀 Starting Data Pipeline...")

    # Connect to DuckDB
    con = duckdb.connect("pokemon.db")

    # User input
    num = int(input("Enter number of Pokemon: "))
    print(f"Fetching {num} Pokemon...")

    # Get existing IDs (incremental load)
    try:
        existing_ids = con.execute(
            "SELECT id FROM staging_pokemon"
        ).fetchdf()["id"].tolist()
    except:
        existing_ids = []

    pokemon_list = []

    # Fetch new data only
    for i in range(1, num + 1):
        if i not in existing_ids:
            pokemon = get_pokemon_data(i)
            pokemon_list.append(pokemon)

    df = pd.DataFrame(pokemon_list)

    print(f"New records fetched: {len(df)}")

    # Load into staging
    if len(existing_ids) == 0:
        con.execute("CREATE TABLE IF NOT EXISTS staging_pokemon AS SELECT * FROM df")
    else:
        if len(df) > 0:
            con.execute("INSERT INTO staging_pokemon SELECT * FROM df")

    print("Data stored in staging table")

    # Transform (clean layer)
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

    print("Data transformed (clean table created)")

    # Query result
    result = con.execute("SELECT * FROM clean_pokemon").fetchdf()

    print("\nClean Data:")
    print(result)

    # Export
    result.to_csv("pokemon_output.csv", index=False)

    print("CSV file created successfully")
    print("Pipeline completed successfully ✅")


if __name__ == "__main__":
    main()