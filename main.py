from extract import get_pokemon_data
from config import DB_NAME
import duckdb
import pandas as pd
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Starting Data Pipeline")
    time.sleep(1)

    # Connect to DuckDB
    con = duckdb.connect(DB_NAME)

    # User input
    num = int(input("Enter number of Pokemon: "))
    logging.info(f"Fetching {num} Pokemon...")
    time.sleep(1)

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
            if pokemon is not None:
                pokemon_list.append(pokemon)

    df = pd.DataFrame(pokemon_list)

    logging.info(f"New records fetched: {len(df)}")
    time.sleep(1)

    # Load into staging
    if len(existing_ids) == 0:
        con.execute("CREATE TABLE IF NOT EXISTS staging_pokemon AS SELECT * FROM df")
    else:
        if len(df) > 0:
            con.execute("INSERT INTO staging_pokemon SELECT * FROM df")

    logging.info("Data stored in staging table")
    time.sleep(1)

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

    logging.info("Transformation completed")
    time.sleep(1)

    # Query result
    result = con.execute("SELECT * FROM clean_pokemon").fetchdf()

    print("\nClean Data:")
    time.sleep(2)
    print(result)
    logging.info("Clean data displayed")
    time.sleep(1)

    # Export
    result.to_csv("pokemon_output.csv", index=False)
    logging.info("CSV file created successfully")
    time.sleep(1)

    logging.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()