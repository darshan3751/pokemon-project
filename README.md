# 🚀 Pokémon Data Pipeline Project

## 📌 Overview
This project demonstrates an end-to-end data pipeline using Python and SQL.

## ⚙️ Pipeline Flow
API → Staging → Clean → Fact → Dimension → Analysis → CSV Export

## 🛠️ Tech Stack
- Python
- DuckDB
- SQL
- Pandas
- Git & GitHub

## 📊 Features
- Extract data from PokeAPI
- Store raw data in staging layer
- Perform data transformation using SQL
- Create fact and dimension tables (Star Schema)
- Perform analysis queries
- Export results to CSV
- Dynamic user input for dataset size

## ▶️ How to Run
1. Install dependencies:
   pip install -r requirements.txt

2. Run project:
   python main.py

## 📁 Output
- Clean dataset stored in DuckDB
- CSV file generated for visualization