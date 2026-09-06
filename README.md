# Global Banks Data ETL Pipeline

An end-to-end ETL pipeline built with Python to extract data on the world's largest banks, transform market capitalization values into multiple currencies, and load the processed data into CSV and SQLite.

## Overview

The project demonstrates a complete **Extract → Transform → Load** workflow:

- Extracts the top 10 banks from a web-based data source
- Transforms market capitalization from USD into GBP, EUR, and INR
- Saves the processed data to CSV
- Loads the data into a SQLite database
- Performs basic SQL queries on the dataset
- Logs the ETL process

## Pipeline

```text
Web Data
   ↓
Extract
   ↓
Transform
   ↓
USD → GBP / EUR / INR
   ↓
Load
 ↙     ↘
CSV   SQLite
       ↓
   SQL Queries
```

## Technologies

- Python
- Pandas
- NumPy
- BeautifulSoup
- Requests
- SQLite
- SQL

## Dataset

The processed dataset contains:

| Column | Description |
|---|---|
| `Name` | Bank name |
| `MC_USD_Billion` | Market cap in USD |
| `MC_GBP_Billion` | Market cap in GBP |
| `MC_EUR_Billion` | Market cap in EUR |
| `MC_INR_Billion` | Market cap in INR |

## Project Structure

```text
├── project file.py
├── Largest_banks_data.csv
├── exchange_rate.csv
└── README.md
```

## Running the Project

Install the dependencies:

```bash
pip install requests beautifulsoup4 pandas numpy
```

Run the pipeline:

```bash
python "project file.py"
```

## Key Concepts

This project demonstrates:

- Web data extraction
- Data transformation with Pandas
- Currency conversion
- CSV and SQLite data handling
- SQL querying
- ETL pipeline design
- Process logging

