# Global Banks Data ETL Pipeline

An end-to-end ETL pipeline built with Python that extracts data on the world's largest banks, transforms market-cap data into multiple currencies, and loads the processed dataset into both CSV and SQLite.

## Overview

This project demonstrates a complete **Extract → Transform → Load (ETL)** workflow using real-world web data.

The pipeline:

1. Extracts the top 10 banks and their market capitalization data from a historical Wikipedia source.
2. Transforms the market capitalization values from USD into GBP, EUR, and INR.
3. Loads the processed data into a CSV file.
4. Loads the same dataset into a SQLite database.
5. Executes SQL queries on the resulting database table.
6. Records the progress of each stage in a log file.

## ETL Pipeline

```text
Historical Web Source
        ↓
     Extract
        ↓
   Top 10 Banks
        ↓
    Transform
        ↓
USD → GBP / EUR / INR
        ↓
      Load
     ↙   ↘
   CSV   SQLite
          ↓
     SQL Queries
