import os
import requests
import sqlite3
import pandas as pd
import schedule
import time

# Dataset URLs
urls = [
    "https://data.cityofnewyork.us/resource/5uac-w243.csv",
    "https://data.cityofnewyork.us/resource/ia2d-e54m.csv"
]

# Output directory and database
output_dir = "data/"
db_path = "nyc_parking_analysis.db"

os.makedirs(output_dir, exist_ok=True)

# Function to download datasets
def download_datasets(urls, output_dir):
    for url in urls:
        filename = os.path.join(output_dir, url.split("/")[-1])
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {filename}")
        else:
            print(f"Failed to download: {url}")

# Function to store data in SQLite
def store_csv_in_db(file_path, table_name, conn):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Data from {file_path} stored in table '{table_name}'.")

# Function to clean and transform data
def clean_data(conn):
    # Clean data from the violations table
    query = """
    SELECT 
        * 
    FROM violations
    WHERE cmplnt_fr_dt IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)

    # Convert date column to datetime
    df["cmplnt_fr_dt"] = pd.to_datetime(df["cmplnt_fr_dt"], errors="coerce")
    df.dropna(subset=["cmplnt_fr_dt"], inplace=True)

    # Store cleaned data
    df.to_sql("cleaned_violations", conn, if_exists="replace", index=False)
    print("Cleaned data stored in 'cleaned_violations' table.")

# Function to analyze hotspots
def hotspot_analysis(conn):
    query = """
    SELECT boro_nm, COUNT(*) as count
    FROM cleaned_violations
    GROUP BY boro_nm
    ORDER BY count DESC
    LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    print(df)  # Print the hotspot analysis results for logging purposes

# Complete pipeline function
def pipeline():
    print("Starting the pipeline...")
    conn = sqlite3.connect(db_path)
    download_datasets(urls, output_dir)
    store_csv_in_db("data/5uac-w243.csv", "violations", conn)
    store_csv_in_db("data/ia2d-e54m.csv", "related_data", conn)
    clean_data(conn)
    hotspot_analysis(conn)
    conn.close()
    print("Pipeline completed successfully!")

# Schedule the pipeline
schedule.every().day.at("02:00").do(pipeline)

# Keep the script running for scheduling
if __name__ == "__main__":
    pipeline()  # Run once immediately
    while True:
        schedule.run_pending()
        time.sleep(1)
