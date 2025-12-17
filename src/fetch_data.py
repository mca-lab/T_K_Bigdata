import os
import requests
import pandas as pd
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


datasets = {
    "population": "https://raw.githubusercontent.com/datasets/population/master/data/population.csv",
    "gdp": "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv"
}


def download_csv(name: str, url: str) -> Path:
   
    output_path = RAW_DIR / f"{name}.csv"

    print(f"Downloading {name} dataset...")
    response = requests.get(url, timeout=30)
    response.raise_for_status() 

    with open(output_path, "wb") as f:
        f.write(response.content)

    print(f"Saved {output_path}")
    return output_path


def convert_to_parquet(csv_path: Path):
   
    print(f"Converting {csv_path.name} → Parquet ...")
    df = pd.read_csv(csv_path)

   
    df.columns = [c.strip() for c in df.columns]
    df = df.drop_duplicates()

   
    parquet_path = csv_path.with_suffix(".parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"Saved {parquet_path}")
    return parquet_path


if __name__ == "__main__":
    print("Starting Module 1: Data Ingestion\n")

    for name, url in datasets.items():
        try:
            csv_path = download_csv(name, url)
            convert_to_parquet(csv_path)
        except Exception as e:
            print(f"Failed to process {name}: {e}")

    print("\n Module 1 complete! Files saved in 'data/raw/'")
