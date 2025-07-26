import os
import tempfile
import databento as db

def download_and_convert(
    api_key: str,
    dataset: str,
    schema: str,
    symbols: list,
    start_date: str,
    end_date: str,
    parquet_path: str,
):
    client = db.Historical(api_key)

    print(f"Downloading {dataset} data for symbols {symbols} from {start_date} to {end_date}...")

    data = client.timeseries.get_range(
        dataset=dataset,
        schema=schema,
        symbols=symbols,
        start=start_date,
        end=end_date,
    )

    print(f"Saving to Parquet at {parquet_path}...")
    data.to_parquet(parquet_path)

    print("Download and conversion complete.")


def main():
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        raise ValueError("Please set your DATABENTO_API_KEY environment variable.")

    dataset = "GLBX.MDP3"
    schema = "mbp-1"
    symbols = ["MNQ"] 
    start_date = "2024-07-25"
    end_date = "2025-07-25"

    parquet_path = "/ssd/databases/parquet/data/futures/MNQ_L1_Databento.parquet"

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    download_and_convert(api_key, dataset, schema, symbols, start_date, end_date, parquet_path)


if __name__ == "__main__":
    main()
