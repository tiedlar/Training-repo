############################################################################################
# Dagfinn Larsen, aug 2023
# lage parquet fil fra sas7bdat fil
############################################################################################
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat

# Load SAS file into pandas DataFrame
sas_file_path = "/home/mistralnett/G015170/python/data/t02phy_fileinformation.sas7bdat"
print(f"Laster SAS fil fra {sas_file_path}")
df, meta = pyreadstat.read_sas7bdat(sas_file_path)
print("SAS fil ferdig lastet")

# Convert pandas DataFrame to Arrow Table
print("Konverterer pandas DataFrame til Arrow Tabell...")
table = pa.Table.from_pandas(df)

# Specify the Parquet file path
parquet_file_path = "/home/mistralnett/G015170/python/data/t02phy_fileinformation.parquet"

# Write Arrow Table to Parquet file
print(f"Skriver Arrow Tabell til Parquet fil fra {parquet_file_path}")
pq.write_table(table, parquet_file_path)
print("Parquet fil er ferdig skrevet")
