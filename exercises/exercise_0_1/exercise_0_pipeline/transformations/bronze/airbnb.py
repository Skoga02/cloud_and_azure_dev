import dlt
from pyspark.sql.functions import *
import re

@dlt.table(
    name="airbnb_bronze",
    comment="Raw Airbnb data ingested from CSV"
)
def airbnb_bronze():
    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Volumes/airbnb/hosts/csv_files/bronze/")
    )
    
    cleaned_cols = [df[col].alias(re.sub(r'[ ,;{}()\n\t=]', '_', col)) for col in df.columns]
    return df.select(cleaned_cols)