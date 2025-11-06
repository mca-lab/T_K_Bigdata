# src/clean_data.py
"""
PySpark cleaning & integration script for world population and GDP datasets.
Usage (local inside container):
  spark-submit src/clean_data.py --raw-dir data/raw --out-dir data/processed --mode overwrite
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, trim, expr
from pyspark.sql.types import IntegerType, DoubleType

def init_spark(app_name="clean_data"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark

def list_csvs(raw_dir):
    files = []
    for f in os.listdir(raw_dir):
        if f.lower().endswith(".csv"):
            files.append(os.path.join(raw_dir, f))
    return files

def wide_to_long(df, id_vars):
    # Convert wide Year-columns into (year, value) rows using stack.
    # Detect year-like columns (4-digit)
    year_cols = [c for c in df.columns if c.strip().isdigit() and len(c.strip()) == 4]
    if not year_cols:
        return None
    n = len(year_cols)
    # build stack expression: stack(n, 'yr1', col1, 'yr2', col2, ...)
    pairs = []
    for y in year_cols:
        pairs.append(f"'{y}', `{y}`")
    stack_expr = f"stack({n}, {', '.join(pairs)}) as (year, value)"
    id_cols_sql = ", ".join([f"`{c}`" for c in id_vars])
    long = df.selectExpr(id_cols_sql + ", " + stack_expr)
    return long

def read_and_normalize(spark, path, expected_measure, id_vars_default):
    # expected_measure: string label like "population" or "gdp"
    df = spark.read.options(header=True, inferSchema=False, multiLine=True).csv(path)
    # Clean column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    cols = df.columns
    # Heuristics: if there is a "Year" or "year" column, assume long format
    lc = [c.lower() for c in cols]
    if "year" in lc:
        # standard long format: Country Name, Country Code, Year, Value (or indicator)
        # locate value-like column: choose numeric-looking column or last column
        value_col = None
        # prefer column named "Value" or "value" or expected_measure
        for candidate in cols[::-1]:
            if candidate.lower() in ("value", expected_measure.lower(), "val"):
                value_col = candidate
                break
        if not value_col:
            # fallback: take last column that is not country/year/code
            non_id = [c for c in cols if c.lower() not in ("country name","country", "country code","year")]
            value_col = non_id[-1] if non_id else cols[-1]
        long = df.selectExpr("`Country Name` as country_name",
                             "`Country Code` as country_code",
                             "cast(Year as int) as year",
                             f"cast(`{value_col}` as double) as {expected_measure}")
        return long
    else:
        # attempt wide -> long
        id_vars = [c for c in cols if c.lower() in (v.lower() for v in id_vars_default)]
        if not id_vars:
            # fallback to first two columns as id
            id_vars = cols[:2]
        long = wide_to_long(df, id_vars)
        if long is None:
            raise ValueError(f"Could not interpret file {path} as long or wide year-format CSV.")
        # rename id vars to standard names
        rename_map = {}
        if len(id_vars) >= 2:
            rename_map[id_vars[0]] = "country_name"
            rename_map[id_vars[1]] = "country_code"
        else:
            rename_map[id_vars[0]] = "country_name"
            rename_map[id_vars[0]] = "country_code"
        for old, new in rename_map.items():
            long = long.withColumnRenamed(old, new)
        # cast year, value
        long = long.withColumn("year", col("year").cast(IntegerType())) \
                   .withColumnRenamed("value", expected_measure) \
                   .withColumn(expected_measure, col(expected_measure).cast(DoubleType()))
        return long

def simple_clean(df, measure_name):
    # common cleaning: trim names, remove commas from codes, drop rows without year
    df = df.withColumn("country_name", trim(col("country_name")))
    if "country_code" in df.columns:
        df = df.withColumn("country_code", trim(regexp_replace(col("country_code"), r"\\s+", "")))
    df = df.filter(col("year").isNotNull())
    # remove impossible negative values (if any)
    if measure_name in df.columns:
        df = df.filter((col(measure_name).isNotNull()))
    return df

def main(raw_dir, out_dir, mode):
    spark = init_spark("clean_data_pipeline")
    csvs = list_csvs(raw_dir)
    if not csvs:
        print(f"No CSV files found in {raw_dir}. Exiting.")
        return

    pop_df = None
    gdp_df = None

    for p in csvs:
        name = os.path.basename(p).lower()
        try:
            if "pop" in name or "population" in name:
                pop_df = read_and_normalize(spark, p, "population", ["Country Name", "Country Code"])
                pop_df = simple_clean(pop_df, "population")
                print(f"Loaded population from {p}; rows={pop_df.count()}")
            elif "gdp" in name or "gross" in name:
                gdp_df = read_and_normalize(spark, p, "gdp", ["Country Name", "Country Code"])
                gdp_df = simple_clean(gdp_df, "gdp")
                print(f"Loaded gdp from {p}; rows={gdp_df.count()}")
            else:
                # try to auto-detect by schema: if there's a column named population/gdp
                tmp = read_and_normalize(spark, p, "value", ["Country Name", "Country Code"])
                # if sample values look like population (large numbers)
                sample = tmp.limit(100).collect()
                avg = None
                if sample:
                    # compute avg of absolute values
                    vals = [r[3] for r in sample if r[3] is not None]
                    if vals:
                        avg = sum([abs(float(v)) for v in vals]) / len(vals)
                if avg and avg > 1e6:
                    pop_df = tmp.withColumnRenamed("value", "population")
                    pop_df = simple_clean(pop_df, "population")
                    print(f"Auto-detected population for {p}; rows={pop_df.count()}")
                else:
                    gdp_df = tmp.withColumnRenamed("value", "gdp")
                    gdp_df = simple_clean(gdp_df, "gdp")
                    print(f"Auto-detected gdp for {p}; rows={gdp_df.count()}")
        except Exception as e:
            print(f"Skipping {p} due to error: {e}")

    # If one dataset missing raise informative message
    if pop_df is None or gdp_df is None:
        print("Warning: One of population or gdp datasets was not detected. Found:",
              "population" if pop_df is not None else "NO population",
              "gdp" if gdp_df is not None else "NO gdp")

    # Standardize join keys
    for df in (pop_df, gdp_df):
        if df is not None:
            if "country_code" not in df.columns:
                df = df.withColumn("country_code", lit(None))
    # prefer join on country_code; fallback to country_name
    if pop_df is not None and gdp_df is not None:
        joined = pop_df.join(gdp_df, on=["country_code", "year"], how="outer") \
                .select(
                    coalesce_col(pop_df, "country_name", gdp_df, "country_name").alias("country_name"),
                    coalesce_col(pop_df, "country_code", gdp_df, "country_code").alias("country_code"),
                    col("year"),
                    col("population"),
                    col("gdp")
                )
    else:
        # if only one exists, write it through with unified schema
        joined = None
        if pop_df is not None:
            joined = pop_df.select("country_name","country_code","year","population").withColumn("gdp", lit(None).cast(DoubleType()))
        elif gdp_df is not None:
            joined = gdp_df.select("country_name","country_code","year","gdp").withColumn("population", lit(None).cast(DoubleType()))

    if joined is None:
        print("No data to write after processing. Exiting.")
        return

    # Remove duplicates (keeping first)
    joined = joined.dropDuplicates(["country_code","year"])

    # Save combined parquet partitioned by year
    out_path = os.path.join(out_dir, "combined")
    joined.write.mode(mode).partitionBy("year").parquet(out_path)
    print(f"Wrote processed data to {out_path}")

    spark.stop()

# helper used in select above (coalesce from two dataframes)
from pyspark.sql.functions import coalesce
def coalesce_col(df1, col1, df2, col2):
    # returns coalesce(df1.col1, df2.col2) expression
    return coalesce(df1[col1], df2[col2])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=str, default="data/raw")
    parser.add_argument("--out-dir", type=str, default="data/processed")
    parser.add_argument("--mode", type=str, default="overwrite", choices=["overwrite","append","error","ignore"])
    args = parser.parse_args()
    main(args.raw_dir, args.out_dir, args.mode)

