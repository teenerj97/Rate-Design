import boto3
import polars as pl
import io
from botocore.config import Config
from botocore import UNSIGNED
from datetime import datetime
import os

# Anonymous S3 access
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

def download_building_load_profile(state, id, upgrade):
    bucket = "oedi-data-lake"
    key = f"nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/timeseries_individual_buildings/by_state/upgrade={upgrade}/state={state}/{id}-{upgrade}.parquet"

    response = s3.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response['Body'].read())
    return pl.read_parquet(buffer)

def get_load_profiles(state, ids, upgrade=0, write=True):
    """
    Retrieve, process, and export energy load profiles for a list of buildings in a given U.S. state.

    This function downloads hourly energy consumption data for each specified building, normalizes
    timestamps to the current year, and generates hourly aggregates. The results are exported to CSV
    files.

    Args:
        state (str): U.S. state abbreviation (e.g., "CA", "TX").
        ids (list[str] or list[int]): List of building identifiers to retrieve and process.
        
        Optional:
        upgrade: From 0-15 corresponding to ResStock's 16 upgrade measures. Defaults to 0.

    Returns:
        list[pl.DataFrame]: A list of Polars DataFrames containing the load profile data for
        each building.
    """

    buildings = []

    current_year = datetime.now().year

    for id in ids:
        try:
            cur_bldg = download_building_load_profile(state, id, upgrade)
        except:
            continue
        cur_bldg = cur_bldg.filter(pl.col("timestamp").dt.year()==2018).with_columns(
            pl.Series("bldg_id", [id] * (cur_bldg.height-1)),
            pl.col("timestamp").dt.replace(year=current_year).alias("timestamp")
        )

        if os.path.exists(f"load_profiles/{state}/{id}-{upgrade}.csv"):
            agg_bldg = pl.read_csv(f"load_profiles/{state}/{id}-{upgrade}.csv")
        else:
            # hourly aggregation
            agg_bldg = (
                cur_bldg
                .with_columns(pl.col("timestamp").dt.truncate("1h").dt.strftime("%Y-%m-%dT%H:%M:%S.%f").alias("timestamp"))
                .group_by(["bldg_id","timestamp"])
                .agg([
                    pl.col("out.electricity.total.energy_consumption").sum().alias("electricity.total"),
                    pl.col("out.electricity.heating.energy_consumption").sum().alias("electrictiy.heating"),
                    pl.col("out.electricity.heating_hp_bkup.energy_consumption").sum().alias("electricity.secondary_heating"),
                    pl.col("out.electricity.cooling.energy_consumption").sum().alias("electricity.cooling"),
                    pl.col("out.natural_gas.total.energy_consumption").sum().alias("natural_gas.total"),
                    pl.col("out.natural_gas.heating.energy_consumption").sum().alias("natural_gas.heating")
                ])
            ).sort(["timestamp"])
            if write:
                if not os.path.exists(f"load_profiles/{state}"):
                    os.makedirs(f"load_profiles/{state}", exist_ok=True)
                agg_bldg.write_csv(f"load_profiles/{state}/{id}-{upgrade}.csv")
        
        buildings.append(agg_bldg)

    return buildings
