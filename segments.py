from pathlib import Path
import json
curr = Path(__file__).resolve()
for parent in curr.parents:
    candidate = parent / "filepaths.json"
    if candidate.exists():
        with open(candidate) as f:
            FILEPATHS = json.load(f)

import os
os.chdir(FILEPATHS["Bill Calculator"]["parent"])

import polars as pl

df = pl.read_csv("cleaned_resstock_data/full_data_with_utility.csv")

def segment(state, utility, gas_utility, kwargs):
    """
        Using the state, utility and a dictionary of segment inputs, returns segments from
        resstock with the buildig_ids, weights, and zip_codes formatted as a single string
        separated by | in their respective columns. Raises Exception if inputs don't return any
        sample for a segment or utility name is not EIA compliant.
    """
    
    df_util = df.filter((pl.col("in.utility_name").str.contains(utility, literal=True)) &
                        (pl.col("in.state").str.contains(state, literal=True)) &
                        (~pl.col("income").str.contains("Not")))

    # Electric Utility name invalid for state
    if df_util.is_empty():
        possible_matches = (
            df.filter(pl.col("in.state") == state)
            .select(pl.col("in.utility_name").str.split("|"))
            .explode("in.utility_name")
            .unique()
            .to_series()
            .to_list()
        )
        suggestions = ", ".join(possible_matches)
        raise Exception(f"Utility name invalid. Has to be one of the following: {suggestions}?")
    
    df_util = df_util.filter(pl.col("in.gas_utility_name").str.contains(gas_utility,literal=True))
    
    # Gas Utility name invalid for state or no buildings with the selected electric utility are served gas by the selected gas utility
    if df_util.is_empty():
        possible_matches = (
            df.filter(pl.col("in.state") == state)
            .select(pl.col("in.gas_utility_name").str.split("|"))
            .explode("in.gas_utility_name")
            .unique()
            .to_series()
            .to_list()
        )
        suggestions = ", ".join(possible_matches)
        raise Exception(f"Gas utility name invalid or no buildings with the selected electric utility are served gas by the selected gas utility. Choose one of the following: {suggestions}?")

    for col_name, value in kwargs.items():
        if value == "":
            continue  # Skip empty values
        if col_name not in df.columns:
            raise Exception(f"{col_name} is not a valid name")
        if value not in df[col_name]:
            raise Exception(f"{value} is not valid for {col_name}")


    seg_cols = [seg for seg, val in kwargs.items() if val]

    if seg_cols:
        segments = df_util.group_by(seg_cols).agg([
            pl.len().alias("count"),
            pl.col("bldg_id").cast(str).map_elements(lambda ids: "|".join(ids)).alias("bldg_ids"),
            pl.col("in.zip_code").cast(str).map_elements(lambda zips: "|".join([z if len(z)==5 else "0"+z for z in zips])).alias("zip_codes"),
            pl.col("elec_weight").cast(str).map_elements(lambda weight: "|".join(weight)).alias("elec_weights"),
            pl.col("gas_weight").cast(str).map_elements(lambda weight: "|".join(weight)).alias("gas_weights")
        ]).sort(seg_cols)
    else:
        segments = df_util.select([
            pl.len().alias("count"),
            pl.col("bldg_id").cast(str).implode().map_elements(lambda ids: "|".join(ids)).alias("bldg_ids"),
            pl.col("in.zip_code").cast(str).implode().map_elements(lambda zips: "|".join([z if len(z)==5 else "0"+z for z in zips])).alias("zip_codes"),
            pl.col("elec_weight").cast(str).implode().map_elements(lambda weight: "|".join(weight)).alias("elec_weights"),
            pl.col("gas_weight").cast(str).implode().map_elements(lambda weight: "|".join(weight)).alias("gas_weights")
        ])
    segments.write_csv(f"segments_by_utility/{utility} x {gas_utility}.csv")

    return segments

if __name__=="__main__":
    
    state = "NY"
    utility = "Consolidated Edison Co-NY Inc"  # or any utility you're filtering for
    gas_utility = "ConEd"
    segment(state,utility,gas_utility,{
        "heating_type":"Natural Gas",
        "building_type":"SF",
        "area":"",
        "income":"",
        "vintage":"",
        "heating_efficiency":"",
        "climate_zone":"",
        "insulation_level":"",
        "has_solar":""
    })