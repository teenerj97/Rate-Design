import polars as pl
from difflib import get_close_matches
import os
os.chdir("c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/bill calculator")

df = pl.read_csv("cleaned_resstock_data/full_data_with_utility.csv")

def segment(state, utility, kwargs):
    """
        Using the state, utility and a dictionary of segment inputs, returns segments from
        resstock with the buildig_ids, weights, and zip_codes formatted as a single string
        separated by | in their respective columns. Raises Exception if inputs don't return any
        sample for a segment or utility name is not EIA compliant.
    """

    if utility not in df["in.utility_name"]:
        possible_matches = get_close_matches(utility, df["in.utility_name"].unique(), n=5)
        suggestions = ", ".join(possible_matches) if possible_matches else "No suggestions available"
        raise Exception(f"Utility Name Invalid. Did you mean any of the following: {suggestions}?")
    for col_name, value in kwargs.items():
        if value == "":
            continue  # Skip empty values
        if col_name not in df.columns:
            raise Exception(f"{col_name} is not a valid name")
        if value not in df[col_name]:
            raise Exception(f"{value} is not valid for {col_name}")
        #below line filters the dataframe based on the column name and value (basically filters for desired segments)
        df_filter = df.filter(pl.col(col_name) == value)
    df_util = df_filter.filter((pl.col("in.utility_name").str.contains(utility, literal=True)) &
                        (pl.col("in.state").str.contains(state, literal=True)) &
                        (~pl.col("income").str.contains("Not")))

    """seg_cols = [seg for seg, val in kwargs.items() if val]

    segments = df_util.group_by(seg_cols).agg([
            pl.len().alias("count"),
            pl.col("bldg_id").cast(str).map_elements(lambda ids: "|".join(ids)).alias("bldg_ids"),
            pl.col("in.zip_code").cast(str).map_elements(lambda zips: "|".join(zips)).alias("zip_codes"),
            pl.col("elec_weight").cast(str).map_elements(lambda weight: "|".join(weight)).alias("elec_weights"),
            pl.col("gas_weight").cast(str).map_elements(lambda weight: "|".join(weight)).alias("gas_weights"),
            pl.col("in.gas_utility_name")
                .cast(str)
                .str.split("|")
                .list.get(0).map_elements(lambda names: "|".join(names)).alias("gas_utility")
        ]).sort(seg_cols)""" 

    df_util.write_csv(f"segments_by_utility/{utility}.csv")
#changed segments.write_csv to df_util.write_csv above
    return df_util

if __name__=="__main__":
    
    state = "CA"
    utility = "Pacific Gas & Electric Co."  # or any utility you're filtering for
    segment(state,utility,{
        "heating_type":"Natural Gas",
        "building_type":"SF",
        "area":"0-1499",
        "income":"Low Income",
        "vintage":"1960-2000",
        "heating_efficiency":"Low Htg Eff",
        "climate_zone":"Hot-Dry",
        "insulation_level":"Good Insulation",
        "has_solar":"No"
    })