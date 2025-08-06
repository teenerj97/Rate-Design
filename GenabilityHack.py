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

import requests
import polars as pl
from collections import defaultdict
from datetime import datetime


def get_tariff_gen(elecTariff, utility, zip_code, building):

    # First get the territory of the building we are using
    app_id = "3df8e135-968d-4399-9879-2a1c6a3de30c"
    app_key = "e51974c7-996b-4698-9628-71950d223364"

    url = "https://api.genability.com/rest/public/territories"
    params = {
        "masterTariffId": elecTariff,
        "zipCode": zip_code
    }

    response = requests.get(url, auth=(app_id, app_key), params=params).json()
    territoryId = response["results"][0]["territoryId"] if len(response["results"])>0 else 0
    territoryName = response["results"][0]["territoryName"] if territoryId else ""

    search = f"{territoryName}_{elecTariff}" if territoryName else f"{elecTariff}"
    for root,_,files in os.walk("Electric_Tariffs"):
        for f in files:
            if search in f:
                df = pl.read_csv(os.path.join(root,f))
                return df

    # Run a single calculation to get all the rates from the tariff
    url = "https://api.genability.com/rest/v1/ondemand/calculate"
    params = {
        "masterTariffId": elecTariff,
        "zipCode": zip_code,
        "fromDateTime": building["timestamp"].first(),
        "toDateTime": building["timestamp"].last(),
        "groupBy": "YEAR",
        "propertyInputs" : [{
            "keyName": "consumption",
            "unit": "kWh",
            "fromDateTime": building["timestamp"].first(),
            "duration": 900000, # 15 mins
            "dataSeries": building["electricity.total"].to_list()
        }]
    }
    if territoryId:
        params["propertyInputs"].append({"keyName": "territoryId", "dataValue": territoryId})

    response = requests.post(url, auth=(app_id, app_key), json=params)
    data = response.json()["results"][0]
    tariff_name = data["tariffName"]
    costs_breakdown = pl.from_dicts(response.json()["results"][0]["items"])
    cols = ["rateName", "rateAmount", "itemQuantity","cost","period"] if "period" in costs_breakdown.columns else ["rateName", "itemQuantity", "rateAmount","cost"]
    costs_breakdown = costs_breakdown.select(cols).sort("rateName")

    # Get the tariff from genability for cross-refercing rates from the calculation
    url = "https://api.genability.com/rest/public/tariffs"

    params = {
        "masterTariffId": elecTariff,
        "zipCode": zip_code,
        "effectiveOn": datetime.now().strftime("%Y-%m-%d"),
        "fromDateTime": building["timestamp"].first(),
        "toDateTime": building["timestamp"].last(),
        "populateRates": True
    }

    response = requests.get(url, auth=(app_id, app_key), params=params)
    data = response.json()["results"][0]

    # Filter the rates from the tariff according to whether they appear in the cost calculation or whether they have non-zero values in the tariff itself
    # (filtered by territory to ensure only relveant rates get captured - also captures rates where no territory specified)
    rates = [r for r in data["rates"] if r["rateName"] in costs_breakdown["rateName"] and \
            (any(c["rateAmount"]!=0 and c["itemQuantity"]!=0 for c in costs_breakdown.filter(pl.col("rateName")==r["rateName"]).to_dicts()) or \
             any(b["rateAmount"]!=0 for b in r["rateBands"])) and \
            r.get("territory",{"territoryId":territoryId})["territoryId"]==territoryId]
    
    # Update the logic with rate determinant, seasonal, time of use, and block handling
    rows = []
    for rate in rates:
        rate_name = rate["rateName"]
        eff_date = rate["fromDateTime"].split("T")[0]
        category = rate.get("chargeClass", "")
        
        # Rate Determinant logic
        if rate.get("chargeType") == "FIXED_PRICE" and \
            any(c>=0 for c in costs_breakdown.filter(pl.col("rateName")==rate["rateName"])["rateAmount"]) and \
            any(q==12 or q==13 for q in costs_breakdown.filter(pl.col("rateName")==rate["rateName"])["itemQuantity"]):
            rate_determinant = "per month"
        elif rate.get("chargeType") == "FIXED_PRICE" and any(q<12 and q==round(q) for q in costs_breakdown.filter(pl.col("rateName")==rate["rateName"])["itemQuantity"]):
            rate_determinant = "per year"
        elif rate.get("chargeType") == "QUANTITY" and (rate["rateBands"] or [{1:""}])[0].get("rateUnit")=="PERCENTAGE":
            rate_determinant = "percent"
        elif rate.get("chargeType") == "DEMAND_BASED":
            if "60min" in rate["quantityKey"]:
                rate_determinant = "per 60min kw"
            else:
                rate_determinant = "per 30min kw"
        elif rate.get("chargeType") == "CONSUMPTION_BASED":
            rate_determinant = "per kwh"
        else:
            continue

        # Season Logic - parsed as mm/dd-mm/dd
        season = ""
        if rate.get("season",rate.get("timeOfUse",{}).get("season")):
            s = rate.get("season",rate.get("timeOfUse",{}).get("season"))
            season = f'{s["seasonFromMonth"]:02d}/{s["seasonFromDay"]:02d}-{s["seasonToMonth"]:02d}/{s["seasonToDay"]:02d}'
        
        # Time of Use Logic - parsed as (([wd,wd],[hh,hh]),...)
        tou_type = ""
        tou = ""
        if "timeOfUse" in rate:
            tou=[]
            for p in rate["timeOfUse"]["touPeriods"]:
                p["toHour"] = p["toHour"] if p["toHour"] else 24
                tou.append(([p["fromDayOfWeek"]+1,p["toDayOfWeek"]+1],[p["fromHour"], p["toHour"]]))
            tou = str(tou)
            tou_type = rate["timeOfUse"].get("touType", "OFF_PEAK")
        
        # Block Logic
        bands = rate.get("rateBands", [])
        has_cons_limits = any(b.get("hasConsumptionLimit") for b in bands)
        if not has_cons_limits or len(bands) == 1:
            rows.append([tariff_name, rate_name, eff_date, category, "", rate_determinant, "", "", season, tou, tou_type])
        else:
            limits = [b.get("consumptionUpperLimit") for b in bands]
            prev_limit = None
            for i, limit in enumerate(limits):
                if limit == prev_limit:
                    continue  # skip dupes
                start = "" if i == 0 else prev_limit
                end = limit if limit is not None else ""
                rows.append([tariff_name, rate_name, eff_date, category, "", rate_determinant, start, end, season, tou, tou_type])
                prev_limit = limit

    df = pl.DataFrame(rows, schema=[
        "tariff","rateName", "EffDate", "Category", "Rate", "Rate Determinant",
        "Start", "End", "Season", "tou", "period"
    ])

    # This logic is to handle when the calculate API returns multiple costs per rate
    # It also retrievs the rate from the tariff, overriding the Calculate API result when applicable

    # group df by rateName
    df_grouped = df.group_by("rateName","Start","End","Rate Determinant",maintain_order=True).agg(pl.len().alias("df_count"))
    cb_grouped = costs_breakdown.group_by("rateName",maintain_order=True).agg([
        pl.len().alias("cb_count"),
        (pl.col("rateAmount") * pl.col("itemQuantity")).sum().alias("weighted_sum"),
        (pl.col("cost").sum()/building["electricity.total"].sum()).alias("sum_%"), # Converting percentage based to per kwh - idea is to use first pass for rate then scale based on consumption of new buildings
        pl.col("itemQuantity").sum().alias("total_qty"),
        pl.col("rateAmount").alias("rate_list"),  # keep as list for ordered match
        (pl.col("cost")/building["electricity.total"].sum()).alias("rate_list_%")  # keep as list for ordered match
    ])

    # join stats
    summary = df_grouped.join(cb_grouped, on="rateName", how="left")

    # result mapping: rateName -> list of final rates
    rate_map = {}
    for row in summary.iter_rows(named=True):
        name = row["rateName"]
        determinant = row["Rate Determinant"]
        dcount = row["df_count"]
        ccount = row["cb_count"]
        factor = row["total_qty"] if determinant=="per year" else 1

        if dcount == 1 and ccount>1:
            # assign weighted avg
            avg = factor * row["weighted_sum"] / row["total_qty"] if determinant!="percent" and row["total_qty"] else row["sum_%"] if determinant=="percent" else 0
            rate_map[name] = [avg]

        elif ccount == dcount:
            # respect order
            rate_map[name] = [r*factor for r in row["rate_list"]] if determinant!="percent" else row["rate_list_%"]
        
        else:
            # mismatch, fallback: NaNs
            rate_map[name] = [None] * dcount

    final_rates = []
    group_counts = {}

    for row in df.iter_rows(named=True):
        name = row["rateName"]
        from_tariff = [band["rateAmount"] * (-1 if band["isCredit"] else 1) for r in rates if r["rateName"]==name for band in r["rateBands"] if band["rateUnit"]!="PERCENTAGE"]
        if name not in group_counts:
            group_counts[name] = 0
        idx = group_counts[name]
        # If rate from tariff has any non-zero values, use those instead of mapping
        if any([c!=0 for c in from_tariff]):
            val_list=from_tariff
        else:
            val_list = rate_map.get(name, []) if "per kw" != row["Rate Determinant"] else [r/12 for r in rate_map.get(name, [])]
        val = val_list[idx] if idx < len(val_list) else None
        final_rates.append(val)
        group_counts[name] += 1

    df = df.with_columns(pl.Series("Rate", final_rates))
    
    if not os.path.exists(f"Electric_Tariffs/{utility}"):
        os.makedirs(f"Electric_Tariffs/{utility}", exist_ok=True)

    name = f"{tariff_name}_{territoryName}_{elecTariff}" if territoryName else f"{tariff_name}_{elecTariff}"
    df = df.unique().sort("rateName")
    df.write_csv(f"Electric_Tariffs/{utility}/{name}.csv")

    return df


def calculate_bill_electric(df, building):
    
    # Drop rows with null Rate
    df = df.filter(pl.col("Rate").is_not_null())

    # Prepare load data with month, hour, date and weekday columns
    building = building.with_columns([
        pl.col("timestamp").str.to_datetime().alias("timestamp")
    ])
    building = building.with_columns([
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.date().alias("date"),
        pl.col("timestamp").dt.weekday().alias("weekday")
    ])

    # Initialize charges
    total_charges = defaultdict(float)

    # Convert rate table to row-wise dict for iteration
    for row in df.iter_rows(named=True):
        name = row["rateName"]
        rate = row["Rate"]
        determinant = str(row["Rate Determinant"]).lower()
        season = row["Season"]
        tou = row["tou"]
        start = float(row["Start"]) if row["Start"] else 0.0
        end = float(row["End"]) if row["End"] else float("inf")

        # Get season months
        if isinstance(season, str) and "/" in season:
            mo1, day1 = map(int, season.split("-")[0].split("/"))
            mo2, day2 = map(int, season.split("-")[1].split("/"))
            season_months = list(range(mo1, mo2 + 1)) if mo1 <= mo2 else list(range(1, mo2 + 1)) + list(range(mo1, 13))
        else:
            season_months = list(range(1, 13))

        # Filter load for season
        building_filter = building.filter(pl.col("month").is_in(season_months))

        # Filter for TOU if applicable
        if tou:
            temp = []
            tou = eval(tou)
            for t_d, t_h in tou:
                start_day, end_day = t_d
                start_time, end_time = t_h
                if start_time < end_time:
                    temp.append(building_filter.filter((pl.col("hour") >= start_time) & (pl.col("hour") < end_time) &
                                                    (pl.col("weekday")>= start_day) & (pl.col("weekday") <= end_day)))
                else:
                    temp.append(building_filter.filter(((pl.col("hour") >= start_time) | (pl.col("hour") < end_time)) &
                                                    (pl.col("weekday")>= start_day) & (pl.col("weekday") <= end_day)))
            building_filter = pl.concat(temp)

        if "kwh" in determinant:
            # Compute monthly totals
            building_filter = (
                building_filter.group_by("month")
                .agg(pl.col("electricity.total").sum().alias("month_total"))
            )
            
            # Consumption limit handling
            building_filter = building_filter.with_columns([
                pl.when(pl.col("month_total") > start)
                .then(
                    pl.when(pl.col("month_total") > end)
                        .then(end - start)
                        .otherwise(pl.col("month_total") - start)
                )
                .otherwise(0)
                .alias("month_total")
            ])

            kwh = building_filter["month_total"].sum()
            charge = rate * kwh
            total_charges[name] += charge

        elif "kw" in determinant:
            # Since we did tou filtering, we need to ensure our rolling sum only captures contiguous timestamps
            if "60min" in determinant:
                building_filter = (
                    building_filter
                    .sort("timestamp")
                    .with_columns([
                        pl.col("electricity.total").rolling_sum(window_size=4).alias("kw_60min"),
                        pl.col("timestamp").diff(n=1).dt.total_seconds().alias("d1"),
                        pl.col("timestamp").diff(n=2).dt.total_seconds().alias("d2"),
                        pl.col("timestamp").diff(n=3).dt.total_seconds().alias("d3")
                    ])
                    .filter(
                        (pl.col("kw_60min").is_not_null()) &
                        (pl.col("d1") == 900) &     # 15min
                        (pl.col("d2") == 1800) &    # 30min
                        (pl.col("d3") == 2700)      # 45min
                    )
                    .group_by("month")
                    .agg(pl.col("kw_60min").max().alias("peak_demand"))
                )
            else:
                building_filter = (
                    building_filter
                    .sort("timestamp")
                    .with_columns([
                        pl.col("electricity.total").rolling_sum(window_size=2).alias("kw_30min"),
                        pl.col("timestamp").diff(n=1).dt.total_seconds().alias("d1")
                    ])
                    .filter(
                        (pl.col("kw_30min").is_not_null()) &
                        (pl.col("d1") == 900)         # 15min
                    )
                    .group_by("month")
                    .agg(pl.col("kw_30min").max().alias("peak_demand"))
                )
            
            kw = building_filter["peak_demand"].sum()
            charge = rate * kw
            total_charges[name] += charge

        elif "month" in determinant or "bill" in determinant:
            months = building_filter["month"].n_unique()
            total_charges[name] += rate * months

        elif "day" in determinant:
            days = building_filter["date"].n_unique()
            total_charges[name] += rate * days

        elif "year" in determinant:
            total_charges[name] += rate
    
        elif "percent" in determinant:
            total_charges[name] += building_filter["electricity.total"].sum() * rate

    return total_charges
