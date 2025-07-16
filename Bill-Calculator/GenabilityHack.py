import requests
import polars as pl
from collections import defaultdict
import pandas as pd
from ts_tariffs.utils import Block
from ts_tariffs.billing import Bill
from ts_tariffs.meters import MeterData
from ts_tariffs.ts_utils import SampleRate, DateWindow
from ts_tariffs.tariffs import tariffs_map

def get_tariff_gen(elecTariff, zip_code, building):

    app_id = "3df8e135-968d-4399-9879-2a1c6a3de30c"
    app_key = "e51974c7-996b-4698-9628-71950d223364"

    url = "https://api.genability.com/rest/public/territories"
    params = {
        "masterTariffId": elecTariff,
        "zipCode": zip_code
    }

    response = requests.get(url, auth=(app_id, app_key), params=params).json()
    territoryId = response["results"][0]["territoryId"] if len(response["results"])>0 else 0

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
            "duration": 3600000, # 1 hour
            "dataSeries": building["electricity.total"].to_list()
        }]
    }
    if territoryId:
        params["propertyInputs"].append({"keyName": "territoryId", "dataValue": territoryId})

    response = requests.post(url, auth=(app_id, app_key), json=params)
    data = response.json()["results"][0]
    name = data.get("tariffName")
    if name is None:
        raise ValueError(f"'tariffName' not found in API response: {data}")
    name = data["tariffName"]
    costs_breakdown = pl.from_dicts(response.json()["results"][0]["items"])
    cols = ["rateName", "rateAmount", "itemQuantity","cost","period"] if "period" in costs_breakdown.columns else ["rateName", "itemQuantity", "rateAmount","cost"]
    costs_breakdown = costs_breakdown.select(cols).sort("rateName")

    url = "https://api.genability.com/rest/public/tariffs"

    params = {
        "masterTariffId": elecTariff,
        "zipCode": zip_code,
        "effectiveOn": "2025-05-01",
        "fromDateTime": building["timestamp"].first(),
        "toDateTime": building["timestamp"].last(),
        "populateRates": True
    }

    response = requests.get(url, auth=(app_id, app_key), params=params)
    rates = [r for r in response.json()["results"][0]["rates"] if r["rateName"] in costs_breakdown["rateName"] and \
                any(c!=0 for c in costs_breakdown.filter(pl.col("rateName")==r["rateName"])["rateAmount"]) and \
                r.get("territory",{"territoryId":territoryId})["territoryId"]==territoryId]
    
    # Update the logic with rate determinant handling
    rows = []
    for rate in rates:
        rate_name = rate["rateName"]
        eff_date = rate["fromDateTime"].split("T")[0]
        
        # Rate Determinant logic
        if rate.get("chargeType") == "FIXED_PRICE" and all(c>=0 for c in costs_breakdown.filter(pl.col("rateName")==rate["rateName"])["rateAmount"]):
            rate_determinant = "per month"
        elif rate.get("chargeType") == "FIXED_PRICE" and any(c>=0 for c in costs_breakdown.filter(pl.col("rateName")==rate["rateName"])["rateAmount"]):
            rate_determinant = "per year"
        else:
            rate_determinant = "per kwh"
        
        season = ""
        if "season" in rate or "season" in rate.get("timeOfUse",[]):
            s = rate.get("season",rate.get("timeOfUse",{}).get("season"))
            season = f'{s["seasonFromMonth"]:02d}/{s["seasonFromDay"]:02d}-{s["seasonToMonth"]:02d}/{s["seasonToDay"]:02d}'
        
        tou_type = ""
        tou = ""
        if "timeOfUse" in rate:
            p = rate["timeOfUse"]["touPeriods"][0]
            start_hour, end_hour = p["fromHour"], p["toHour"]

            # fix start if it's 0 by scanning matching rategroup
            if start_hour == 0:
                rg = rate.get("rateGroupName")
                season_str = ""
                if "season" in rate:
                    s = rate["season"]
                    season_str = f'{s["seasonFromMonth"]:02d}/{s["seasonFromDay"]:02d}-{s["seasonToMonth"]:02d}/{s["seasonToDay"]:02d}'

                # get matching toHours in same rategroup + season
                matches = [
                    r for r in rates
                    if r.get("rateGroupName") == rg and
                    "timeOfUse" in r and
                    "season" in r and
                    f'{r["season"]["seasonFromMonth"]:02d}/{r["season"]["seasonFromDay"]:02d}-{r["season"]["seasonToMonth"]:02d}/{r["season"]["seasonToDay"]:02d}' == season_str and
                    r["timeOfUse"]["touPeriods"][0]["fromHour"] != 0
                ]

                if matches:
                    max_to = max(m["timeOfUse"]["touPeriods"][0]["toHour"] for m in matches)
                    start_hour = max_to  # fix the 0
            tou = str([start_hour, end_hour, 24])
            tou_type = rate["timeOfUse"].get("touType", "OFF_PEAK")
        
        bands = rate.get("rateBands", [])
        has_cons_limits = any(b.get("hasConsumptionLimit") for b in bands)
        if not has_cons_limits or len(bands) == 1:
            rows.append([name, rate_name, eff_date, "", rate_determinant, "", "", season, tou, tou_type])
        else:
            limits = [b.get("consumptionUpperLimit") for b in bands]
            prev_limit = None
            for i, limit in enumerate(limits):
                if limit == prev_limit:
                    continue  # skip dupes
                start = "" if i == 0 else prev_limit
                end = limit if limit is not None else ""
                rows.append([name, rate_name, eff_date, "", rate_determinant, start, end, season, tou, tou_type])
                prev_limit = limit

    df = pl.DataFrame(rows, schema=[
        "tariff","rateName", "EffDate", "Rate", "Rate Determinant",
        "Start", "End", "Season", "tou", "period"
    ])

    # group df by rateName
    df_grouped = df.group_by("rateName",maintain_order=True).agg(pl.len().alias("df_count"))
    cb_grouped = costs_breakdown.group_by("rateName",maintain_order=True).agg([
        pl.len().alias("cb_count"),
        (pl.col("rateAmount") * pl.col("itemQuantity")).sum().alias("weighted_sum"),
        pl.col("itemQuantity").sum().alias("total_qty"),
        pl.col("rateAmount").alias("rate_list")  # keep for ordered match
    ])

    # join stats
    summary = df_grouped.join(cb_grouped, on="rateName", how="left")

    # result mapping: rateName -> list of final rates
    rate_map = {}
    for row in summary.iter_rows(named=True):
        name = row["rateName"]
        dcount = row["df_count"]
        ccount = row["cb_count"]

        if ccount == 1 and dcount > 1:
            # broadcast single rate
            val = row["rate_list"][0]
            rate_map[name] = [val] * dcount

        elif dcount == 1:
            # assign weighted avg
            avg = row["weighted_sum"] / row["total_qty"] if row["total_qty"] else 0
            rate_map[name] = [avg]

        elif ccount == dcount:
            # respect order
            rate_map[name] = row["rate_list"]
        
        else:
            # mismatch, fallback: NaNs
            rate_map[name] = [None] * dcount

    final_rates = []
    group_counts = {}

    for name in df["rateName"]:
        if name not in group_counts:
            group_counts[name] = 0
        idx = group_counts[name]
        val_list = rate_map.get(name, [])
        val = val_list[idx] if idx < len(val_list) else None
        final_rates.append(val)
        group_counts[name] += 1


    return df.with_columns(pl.Series("Rate", final_rates)).sort("rateName"), name


def calculate_bill_electric(df, building):
    # Accumulators for tiers
    global_blocks   = defaultdict(lambda: {"blocks": [], "rates": [], "labels": []})
    seasonal_blocks = defaultdict(lambda: {"blocks": [], "rates": [], "labels": []})

    charges = []

    for row in df.iter_rows(named=True):
        if not row["Rate"]:
            continue
        if row.get("Location"):
            continue

        comp       = row["rateName"]
        tou_raw    = row.get("tou")
        rate       = float(row["Rate"])
        if tou_raw:
            time_bins  = eval(tou_raw)
            if time_bins[0] > time_bins[1]:
                rate = [rate, 0.0, rate]
                time_bins = [time_bins[1], time_bins[0], time_bins[2]]
            else:
                rate = [0.0, rate, 0.0]
        det        = row.get("Rate Determinant") or ""
        unit       = det.split("per ")[1].lower() if "per " in det else "therm"
        start      = row.get("Start")
        end        = row.get("End")
        season_txt = row.get("Season")

        # 1) seasonal block rows
        if season_txt and (start or end):
            key = (comp, unit, season_txt)
            s = float(start) if start else 0.0
            e = float(end)   if end   else float("inf")
            sb = seasonal_blocks[key]
            sb["blocks"].append(Block(min=s, max=e))
            sb["rates"].append(rate)
            sb["labels"].append("")
            continue

        # 2) seasonal single-rates (no block thresholds)
        if season_txt and not (start or end):
            sd, ed = (s.strip() for s in season_txt.split("-"))
            ms, ds = map(int, sd.split("/"))
            me, de = map(int, ed.split("/"))

            windows = []
            if (me, de) < (ms, ds):
                windows = [
                    (f"2025-{ms:02d}-{ds:02d}", "2025-12-31"),
                    ("2025-01-01", f"2025-{me:02d}-{de:02d}")
                ]
            else:
                windows = [(f"2025-{ms:02d}-{ds:02d}", f"2025-{me:02d}-{de:02d}")]

            for ws, we in windows:
                if tou_raw:
                    charges.append({
                        "name": f"{comp}_{unit}_{ws[-5:]}_to_{we[-5:]}_tou",
                        "charge_type": "TouTariff",
                        "tou":{
                            "bin_rates": rate,
                            "bin_labels": ["", "", ""],
                            "time_bins": time_bins,
                        },
                        "consumption_unit": unit,
                        "rate_unit": f"dollars / {unit}",
                        "sample_rate": None,
                        "adjustment_factor": 1.0,
                        "__season_window__": (ws, we)
                    })
                else:
                    charges.append({
                        "name": f"{comp}_{unit}_{ws[-5:]}_to_{we[-5:]}_single",
                        "charge_type": "SingleRateTariff",
                        "rate": rate,
                        "consumption_unit": unit,
                        "rate_unit": f"dollars / {unit}",
                        "sample_rate": None,
                        "adjustment_factor": 1.0,
                        "__season_window__": (ws, we)
                    })
            continue

        # 3) non‐seasonal block rows
        if start or end:
            key = (comp, unit)
            s = float(start) if start else 0.0
            e = float(end)   if end   else float("inf")
            gb = global_blocks[key]
            gb["blocks"].append(Block(min=s, max=e))
            gb["rates"].append(rate)
            gb["labels"].append("")
            continue

        # 4) connection charges
        if unit in {"day", "month", "year", "bill"}:
            if unit =="bill": 
                unit = "month"
            charges.append({
                "name": f"{comp}_{unit}_connection",
                "charge_type": "ConnectionTariff",
                "rate": rate,
                "consumption_unit": unit,
                "rate_unit": f"dollars / {unit}",
                "frequency_applied": unit,
                "sample_rate": None,
                "adjustment_factor": 1.0,
            })
            continue

        # 5) flat TOU (non-seasonal, non-block)
        if "tou" in comp.lower():
            charges.append({
                "name": f"{comp}_{unit}_tou_single",
                "charge_type": "TouTariff",
                "tou": {
                    "bin_rates": rate,
                    "bin_labels": ["", "", ""],
                    "time_bins": time_bins
                },
                "consumption_unit": unit,
                "rate_unit": f"dollars / {unit}",
                "sample_rate": None,
                "adjustment_factor": 1.0,
            })
            continue

        # 6) single‐rate
        charges.append({
            "name": f"{comp}_{unit}_single",
            "charge_type": "SingleRateTariff",
            "rate": rate,
            "consumption_unit": unit,
            "rate_unit": f"dollars / {unit}",
            "sample_rate": None,
            "adjustment_factor": 1.0,
        })
        continue

    # Emit seasonal BlockTariffs
    for (comp, unit, season_txt), sb in seasonal_blocks.items():
        start_dd, end_dd = (s.strip() for s in season_txt.split("-"))
        ms, ds = map(int, start_dd.split("/"))
        me, de = map(int, end_dd.split("/"))

        windows = []
        if (me, de) < (ms, ds):
            windows.append((f"2025-{ms:02d}-{ds:02d}", "2025-12-31"))
            windows.append(("2025-01-01", f"2025-{me:02d}-{de:02d}"))
        else:
            windows.append((f"2025-{ms:02d}-{ds:02d}", f"2025-{me:02d}-{de:02d}"))

        for ws, we in windows:
            # BlockTariff
            charges.append({
                "name": f"{comp}_{unit}_{ws[-5:]}_to_{we[-5:]}_block",
                "charge_type": "BlockTariff",
                "frequency_applied": "month",
                "blocks": sb["blocks"],
                "bin_rates": sb["rates"],
                "bin_labels": sb["labels"],
                "consumption_unit": unit,
                "rate_unit": f"dollars / {unit}",
                "sample_rate": None,
                "adjustment_factor": 1.0,
                "__season_window__": (ws, we)
            })

    # Emit global BlockTariffs
    for (comp, unit), gb in global_blocks.items():
        charges.append({
            "name": f"{comp}_{unit}_block",
            "charge_type": "BlockTariff",
            "frequency_applied": "month",
            "blocks": gb["blocks"],
            "bin_rates": gb["rates"],
            "bin_labels": gb["labels"],
            "consumption_unit": unit,
            "rate_unit": f"dollars / {unit}",
            "sample_rate": None,
            "adjustment_factor": 1.0,
        })

    tariff=charges
    #Rosie: changes made here, error handling from RateAcuity line 297 (6/30/2025)
    # If ts is a list, extract the first element if that's the DataFrame/dict you want
    if isinstance(building, list):
        if len(building) == 1:
            building = building[0]
        else:
            # If multiple, try to concatenate or handle as needed
            try:
                building = pd.concat(building)
            except Exception:
                building = pd.DataFrame(building)
    #Rosie: changes ended here
    ts = pd.Series(building['electricity.total'].to_numpy(), index=pd.to_datetime(building['timestamp'].to_list()))

    # Define the sample rate (hourly)
    sample_rate = SampleRate(multiplier=1, base_freq="hours")

    # Create the MeterData object
    meter = MeterData(
        name="natural_gas",
        tseries=ts,
        sample_rate=sample_rate,
        units="therm"
    )

    # tariff generated from get_tariff_RA
    regime_dict = {
        'name': "residential_tariff_regime",
        'tariffs': tariff
    }

    applied_charges = []
    for t in regime_dict['tariffs']:
        season = t.pop("__season_window__", None)
        tariff_obj = tariffs_map[t['charge_type']].from_dict(t)
        if season:
            window = DateWindow(start=season[0], end=season[1])
            sliced = meter.window_slice(window)
            sliced = MeterData(
                name=meter.name,
                tseries=sliced,
                sample_rate=meter.sample_rate,
                units=meter.units
            )
            applied_charges.append(tariff_obj.apply(sliced))
        else:
            applied_charges.append(tariff_obj.apply(meter))

    return Bill(name="my_combined_bill", charges=applied_charges)
