import requests
import os
import polars as pl
import importlib
import random
import scipy.stats as st
from collections import defaultdict
from difflib import get_close_matches
import get_load_profiles
importlib.reload(get_load_profiles)
from get_load_profiles import get_load_profiles
import RateAcuity
importlib.reload(RateAcuity)
from RateAcuity import calculate_bill, get_tariff_RA
import segments
importlib.reload(segments)
from segments import segment

app_id = "3df8e135-968d-4399-9879-2a1c6a3de30c"
app_key = "e51974c7-996b-4698-9628-71950d223364"

def electric_bill(elecTariff, state, zip_codes, buildings, weights, N, upgrade=0):

    # API call to Genability
    bills = []
    annual_bill = 0
    for zip_code, building in zip(zip_codes, buildings):
        # Use cached bills if available
        if os.path.exists(f"Cost_Detail/{state}/{building['bldg_id'][0]}/electric-{upgrade}-{elecTariff}.csv"):
            bill = pl.read_csv(f"Cost_Detail/{state}/{building['bldg_id'][0]}/electric-{upgrade}-{elecTariff}.csv")
            name = bill["name"][0]
            bill = bill["cost"].sum()
            bills.append(bill)
        # If not cached, make API call
        else:
            # Get territory ID for the zip code and given tariff
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
            name = data["tariffName"]
            costs_breakdown = pl.from_dicts(response.json()["results"][0]["items"]).select(["rateName","rateAmount","itemQuantity","cost"])
            costs_breakdown = costs_breakdown.with_columns(pl.lit(name).alias("name"))
            if not os.path.exists(f"Cost_Detail/{state}/{building['bldg_id'][0]}"):
                os.makedirs(f"Cost_Detail/{state}/{building['bldg_id'][0]}")
            costs_breakdown.write_csv(f"Cost_Detail/{state}/{building['bldg_id'][0]}/electric-{upgrade}-{elecTariff}.csv")
            bill = float(data['totalCost'])
            bills.append(bill)

    annual_bill = sum(bill*weight for bill,weight in zip(bills,weights))/sum(weights)

    # Calculate the margin of error using finite population correction and std dev
    n = len(bills)
    den = sum(weights) - sum(wi*wi for wi in weights)/sum(weights)
    wv = sum(wi*(ci - annual_bill)**2 for ci,wi in zip(bills, weights)) / max(den,1)
    n_eff = sum(weights)**2 / sum(wi*wi for wi in weights)
    se = (wv / n_eff)**0.5
    if N>10:
        se *= ((N - n) / (N - 1))**0.5
    moe = st.norm.ppf(0.95) * se

    if upgrade:
        return (f"{name} tariff with upgrade measure {upgrade}", f"${annual_bill:.2f} ± {moe:.2f}")
    else:
        return (f"{name} tariff with original profile", f"${annual_bill:.2f} ± {moe:.2f}")

def gas_bill(gasTariff, state, gas_utility, buildings, weights, N, upgrade=0):
    gas_utility = gas_utility.replace("&", "and")
    if all(b["natural_gas.total"].sum()==0 for b in buildings):
        annual_bill = 0
        moe = 0
    else:
        annual_bill = 0
        bills = []
        for building in buildings:
            # Use cached bills if available
            if os.path.exists(f"Cost_Detail/{state}/{building['bldg_id'][0]}/gas-{upgrade}-{gasTariff}.csv"):
                bill = pl.read_csv(f"Cost_Detail/{state}/{building['bldg_id'][0]}/gas-{upgrade}-{gasTariff}.csv")
                bill = bill["cost"].sum()
                bills.append(bill)
            # If not cached, calculate bill
            else:
                tariff = get_tariff_RA(state, gas_utility, gasTariff)
                bill = calculate_bill(tariff, building)
                costs_breakdown = pl.DataFrame({"rateName":bill.as_series.index, "cost":bill.as_series.values})
                if not os.path.exists(f"Cost_Detail/{state}/{building['bldg_id'][0]}"):
                    os.makedirs(f"Cost_Detail/{state}/{building['bldg_id'][0]}")
                costs_breakdown.write_csv(f"Cost_Detail/{state}/{building['bldg_id'][0]}/gas-{upgrade}-{gasTariff}.csv")
                bills.append(bill.total)
        
        annual_bill = sum(bill*weight for bill,weight in zip(bills,weights))/sum(weights)

        # Calculate the margin of error using finite population correction and std dev
        n = len(bills)
        den = sum(weights) - sum(wi*wi for wi in weights)/sum(weights)
        wv = sum(wi*(ci - annual_bill)**2 for ci,wi in zip(bills, weights)) / max(den,1)
        n_eff = sum(weights)**2 / sum(wi*wi for wi in weights)
        se = (wv / n_eff)**0.5
        if N>10:
            se *= ((N - n) / (N - 1))**0.5
        moe = st.norm.ppf(0.95) * se

    if upgrade:
        return (f"{gasTariff} tariff with upgrade measure {upgrade}", f"${annual_bill:.2f} ± {moe:.2f}")
    else:
        return (f"{gasTariff} tariff with original profile", f"${annual_bill:.2f} ± {moe:.2f}")

def genability_costs(elecTariffs, gasTariff, state, utility, gas_utility="",kwargs={}, upgrades=0):
    """
    Estimate the average annual electricity and gas bill for a specific building segment using Genability's Calculate API.

    This function retrieves hourly electricity and gas consumption profiles for a max of 10 buildings,
    submits them to Genability's API and RateAcuity, and returns the weighted average annual cost based on the
    selected tariffs.
    
    If inputs are empty, a default profile for a single California building in Basleine Territory S is used.
    ("Natural Gas","0-1499","Low Income","1960-2000","Low Htg Eff","Good Insulation")

    Args:
        elecTariff (str or int): The Genability tariff ID to use for cost calculation.
        gasTariff (str or int): The RateAcuity tariff name to use for cost calculation.
        state (str): U.S. state abbreviation (e.g., "CA"). Required if using segment filters.
        utility (str): Utility name or code, use name from EIA. Required if using segment filters.
        segments: A dictionary of building segment characteristics, any combination of the following as strings:
            - heating_type: Electric HP | Electric Resistance | Natural Gas | Propane | Other
            - building_type: SF | Small MF | Large MF | Mobile
            - area: 0-1499 | 1500-2499 | 2500-3999 | 4000+
            - income: Low Income (<40,000) | Moderate Income (40,000-99,999) | High Income (>100,000)
            - vintage: <1960 | 1960-2000 | >2000
            - climate_zone: Cold | Hot-Dry | Hot-Humid | Marine | Mixed-Dry | Mixed-Humid | Very Cold
            - heating_efficiency: Low Htg Eff | Medium Htg Eff | High Htg Eff | None/Shared Heating
            - cooling_type: Heat Pump | High Eff AC | Low Eff AC | Room AC | None
            - insulation_level: Good Insulation | Average Insulation | Poor Insulation
            - has_solar: Yes | No
        upgrades (int, optional): If >0, calculate costs for upgradesd load profiles in addition to base profiles.

    Returns:
        tuple: A 4-tuple containing:
            - annual_base_electric_bill (float): Weighted average annual electricity cost (USD).
            - annual_upgrades_electric_bill (float): Weighted annual electricity cost with upgrades applied (USD).
            - annual_base_gas_bill (float): Weighted average annual gas cost (USD).
            - annual_upgrades_gas_bill (float): Weighted annual gas cost with upgrades applied (USD).

    Note:
        - The function assumes Genability API credentials (`app_id` and `app_key`) are available in scope.
        - Time-of-use energy data is assumed to be hourly granularity.
        - Input building data is either retrieved from a filtered segment or uses a built-in default.
        - If `upgrades` is specified (>0), the function will also fetch and calculate costs for the upgradesd
            load profiles, returning both base and upgradesd bills.
    """
    if any(v for k,v in kwargs.items()) and state and utility:
        filtering = None
        for col_name, value in kwargs.items():
            if value:
                condition = pl.col(col_name) == value
                filtering = condition if filtering is None else (filtering & condition)
        try:
            chosen_segment = segment(state, utility, kwargs).filter(filtering)
        except Exception as e:
            print(f"Segment entries or utility name led to an error, check them based on allowed options\n{e}")
            return
        
        if chosen_segment.is_empty():
            print("Segment is too narrow for this utility and has no samples in ResStock.\nCheck sgements_by_utility folder for avilable segments for this utility.")
            return

        building_ids = chosen_segment["bldg_ids"].item().split("|")
        zip_codes = chosen_segment["zip_codes"].item().split("|")
        elec_weights = [float(w) for w in chosen_segment["elec_weights"].item().split("|")]
        gas_weights = [float(w) for w in chosen_segment["gas_weights"].item().split("|")]
        gas_utilities = chosen_segment["gas_utility"].item().split("|")
        full_segment_size = len(building_ids) # Used for Finite Population MOE Correction
        print("Input gasTariff:", gasTariff)
        print("Input state:", state)
        print("Input utility:", utility)
        print("Input segment:", segment)  

    else:
        elecTariffs = ("3289575",)
        gasTariff = "G-1-RESIDENTIAL SERVICE-Baseline Territory S--"
        utility = "Pacific Gas & Electric Co."
        state = "CA"
        gas_utilities = ["Pacific Gas and Electric Company"]
        building_ids = ["128427"]
        zip_codes = ["95206"]
        elec_weights = [252.3016387]
        gas_weights = [252.3016387]
        full_segment_size = 1

    if not gas_utility:
        # Breakout into groups of buildings that share the same gas utility and choose one
        grouped = defaultdict(list)
        for bldg, zipc, elec_w, gas_w, utility in zip(building_ids, zip_codes, elec_weights, gas_weights, gas_utilities):
            grouped[utility].append((bldg, zipc, elec_w, gas_w, utility))

        matches = get_close_matches(utility, grouped.keys(), n=1, cutoff=0.6)

        if matches:
            chosen_utility_group = grouped[matches[0]]
        else:
            chosen_utility_group = max(grouped.values(), key=len)
            
        sampled = random.sample(chosen_utility_group, k=min(10, len(chosen_utility_group)))

        building_ids, zip_codes, elec_weights, gas_weights, gas_utilities = map(list, zip(*sampled))
        gas_utility = gas_utilities[0]

    # Get load profiles
    buildings = get_load_profiles(state, building_ids)
    buildings_upgrades = []
    if upgrades:
        for u in upgrades:
            buildings_upgrades.append((u, get_load_profiles(state, building_ids, u)))

    # Calculate bills
    annual_electric_bill = {}
    annual_gas_bill = {}
    
    annual_gas_bill.update([gas_bill(gasTariff, state, gas_utility, buildings, gas_weights, full_segment_size)])
    if upgrades:
        for upgrade,buildings_upgrade in buildings_upgrades:
            annual_gas_bill.update([gas_bill(gasTariff, state, gas_utility, buildings_upgrade, gas_weights, full_segment_size, upgrade)])

    for elecTariff in elecTariffs:
        annual_electric_bill.update([electric_bill(elecTariff, state, zip_codes, buildings, elec_weights, full_segment_size)])
        if upgrades:
            for upgrade,buildings_upgrade in buildings_upgrades:
                annual_electric_bill.update([electric_bill(elecTariff, state, zip_codes, buildings_upgrade, elec_weights, full_segment_size, upgrade)])
    
    return annual_electric_bill, annual_gas_bill