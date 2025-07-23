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

import warnings
warnings.filterwarnings("ignore")
from itertools import cycle
import importlib
import genability_cost
importlib.reload(genability_cost)
from genability_cost import genability_costs
import genability_cost_copy
importlib.reload(genability_cost_copy)
from genability_cost_copy import genability_costs_hack

elecTariff = ("2606",)
gasTariff = ("R-3-RESIDENTIAL HEATING---", "R-1-RESIDENTIAL NON-HEATING---")
state = ""
utility = "NSTAR Electric Company"
gas_utility = "Eversource Energy (NStar) Eastern"

# Open below for tariff and utility details
"""
elecTariff: Required. Use the masterTariffId from Genability. Give as tuple. If one give it as (x,).
gasTariff: Required. Leave blank if you don't know it, it will give you suggestions. Use exact RateActuity name when you know it.
           Provide as a tuple of heating and non-heating rate in that order. Some states have a separate non-heating rate for non-gas heating.
           If they don't just put the same rate twice.
state: Required. Use the state abbreviation (e.g., "CA", "TX").
utility: Required. Use any utility name based on EIA name, it will give you suggestions if not valid.
gas_utility: Required. If you leave this blank, or if your input is invalid, it will give options.
"""

upgrade = [3,4,11]

# Open below for upgrade options
"""
Give as list. If one give it as [x,]. If you do this it will calculate costs for upgraded
load profiles in addition to base profiles. Don't include baseline 0 in list.
  0: "Baseline",
  1: "ENERGY STAR heat pump with elec backup",
  2: "High efficiency cold-climate heat pump with elec backup",
  3: "Ultra high efficiency heat pump with elec backup",
  4: "ENERGY STAR heat pump with existing system as backup",
  5: "Geothermal heat pump",
  6: "ENERGY STAR heat pump with elec backup + Light Touch Envelope",
  7: "High efficiency cold-climate heat pump with elec backup + Light Touch Envelope",
  8: "Ultra high efficiency heat pump with elec backup + Light Touch Envelope",
  9: "ENERGY STAR heat pump with existing system as backup + Light Touch Envelope",
  10: "Geothermal heat pump + Light Touch Envelope",
  11: "ENERGY STAR heat pump with elec backup + Light Touch Envelope + Full Appliance Electrification with Efficiency",
  12: "High efficiency cold-climate heat pump with elec backup + Light Touch Envelope + Full Appliance Electrification with Efficiency",
  13: "Ultra high efficiency heat pump with elec backup + Light Touch Envelope + Full Appliance Electrification with Efficiency",
  14: "ENERGY STAR heat pump with existing system as backup + Light Touch Envelope + Full Appliance Electrification with Efficiency",
  15: "Geothermal heat pump + Light Touch Envelope + Full Appliance Electrification with Efficiency",
  16: "Envelope Only - Light Touch Envelope"
"""

# Any combination of these is fine, no need to fill them all
segment = {
    "heating_type":         "Natural Gas",
    "building_type":        "SF",
    "area":                 "0-1499",
    "income":               "High Income",
    "climate_zone":         "Cold",
    "heating_efficiency":   "",
    "cooling_type":         "",
    "vintage":              "<1960",
    "insulation_level":     "Poor Insulation",
    "has_solar":            ""
}

# Open below for segment options
"""
heating_type: Electric HP | Electric Resistance | Natural Gas | Propane | Other
building_type: SF | Small MF | Large MF | Mobile
area: 0-1499 | 1500-2499 | 2500-3999 | 4000+
income: Low Income (<40,000) | Moderate Income (40,000-99,999) | High Income (>100,000)
climate_zone: Cold | Hot-Dry | Hot-Humid | Marine | Mixed-Dry | Mixed-Humid | Very Cold
heating_efficiency: Low Htg Eff | Medium Htg Eff | High Htg Eff | None/Shared Heating
cooling_type: Heat Pump | High Eff AC | Low Eff AC | Room AC | None
vintage: <1960 | 1960-2000 | >2000
insulation_level: Good Insulation | Average Insulation | Poor Insulation
has_solar: Yes | No

# Defaults to ("Natural Gas","0-1499","Low Income","1960-2000","Low Htg Eff","Good Insulation")
# for PG&E using default tariffs for a zip code in Basleine Territory S if all are empty
"""

gen_electric_bills, _ = genability_costs(elecTariff,gasTariff,state,utility,gas_utility,segment,upgrade)
hack_electric_bills, annual_gas_bills = genability_costs_hack(elecTariff,gasTariff,state,utility,gas_utility,segment,upgrade)

length = len(hack_electric_bills[0]["distribution"])
print(f"""
{length} building(s), {min(length,10)} of which were sampled for genability API cost calculation

Annual Electric Bills:""")

for gen_bill, hack_bill in zip(gen_electric_bills,hack_electric_bills):
    print(f"  {gen_bill['Name']} with {'upgrade measure '+str(gen_bill['Upgrade']) if gen_bill['Upgrade'] else 'baseline profile'}: ${gen_bill['average']:,.2f} ± {gen_bill['moe']:,.2f}, Hack result is ${hack_bill['average']:,.2f} ± {hack_bill['moe']:,.2f}")

print("Annual Gas Bills:")
for bill in annual_gas_bills:
    print(f"  {bill['Name']} with {'upgrade measure '+str(bill['Upgrade']) if bill['Upgrade'] else 'baseline profile'}: ${bill['average']:,.2f} ± {bill['moe']:,.2f}")

print("Total Bills:")
for electric_bill, gas_bill in zip(gen_electric_bills,cycle(annual_gas_bills)):
    print(f"  {'Upgrade measure '+str(electric_bill['Upgrade']) if electric_bill['Upgrade'] else 'Baseline Profile'}: ${electric_bill['average']+gas_bill['average']:,.2f} ± {electric_bill['moe']+gas_bill['moe']:,.2f}")