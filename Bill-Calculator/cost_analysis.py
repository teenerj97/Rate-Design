
import os
os.chdir("c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/resstock2")
print(os.getcwd())  
import warnings
warnings.filterwarnings("ignore")
import importlib
import genability_cost
importlib.reload(genability_cost)
from genability_cost import genability_costs

elecTariff = ("698",)
gasTariff = "-RESIDENTIAL SALES---"
state = "MN"
utility = "Northern States Power Co - Minnesota"
gas_utility = ""

# Open below for tariff and utility details
"""
elecTariff: Required. Use the masterTariffId from Genability. Give as tuple. If one give it as (x,).
gasTariff: Required. Leave blank if you don't know it, it will give you suggestions. Use exact RateActuity name when you know it.
state: Required. Use the state abbreviation (e.g., "CA", "TX").
utility: Required. Use any utility name based on EIA name, it will give you suggestions if not valid.
gas_utility: If you leave this blank (preferred), it will choose the utility that has the largest number of buildings in the ResStock
segment. Will also give suggestions if not valid.
"""

upgrade = (3,4,11)

# Open below for upgrade options
"""
Give as tuple. If one give it as (x,). If you do this it will calculate costs for upgraded
load profiles in addition to base profiles.
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
    "heating_type":         "Electric HP",
    "building_type":        "",
    "area":                 "",
    "income":               "",
    "climate_zone":         "",
    "heating_efficiency":   "",
    "cooling_type":         "",
    "vintage":              "",
    "insulation_level":     "",
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

annual_electric_bills, annual_gas_bills = genability_costs(elecTariff,gasTariff,state,utility,gas_utility,segment,upgrade)

print("Annual Electric Bills:")
for label, bill in annual_electric_bills.items():
    print(f"  {label}: {bill}")

print("Annual Gas Bills:")
for label, bill in annual_gas_bills.items():
    print(f"  {label}: {bill}")
