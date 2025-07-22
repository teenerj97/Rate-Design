import os
os.chdir("c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/resstock2")
print(os.getcwd())  
import warnings
warnings.filterwarnings("ignore")
import importlib
#import genability_cost
#importlib.reload(genability_cost)
#from genability_cost import genability_costs
import requests
import os
import polars as pl
import pandas as pd
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
from RateAcuity import download_tariff, calculate_bill, get_tariff_RA
import segments
importlib.reload(segments)
from segments import segment
from genability_cost import electric_bill, gas_bill
import GenabilityHack as GenabilityHack
importlib.reload(GenabilityHack)
from GenabilityHack import get_tariff_gen, calculate_bill_electric 
import polars as pl



#below, I was printing the building loads to test how the function was working. commented it out for now because it was printing a lot of data.
#print(building_loads)
#print(type(building_loads))

#below, the tariff and utility information is set up.
#the tariff is the EIA code for the electric tariff to pull from Geneability, and the gasTariff is the name of the gas tariff from RateAcuity.

elecTariff = 698
gasTariff = "-RESIDENTIAL SALES---"
state = "MN"
utility = "Northern States Power Co - Minnesota"                                                                                                                                                                    
gas_utility = "CenterPoint Energy"

#not testing upgrades for now, but we can use this to test them. another next step is to alter this to meet our methodology for upgrades. 
upgrade = ()

# Any combination of these is fine, no need to fill them all. The Segment function will filter the data based on the provided parameters.
segment_MN = {
    "heating_type":         "",
    "building_type":        "SF",
    "area":                 "",
    "income":               "",
    "climate_zone":         "",
    "heating_efficiency":   "",
    "cooling_type":         "",
    "vintage":              "",
    "insulation_level":     "",
    "has_solar":            ""
}

MN_segment = segment(state, utility, segment_MN)

building_ids = MN_segment["bldg_id"]
zip_codes = MN_segment["in.zip_code"]
elec_weights = [float(w) for w in MN_segment["elec_weight"]]
gas_weights = [float(w) for w in MN_segment["gas_weight"]]
gas_utilities = MN_segment["in.gas_utility_name"]
#comment out the below if you don't want to see - it's just another check, but also a lot of data.

#example of extracting load profiles, starting w/ just one building id from ResStock. Next step is to run bill calculator on all bill profiles.
building_loads = get_load_profiles("MN", [37]) 


#calculate the electric bill and print. First line is to download the tariff (line 80) which i've already done, so it's commented out. The next line imports the tariff for the calculation.
elec_tariff, name = get_tariff_gen(elecTariff, zip_codes[0], building_loads[0])
elec_df = pl.read_csv(
            f"c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/bill calculator/elec_tariffs/{zip_codes[0]}_{elecTariff}.csv"
            )   

bill = calculate_bill_electric(elec_df, building_loads[0])
print(bill.total)


#gasTariff = download_tariff(state, gas_utility, gasTariff)
gas_tariff = get_tariff_RA(state, gas_utility, gasTariff)
#calculate gas bills and print
bill = calculate_bill(gas_tariff, building_loads)
print(type(bill))
print(bill.total)
#bill_df = pd.DataFrame([bill.__dict__])
#bill_df.to_csv("gas_bill.csv", index=False)
#print(gas_tariff)
#tariff, name = get_tariff_gen("698", "MN", "Northern States Power Co - Minnesota")



#FLOW:
#SEGMENT GIVES US ZIP CODES AND BUILDING IDS WE NEED
#