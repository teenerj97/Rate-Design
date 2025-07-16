import os
os.chdir(r"C:\Users\jack.teener\OneDrive - RMI\Desktop\Rate Design\bill calculator 2")
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


#example of extracting load profiles, starting w/ just one building id from ResStock. Next step is to run bill calculator on all bill profiles.
building_loads = get_load_profiles("MN", [37], upgrade = 1) 

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


#calculate the electric bill and print 
#elec_tariff, name = get_tariff_gen(elecTariff, zip_codes[0], building_loads[0])
#bill = calculate_bill_electric(elec_tariff, building_loads[0])
## print(bill.total)


# Iterate through building IDs and calculate gas and electric bills
building_data = pd.read_csv("segments_by_utility/Northern States Power Co - Minnesota.csv")
building_ids = building_data["bldg_id"].tolist()
#print(building_ids)

# Calculate gas bills for all buildings in the segment
# Get the gas tariff on RateAcuity
gas_tariff = get_tariff_RA(state, gas_utility, gasTariff)

# Initialize a list to collect bill data
all_gas_bills = []
import traceback
# # Iterate through building IDs and calculate gas bills
for bldg_id in building_ids:
    try:
        building_load = get_load_profiles(state, [bldg_id])
        
        if not building_load or len(building_load) == 0:
            print(f"Warning: No load profile returned for building ID {bldg_id}")
            continue
        
        bill = calculate_bill(gas_tariff, building_load)
        
        # Only keep the total bill
        all_gas_bills.append({
            "bldg_id": bldg_id,
            "total_gas_bill": bill.total
        })
    
    except Exception as e:
        print(f"Error processing building ID {bldg_id}: {e}")
        traceback.print_exc()

# # Save to CSV
bills_df = pd.DataFrame(all_gas_bills)
bills_df.to_csv("gas_bills_total_only.csv", index=False)
print("Total gas bills saved to 'gas_bills_total_only.csv'.")


# Get the elec tariff (from Genability, but stored locally so we don't have to do API call)
elec_df = pl.read_csv(f'c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/bill calculator 2/elec_tariffs/{zip_codes[0]}_{elecTariff}.csv')


# Calculate electric bills for all buildings in the segment
all_electric_bills = []
#building_load = get_load_profiles(state, [37])
#bill = calculate_bill_electric(elec_df, building_load) 


# Iterate through building IDs and calculate elec bills
for bldg_id in building_ids:
    try:
        building_load = get_load_profiles(state, [bldg_id])
        
        if not building_load or len(building_load) == 0:
            print(f"Warning: No load profile returned for building ID {bldg_id}")
            continue
        
        bill = calculate_bill_electric(elec_df, building_load)  # need to change to slices or indices instead of strings
        
        # Only keep the total bill
        all_electric_bills.append({
            "bldg_id": bldg_id,
            "total_elec_bill": bill.total
        })
    
    except Exception as e:
        print(f"Error processing building ID {bldg_id}: {e}")
        traceback.print_exc()

print(type(bill))


bills_df = pd.DataFrame(all_electric_bills)
bills_df.to_csv("elec_bills_total_only.csv", index=False)
print("Total electric bills saved to 'elec_bills_total_only.csv'.")
# Save to CSV