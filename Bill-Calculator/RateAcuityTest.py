
import importlib
import RateAcuity
importlib.reload(RateAcuity)
from RateAcuity import download_tariff, calculate_bill, get_tariff_RA


elecTariff = 698
gasTariff = "-RESIDENTIAL SALES---"
state = "MN"
utility = "Northern States Power Co - Minnesota"                                                                                                                                                                    
gas_utility = "CenterPoint Energy"

gas_tariff = download_tariff(state, gas_utility, gasTariff)