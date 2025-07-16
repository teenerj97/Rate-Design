import os
import time
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.edge.service import Service

import polars as pl
import re
from difflib import get_close_matches
from collections import defaultdict
from typing import List
from ts_tariffs.utils import Block
from ts_tariffs.billing import Bill
from ts_tariffs.meters import MeterData
from ts_tariffs.ts_utils import SampleRate, DateWindow
from ts_tariffs.tariffs import tariffs_map
import pandas as pd

# This function uses Selenium to automate web browser steps for downloading and processing the tariff data from RateAcuity.
def download_tariff(state, utility, schedule):

    # Configure Microsoft Edge WebDriver
    edge_options = Options()
    edge_options.use_chromium = True
    # edge_options.add_argument("--headless")  # Run in headless mode (remove for debugging)
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36")
    edge_options.add_argument("--log-level=3")
    edge_options.add_experimental_option("useAutomationExtension", False)  # Disable automation extension
    edge_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])  # Remove automation flag
    edge_options.add_experimental_option("prefs", {
    "download.default_directory": r"C:\Users\jack.teener\OneDrive - RMI\Desktop\Rate Design\resstock2\scraper\Gas_Tariffs",
    "download.prompt_for_download": False,
    "directory_upgrade": True
    })

    service = Service(log_path=os.devnull)

    # Initialize Edge WebDriver
    driver = webdriver.Edge(service=service,options=edge_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    LINK = "https://secure.rateacuity.com/RateAcuityPortal/Account/Login"
    
    # Navigate to login page
    driver.get(LINK)

    # Check if the 'Log off' element exists and click it if present
    try:
        logoff_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//a[@href=\"javascript:document.getElementById('logoutForm').submit()\"]"))
        )
        logoff_element.click()
    except:
        pass  # If the element doesn't exist, continue without error
    
    # Login configuration
    EMAIL_ADDRESS = "al.qarooni@rmi.org"
    PASSWORD = "Power200"

    # Login to the page
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'UserName'))).send_keys(EMAIL_ADDRESS)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Password'))).send_keys(PASSWORD)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Log in']"))
    ).click()

    # Click on 'Rate Acuity Gas Reports' link
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(normalize-space(text()), 'Rate Acuity Gas Reports')]"))
    ).click()

    # Select a state from the dropdown
    state_dropdown = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'StateSelect')))
    select = Select(state_dropdown)
    select.select_by_value(state)

    # Get utilities list
    utility_dropdown = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'UtilitySelect')))
    utility_options = utility_dropdown.find_elements(By.TAG_NAME, 'option')
    option_texts = [option.text.strip() for option in utility_options]
    match = get_close_matches(utility, option_texts, cutoff=0.6)
    if not match:
        raise ValueError(f"No close match found for gas utility '{utility}'. Options are: {option_texts}")
    select = Select(utility_dropdown)
    select.select_by_visible_text(match[0])

    # Get schedules list
    schedule_dropdown = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'ScheduleSelect')))
    schedule_options = schedule_dropdown.find_elements(By.TAG_NAME, 'option')
    option_texts = [option.text.strip() for option in schedule_options if 'res' in option.text.strip().lower() or 'multi' in option.text.strip().lower()]
    if schedule not in option_texts:
        raise ValueError(f"Choose one of the following schedules: {option_texts}")
    select = Select(schedule_dropdown)
    select.select_by_visible_text(schedule)

    # Click on 'Create Excel Spreadsheet'
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, 'Create Excel Spreadsheet'))).click()
    time.sleep(2)

    download_path = r"C:\Users\jack.teener\OneDrive - RMI\Desktop\Rate Design\resstock2\scraper\Gas_Tariffs"  # Adjust path if necessary
    excel_files = [f for f in os.listdir(download_path) if f.endswith('.xlsx')]
    latest_file = max(excel_files, key=lambda x: os.path.getctime(os.path.join(download_path, x)))
    latest_file_path = os.path.join(download_path, latest_file)

    directory_name = f"{state}-{utility}"
    target_dir = os.path.join(download_path, directory_name)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir, exist_ok=True)

    # --- Load Excel file with Polars ---
    # --- Detect "Component Description" in the first column and set as header row ---
    with open(latest_file_path, 'rb') as f:
        raw_data = pl.read_excel(f, has_header=False)
    header_row_index = None

    for i, row in enumerate(raw_data.iter_rows()):
        if "Component Description" in row[0]:
            header_row_index = i
            break

    # Organize Tariffs
    df = pl.read_excel(
        latest_file_path,
        engine="calamine",
        read_options={"header_row": header_row_index}
    )

    df.write_csv(os.path.join(target_dir,schedule + ".csv"))
    os.remove(latest_file_path) # Remove the original file after processing
    return df

# This function formats the tariff data for input into the billing model from the ts-tariffs library.
def get_tariff_RA(state, utility, schedule) -> List[dict]:
    if not os.path.exists(f"c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/resstock2/Gas_Tariffs/{state}-{utility}.csv"):
        df = print("tariff data needs to be downloaded from RateAcuity, not in folder yet.")
    else:
        df = pl.read_csv(
            f"c:/Users/jack.teener/OneDrive - RMI/Desktop/Rate Design/resstock2/Gas_Tariffs/{state}-{utility}.csv", skip_rows=1
        )
    charges = []
    print("Columns in df:", df.columns)
    # Accumulators for tiers
    global_blocks   = defaultdict(lambda: {"blocks": [], "rates": [], "labels": []})
    seasonal_blocks = defaultdict(lambda: {"blocks": [], "rates": [], "labels": []})

    for row in df.iter_rows(named=True):
        if not row["Rate"]:
            continue
        if row.get("Location"):
            continue

        comp       = row["Component Description"]
        rate       = float(row["Rate"])
        det        = row.get("Rate Determinant") or ""
        unit       = det.split("per ")[1].lower() if "per " in det else "therm"
        start      = row.get("Start")
        end        = row.get("End")
        season_txt = row.get("Season")  # e.g. "11/01-04/30" or "04/01-10/31"

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
            # parse season into windows in 2025
            sd, ed = (s.strip() for s in season_txt.split("-"))
            ms, ds = map(int, sd.split("/"))
            me, de = map(int, ed.split("/"))
            # decide if crosses year
            windows = []
            if (me, de) < (ms, ds):
                windows = [
                    (f"2025-{ms:02d}-{ds:02d}", "2025-12-31"),
                    ("2025-01-01", f"2025-{me:02d}-{de:02d}")
                ]
            else:
                windows = [(f"2025-{ms:02d}-{ds:02d}", f"2025-{me:02d}-{de:02d}")]

            # emit one SingleRateTariff per window
            for ws, we in windows:
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

        # 5) single‐rate
        charges.append({
            "name": f"{comp}_{unit}_single",
            "charge_type": "SingleRateTariff",
            "rate": rate,
            "consumption_unit": unit,
            "rate_unit": f"dollars / {unit}",
            "sample_rate": None,
            "adjustment_factor": 1.0,
        })

    # Emit seasonal BlockTariffs with window metadata
    for (comp, unit, season_txt), sb in seasonal_blocks.items():
        # parse season range into one or two calendar windows in 2025
        start_dd, end_dd = (s.strip() for s in season_txt.split("-"))
        ms, ds = map(int, start_dd.split("/"))
        me, de = map(int, end_dd.split("/"))

        windows = []
        if (me, de) < (ms, ds):
            # wraps year → split
            windows.append((f"2025-{ms:02d}-{ds:02d}", "2025-12-31"))
            windows.append(("2025-01-01", f"2025-{me:02d}-{de:02d}"))
        else:
            windows.append((f"2025-{ms:02d}-{ds:02d}", f"2025-{me:02d}-{de:02d}"))

        for ws, we in windows:
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

    return charges

def calculate_bill(tariff, ts):
    print(type(ts))
    # If ts is a list, extract the first element if that's the DataFrame/dict you want
    if isinstance(ts, list):
        if len(ts) == 1:
            ts = ts[0]
        else:
            # If multiple, try to concatenate or handle as needed
            try:
                ts = pd.concat(ts)
            except Exception:
                ts = pd.DataFrame(ts)
    ts = pd.Series(ts["natural_gas.total"].to_numpy()*0.03412, index=pd.to_datetime(ts["timestamp"].to_list()))

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

    bill = Bill(name="my_combined_bill", charges=applied_charges)
    return bill