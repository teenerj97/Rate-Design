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

import time
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.edge.service import Service

import polars as pl
from collections import defaultdict

# This function uses Selenium to automate web browser steps for downloading and processing the tariff data from RateAcuity.
def get_tariff_RA(state, utility, schedule):
    # only download if not already done
    if os.path.exists(f"Gas_Tariffs/{state}-{utility}/{schedule}.csv"):
        return pl.read_csv(
            f"Gas_Tariffs/{state}-{utility}/{schedule}.csv"
        )

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
    "download.default_directory": FILEPATHS["Bill Calculator"]["RateAcuity"],
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
    if utility not in option_texts:
        raise ValueError(f"Gas utility name invalid. Options are: {option_texts}")
    select = Select(utility_dropdown)
    select.select_by_visible_text(utility)

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

    download_path = FILEPATHS["Bill Calculator"]["RateAcuity"]  # Adjust path if necessary
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

def calculate_bill_gas(df, building):
    
    # Drop rows with null Rate
    df = df.filter(pl.col("Rate").is_not_null())

    # Prepare load data with month, hour, and date columns, and convert from kwh to therms
    building = building.with_columns([
        pl.col("timestamp").str.to_datetime().alias("timestamp"),
        (pl.col("natural_gas.total")*0.03412).alias("natural_gas.total")
    ])
    building = building.with_columns([
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.date().alias("date")
    ])

    # Initialize charges
    total_charges = defaultdict(float)

    # Convert rate table to row-wise dict for iteration
    for row in df.iter_rows(named=True):
        if not row["Rate"]:
            continue
        if row.get("Location"):
            continue
        name = row["Component Description"]
        rate = row["Rate"]
        determinant = str(row["Rate Determinant"]).lower()
        season = row["Season"]
        start = float(row["Start"]) if row.get("Start") else 0.0
        end = float(row["End"]) if row.get("End") else float("inf")

        # Get season months
        if isinstance(season, str) and "/" in season:
            mo1, day1 = map(int, season.split("-")[0].split("/"))
            mo2, day2 = map(int, season.split("-")[1].split("/"))
            season_months = list(range(mo1, mo2 + 1)) if mo1 <= mo2 else list(range(mo1, 13)) + list(range(1, mo2 + 1))
        else:
            season_months = list(range(1, 13))

        # Filter load for season
        building_filter = building.filter(pl.col("month").is_in(season_months))

        if "therm" in determinant:
            
            # Compute monthly totals
            building_filter_monthly_total = (
                building_filter.group_by("month")
                .agg(pl.col("natural_gas.total").sum().alias("month_total"))
            )
            building_filter = building_filter.join(building_filter_monthly_total, on="month")
            
            # Consumption limit handling
            building_filter = building_filter.with_columns([
                pl.when(pl.col("month_total") > start)
                .then(
                    pl.when(pl.col("month_total") > end)
                        .then(end - start)
                        .otherwise(pl.col("month_total") - start)
                )
                .otherwise(1)
                .alias("adjusted_therm_factor")
            ])

            building_filter = building_filter.with_columns([
                (pl.col("natural_gas.total") * pl.col("adjusted_therm_factor") / pl.col("month_total"))
                .fill_nan(0)
                .alias("adjusted_therms")
            ])

            kwh = building_filter["adjusted_therms"].sum()
            charge = rate * kwh
            total_charges[name] += charge

        elif "month" in determinant or "bill" in determinant:
            months = building_filter["month"].n_unique()
            total_charges[name] += rate * months

        elif "day" in determinant:
            days = building_filter["date"].n_unique()
            total_charges[name] += rate * days

        elif "year" in determinant:
            total_charges[name] += rate

    return total_charges