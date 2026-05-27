#!/usr/bin/env python3
"""
Z2Data BOM Scrubber
- Supports parallel processing (up to 10 concurrent browser tabs)
- Waits for login completion once, then processes all MPNs
- Captures tab snapshots + extracts requested column values into CSV
"""

import csv
import os
import time
import getpass
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None


# ================= CONFIG =================
INPUT_FILE = "mpn_list.xlsx"
OUTPUT_DIR = Path("output_html")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = Path("z2_columns_output.csv")

Z2_DASHBOARD = "https://app.z2data.com/Parts/home"

TABS = [
    "Overview", "Part Options", "Manufacturing",
    "Manufacturing Location", "Package/Packing",
    "Lifecycle/Forecast", "Qualification",
    "Documents", "Counterfeit Risk",
    "PCNs & GIDEP", "Replacement"
]

MAX_CONCURRENT_TABS = 10
SPA_TIMEOUT = 30
TAB_WAIT = 12

REQUESTED_COLUMNS = [c.strip() for c in """Item Number,Uploaded Part Number,Uploaded Manufacturer,PLM Items,STD Cost,Uploaded Description,Qty,Commodity,Location,# of Uploaded Manufacturer,Manufacturer,BOM Name,BOM Type,BOM Stage,BOM Number,BOM Family,BOM Revision Number,BOM Comments,MPN Preferred Status,Supplier Preferred Status,Z2Manufacturer Id,Part ID,Imported Row #,Z2 Part Number,Z2 Manufacturer,Family Name,Description,Product Name,Country ISO Code(2digits),Country of Origin,Item Type,Z2 Part Risk,Z2 Part Score,Datasheet Link,Split,Part Match Status,Crosses Count,Family Introduction Date,Part Introduction Year,Main Category,Sub Category 0,Category,Part Tags,Packing Type,Reel Diameter,Packing Quantity,Match Reason,Match Comment,# of PCNs,# of GIDEP,Tape Width,Note,Applications,Lifecycle Status,LC Source,Last Time Buy Date,Last Ship Date,Obsolescence Comment,Obsolescence Type,EOL Based On Product Lifecycle,Estimated Years to EOL,Manufacturer Lifecycle Forecast,Forecast date,Part Replacements,Manufacturer Lifecycle Forecast Source,Lifecycle Comment,Obsolescence Year,RoHS - Status,RoHS Version,RoHS Exemption,RoHS Source,REACH Status,CoC  Link,REACH Source,REACH - Version,China REACH Status,China RoHS Status,China REACH Source,ELV Directive Status,Lead Free Status,SCIP Number,California Proposition 65_ Source,TSCA,PFAS Source,POPS Source,POPS,PFAS,Halogen Free Status,TSCA Source,California Proposition 65,Full Material Declaration,IPC Z2 Source,EU REACH Restricted,EU REACH Restricted Source,REACH FEB 2026 (251),REACH NOV 2025 (251),Status-RoHS 2015 (863),Exemption-RoHS 2015 (863),RoHS Source Type,MSL,Maximum Reflow Temperature,Maximum Wave Temperature,# of Reflow Cycle,Base Material,Normalized Lead Finish,Lead Finish,Net Weight,Reflow Solder Time (Sec),Shelf Life [Month],Wave Solder Time,Lead Free Process Capability,Shelf Life Start,Shelf Life Condition,Net Weight_Source Type,IPN Multi-sourcing Status,# of Drop-in Crosses,# of Drop-In-A,# of Active Drop-In-A,# of Active Drop-In-B,# of Drop-In-B,# of Drop-In-C,# of Active Drop-In-C,Drop-In Same Manufacturer,China Tariff,China Tariff List,China Tariff Duty Rate,General Market Status,Current Market Status,Max Lead Time,Min Lead Time,Manufacturer NCNR,Standard Lead Time,Counterfeit Risk,Total Quantity,Lowest Cost Inventory,Lowest Cost,Quantity_ Largest Inventory,Largest Inventory,Quantity_ Second Largest Inventory,Second Largest Inventory,Average Cost,Highest Cost,Highest Cost Inventory,Number of Sellers,Min Price Break,Min Price Seller Stock,Normalized Package Name,Package Name,Pin Count,Dimensions (mm),Maximum Diameter (mm),Maximum Height (mm),Lead Shape,Mounting Type,Pin Pitch (mm),Package Dimension Note,Body Length (mm),Body Width (mm),Body Height (mm),Overall Height (mm),Conflict Mineral Status,Conflict Mineral Source,Conflict Mineral Year,Conflict Mineral Source Type,Part Grade,Temp Range,Flammability Rating,ESD Protection,MTBF/MTTF(Hrs),Reliability,MTBF/MTTF_Link,ESD Class,Flammability,ESD Source,Maximum ESD Protection Voltage(KV),Flammability Test,Automotive Qualified,Automotive Standards,Military Standards,Military Qualified,Military Source,UL Standard Details,UL File Number,Agency Approvals,Calculated Failure Rate(FIT),Agency Approvals Type,CSA Standard Details,CSA File Number,Compliance Level,Test Temperature,Activation Energy(eV),Confidence Level(UCL),Sample Size,Reliability Source Type,Reliability Source,Test Duration(Hrs),Fails Quantity,Agency Approvals Source Type,IEC Standard Details,VDE File Number,VDE Standard Details,ECCN-EU,ECCN-US,HTS-EU,UNSPSC,HTS-US,Schedule B,Minimum Operating Temperature,Maximum Supply Voltage,Minimum Supply Voltage,Temperature Coefficient,Mounting,Minimum Junction Temperature,Power Rating,Temperature Coefficient Code,Maximum Operating Temperature,Maximum Junction Temperature,Contact Finish,Pitch,Maximum Current Rating,Connector Type,Number of Positions Contacts,Thickness,Material,Lifetime @ Temp.,Tolerance,Composition,Maximum Power Dissipation,Voltage Rating,Resistance,Maximum Supply Current,Capacitance,Type,Maximum Working Voltage,Plating,Width,Features,Overall Score %,Overall Risk,Obsolescence Risk,Obsolescence Score %,Compliance Score %,Compliance Risk,Market Risk,Market Score %,Multi-Sourcing Score %,Multi-Sourcing Risk,Technology Risk,Supplier Score %,Supplier Score,Technology Factors,Technology Stage,Item Score,Item Max Forecasted Lifecycle,Item Description,# of MPN,Item Lifecycle,Item dimension,# of Suppliers,IPN Lead Finish,First Introduction Date,# of Product,Product,Sub Assembly,# of Sub Assembly,Supplier Vendor Code,Overall Supplier Score,Supplier Ownership,Parent Supplier Name,Overall Supplier Risk,DUNS Number,HQ CAGE Code,HQ Address,Year Founded,# of Employees,SIC Code,CIK,Industry,Stock Exchange,SmeltersCount,Youtube,Twitter,LinkedIn,FaceBook,Dialing code,Email,Phone,Website""".split(",")]


def start_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    local = Path("chromedriver.exe")
    if local.exists():
        service = Service(str(local))
    elif ChromeDriverManager:
        service = Service(ChromeDriverManager().install())
    else:
        raise RuntimeError("chromedriver.exe not found")

    return webdriver.Chrome(service=service, options=options)


def wait_for_spa(driver):
    WebDriverWait(driver, SPA_TIMEOUT).until(
        EC.presence_of_element_located((By.TAG_NAME, "app-root"))
    )


def find_search_input(driver):
    return WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "input[type='search'], input[placeholder*='Search']")
        )
    )


def js_set_input(driver, el, text):
    driver.execute_script(
        """
        arguments[0].focus();
        arguments[0].value = arguments[1];
        arguments[0].dispatchEvent(new Event('input',{bubbles:true}));
        arguments[0].dispatchEvent(new Event('change',{bubbles:true}));
        """,
        el, text
    )


def click_yellow_highlight(driver, search_input):
    try:
        item = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//cdk-virtual-scroll-viewport//app-searchautocomplete-item[1]"
            ))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", item)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", item)
    except Exception:
        search_input.send_keys(Keys.ARROW_DOWN)
        time.sleep(0.2)
        search_input.send_keys(Keys.ENTER)


def click_tab(driver, tab):
    try:
        el = WebDriverWait(driver, TAB_WAIT).until(
            EC.element_to_be_clickable((By.XPATH, f"//*[normalize-space()='{tab}']"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", el)
        time.sleep(0.2)
        el.click()
        return True
    except Exception:
        return False


def capture_html(driver):
    return driver.execute_script(
        "return document.querySelector('app-partdetails-layout')?.outerHTML || document.body.outerHTML;"
    )


def extract_requested_columns(driver, mpn):
    script = """
    const keys = arguments[0];
    const out = {};
    const norm = s => (s || '').replace(/\\s+/g,' ').trim().toLowerCase();
    const all = Array.from(document.querySelectorAll('body *'));

    function valForLabel(el) {
      const row = el.closest('tr,li,div,section,article,dl');
      if (!row) return '';
      const candidates = [];
      if (el.nextElementSibling) candidates.push(el.nextElementSibling.innerText || '');
      if (row.querySelector('td:last-child')) candidates.push(row.querySelector('td:last-child').innerText || '');
      if (row.querySelector('dd')) candidates.push(row.querySelector('dd').innerText || '');
      const own = (row.innerText || '').replace((el.innerText || ''), '').trim();
      if (own) candidates.push(own);
      return (candidates.find(v => v && v.trim()) || '').trim();
    }

    for (const k of keys) {
      const nk = norm(k);
      let found = '';
      for (const el of all) {
        const t = norm(el.innerText);
        if (t === nk || t.startsWith(nk + ':')) {
          found = valForLabel(el);
          break;
        }
      }
      out[k] = found;
    }
    return out;
    """
    data = driver.execute_script(script, REQUESTED_COLUMNS)
    data["MPN"] = mpn
    return data


def process_single_tab(driver, handle, mpn):
    driver.switch_to.window(handle)
    driver.get(Z2_DASHBOARD)
    wait_for_spa(driver)

    search = find_search_input(driver)
    js_set_input(driver, search, mpn)
    search.send_keys(" ")
    time.sleep(1)
    click_yellow_highlight(driver, search)
    time.sleep(3)

    html = ["<html><body>", f"<h1>{mpn}</h1>"]
    for tab in TABS:
        if click_tab(driver, tab):
            time.sleep(1.5)
            html.append(f"<h2>{tab}</h2>")
            html.append(capture_html(driver))

    out = OUTPUT_DIR / f"{mpn}.html"
    out.write_text("".join(html + ["</body></html>"]), encoding="utf-8")
    row = extract_requested_columns(driver, mpn)
    return row


def main():
    print("🚀 Z2Data BOM Scrubber – MULTI-TAB VERSION")
    if not os.path.exists(INPUT_FILE):
        print("❌ mpn_list.xlsx not found")
        return

    df = pd.read_excel(INPUT_FILE)
    col = next((c for c in df.columns if c.lower() in ("mpn", "part number")), None)
    if not col:
        print("❌ Excel must contain MPN column")
        return

    parts = df[col].dropna().astype(str).tolist()[:10]
    driver = start_driver()

    driver.get(Z2_DASHBOARD)
    wait_for_spa(driver)
    user = input("Username: ").strip()
    pwd = getpass.getpass("Password: ")
    try:
        driver.find_element(By.CSS_SELECTOR, "input[type='email']").send_keys(user)
        p = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        p.send_keys(pwd)
        p.send_keys(Keys.ENTER)
    except Exception:
        pass

    input("Complete login/2FA if prompted, then press ENTER to continue: ")
    driver.get(Z2_DASHBOARD)
    wait_for_spa(driver)

    while len(driver.window_handles) < min(MAX_CONCURRENT_TABS, len(parts)):
        driver.switch_to.new_window('tab')
        driver.get('about:blank')

    handles = driver.window_handles[:min(MAX_CONCURRENT_TABS, len(parts))]
    results = []
    for idx, mpn in enumerate(parts, 1):
        handle = handles[(idx - 1) % len(handles)]
        try:
            print(f"[{idx}/{len(parts)}] {mpn} on tab {((idx - 1) % len(handles)) + 1}")
            results.append(process_single_tab(driver, handle, mpn))
            print(f"✅ {mpn} done")
        except Exception:
            print(f"❌ Failed on {mpn}")
            traceback.print_exc()

    if results:
        fieldnames = ["MPN"] + REQUESTED_COLUMNS
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(results)
        print(f"📄 Column output saved: {OUTPUT_CSV}")

    driver.quit()
    print("🎉 Completed")


if __name__ == "__main__":
    main()
