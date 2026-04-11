#!/usr/bin/env python3
"""
Z2Data BOM Scrubber
- Selects yellow highlighted dropdown item (Angular CDK)
- Opens Parametric using exact XPath provided by user
- Saves full HTML per MPN
"""

import os
import time
import getpass
import traceback
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

Z2_DASHBOARD = "https://app.z2data.com/Parts/home"

TABS = [
    "Overview", "Part Options", "Manufacturing",
    "Manufacturing Location", "Package/Packing",
    "Lifecycle/Forecast", "Qualification",
    "Documents", "Counterfeit Risk",
    "PCNs & GIDEP", "Replacement"
]

SPA_TIMEOUT = 30
TAB_WAIT = 12


# ================= DRIVER =================
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


# ================= SEARCH =================
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
    """
    Clicks highlighted dropdown item using exact Angular CDK XPath
    """
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
        # keyboard fallback
        search_input.send_keys(Keys.ARROW_DOWN)
        time.sleep(0.2)
        search_input.send_keys(Keys.ENTER)


# ================= PARAMETRIC =================
def open_parametric(driver):
    """
    Opens Parametric using EXACT XPath provided by user
    """
    param_xpath = (
        "/html/body/app-root/div[3]/app-partdetails-layout/"
        "div/div[1]/div/div[2]/a/div[2]"
    )

    try:
        el = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, param_xpath))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", el)
        time.sleep(0.3)
        driver.execute_script("arguments[0].click();", el)
        time.sleep(5)
        return True
    except Exception as e:
        print("⚠ Parametric click failed:", e)
        return False


# ================= TAB =================
def click_tab(driver, tab):
    try:
        el = WebDriverWait(driver, TAB_WAIT).until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[normalize-space()='{tab}']")
            )
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


# ================= MAIN =================
def main():
    print("🚀 Z2Data BOM Scrubber – FINAL VERSION")

    if not os.path.exists(INPUT_FILE):
        print("❌ mpn_list.xlsx not found")
        return

    df = pd.read_excel(INPUT_FILE)
    col = next((c for c in df.columns if c.lower() in ("mpn", "part number")), None)
    if not col:
        print("❌ Excel must contain MPN column")
        return

    parts = df[col].dropna().astype(str).tolist()
    driver = start_driver()

    # ---------- LOGIN ----------
    driver.get(Z2_DASHBOARD)
    wait_for_spa(driver)

    user = input("Username: ").strip()
    pwd = getpass.getpass("Password: ")

    try:
        driver.find_element(By.CSS_SELECTOR, "input[type='email']").send_keys(user)
        p = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        p.send_keys(pwd)
        p.send_keys(Keys.ENTER)
        time.sleep(5)
    except Exception:
        input("Login manually and press ENTER")

    # ---------- PROCESS ----------
    for idx, mpn in enumerate(parts, 1):
        print(f"\n[{idx}/{len(parts)}] Processing: {mpn}")

        try:
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
                    time.sleep(2)
                    html.append(f"<h2>{tab}</h2>")
                    html.append(capture_html(driver))

            # ---------- PARAMETRIC ----------
            print("    → Opening Parametric")
            if open_parametric(driver):
                html.append("<h2>Parametric</h2>")
                html.append(driver.page_source)

            html.append("</body></html>")

            out = OUTPUT_DIR / f"{mpn}.html"
            out.write_text("".join(html), encoding="utf-8")
            print(f"✅ Saved: {out}")

        except Exception:
            print(f"❌ Failed on {mpn}")
            traceback.print_exc()

    driver.quit()
    print("\n🎉 COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
