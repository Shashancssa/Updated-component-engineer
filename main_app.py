import streamlit as st
import pandas as pd
import sqlite3
import time
import io
import re
import os
import json
import difflib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests
from pathlib import Path
from urllib import parse, request
from urllib.error import HTTPError, URLError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service 
def ensure_live_cache_table():
    ...


# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "components.db"
HTML_DIR = BASE_DIR / "output_html"
HTML_DIR.mkdir(exist_ok=True)
LOGO_PATH = BASE_DIR / "logo.png"

TABS = [
    "Overview", "Part Options", "Manufacturing",
    "Manufacturing Location", "Package/Packing",
    "Lifecycle/Forecast", "Qualification",
    "PCNs & GIDEP", "Replacement"
]

DEV_INFO = "Developed by :- Shashank C | Mail ID:- shashank.c@kaynestechnology.net"
SOURCE_MOUSER = "Mouser"
SOURCE_DIGIKEY = "Digi-Key"
# Prefer environment variables first, then built-in defaults so manual entry is not required each run.
MOUSER_API_KEY_FALLBACK = os.getenv("MOUSER_API_KEY", "a5d0cdf4-c5b6-4600-88ab-12290f19e2cc")
DIGIKEY_CLIENT_ID_FALLBACK = os.getenv("DIGIKEY_CLIENT_ID", "AyNFvUvmDoGUTtIyeDAhqE1BsHzQ9HNlMM2CoKurruURHJPl")
DIGIKEY_CLIENT_SECRET_FALLBACK = os.getenv("DIGIKEY_CLIENT_SECRET", "k5bDnbn49OFWrYQtuQlAgG2YOdeLrr5BCxK8eihKJzDTz3WHQBpnGkN84lLKdwQE")
NEXAR_CLIENT_ID_FALLBACK = os.getenv("NEXAR_CLIENT_ID", "2000628d-be02-44fc-bfff-f7a90ad13926")
NEXAR_CLIENT_SECRET_FALLBACK = os.getenv("NEXAR_CLIENT_SECRET", "ECZ622yjXXrCVDpXOmgJHrulfQI3AWJh_sz0")
_DIGIKEY_TOKEN_CACHE = {}

st.set_page_config(layout="wide", page_title="COMPONENT ENGINEER DATABASE", page_icon="🛡️")

# ==========================================
# UI COMPONENTS
# ==========================================

def show_sidebar_logo():
    if LOGO_PATH.exists():
        st.sidebar.image(str(LOGO_PATH), width="stretch")
    st.sidebar.title("⚙️ Global Settings")
    mode = st.sidebar.toggle("Headless Mode (Background)", value=True)
    return mode

def show_footer():
    st.markdown(f"""
        <div style="position: fixed; bottom: 0; left: 0; width: 100%; background-color: white; 
        text-align: center; padding: 10px; border-top: 1px solid #eaeaea; z-index: 100;">
            <p style="margin:0; font-size:12px; color: #555; font-weight: bold;">{DEV_INFO}</p>
        </div>
        <br><br>
    """, unsafe_allow_html=True)


def _extract_attribute_value(attributes, *candidate_keys):
    """
    Return first matching attribute value by fuzzy key match.
    attributes: list[{"Attribute": "...", "Value": "..."}]
    """
    if not isinstance(attributes, list):
        return ""
    normalized_keys = [str(k).strip().lower() for k in candidate_keys if str(k).strip()]
    if not normalized_keys:
        return ""
    for attr in attributes:
        if not isinstance(attr, dict):
            continue
        a_name = str(attr.get("Attribute", "")).strip().lower()
        a_val = str(attr.get("Value", "")).strip()
        if not a_name or not a_val:
            continue
        if any(k in a_name for k in normalized_keys):
            return a_val
    return ""


def _extract_price_summary(pricing_rows):
    if not isinstance(pricing_rows, list) or not pricing_rows:
        return ""
    row = pricing_rows[0] if isinstance(pricing_rows[0], dict) else {}
    qty = row.get("Quantity", row.get("Break Quantity", ""))
    price = row.get("Price", row.get("Unit Price", ""))
    currency = row.get("Currency", "")
    if qty or price or currency:
        return f"Qty {qty} -> {price} {currency}".strip()
    return ""


def _extract_component_thickness(attributes):
    if not isinstance(attributes, list):
        return ""
    primary = _extract_attribute_value(
        attributes,
        "height",
        "height seated",
        "seated height",
        "maximum height",
        "max height",
        "package height",
        "component thickness",
        "thickness",
    )
    if primary:
        return primary
    # Fallback: Size / Dimension fields sometimes encode L x W x H.
    size_txt = _extract_attribute_value(attributes, "size / dimension", "size", "dimensions")
    if size_txt and "x" in str(size_txt).lower():
        parts = [p.strip() for p in re.split(r"[xX×]", str(size_txt)) if p.strip()]
        if len(parts) >= 3:
            return parts[-1]
    return ""


def normalize_mpn(value):
    txt = str(value or "").strip()
    if not txt:
        return ""
    txt = txt.strip().strip('"').strip("'").strip()
    return txt


def build_mpn_search_candidates(value):
    raw = normalize_mpn(value)
    if not raw:
        return []
    candidates = [raw]
    no_dash = raw.replace("-", "")
    if no_dash and no_dash not in candidates:
        candidates.append(no_dash)
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw)
    if alnum and alnum not in candidates:
        candidates.append(alnum)
    lz = raw.lstrip("0")
    if lz and lz not in candidates:
        candidates.append(lz)
    return candidates


def _is_effectively_empty(value):
    txt = str(value or "").strip()
    if not txt:
        return True
    return txt.lower() in {"none", "null", "nan", "n/a", "na", "-"}


def add_enrichment_fields(parts, attributes, pricing):
    """
    Add normalized fields requested for DB/export compatibility:
    MSD LEVEL, REFLOW SOLDERING TEMPERATURE, THERMAL CYCLE,
    WAVE SOLDERING TEMPERATURE, LSL DETAILS, PRICE DETAILS, OPERATING TEMPERATURE.
    """
    if not isinstance(parts, list):
        return []
    msd = _extract_attribute_value(attributes, "msl", "msd", "moisture sensitivity", "moisture sensitive")
    reflow = _extract_attribute_value(attributes, "reflow")
    thermal_cycle = _extract_attribute_value(attributes, "thermal cycle")
    wave = _extract_attribute_value(attributes, "wave solder")
    operating_temp = _extract_attribute_value(attributes, "operating temperature", "temperature range", "operating temp")
    lsl = _extract_attribute_value(attributes, "lsl", "land side", "lead surface")
    package = _extract_attribute_value(attributes, "package", "case", "mounting package")
    component_thickness = _extract_component_thickness(attributes)
    reach = _extract_attribute_value(attributes, "reach")
    reflow_time = _extract_attribute_value(attributes, "reflow soldering time", "reflow time", "time at reflow")
    wave_time = _extract_attribute_value(attributes, "wave soldering time", "wave time")
    body_mark = _extract_attribute_value(attributes, "body mark", "marking")
    price_details = _extract_price_summary(pricing)
    for row in parts:
        if not isinstance(row, dict):
            continue
        row["MSD LEVEL"] = row.get("MSD LEVEL", "") or msd
        row["REFLOW SOLDERING TEMPERATURE"] = row.get("REFLOW SOLDERING TEMPERATURE", "") or reflow
        row["THERMAL CYCLE"] = row.get("THERMAL CYCLE", "") or thermal_cycle
        row["WAVE SOLDERING TEMPERATURE"] = row.get("WAVE SOLDERING TEMPERATURE", "") or wave
        row["LSL DETAILS"] = row.get("LSL DETAILS", "") or lsl
        row["PACKAGE"] = row.get("PACKAGE", "") or package
        row["PRICE DETAILS"] = row.get("PRICE DETAILS", "") or price_details
        row["OPERATING TEMPERATURE"] = row.get("OPERATING TEMPERATURE", "") or operating_temp
        row["COMPONENT THICKNESS"] = row.get("COMPONENT THICKNESS", "") or component_thickness
        row["REACH"] = row.get("REACH", "") or reach
        row["REFLOW SOLDERING TIME"] = row.get("REFLOW SOLDERING TIME", "") or reflow_time
        row["WAVE SOLDERING TIME"] = row.get("WAVE SOLDERING TIME", "") or wave_time
        row["BODY MARK"] = row.get("BODY MARK", "") or body_mark
        row["DATASHEETLINK"] = row.get("DATASHEETLINK", "") or row.get("Data Sheet URL", "")
    return parts


def _read_mpn_list_from_upload(file_obj):
    if not file_obj:
        return []
    try:
        if str(file_obj.name).lower().endswith(".csv"):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
        if df.empty:
            return []
        return [normalize_mpn(x) for x in df.iloc[:, 0].dropna().astype(str).tolist() if normalize_mpn(x)]
    except Exception:
        return []


def import_unified_from_excel(file_obj):
    if not file_obj:
        return {"loaded": 0, "skipped": 0, "error": "No file selected"}
    try:
        if str(file_obj.name).lower().endswith(".csv"):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
    except Exception as ex:
        return {"loaded": 0, "skipped": 0, "error": f"Read failed: {ex}"}

    if df.empty:
        return {"loaded": 0, "skipped": 0, "error": "File has no rows"}

    ensure_unified_parts_table()
    def _norm_col_name(value):
        txt = str(value).strip().lower()
        txt = re.sub(r"[^a-z0-9]+", " ", txt)
        return re.sub(r"\s+", " ", txt).strip()

    col_lookup = {_norm_col_name(c): c for c in df.columns}

    aliases = {
        "mpn": ["mpn", "requested mpn"],
        "manufacturer": ["manufacturer", "manufacture", "mfr", "manufac"],
        "manufacturer_part_number": ["manufacturer part number", "manufacture part number", "mfr part number", "manufacture part no", "manufacturer pn"],
        "supplier_part_number": ["supplier part number", "part number", "supplier pn"],
        "description": ["description"],
        "category": ["category"],
        "lifecycle_status": ["lifecycle", "lifecycle status"],
        "rohs": ["rohs", "rohs status"],
        "stock": ["stock", "quantity available"],
        "datasheet_url": ["datasheetlink", "datasheet", "data sheet url"],
        "product_url": ["product url"],
        "msd_level": ["msd level", "msl", "moisture sensitivity level", "moisture sensitive level"],
        "reflow_soldering_temperature": ["reflow soldering temperature", "reflow temperature"],
        "thermal_cycle": ["thermal cycle", "reflow cycle"],
        "wave_soldering_temperature": ["wave soldering temperature", "wave solder"],
        "lsl_details": ["lsl details", "lsl"],
        "package_details": ["package", "package details"],
        "price_details": ["price details", "price"],
        "operating_temperature": ["operating temperature", "temp range", "temperature range"],
        "component_thickness": ["component thickness", "thickness", "height"],
        "reach": ["reach", "reach status"],
        "reflow_soldering_time": ["reflow soldering time", "reflow time"],
        "wave_soldering_time": ["wave soldering time", "wave time"],
        "body_mark": ["body mark", "marking"],
    }

    def _pick_value(row, key):
        for a in aliases.get(key, []):
            c = col_lookup.get(_norm_col_name(a))
            if c is None:
                continue
            v = row.get(c, "")
            if str(v).strip() and str(v).strip().lower() != "nan":
                return str(v).strip()
        # Partial fallback for slightly truncated/variant headers (e.g., "manufac", "datashee")
        for a in aliases.get(key, []):
            needle = _norm_col_name(a)
            if not needle:
                continue
            for normalized_col, original_col in col_lookup.items():
                if needle in normalized_col or normalized_col in needle:
                    v = row.get(original_col, "")
                    if str(v).strip() and str(v).strip().lower() != "nan":
                        return str(v).strip()
        return ""

    loaded = 0
    skipped = 0
    cols = [
        "mpn", "manufacturer", "manufacturer_part_number", "supplier_part_number", "description", "category",
        "lifecycle_status", "rohs", "stock", "datasheet_url", "product_url", "msd_level",
        "reflow_soldering_temperature", "thermal_cycle", "wave_soldering_temperature", "lsl_details",
        "package_details", "price_details", "operating_temperature", "component_thickness",
        "reach", "reflow_soldering_time", "wave_soldering_time", "body_mark", "source_trace"
    ]
    with sqlite3.connect(DB_PATH) as conn:
        for _, row in df.iterrows():
            mpn = normalize_mpn(_pick_value(row, "mpn"))
            if not mpn:
                skipped += 1
                continue
            current = conn.execute(
                "SELECT manufacturer, manufacturer_part_number, supplier_part_number, description, category, lifecycle_status, rohs, stock, datasheet_url, product_url, msd_level, reflow_soldering_temperature, thermal_cycle, wave_soldering_temperature, lsl_details, package_details, price_details, operating_temperature, component_thickness, reach, reflow_soldering_time, wave_soldering_time, body_mark, source_trace FROM unified_part_cache WHERE mpn=?",
                (mpn,),
            ).fetchone()
            existing = dict(zip(cols[1:], current)) if current else {k: "" for k in cols[1:]}
            merged = {"mpn": mpn}
            for k in cols[1:-1]:
                merged[k] = _pick_value(row, k) or _as_text(existing.get(k, ""))
            prior_trace = _as_text(existing.get("source_trace", "")).strip()
            merged["source_trace"] = ", ".join([x for x in [prior_trace, "ExcelImport"] if x])

            conn.execute(
                """
                INSERT OR REPLACE INTO unified_part_cache
                (mpn, manufacturer, manufacturer_part_number, supplier_part_number, description, category, lifecycle_status, rohs, stock, datasheet_url, product_url, msd_level, reflow_soldering_temperature, thermal_cycle, wave_soldering_temperature, lsl_details, package_details, price_details, operating_temperature, component_thickness, reach, reflow_soldering_time, wave_soldering_time, body_mark, source_trace, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    merged["mpn"], merged["manufacturer"], merged["manufacturer_part_number"], merged["supplier_part_number"],
                    merged["description"], merged["category"], merged["lifecycle_status"], merged["rohs"], merged["stock"],
                    merged["datasheet_url"], merged["product_url"], merged["msd_level"], merged["reflow_soldering_temperature"],
                    merged["thermal_cycle"], merged["wave_soldering_temperature"], merged["lsl_details"], merged["package_details"],
                    merged["price_details"], merged["operating_temperature"], merged["component_thickness"], merged["reach"],
                    merged["reflow_soldering_time"], merged["wave_soldering_time"], merged["body_mark"], merged["source_trace"], datetime.now(timezone.utc).isoformat(),
                ),
            )
            loaded += 1
        conn.commit()
    return {"loaded": loaded, "skipped": skipped, "error": ""}


def fetch_mouser_part_data(mpn, api_key, timeout=30):
    """
    Query Mouser Search API and return normalized tables:
      - parts (main card style fields)
      - pricing (quantity breaks)
      - attributes (specification rows)
      - documents (datasheet/product links)
    """
    safe_api_key = parse.quote(str(api_key).strip())
    url = f"https://api.mouser.com/api/v1/search/partnumber?apiKey={safe_api_key}"
    payload = {
        "SearchByPartRequest": {
            "mouserPartNumber": str(mpn).strip(),
            "partSearchOptions": "None",
        }
    }
    req = request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        raw_body = resp.read().decode("utf-8")
    data = json.loads(raw_body) if raw_body else {}
    parts = data.get("SearchResults", {}).get("Parts", []) or []

    parsed_parts, parsed_prices, parsed_attributes, parsed_documents = [], [], [], []
    for part in parts:
        if not isinstance(part, dict):
            continue

        price_breaks = part.get("PriceBreaks", []) or []
        parsed_parts.append(
            {
                "Requested MPN": str(mpn).strip(),
                "Supplier Part Number": part.get("MouserPartNumber", ""),
                "Manufacturer Part Number": part.get("ManufacturerPartNumber", ""),
                "Manufacturer": part.get("Manufacturer", ""),
                "Description": part.get("Description", ""),
                "Category": part.get("Category", ""),
                "Lifecycle Status": part.get("LifecycleStatus", ""),
                "Availability": part.get("Availability", ""),
                "Stock": part.get("AvailabilityInStock", ""),
                "On Order": part.get("AvailabilityOnOrder", ""),
                "Lead Time": part.get("LeadTime", ""),
                "ROHS": part.get("ROHSStatus", ""),
                "Data Sheet URL": part.get("DataSheetUrl", ""),
                "Product URL": part.get("ProductDetailUrl", ""),
                "Image URL": part.get("ImagePath", ""),
            }
        )

        for pb in price_breaks:
            if not isinstance(pb, dict):
                continue
            parsed_prices.append(
                {
                    "Requested MPN": str(mpn).strip(),
                    "Supplier Part Number": part.get("MouserPartNumber", ""),
                    "Quantity": pb.get("Quantity", ""),
                    "Price": pb.get("Price", ""),
                    "Currency": pb.get("Currency", ""),
                }
            )

        attributes = part.get("ProductAttributes", []) or []
        for attr in attributes:
            if not isinstance(attr, dict):
                continue
            parsed_attributes.append(
                {
                    "Requested MPN": str(mpn).strip(),
                    "Supplier Part Number": part.get("MouserPartNumber", ""),
                    "Attribute": (
                        attr.get("AttributeName")
                        or attr.get("AttributeLabel")
                        or attr.get("Name")
                        or ""
                    ),
                    "Value": (
                        attr.get("AttributeValue")
                        or attr.get("Value")
                        or attr.get("AttributeValueDisplay")
                        or ""
                    ),
                }
            )

        if part.get("DataSheetUrl"):
            parsed_documents.append(
                {
                    "Requested MPN": str(mpn).strip(),
                    "Type": "Datasheet",
                    "URL": part.get("DataSheetUrl", ""),
                }
            )
        if part.get("ProductDetailUrl"):
            parsed_documents.append(
                {
                    "Requested MPN": str(mpn).strip(),
                    "Type": "Product Detail",
                    "URL": part.get("ProductDetailUrl", ""),
                }
            )

    parsed_parts = add_enrichment_fields(parsed_parts, parsed_attributes, parsed_prices)

    return {
        "parts": parsed_parts,
        "pricing": parsed_prices,
        "attributes": parsed_attributes,
        "documents": parsed_documents,
    }


def fetch_mouser_batch(mpns, api_key):
    """
    Fetch Mouser basic rows for a list of MPNs.
    Always returns rows without crashing on malformed API payloads.
    """
    batch_rows = []
    for mpn in mpns:
        clean_mpn = str(mpn).strip()
        if not clean_mpn:
            continue
        try:
            mouser_data = fetch_mouser_part_data(clean_mpn, api_key)
            part_rows = mouser_data.get("parts", [])
            if part_rows:
                for row in part_rows:
                    if not isinstance(row, dict):
                        continue
                    batch_rows.append(row)
            else:
                batch_rows.append({
                    "Requested MPN": clean_mpn,
                    "Description": f"No result found from {SOURCE_MOUSER}",
                })
        except Exception as ex:
            batch_rows.append({
                "Requested MPN": clean_mpn,
                "Description": f"Error: {ex}",
            })
    return batch_rows


def get_digikey_access_token(client_id, client_secret, use_sandbox=False, timeout=30, scope=None):
    cache_key = (str(client_id).strip(), bool(use_sandbox), str(scope or "").strip())
    cached = _DIGIKEY_TOKEN_CACHE.get(cache_key, {})
    if cached and float(cached.get("expires_at", 0)) > time.time():
        return str(cached.get("token", ""))

    host = "sandbox-api.digikey.com" if use_sandbox else "api.digikey.com"
    url = f"https://{host}/v1/oauth2/token"
    form_data = {
        "client_id": str(client_id).strip(),
        "client_secret": str(client_secret).strip(),
        "grant_type": "client_credentials",
    }
    if scope:
        form_data["scope"] = str(scope).strip()
    payload = parse.urlencode(form_data).encode("utf-8")
    req = request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            token_data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        raise RuntimeError(f"{SOURCE_DIGIKEY} token error {e.code}: {detail[:400]}") from e
    token = token_data.get("access_token")
    if not token:
        raise ValueError(f"{SOURCE_DIGIKEY} access token not returned.")
    expires_in = int(token_data.get("expires_in", 900) or 900)
    _DIGIKEY_TOKEN_CACHE[cache_key] = {
        "token": str(token),
        "expires_at": time.time() + max(60, expires_in - 30),
    }
    return token


def format_source_b_error(ex):
    msg = str(ex)
    msg_l = msg.lower()
    if "401" in msg and "invalid clientid" in msg_l:
        return (
            f"{SOURCE_DIGIKEY} returned 401 invalid client id. If Sandbox is ON, you must use sandbox-approved "
            f"{SOURCE_DIGIKEY} credentials. If you only have production credentials, turn Sandbox OFF."
        )
    if "403" in msg and "not authorized to perform this request" in msg_l:
        return (
            f"{SOURCE_DIGIKEY} returned 403 Forbidden. Use your own approved {SOURCE_DIGIKEY} API app credentials, "
            "ensure Product Information API access is enabled, and make sure Sandbox/Production mode "
            "matches your credential type."
        )
    return f"{SOURCE_DIGIKEY} fetch issue: {msg}"


def fetch_digikey_part_data(
    part_number,
    client_id,
    client_secret,
    site="IN",
    currency="INR",
    language="en",
    use_sandbox=False,
    timeout=30,
    scope=None,
):
    def _keyword_candidates(raw_part):
        raw = str(raw_part or "").strip()
        cands = [raw]
        compact = raw.replace(" ", "")
        if compact not in cands:
            cands.append(compact)
        alnum = re.sub(r"[^A-Za-z0-9]", "", raw)
        if alnum and alnum not in cands:
            cands.append(alnum)
        lz = raw.lstrip("0")
        if lz and lz not in cands:
            cands.append(lz)
        return [c for c in cands if c]

    def _norm_mpn(value):
        return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())

    def _pick_best_digikey_product(products, requested_mpn):
        if not isinstance(products, list):
            return {}
        req_norm = _norm_mpn(requested_mpn)
        best = {}
        best_score = -1
        for prod in products:
            if not isinstance(prod, dict):
                continue
            mfr_norm = _norm_mpn(prod.get("ManufacturerPartNumber", ""))
            dk_norm = _norm_mpn(prod.get("DigiKeyPartNumber", ""))
            score = 0
            if req_norm and mfr_norm == req_norm:
                score = 100
            elif req_norm and req_norm in mfr_norm:
                score = 80
            elif req_norm and dk_norm == req_norm:
                score = 60
            elif req_norm and req_norm in dk_norm:
                score = 40
            elif mfr_norm:
                score = 10
            if score > best_score:
                best = prod
                best_score = score
        return best if isinstance(best, dict) else {}

    try:
        token = get_digikey_access_token(
            client_id,
            client_secret,
            use_sandbox=use_sandbox,
            timeout=timeout,
            scope=scope,
        )
        host = "sandbox-api.digikey.com" if use_sandbox else "api.digikey.com"
    except Exception as ex:
        # If user toggles sandbox with production credentials, auto-retry production once.
        if use_sandbox and "invalid clientid" in str(ex).lower():
            token = get_digikey_access_token(
                client_id,
                client_secret,
                use_sandbox=False,
                timeout=timeout,
                scope=scope,
            )
            host = "api.digikey.com"
        else:
            raise
    locale_attempts = []
    for one_site, one_currency, one_language in [
        (site, currency, language),
        ("US", "USD", "en"),
        ("IN", "INR", "en"),
    ]:
        key = (str(one_site).strip(), str(one_currency).strip(), str(one_language).strip())
        if key not in locale_attempts:
            locale_attempts.append(key)

    product = {}
    keyword_errors = []
    for one_site, one_currency, one_language in locale_attempts:
        common_headers = {
            "Authorization": f"Bearer {token}",
            "X-DIGIKEY-Client-Id": str(client_id).strip(),
            "X-DIGIKEY-Locale-Site": one_site,
            "X-DIGIKEY-Locale-Currency": one_currency,
            "X-DIGIKEY-Locale-Language": one_language,
            "Accept": "application/json",
        }

        # 1) Exact productdetails path
        encoded_part = parse.quote(str(part_number).strip())
        details_url = f"https://{host}/products/v4/search/{encoded_part}/productdetails"
        details_req = request.Request(url=details_url, headers=common_headers, method="GET")
        try:
            with request.urlopen(details_req, timeout=timeout) as resp:
                details_data = json.loads(resp.read().decode("utf-8") or "{}")
            possible = details_data.get("Product", details_data) if isinstance(details_data, dict) else {}
            if isinstance(possible, dict) and any(possible.values()):
                product = possible
                break
        except HTTPError:
            product = {}

        # 2) Keyword fallback (more tolerant for MPN formats)
        keyword_url = f"https://{host}/products/v4/search/keyword"
        for kw in _keyword_candidates(part_number):
            keyword_payload = {
                "Keywords": kw,
                "RecordCount": 25,
            }
            keyword_req = request.Request(
                url=keyword_url,
                data=json.dumps(keyword_payload).encode("utf-8"),
                headers={**common_headers, "Content-Type": "application/json"},
                method="POST",
            )
            try:
                with request.urlopen(keyword_req, timeout=timeout) as resp:
                    key_data = json.loads(resp.read().decode("utf-8") or "{}")
                products = key_data.get("Products", []) if isinstance(key_data, dict) else []
                picked = _pick_best_digikey_product(products, part_number)
                if picked:
                    product = picked
                    break
            except HTTPError as e:
                detail = ""
                try:
                    detail = e.read().decode("utf-8")
                except Exception:
                    detail = str(e)
                keyword_errors.append(f"{one_site}/{one_currency}/{kw}: {e.code} {detail[:120]}")
        if product:
            break

    if not product and keyword_errors:
        raise RuntimeError(f"{SOURCE_DIGIKEY} keyword error: {' | '.join(keyword_errors[:3])}")

    if not isinstance(product, dict):
        product = {}

    def _dk_text(val):
        if isinstance(val, dict):
            return str(
                val.get("Name")
                or val.get("Value")
                or val.get("Status")
                or val.get("Text")
                or ""
            ).strip()
        return str(val or "").strip()

    def _digikey_lifecycle(prod, params):
        direct = _dk_text(prod.get("ProductStatus")) or _dk_text(prod.get("PartStatus"))
        if direct:
            return direct
        for p in params or []:
            if not isinstance(p, dict):
                continue
            key = str(p.get("ParameterText", "") or p.get("Parameter", "")).strip().lower()
            val = str(p.get("ValueText", "") or p.get("Value", "")).strip()
            if val and any(k in key for k in ["status", "lifecycle", "part status", "life cycle"]):
                return val
        for k, v in (prod or {}).items():
            key = str(k).strip().lower()
            val = _dk_text(v)
            if val and any(x in key for x in ["status", "lifecycle"]) and len(val) < 80:
                return val
        return ""

    parameters = product.get("Parameters") if isinstance(product.get("Parameters"), list) else []
    lifecycle_status = _digikey_lifecycle(product, parameters)

    manufacturer_pn = _dk_text(product.get("ManufacturerPartNumber", ""))
    digikey_pn = _dk_text(product.get("DigiKeyPartNumber", ""))
    preferred_ref = manufacturer_pn or digikey_pn

    part_row = {
        "Requested MPN": str(part_number).strip(),
        "Supplier Part Number": preferred_ref,
        "Manufacturer Part Number": manufacturer_pn,
        "Digi-Key Part Number": digikey_pn,
        "Manufacturer": _dk_text((product.get("Manufacturer", {}) or {}).get("Name", "") if isinstance(product.get("Manufacturer", {}), dict) else product.get("Manufacturer", "")),
        "Description": _dk_text(product.get("ProductDescription", "") or product.get("Description", "")),
        "Category": _dk_text((product.get("Category", {}) or {}).get("Name", "") if isinstance(product.get("Category", {}), dict) else product.get("Category", "")),
        "Lifecycle Status": lifecycle_status,
        "Quantity Available": _dk_text(product.get("QuantityAvailable", "")),
        "Lead Time Weeks": _dk_text(product.get("ManufacturerLeadWeeks", "")),
        "Product URL": _dk_text(product.get("ProductUrl", "")),
        "Data Sheet URL": _dk_text(product.get("DatasheetUrl", "")),
        "RoHS": _dk_text(product.get("RoHSStatus", "")),
    }

    pricing_rows = []
    for p in (product.get("StandardPricing") or []):
        if not isinstance(p, dict):
            continue
        pricing_rows.append(
            {
                "Requested MPN": str(part_number).strip(),
                "Supplier Part Number": part_row["Supplier Part Number"],
                "Break Quantity": p.get("BreakQuantity", ""),
                "Unit Price": p.get("UnitPrice", ""),
                "Total Price": p.get("TotalPrice", ""),
            }
        )

    attribute_rows = []
    for a in parameters:
        if not isinstance(a, dict):
            continue
        attribute_rows.append(
            {
                "Requested MPN": str(part_number).strip(),
                "Supplier Part Number": part_row["Supplier Part Number"],
                "Attribute": a.get("ParameterText", "") or a.get("Parameter", ""),
                "Value": a.get("ValueText", "") or a.get("Value", ""),
            }
        )

    docs_rows = []
    if part_row["Data Sheet URL"]:
        docs_rows.append({"Requested MPN": str(part_number).strip(), "Type": "Datasheet", "URL": part_row["Data Sheet URL"]})
    if part_row["Product URL"]:
        docs_rows.append({"Requested MPN": str(part_number).strip(), "Type": "Product Detail", "URL": part_row["Product URL"]})

    parts_list = [part_row] if any(part_row.values()) else []
    parts_list = add_enrichment_fields(parts_list, attribute_rows, pricing_rows)

    return {
        "parts": parts_list,
        "pricing": pricing_rows,
        "attributes": attribute_rows,
        "documents": docs_rows,
    }


def fetch_digikey_batch(mpns, client_id, client_secret, site="US", currency="USD", language="en", use_sandbox=False, scope=None):
    rows = []
    for mpn in mpns:
        clean = str(mpn).strip()
        if not clean:
            continue
        try:
            out = fetch_digikey_part_data(
                clean,
                client_id=client_id,
                client_secret=client_secret,
                site=site,
                currency=currency,
                language=language,
                use_sandbox=use_sandbox,
                scope=scope,
            )
            if out.get("parts"):
                rows.extend(out["parts"])
            else:
                rows.append({"Requested MPN": clean, "Description": f"No result found from {SOURCE_DIGIKEY}"})
        except Exception as ex:
            rows.append({"Requested MPN": clean, "Description": f"Error: {ex}"})
    return rows


def lifecycle_to_risk(lifecycle_text):
    t = str(lifecycle_text or "").lower()
    if any(x in t for x in ["obsolete", "eol", "end of life", "discontinued"]):
        return "High Risk"
    if any(x in t for x in ["nrnd", "not recommended"]):
        return "Medium Risk"
    if any(x in t for x in ["active", "production"]):
        return "Low Risk"
    return "Unknown"


def get_nexar_access_token(client_id, client_secret, scope="supply.domain", timeout=30):
    token_url = "https://identity.nexar.com/connect/token"
    form = {
        "grant_type": "client_credentials",
        "client_id": str(client_id).strip(),
        "client_secret": str(client_secret).strip(),
    }
    if scope:
        form["scope"] = scope
    req = request.Request(
        url=token_url,
        data=parse.urlencode(form).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Nexar token error {e.code}: {body[:400]}") from e
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Nexar token missing access_token")
    return token


def extract_lifecycle_from_specs(specs):
    if not isinstance(specs, list):
        return ""
    for s in specs:
        if not isinstance(s, dict):
            continue
        attr = s.get("attribute", {}) if isinstance(s.get("attribute"), dict) else {}
        key = f"{attr.get('name', '')} {attr.get('shortname', '')}".lower()
        if "life" in key or "status" in key:
            return str(s.get("displayValue") or s.get("value") or "").strip()
    return ""


def fetch_nexar_part_data(mpn, client_id, client_secret, timeout=30):
    token = get_nexar_access_token(client_id, client_secret, timeout=timeout)
    gql_url = "https://api.nexar.com/graphql"
    query = """
    query ($q: String!) {
      supSearch(q: $q, limit: 1) {
        results {
          part {
            mpn
            shortDescription
            manufacturer { name }
            specs {
              attribute { name shortname }
              displayValue
              value
            }
          }
        }
      }
    }
    """
    payload = {"query": query, "variables": {"q": str(mpn).strip()}}
    req = request.Request(
        url=gql_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Nexar query error {e.code}: {body[:400]}") from e

    results = (((data.get("data") or {}).get("supSearch") or {}).get("results") or [])
    if not results or not isinstance(results[0], dict):
        return {"parts": [], "pricing": [], "attributes": [], "documents": []}

    part = results[0].get("part") if isinstance(results[0].get("part"), dict) else {}
    specs = part.get("specs") if isinstance(part.get("specs"), list) else []
    lifecycle = extract_lifecycle_from_specs(specs)
    row = {
        "Requested MPN": str(mpn).strip(),
        "Supplier Part Number": part.get("mpn", ""),
        "Manufacturer Part Number": part.get("mpn", ""),
        "Manufacturer": (part.get("manufacturer", {}) or {}).get("name", "") if isinstance(part.get("manufacturer"), dict) else "",
        "Description": part.get("shortDescription", ""),
        "Category": "",
        "Lifecycle Status": lifecycle,
        "Quantity Available": "",
        "Lead Time Weeks": "",
        "Product URL": "",
        "Data Sheet URL": "",
    }
    attr_rows = []
    for s in specs:
        if not isinstance(s, dict):
            continue
        attr = s.get("attribute", {}) if isinstance(s.get("attribute"), dict) else {}
        name = attr.get("name") or attr.get("shortname") or ""
        val = s.get("displayValue") or s.get("value") or ""
        if name or val:
            attr_rows.append(
                {
                    "Requested MPN": str(mpn).strip(),
                    "Supplier Part Number": row["Supplier Part Number"],
                    "Attribute": str(name),
                    "Value": str(val),
                }
            )
    parts_list = add_enrichment_fields([row], attr_rows, [])
    return {"parts": parts_list, "pricing": [], "attributes": attr_rows, "documents": []}


def get_similar_reference_parts(target_mpn, max_items=10):
    """
    Suggest similar reference MPNs from local DB using prefix + fuzzy match.
    """
    if not DB_PATH.exists():
        return []
    try:
        with sqlite3.connect(DB_PATH) as conn:
            pool = pd.read_sql("SELECT DISTINCT mpn FROM sections", conn)["mpn"].dropna().astype(str).tolist()
    except Exception:
        return []

    q = str(target_mpn).strip().upper()
    if not q:
        return []

    prefix_matches = [p for p in pool if p.upper().startswith(q[:5])]
    fuzzy_matches = difflib.get_close_matches(q, [p.upper() for p in pool], n=max_items, cutoff=0.45)
    fuzzy_original = [p for p in pool if p.upper() in fuzzy_matches]

    combined = []
    seen = set()
    for p in prefix_matches + fuzzy_original:
        if p not in seen and p.upper() != q:
            combined.append(p)
            seen.add(p)
        if len(combined) >= max_items:
            break
    return combined


def ensure_live_cache_table():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS live_part_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mpn TEXT NOT NULL,
                selected_source TEXT NOT NULL,
                fetched_at_utc TEXT NOT NULL,
                lifecycle_status TEXT,
                stock_details TEXT,
                manufacturer TEXT,
                description TEXT,
                data_json TEXT NOT NULL
            );
            """
        )
        conn.commit()


def save_live_result_to_db(mpn, selected_source, payload, on_exists="continue"):
    """
    Save chosen live-result payload into local DB for traceability and reuse.
    """
    part = {}
    if isinstance(payload, dict):
        parts = payload.get("parts", [])
        if isinstance(parts, list) and parts and isinstance(parts[0], dict):
            part = parts[0]
    with sqlite3.connect(DB_PATH) as conn:
        exists_df = pd.read_sql(
            "SELECT COUNT(1) AS cnt FROM live_part_cache WHERE mpn = ? AND selected_source = ?",
            conn,
            params=(str(mpn).strip(), selected_source),
        )
        exists = int(exists_df.iloc[0]["cnt"]) > 0 if not exists_df.empty else False

        if exists and on_exists == "skip":
            return False
        if exists and on_exists == "overwrite":
            conn.execute(
                "DELETE FROM live_part_cache WHERE mpn = ? AND selected_source = ?",
                (str(mpn).strip(), selected_source),
            )
        conn.execute(
            """
            INSERT INTO live_part_cache
            (mpn, selected_source, fetched_at_utc, lifecycle_status, stock_details, manufacturer, description, data_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(mpn).strip(),
                selected_source,
                datetime.now(timezone.utc).isoformat(),
                str(part.get("Lifecycle Status", "")),
                str(part.get("Stock", "") or part.get("Quantity Available", "")),
                str(part.get("Manufacturer", "")),
                str(part.get("Description", "")),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        conn.commit()
    return True


def ensure_unified_parts_table():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unified_part_cache (
                mpn TEXT PRIMARY KEY,
                manufacturer TEXT,
                manufacturer_part_number TEXT,
                supplier_part_number TEXT,
                description TEXT,
                category TEXT,
                lifecycle_status TEXT,
                rohs TEXT,
                stock TEXT,
                datasheet_url TEXT,
                product_url TEXT,
                msd_level TEXT,
                reflow_soldering_temperature TEXT,
                thermal_cycle TEXT,
                wave_soldering_temperature TEXT,
                lsl_details TEXT,
                package_details TEXT,
                price_details TEXT,
                operating_temperature TEXT,
                component_thickness TEXT,
                reach TEXT,
                reflow_soldering_time TEXT,
                wave_soldering_time TEXT,
                body_mark TEXT,
                source_trace TEXT,
                updated_at_utc TEXT NOT NULL
            );
            """
        )
        existing_cols = {
            str(r[1]).strip()
            for r in conn.execute("PRAGMA table_info(unified_part_cache)").fetchall()
        }
        required = {
            "msd_level": "TEXT",
            "reflow_soldering_temperature": "TEXT",
            "thermal_cycle": "TEXT",
            "wave_soldering_temperature": "TEXT",
            "lsl_details": "TEXT",
            "package_details": "TEXT",
            "price_details": "TEXT",
            "operating_temperature": "TEXT",
            "component_thickness": "TEXT",
            "reach": "TEXT",
            "reflow_soldering_time": "TEXT",
            "wave_soldering_time": "TEXT",
            "body_mark": "TEXT",
        }
        for col, col_type in required.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE unified_part_cache ADD COLUMN {col} {col_type}")
        conn.commit()


def ensure_scrub_queue_tables():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scrub_queue (
                mpn TEXT PRIMARY KEY,
                manufacturer TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                source TEXT,
                last_error TEXT,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scrub_queue_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_mpn TEXT,
                last_status TEXT,
                processed_count INTEGER NOT NULL DEFAULT 0,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO scrub_queue_state (id, last_mpn, last_status, processed_count, updated_at_utc)
            VALUES (1, '', '', 0, ?)
            """,
            (datetime.now(timezone.utc).isoformat(),),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scrub_queue_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mpn TEXT NOT NULL,
                step TEXT NOT NULL,
                status TEXT NOT NULL,
                source TEXT,
                message TEXT,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()


def log_scrub_history(mpn, step, status, message="", source="", conn=None):
    mpn = str(mpn or "").strip()
    if not mpn:
        return
    params = (
        mpn,
        str(step or "").strip() or "unknown",
        str(status or "").strip() or "info",
        str(source or "").strip(),
        str(message or "").strip(),
        datetime.now(timezone.utc).isoformat(),
    )
    if conn is not None:
        conn.execute(
            """
            INSERT INTO scrub_queue_history (mpn, step, status, source, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        return
    for _ in range(3):
        try:
            with sqlite3.connect(DB_PATH, timeout=30) as write_conn:
                write_conn.execute(
                    """
                    INSERT INTO scrub_queue_history (mpn, step, status, source, message, created_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                write_conn.commit()
                return
        except sqlite3.OperationalError as ex:
            if "locked" not in str(ex).lower():
                raise
            time.sleep(0.2)
    with sqlite3.connect(DB_PATH, timeout=30) as write_conn:
        write_conn.execute(
            """
            INSERT INTO scrub_queue_history (mpn, step, status, source, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        write_conn.commit()


def ensure_base_scraper_tables():
    """
    Ensure core scraper tables exist so UI queries don't crash on a fresh DB.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mpn TEXT NOT NULL,
                section_name TEXT NOT NULL,
                section_order INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mpn TEXT NOT NULL,
                section_name TEXT NOT NULL,
                title TEXT NOT NULL,
                table_index INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id INTEGER NOT NULL,
                row_index INTEGER NOT NULL,
                col_index INTEGER NOT NULL,
                header TEXT,
                value TEXT,
                FOREIGN KEY(table_id) REFERENCES tables(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()


def _table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name).strip(),),
    ).fetchone()
    return bool(row)


def get_available_db_mpns():
    """
    Return MPN list from whichever tables are currently available.
    Preference: sections -> unified_part_cache -> live_part_cache.
    """
    candidates = []
    with sqlite3.connect(DB_PATH) as conn:
        if _table_exists(conn, "sections"):
            try:
                df = pd.read_sql("SELECT DISTINCT mpn FROM sections ORDER BY mpn", conn)
                candidates.extend(df.get("mpn", []).tolist())
            except Exception:
                pass
        if _table_exists(conn, "unified_part_cache"):
            try:
                df = pd.read_sql("SELECT DISTINCT mpn FROM unified_part_cache ORDER BY mpn", conn)
                candidates.extend(df.get("mpn", []).tolist())
            except Exception:
                pass
        if _table_exists(conn, "live_part_cache"):
            try:
                df = pd.read_sql("SELECT DISTINCT mpn FROM live_part_cache ORDER BY mpn", conn)
                candidates.extend(df.get("mpn", []).tolist())
            except Exception:
                pass
    return sorted(dict.fromkeys([str(x).strip() for x in candidates if str(x).strip()]))


def _first_non_empty(values):
    for v in values:
        if not _is_effectively_empty(v):
            return str(v).strip()
    return ""


def _as_text(value):
    if _is_effectively_empty(value):
        return ""
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value).strip()


def upsert_unified_part_for_mpn(mpn):
    mpn = normalize_mpn(mpn)
    if not mpn:
        return

    ensure_unified_parts_table()
    unified = {
        "mpn": mpn, "manufacturer": "", "manufacturer_part_number": "", "supplier_part_number": "",
        "description": "", "category": "", "lifecycle_status": "", "rohs": "", "stock": "",
        "datasheet_url": "", "product_url": "", "msd_level": "",
        "reflow_soldering_temperature": "", "thermal_cycle": "",
        "wave_soldering_temperature": "", "lsl_details": "", "package_details": "", "price_details": "", "operating_temperature": "",
        "component_thickness": "", "reach": "", "reflow_soldering_time": "", "wave_soldering_time": "", "body_mark": "",
    }
    used_sources = []
    with sqlite3.connect(DB_PATH) as conn:
        current = conn.execute(
            """
            SELECT manufacturer, manufacturer_part_number, supplier_part_number, description, category,
                   lifecycle_status, rohs, stock, datasheet_url, product_url, msd_level,
                   reflow_soldering_temperature, thermal_cycle, wave_soldering_temperature,
                   lsl_details, package_details, price_details, operating_temperature,
                   component_thickness, reach, reflow_soldering_time, wave_soldering_time, body_mark, source_trace
            FROM unified_part_cache WHERE mpn=?
            """,
            (mpn,),
        ).fetchone()
        if current:
            current_cols = [
                "manufacturer", "manufacturer_part_number", "supplier_part_number", "description", "category",
                "lifecycle_status", "rohs", "stock", "datasheet_url", "product_url", "msd_level",
                "reflow_soldering_temperature", "thermal_cycle", "wave_soldering_temperature",
                "lsl_details", "package_details", "price_details", "operating_temperature",
                "component_thickness", "reach", "reflow_soldering_time", "wave_soldering_time", "body_mark", "source_trace",
            ]
            existing_row = dict(zip(current_cols, current))
            for k in unified.keys():
                if k == "mpn":
                    continue
                val = _as_text(existing_row.get(k, "")).strip()
                unified[k] = "" if _is_effectively_empty(val) else val
            prior_trace = _as_text(existing_row.get("source_trace", "")).strip()
            if prior_trace:
                used_sources.extend([s.strip() for s in prior_trace.split(",") if s.strip()])

        live_df = pd.read_sql(
            "SELECT selected_source, data_json, fetched_at_utc FROM live_part_cache WHERE mpn=? ORDER BY fetched_at_utc DESC",
            conn,
            params=(mpn,),
        )
        source_priority = {
            SOURCE_DIGIKEY: 1,
            "Source B": 1,
            SOURCE_MOUSER: 2,
            "Source A": 2,
        }
        if not live_df.empty:
            live_df["priority"] = live_df["selected_source"].map(source_priority).fillna(99)
            live_df = live_df.sort_values(["priority", "fetched_at_utc"], ascending=[True, False])
        for _, row in live_df.iterrows():
            source = str(row.get("selected_source", "")).strip() or "Live"
            try:
                payload = json.loads(row.get("data_json") or "{}")
            except Exception:
                payload = {}
            parts = payload.get("parts", []) if isinstance(payload, dict) else []
            attrs = payload.get("attributes", []) if isinstance(payload, dict) else []
            p = parts[0] if isinstance(parts, list) and parts and isinstance(parts[0], dict) else {}
            if not p:
                continue
            def _set_if_missing(field_name, value):
                if _is_effectively_empty(unified.get(field_name, "")) and not _is_effectively_empty(value):
                    unified[field_name] = _as_text(value)

            _set_if_missing("manufacturer", p.get("Manufacturer", ""))
            _set_if_missing("manufacturer_part_number", p.get("Manufacturer Part Number", ""))
            _set_if_missing("supplier_part_number", p.get("Supplier Part Number", ""))
            _set_if_missing("description", p.get("Description", ""))
            _set_if_missing("category", p.get("Category", ""))
            _set_if_missing("lifecycle_status", _first_non_empty([p.get("Lifecycle Status", ""), p.get("Part Status", ""), p.get("Product Status", "")]))
            if _is_effectively_empty(unified["lifecycle_status"]):
                _set_if_missing("lifecycle_status", _extract_attribute_value(attrs, "lifecycle", "part status", "product status", "status"))
            _set_if_missing("rohs", _first_non_empty([p.get("ROHS", ""), p.get("RoHS", "")]))
            _set_if_missing("stock", _first_non_empty([p.get("Stock", ""), p.get("Quantity Available", "")]))
            _set_if_missing("datasheet_url", p.get("Data Sheet URL", ""))
            _set_if_missing("product_url", p.get("Product URL", ""))
            _set_if_missing("msd_level", _first_non_empty([p.get("MSD LEVEL", ""), _extract_attribute_value(attrs, "msl", "msd", "moisture sensitivity")]))
            _set_if_missing("reflow_soldering_temperature", _first_non_empty([p.get("REFLOW SOLDERING TEMPERATURE", ""), _extract_attribute_value(attrs, "reflow temperature", "reflow soldering temperature", "reflow")]))
            _set_if_missing("thermal_cycle", _first_non_empty([p.get("THERMAL CYCLE", ""), _extract_attribute_value(attrs, "thermal cycle", "reflow cycle", "number of reflow")]))
            _set_if_missing("wave_soldering_temperature", _first_non_empty([p.get("WAVE SOLDERING TEMPERATURE", ""), _extract_attribute_value(attrs, "wave soldering temperature", "wave solder")]))
            _set_if_missing("lsl_details", _first_non_empty([p.get("LSL DETAILS", ""), _extract_attribute_value(attrs, "lsl", "lead surface", "land side")]))
            _set_if_missing("package_details", _first_non_empty([p.get("PACKAGE", ""), _extract_attribute_value(attrs, "package", "case", "mount")]))
            _set_if_missing("price_details", _first_non_empty([p.get("PRICE DETAILS", ""), _extract_price_summary(payload.get("pricing", []) if isinstance(payload, dict) else [])]))
            _set_if_missing("operating_temperature", _first_non_empty([p.get("OPERATING TEMPERATURE", ""), _extract_attribute_value(attrs, "operating temperature", "temperature range", "operating temp")]))
            _set_if_missing("component_thickness", _first_non_empty([p.get("COMPONENT THICKNESS", ""), _extract_component_thickness(attrs)]))
            _set_if_missing("reach", _first_non_empty([p.get("REACH", ""), _extract_attribute_value(attrs, "reach", "reach compliance", "compliance")]))
            if not str(unified["reach"]).strip():
                reach_candidates = []
                for a in attrs if isinstance(attrs, list) else []:
                    if not isinstance(a, dict):
                        continue
                    nm = str(a.get("Attribute", "")).strip().lower()
                    vv = str(a.get("Value", "")).strip()
                    if not vv:
                        continue
                    if "compliance" in nm or "reach" in nm:
                        reach_candidates.append(vv)
                if reach_candidates:
                    unified["reach"] = "; ".join(dict.fromkeys(reach_candidates))
            _set_if_missing("reflow_soldering_time", _first_non_empty([p.get("REFLOW SOLDERING TIME", ""), _extract_attribute_value(attrs, "reflow time", "reflow soldering time", "time at reflow")]))
            _set_if_missing("wave_soldering_time", _first_non_empty([p.get("WAVE SOLDERING TIME", ""), _extract_attribute_value(attrs, "wave time", "wave soldering time")]))
            _set_if_missing("body_mark", _first_non_empty([p.get("BODY MARK", ""), _extract_attribute_value(attrs, "body mark", "marking")]))
            _set_if_missing("rohs", _extract_attribute_value(attrs, "rohs", "rohs status"))
            pricing_rows = payload.get("pricing", []) if isinstance(payload, dict) else []
            _set_if_missing("price_details", _extract_price_summary(pricing_rows))
            if not str(unified["datasheet_url"]).strip():
                docs = payload.get("documents", []) if isinstance(payload, dict) else []
                if isinstance(docs, list):
                    for d in docs:
                        if not isinstance(d, dict):
                            continue
                        if "datasheet" in str(d.get("Type", "")).strip().lower():
                            unified["datasheet_url"] = _as_text(d.get("URL", ""))
                            if unified["datasheet_url"]:
                                break
            used_sources.append(source)

        scraper_map = {}
        try:
            cell_df = pd.read_sql(
                "SELECT c.header, c.value FROM cells c JOIN tables t ON c.table_id=t.id WHERE t.mpn LIKE ? AND TRIM(COALESCE(c.value,''))<>''",
                conn,
                params=(f"%{mpn}%",),
            )
            for _, r in cell_df.iterrows():
                h = str(r.get("header", "")).strip().lower()
                v = str(r.get("value", "")).strip()
                if h and v and h not in scraper_map:
                    scraper_map[h] = v
        except Exception:
            scraper_map = {}

        def from_scraper(*keys):
            normalized_map = {str(k).strip().lower(): str(v).strip() for k, v in scraper_map.items()}
            for k in keys:
                k_norm = str(k).strip().lower()
                if normalized_map.get(k_norm, ""):
                    return normalized_map[k_norm]
            # Fuzzy contains match to support headers like "Maximum Reflow Temperature"
            for k in keys:
                k_norm = str(k).strip().lower()
                if not k_norm:
                    continue
                for h, v in normalized_map.items():
                    if k_norm in h and str(v).strip():
                        return str(v).strip()
            return ""

        unified["manufacturer"] = unified["manufacturer"] or from_scraper("manufacturer")
        unified["manufacturer_part_number"] = unified["manufacturer_part_number"] or from_scraper("manufacturer part number", "mfr part number")
        unified["supplier_part_number"] = unified["supplier_part_number"] or from_scraper("supplier part number", "part number")
        unified["description"] = unified["description"] or from_scraper("description")
        unified["category"] = unified["category"] or from_scraper("category", "product type")
        unified["lifecycle_status"] = unified["lifecycle_status"] or from_scraper("lifecycle status", "part lifecycle")
        unified["rohs"] = unified["rohs"] or from_scraper("rohs", "rohs (2015/863)")
        unified["stock"] = unified["stock"] or from_scraper("stock", "quantity available")
        unified["datasheet_url"] = unified["datasheet_url"] or from_scraper("datasheet", "data sheet url")
        unified["product_url"] = unified["product_url"] or from_scraper("product url")
        unified["msd_level"] = unified["msd_level"] or from_scraper(
            "msd level", "msl", "moisture sensitivity level", "moisture sensitive level"
        )
        unified["reflow_soldering_temperature"] = unified["reflow_soldering_temperature"] or from_scraper(
            "reflow soldering temperature", "reflow temperature", "maximum reflow temperature"
        )
        unified["thermal_cycle"] = unified["thermal_cycle"] or from_scraper(
            "thermal cycle", "number of reflow cycle", "reflow cycle"
        )
        unified["wave_soldering_temperature"] = unified["wave_soldering_temperature"] or from_scraper(
            "wave soldering temperature", "wave temperature", "wave solder"
        )
        unified["lsl_details"] = unified["lsl_details"] or from_scraper("lsl details", "lsl")
        unified["package_details"] = unified["package_details"] or from_scraper("package", "package/case", "packaging")
        unified["price_details"] = unified["price_details"] or from_scraper("price details", "price")
        unified["operating_temperature"] = unified["operating_temperature"] or from_scraper("operating temperature", "temperature range")
        unified["component_thickness"] = unified["component_thickness"] or from_scraper("component thickness", "thickness", "height")
        unified["reach"] = unified["reach"] or from_scraper("reach", "reach status")
        unified["reflow_soldering_time"] = unified["reflow_soldering_time"] or from_scraper("reflow soldering time", "reflow time")
        unified["wave_soldering_time"] = unified["wave_soldering_time"] or from_scraper("wave soldering time", "wave time")
        unified["body_mark"] = unified["body_mark"] or from_scraper("body mark", "marking")
        if scraper_map:
            used_sources.append("ScraperDB")

        for k in list(unified.keys()):
            unified[k] = _as_text(unified.get(k, "")).strip()

        conn.execute(
            """
            INSERT OR REPLACE INTO unified_part_cache
            (mpn, manufacturer, manufacturer_part_number, supplier_part_number, description, category, lifecycle_status, rohs, stock, datasheet_url, product_url, msd_level, reflow_soldering_temperature, thermal_cycle, wave_soldering_temperature, lsl_details, package_details, price_details, operating_temperature, component_thickness, reach, reflow_soldering_time, wave_soldering_time, body_mark, source_trace, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                unified["mpn"], unified["manufacturer"], unified["manufacturer_part_number"], unified["supplier_part_number"],
                unified["description"], unified["category"], unified["lifecycle_status"], unified["rohs"], unified["stock"],
                unified["datasheet_url"], unified["product_url"], unified["msd_level"], unified["reflow_soldering_temperature"],
                unified["thermal_cycle"], unified["wave_soldering_temperature"], unified["lsl_details"], unified["package_details"], unified["price_details"],
                unified["operating_temperature"], unified["component_thickness"], unified["reach"], unified["reflow_soldering_time"],
                unified["wave_soldering_time"], unified["body_mark"], ", ".join(dict.fromkeys([s for s in used_sources if s])),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()


def rebuild_unified_cache_for_all_mpns():
    ensure_unified_parts_table()
    mpns = set()
    with sqlite3.connect(DB_PATH) as conn:
        try:
            s_df = pd.read_sql("SELECT DISTINCT mpn FROM sections", conn)
            mpns.update([str(x).strip() for x in s_df.get("mpn", []) if str(x).strip()])
        except Exception:
            pass
        try:
            l_df = pd.read_sql("SELECT DISTINCT mpn FROM live_part_cache", conn)
            mpns.update([str(x).strip() for x in l_df.get("mpn", []) if str(x).strip()])
        except Exception:
            pass
    for one in sorted(mpns):
        upsert_unified_part_for_mpn(one)


def _to_float_price(value):
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    cleaned = re.sub(r"[^0-9.]", "", txt.replace(",", ""))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def extract_source_a_price(source_a_payload):
    try:
        pricing = source_a_payload.get("pricing", []) if isinstance(source_a_payload, dict) else []
        if pricing and isinstance(pricing[0], dict):
            return _to_float_price(pricing[0].get("Price"))
    except Exception:
        return None
    return None


def extract_source_b_price(source_b_payload):
    try:
        pricing = source_b_payload.get("pricing", []) if isinstance(source_b_payload, dict) else []
        if pricing and isinstance(pricing[0], dict):
            return _to_float_price(pricing[0].get("Unit Price"))
    except Exception:
        return None
    return None


def _lifecycle_rank(lifecycle):
    txt = str(lifecycle or "").lower()
    if "active" in txt:
        return 3
    if "nrnd" in txt or "not recommended" in txt:
        return 2
    if "obsolete" in txt or "eol" in txt or "discontinued" in txt:
        return 1
    return 0


def fetch_digikey_data(mpn, digikey_id, digikey_secret, digikey_scope=None, digikey_sandbox=False):
    """
    Digi-Key keyword search v4.
    Always returns at least: {"price": None, "lifecycle": None}
    """
    safe = {"price": None, "lifecycle": None, "manufacturer": None}
    try:
        token = get_digikey_access_token(
            digikey_id,
            digikey_secret,
            use_sandbox=digikey_sandbox,
            scope=digikey_scope,
        )
        host = "sandbox-api.digikey.com" if digikey_sandbox else "api.digikey.com"
        url = f"https://{host}/products/v4/search/keyword"
        payload = {"Keywords": str(mpn).strip(), "RecordCount": 25}
        req = request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "X-DIGIKEY-Client-Id": str(digikey_id).strip(),
                "X-DIGIKEY-Locale-Site": "IN",
                "X-DIGIKEY-Locale-Currency": "INR",
                "X-DIGIKEY-Locale-Language": "en",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        products = data.get("Products", []) if isinstance(data, dict) else []
        if not products:
            return safe
        req_norm = re.sub(r"[^A-Z0-9]", "", str(mpn or "").upper())
        def _score(prod):
            if not isinstance(prod, dict):
                return -1
            mfr_norm = re.sub(r"[^A-Z0-9]", "", str(prod.get("ManufacturerPartNumber", "")).upper())
            dk_norm = re.sub(r"[^A-Z0-9]", "", str(prod.get("DigiKeyPartNumber", "")).upper())
            if req_norm and mfr_norm == req_norm:
                return 100
            if req_norm and req_norm in mfr_norm:
                return 80
            if req_norm and dk_norm == req_norm:
                return 60
            if req_norm and req_norm in dk_norm:
                return 40
            return 0
        p0 = max([p for p in products if isinstance(p, dict)], key=_score, default={})
        if not isinstance(p0, dict) or not p0:
            return safe
        pricing = p0.get("StandardPricing", [])
        if pricing and isinstance(pricing[0], dict):
            safe["price"] = _to_float_price(pricing[0].get("UnitPrice"))
        safe["lifecycle"] = p0.get("ProductStatus")
        manufacturer = p0.get("Manufacturer")
        if isinstance(manufacturer, dict):
            safe["manufacturer"] = manufacturer.get("Name")
        return safe
    except Exception:
        return safe


def smart_compare(mpn, mouser_key, digikey_id, digikey_secret, digikey_scope=None, digikey_sandbox=False):
    result = {
        "MPN": str(mpn).strip(),
        "Mouser Price": None,
        "Mouser Lifecycle": None,
        "Digi-Key Price": None,
        "Digi-Key Lifecycle": None,
        "Best Source": None,
        "Best Price": None,
        "Best Lifecycle": None,
        "Mouser Error": "",
        "Digi-Key Error": "",
    }

    try:
        source_a_data = fetch_mouser_part_data(mpn, mouser_key)
        result["Mouser Price"] = extract_source_a_price(source_a_data)
        if source_a_data.get("parts"):
            result["Mouser Lifecycle"] = source_a_data["parts"][0].get("Lifecycle Status")
    except Exception as ex:
        result["Mouser Error"] = str(ex)

    try:
        dk = fetch_digikey_data(
            mpn,
            digikey_id=digikey_id,
            digikey_secret=digikey_secret,
            digikey_scope=digikey_scope,
            digikey_sandbox=digikey_sandbox,
        )
        result["Digi-Key Price"] = dk.get("price")
        result["Digi-Key Lifecycle"] = dk.get("lifecycle")
    except Exception as ex:
        result["Digi-Key Error"] = str(ex)

    candidates = [
        ("Mouser", result["Mouser Lifecycle"], result["Mouser Price"]),
        ("Digi-Key", result["Digi-Key Lifecycle"], result["Digi-Key Price"]),
    ]
    valid = [
        (name, lc, price)
        for name, lc, price in candidates
        if _lifecycle_rank(lc) > 0 or isinstance(price, (int, float))
    ]
    if valid:
        valid.sort(
            key=lambda x: (
                -_lifecycle_rank(x[1]),               # better lifecycle first
                x[2] if isinstance(x[2], (int, float)) else float("inf"),  # then lower price
            )
        )
        best = valid[0]
        result["Best Source"] = best[0]
        result["Best Lifecycle"] = best[1]
        result["Best Price"] = best[2]
    return result


def compare_suppliers_price(mpn, source_a_key, source_b_id, source_b_secret, source_b_scope=None, source_b_sandbox=False):
    # Backward-compatible wrapper
    result = smart_compare(
        mpn,
        mouser_key=source_a_key,
        digikey_id=source_b_id,
        digikey_secret=source_b_secret,
        digikey_scope=source_b_scope,
        digikey_sandbox=source_b_sandbox,
    )
    # legacy keys kept for older UI/data expectations
    result["Source A Price"] = result.get("Mouser Price")
    result["Source B Price"] = result.get("Digi-Key Price")
    result["Source A Error"] = result.get("Mouser Error", "")
    result["Source B Error"] = result.get("Digi-Key Error", "")
    return result


def build_comparison_details(result_row):
    """
    Build a human-readable detailed comparison table for UI.
    """
    mouser_lc = result_row.get("Mouser Lifecycle")
    digikey_lc = result_row.get("Digi-Key Lifecycle")
    mouser_price = result_row.get("Mouser Price")
    digikey_price = result_row.get("Digi-Key Price")
    mouser_rank = _lifecycle_rank(mouser_lc)
    digikey_rank = _lifecycle_rank(digikey_lc)

    details = [
        {
            "Metric": "Lifecycle",
            "Mouser": mouser_lc,
            "Digi-Key": digikey_lc,
            "Winner": "Mouser" if mouser_rank > digikey_rank else ("Digi-Key" if digikey_rank > mouser_rank else "Tie"),
        },
        {
            "Metric": "Lifecycle Rank",
            "Mouser": mouser_rank,
            "Digi-Key": digikey_rank,
            "Winner": "Mouser" if mouser_rank > digikey_rank else ("Digi-Key" if digikey_rank > mouser_rank else "Tie"),
        },
        {
            "Metric": "Price",
            "Mouser": mouser_price,
            "Digi-Key": digikey_price,
            "Winner": (
                "Mouser"
                if isinstance(mouser_price, (int, float)) and (not isinstance(digikey_price, (int, float)) or mouser_price < digikey_price)
                else ("Digi-Key" if isinstance(digikey_price, (int, float)) and (not isinstance(mouser_price, (int, float)) or digikey_price < mouser_price) else "Tie")
            ),
        },
        {
            "Metric": "Final Best Source",
            "Mouser": "",
            "Digi-Key": "",
            "Winner": result_row.get("Best Source"),
        },
    ]
    return pd.DataFrame(details)


def render_live_detail_window(parts_df, pricing_rows, attributes_rows, docs_rows):
    """
    Render a z2-like detail window from live supplier data.
    """
    st.markdown("### 📋 Live Detail Window")
    if parts_df.empty:
        st.info("No part details available to render.")
        return

    first = parts_df.iloc[0].to_dict()
    tags = []
    for key in ["Lifecycle Status", "Category", "Manufacturer", "ROHS"]:
        v = str(first.get(key, "")).strip()
        if v:
            tags.append(v)

    if tags:
        tag_html = " ".join(
            [
                f"<span style='display:inline-block;background:#2b579a;color:white;padding:4px 10px;border-radius:12px;margin:3px;font-size:13px;'>{t}</span>"
                for t in tags[:8]
            ]
        )
        st.markdown("#### Tags")
        st.markdown(tag_html, unsafe_allow_html=True)

    attr_df_all = pd.DataFrame(attributes_rows) if attributes_rows else pd.DataFrame()

    def attr_value_like(*keywords):
        if attr_df_all.empty or "Attribute" not in attr_df_all.columns or "Value" not in attr_df_all.columns:
            return ""
        keys = [str(k).lower() for k in keywords]
        for _, row in attr_df_all.iterrows():
            a = str(row.get("Attribute", "")).lower()
            if any(k in a for k in keys):
                return row.get("Value", "")
        return ""

    c1, c2 = st.columns([2, 1])
    with c1:
        summary_rows = [
            ("Datasheet", first.get("Data Sheet URL", "") or first.get("DataSheet", "")),
            ("Product Type", first.get("Category", "")),
            ("Part Lifecycle", first.get("Lifecycle Status", "")),
            ("RoHS", first.get("ROHS", "") or first.get("RoHS", "")),
            ("Description", first.get("Description", "")),
            ("Manufacturer", first.get("Manufacturer", "")),
            ("Manufacturer Part", first.get("Manufacturer Part Number", "")),
            ("Supplier Part", first.get("Supplier Part Number", "")),
            ("Company Package Name", attr_value_like("package", "case")),
            ("Packing Type", attr_value_like("packing", "package type")),
            ("Packing Quantity", attr_value_like("packing quantity", "qty", "quantity")),
            ("Family/Series", attr_value_like("series", "family")),
        ]
        summary_rows = [(k, v) for k, v in summary_rows if str(v).strip()]
        summary_df = pd.DataFrame(summary_rows, columns=["Field", "Value"])
        st.markdown("#### Part Summary")
        st.dataframe(summary_df, width="stretch", hide_index=True)

        if attributes_rows:
            st.markdown("#### Specifications")
            attr_df = attr_df_all.copy()
            if "Live Source" in attr_df.columns:
                attr_df = attr_df[["Live Source", "Attribute", "Value"]]
            st.dataframe(attr_df, width="stretch", hide_index=True)

    with c2:
        st.markdown("#### Where to Buy")
        if pricing_rows:
            buy_df = pd.DataFrame(pricing_rows)
            display_rows = []
            part_url_by_source = {}
            if "Live Source" in parts_df.columns and "Product URL" in parts_df.columns:
                for _, r in parts_df[["Live Source", "Product URL"]].dropna().drop_duplicates().iterrows():
                    part_url_by_source[str(r["Live Source"])] = r["Product URL"]

            source_series = buy_df["Live Source"].astype(str) if "Live Source" in buy_df.columns else pd.Series(["Unknown"] * len(buy_df))
            for source in source_series.unique():
                src_df = buy_df[source_series == source] if "Live Source" in buy_df.columns else buy_df
                qty = ""
                if "Quantity" in src_df.columns and not src_df["Quantity"].dropna().empty:
                    qty = src_df["Quantity"].dropna().iloc[0]
                elif "Break Quantity" in src_df.columns and not src_df["Break Quantity"].dropna().empty:
                    qty = src_df["Break Quantity"].dropna().iloc[0]

                price = ""
                for pcol in ["Unit Price", "Price"]:
                    if pcol in src_df.columns and not src_df[pcol].dropna().empty:
                        price = src_df[pcol].dropna().iloc[0]
                        break

                display_rows.append(
                    {
                        "Distributor": source,
                        "Qty": qty,
                        "Price": price,
                        "Buy Link": part_url_by_source.get(source, ""),
                    }
                )

            where_buy_df = pd.DataFrame(display_rows)
            st.dataframe(where_buy_df, width="stretch", hide_index=True)
        else:
            st.info("No pricing rows available.")

        st.markdown("#### Document Links")
        if docs_rows:
            ddf = pd.DataFrame(docs_rows)
            keep_cols = [c for c in ["Live Source", "Type", "URL"] if c in ddf.columns]
            st.dataframe(ddf[keep_cols] if keep_cols else ddf, width="stretch", hide_index=True)
        else:
            st.info("No links available.")

# ==========================================
# 1. SCRAPER ENGINE
# ==========================================

def sanitize_mpn(name):
    """Clean MPN for safe Windows filenames and search accuracy."""
    name = str(name).replace('\n', '').replace('\r', '').strip()
    return re.sub(r'[\\/*?:"<>|]', "", name)

def js_set_input(driver, el, text):
    driver.execute_script(
        "arguments[0].focus(); arguments[0].value = arguments[1]; "
        "arguments[0].dispatchEvent(new Event('input',{bubbles:true}));",
        el, text
    )

def run_scrubbing(mpns, user, pwd, is_headless, selected_tabs=None):
    import html_to_sqlite

    options = webdriver.ChromeOptions()
    if is_headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")

    service = Service(str(BASE_DIR / "chromedriver.exe"))
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 15)

    status = st.empty()
    prog = st.progress(0)
    conn = html_to_sqlite.init_db()

    try:
        driver.get("https://login.z2data.com/Account/Login")
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Username']"))).send_keys(user)
        driver.find_element(By.CSS_SELECTOR, "input[placeholder='Password']").send_keys(pwd + Keys.ENTER)
        time.sleep(5)

        selected_tabs = selected_tabs or TABS
        selected_tabs = [str(t).strip() for t in selected_tabs if str(t).strip()]

        for i, raw_m in enumerate(mpns):
            m = sanitize_mpn(raw_m)
            status.info(f"🔍 Scraping {i+1}/{len(mpns)}: **{m}**")
            driver.get("https://app.z2data.com/Parts/home")
            try:
                search = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[placeholder='Search']")))
                js_set_input(driver, search, m)
                search.send_keys(" ")
                time.sleep(2)
                item_xpath = "//cdk-virtual-scroll-viewport//app-searchautocomplete-item[1]"
                item = WebDriverWait(driver, 7).until(EC.element_to_be_clickable((By.XPATH, item_xpath)))
                driver.execute_script("arguments[0].click();", item)
                time.sleep(6)
            except TimeoutException:
                st.warning(f"⚠️ MPN Not Found: {m}. Skipped.")
                continue

            html_data = [f"<html><body><h1>PART: {m}</h1>"]
            for t_name in selected_tabs:
                try:
                    xpath = f"//div[contains(@class, 'ulllinks-text') and normalize-space()='{t_name}']"
                    tab_el = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                    driver.execute_script("arguments[0].click();", tab_el)
                    time.sleep(4)
                    html_data.append(f"<h2>{t_name}</h2>")
                    html_data.append(driver.execute_script("return document.querySelector('app-partdetails-layout')?.outerHTML;"))
                except:
                    continue

            if any(str(t).strip().lower() == "parametric" for t in selected_tabs):
                try:
                    p_xpath = "//div[contains(text(), 'Parametric')] | //a[contains(@href, 'parametric')]"
                    p_btn = driver.find_element(By.XPATH, p_xpath)
                    driver.execute_script("arguments[0].click();", p_btn)
                    time.sleep(5)
                    html_data.append("<h2>Parametric</h2>")
                    html_data.append(driver.page_source)
                except:
                    pass

            html_data.append("</body></html>")
            html_to_sqlite.parse_html_content("".join(html_data), conn, source_name=f"{m}.html")
            build_z2_spec_cache_for_mpn(m)
            prog.progress((i + 1) / len(mpns))

        st.success("✅ Scrubbing Complete! Data saved to DB directly.")
    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        conn.close()
        driver.quit()

# ==========================================
# 2. DATA UTILS
# ==========================================

def pivot_data(df_cells):
    if df_cells.empty: return pd.DataFrame()
    try:
        if df_cells["row_index"].nunique() == len(df_cells):
            return df_cells.set_index("header")["value"].to_frame(name="Value").reset_index()
        return df_cells.pivot_table(index="row_index", columns="header", values="value", aggfunc="first").reset_index(drop=True)
    except: return df_cells


def ensure_z2_spec_tables():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS z2_spec_cache (
                mpn TEXT PRIMARY KEY,
                description TEXT,
                msl TEXT,
                reflow_temt TEXT,
                thermal_cycle TEXT,
                wave TEXT,
                package_details TEXT,
                operating_temperature TEXT,
                lifecycle TEXT,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS z2_parametric_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mpn TEXT NOT NULL,
                section_name TEXT,
                table_title TEXT,
                row_index INTEGER,
                header TEXT,
                value TEXT
            )
            """
        )
        existing_cols = {
            str(r[1]).strip()
            for r in conn.execute("PRAGMA table_info(z2_spec_cache)").fetchall()
        }
        if "package_details" not in existing_cols:
            conn.execute("ALTER TABLE z2_spec_cache ADD COLUMN package_details TEXT")
        if "operating_temperature" not in existing_cols:
            conn.execute("ALTER TABLE z2_spec_cache ADD COLUMN operating_temperature TEXT")
        conn.commit()


def _first_matching_value(df, contains_keys):
    if df.empty:
        return ""
    for _, r in df.iterrows():
        h = str(r.get("header", "")).strip().lower()
        v = str(r.get("value", "")).strip()
        if not h or not v:
            continue
        if any(k in h for k in contains_keys):
            return v
    return ""


def build_z2_spec_cache_for_mpn(mpn):
    ensure_z2_spec_tables()
    mpn = str(mpn).strip()
    if not mpn:
        return
    with sqlite3.connect(DB_PATH) as conn:
        cells = pd.read_sql(
            """
            SELECT t.mpn, t.section_name, t.title as table_title, c.row_index, c.header, c.value
            FROM cells c
            JOIN tables t ON c.table_id = t.id
            WHERE t.mpn LIKE ?
            """,
            conn,
            params=(f"%{mpn}%",),
        )
        if cells.empty:
            return

        overview_df = cells[cells["section_name"].astype(str).str.lower().str.contains("overview", na=False)]
        mfg_df = cells[cells["section_name"].astype(str).str.lower().str.contains("manufact", na=False)]
        lifecycle_df = cells[cells["section_name"].astype(str).str.lower().str.contains("lifecycle", na=False)]
        package_df = cells[
            cells["section_name"].astype(str).str.lower().str.contains("package", na=False)
            | cells["section_name"].astype(str).str.lower().str.contains("packing", na=False)
        ]

        description = _first_matching_value(overview_df, ["description"])
        msl = _first_matching_value(mfg_df, ["msl", "msd", "moisture"])
        reflow_temt = _first_matching_value(mfg_df, ["maximum reflow temperature", "reflow temperature", "reflow"])
        thermal_cycle = _first_matching_value(mfg_df, ["number of reflow cycle", "thermal cycle", "reflow cycle", "cycle"])
        wave = _first_matching_value(mfg_df, ["wave soldering temperature", "wave solder", "wave"])
        package_details = _first_matching_value(package_df, ["package", "packaging", "case"])
        operating_temperature = _first_matching_value(cells, ["operating temperature", "temperature range", "operating temp"])
        lifecycle = _first_matching_value(lifecycle_df, ["lifecycle", "life cycle", "status"])

        conn.execute(
            """
            INSERT OR REPLACE INTO z2_spec_cache
            (mpn, description, msl, reflow_temt, thermal_cycle, wave, package_details, operating_temperature, lifecycle, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mpn,
                description,
                msl,
                reflow_temt,
                thermal_cycle,
                wave,
                package_details,
                operating_temperature,
                lifecycle,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

        conn.execute("DELETE FROM z2_parametric_cache WHERE mpn = ?", (mpn,))
        for _, row in cells.iterrows():
            sec = str(row.get("section_name", "")).strip().lower()
            title = str(row.get("table_title", "")).strip().lower()
            hdr = str(row.get("header", "")).strip().lower()
            is_parametric = ("parametric" in sec) or ("spec" in sec) or ("spec" in title) or ("parameter" in hdr)
            if not is_parametric:
                continue
            conn.execute(
                """
                INSERT INTO z2_parametric_cache (mpn, section_name, table_title, row_index, header, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    mpn,
                    str(row.get("section_name", "")),
                    str(row.get("table_title", "")),
                    int(row.get("row_index", 0) or 0),
                    str(row.get("header", "")),
                    str(row.get("value", "")),
                ),
            )
        conn.commit()


def save_live_payload_to_cells(mpn, payload, section_name="Live Combo", title="Digi-Key + Mouser"):
    mpn = str(mpn).strip()
    if not mpn or not isinstance(payload, dict):
        return
    part = {}
    if isinstance(payload.get("parts"), list) and payload["parts"] and isinstance(payload["parts"][0], dict):
        part = payload["parts"][0]
    if not part:
        return
    attributes = payload.get("attributes", []) if isinstance(payload.get("attributes"), list) else []
    preferred_rows = [
        ("Manufacturer", part.get("Manufacturer", "")),
        ("Manufacturer Part Number", part.get("Manufacturer Part Number", "")),
        ("Supplier Part Number", part.get("Supplier Part Number", "")),
        ("Description", part.get("Description", "")),
        ("Lifecycle Status", part.get("Lifecycle Status", "")),
        ("RoHS", part.get("ROHS", "") or part.get("RoHS", "")),
        ("MSL", part.get("MSD LEVEL", "")),
        ("Maximum Reflow Temperature", part.get("REFLOW SOLDERING TEMPERATURE", "")),
        ("Number Of Reflow Cycle", part.get("THERMAL CYCLE", "")),
        ("Wave Soldering Temperature", part.get("WAVE SOLDERING TEMPERATURE", "")),
        ("Operating Temperature", part.get("OPERATING TEMPERATURE", "")),
        ("Package", part.get("PACKAGE", "")),
        ("Data Sheet URL", part.get("Data Sheet URL", "")),
        ("Price Details", part.get("PRICE DETAILS", "")),
    ]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "DELETE FROM cells WHERE table_id IN (SELECT id FROM tables WHERE mpn = ? AND section_name = ? AND title = ?)",
            (mpn, section_name, title),
        )
        conn.execute(
            "DELETE FROM cells WHERE table_id IN (SELECT id FROM tables WHERE mpn = ? AND section_name = ? AND title = ?)",
            (mpn, "Live Parametric", "Merged Attributes"),
        )
        conn.execute(
            "DELETE FROM tables WHERE mpn = ? AND section_name = ? AND title = ?",
            (mpn, section_name, title),
        )
        conn.execute(
            "DELETE FROM tables WHERE mpn = ? AND section_name = ? AND title = ?",
            (mpn, "Live Parametric", "Merged Attributes"),
        )
        next_idx = conn.execute(
            "SELECT COALESCE(MAX(table_index), 0) + 1 FROM tables WHERE mpn = ? AND section_name = ?",
            (mpn, section_name),
        ).fetchone()[0]
        cur = conn.execute(
            "INSERT INTO tables (mpn, section_name, title, table_index) VALUES (?, ?, ?, ?)",
            (mpn, section_name, title, int(next_idx)),
        )
        table_id = cur.lastrowid
        row_idx = 0
        for header, value in preferred_rows:
            if str(value).strip():
                conn.execute(
                    "INSERT INTO cells (table_id, row_index, col_index, header, value) VALUES (?, ?, ?, ?, ?)",
                    (table_id, row_idx, 0, str(header), str(value)),
                )
                row_idx += 1
        if attributes:
            p_idx = conn.execute(
                "SELECT COALESCE(MAX(table_index), 0) + 1 FROM tables WHERE mpn = ? AND section_name = ?",
                (mpn, "Live Parametric"),
            ).fetchone()[0]
            p_cur = conn.execute(
                "INSERT INTO tables (mpn, section_name, title, table_index) VALUES (?, ?, ?, ?)",
                (mpn, "Live Parametric", "Merged Attributes", int(p_idx)),
            )
            p_table_id = p_cur.lastrowid
            p_row_idx = 0
            for one in attributes:
                if not isinstance(one, dict):
                    continue
                hdr = str(one.get("Attribute", "")).strip()
                val = str(one.get("Value", "")).strip()
                if not hdr or not val:
                    continue
                conn.execute(
                    "INSERT INTO cells (table_id, row_index, col_index, header, value) VALUES (?, ?, ?, ?, ?)",
                    (p_table_id, p_row_idx, 0, hdr, val),
                )
                p_row_idx += 1
        conn.commit()


def _rotating_priority_for_index(idx, split_mode=False):
    default_order = ["digikey", "mouser"]
    if not split_mode:
        return default_order
    rr_base = ["digikey", "mouser"]
    start = int(idx) % len(rr_base)
    return rr_base[start:] + rr_base[:start]


def fetch_live_into_db_for_mpn(mpn, mouser_key="", digikey_id="", digikey_secret="", digikey_scope="", priority_order=None, save_to_cells=False, fill_empty_from_fallback=True):
    """
    For missing scraper MPNs, fetch from live sources (default priority: Digi-Key -> Mouser)
    and save into same DB via live_part_cache + unified_part_cache.
    """
    def _merge_payload_for_mpn(one_mpn):
        def _call_with_retry(callable_fn, max_attempts=3, retry_delay=0.35):
            last_ex = None
            for attempt in range(max_attempts):
                try:
                    return callable_fn(), ""
                except Exception as ex:
                    last_ex = ex
                    if attempt < max_attempts - 1:
                        time.sleep(retry_delay)
            return None, str(last_ex or "unknown error")

        def _coverage_score(part_row):
            if not isinstance(part_row, dict):
                return 0
            keys = [
                "Manufacturer",
                "Manufacturer Part Number",
                "Description",
                "Lifecycle Status",
                "PACKAGE",
                "Data Sheet URL",
            ]
            return sum(1 for k in keys if str(part_row.get(k, "")).strip())

        payload_by_provider = {}
        provider_errors = []
        provider_order = priority_order or ["digikey", "mouser"]
        best_part = {}
        provider_name_map = {
            "digikey": SOURCE_DIGIKEY,
            "mouser": SOURCE_MOUSER,
        }

        def _build_provider_task(provider_key):
            p = str(provider_key).strip().lower()
            if p == "digikey" and digikey_id.strip() and digikey_secret.strip():
                def _digikey_task():
                    for cand in build_mpn_search_candidates(one_mpn):
                        out = fetch_digikey_part_data(
                            cand,
                            client_id=digikey_id.strip(),
                            client_secret=digikey_secret.strip(),
                            scope=digikey_scope.strip() or None,
                            site="US",
                            currency="USD",
                        )
                        if out and out.get("parts"):
                            for key in ["parts", "pricing", "attributes", "documents"]:
                                rows = out.get(key, [])
                                if isinstance(rows, list):
                                    for row in rows:
                                        if isinstance(row, dict) and "Requested MPN" in row:
                                            row["Requested MPN"] = one_mpn
                            return out
                    return {"parts": [], "pricing": [], "attributes": [], "documents": []}
                return _digikey_task
            if p == "mouser" and mouser_key.strip():
                def _mouser_task():
                    for cand in build_mpn_search_candidates(one_mpn):
                        out = fetch_mouser_part_data(cand, mouser_key.strip())
                        if out and out.get("parts"):
                            for key in ["parts", "pricing", "attributes", "documents"]:
                                rows = out.get(key, [])
                                if isinstance(rows, list):
                                    for row in rows:
                                        if isinstance(row, dict) and "Requested MPN" in row:
                                            row["Requested MPN"] = one_mpn
                            return out
                    return {"parts": [], "pricing": [], "attributes": [], "documents": []}
                return _mouser_task
            return None

        provider_tasks = {}
        for provider in provider_order:
            task = _build_provider_task(provider)
            if task:
                provider_tasks[str(provider).strip().lower()] = task

        if provider_tasks:
            with ThreadPoolExecutor(max_workers=max(1, len(provider_tasks))) as executor:
                future_map = {
                    executor.submit(_call_with_retry, task): provider_key
                    for provider_key, task in provider_tasks.items()
                }
                for future in as_completed(future_map):
                    provider_key = future_map[future]
                    provider_name = provider_name_map.get(provider_key, provider_key)
                    try:
                        payload, err = future.result()
                    except Exception as ex:
                        payload, err = None, str(ex)
                    if payload and isinstance(payload, dict) and payload.get("parts"):
                        payload_by_provider[provider_key] = payload
                    elif err:
                        provider_errors.append(f"{provider_name}: fetch failed")
                    else:
                        provider_errors.append(f"{provider_name}: no match")

        payloads = []
        sources = []
        for provider in provider_order:
            p = str(provider).strip().lower()
            payload = payload_by_provider.get(p)
            if not payload:
                continue
            payloads.append(payload)
            provider_name = provider_name_map.get(p, p)
            sources.append(provider_name)
            part0 = (payload.get("parts") or [{}])[0] if isinstance(payload, dict) else {}
            if isinstance(part0, dict) and _coverage_score(part0) > _coverage_score(best_part):
                best_part = part0
            if payloads and not fill_empty_from_fallback:
                break

        if not payloads:
            return None, "; ".join(provider_errors[:3])

        merged_part = {}
        merged_pricing, merged_attrs, merged_docs = [], [], []
        for p in payloads:
            part0 = (p.get("parts") or [{}])[0] if isinstance(p, dict) else {}
            if isinstance(part0, dict):
                for k, v in part0.items():
                    if _is_effectively_empty(merged_part.get(k, "")) and not _is_effectively_empty(v):
                        merged_part[k] = v
            merged_pricing.extend(p.get("pricing", []) if isinstance(p.get("pricing", []), list) else [])
            merged_attrs.extend(p.get("attributes", []) if isinstance(p.get("attributes", []), list) else [])
            merged_docs.extend(p.get("documents", []) if isinstance(p.get("documents", []), list) else [])

        merged_parts = add_enrichment_fields([merged_part], merged_attrs, merged_pricing)
        return {
            "parts": merged_parts,
            "pricing": merged_pricing,
            "attributes": merged_attrs,
            "documents": merged_docs,
        }, " + ".join(sources)

    mpn = normalize_mpn(mpn)
    if not mpn:
        return {"mpn": "", "source": "", "status": "skipped"}
    payload, source = _merge_payload_for_mpn(mpn)

    if payload is not None:
        save_live_result_to_db(mpn, source, payload, on_exists="overwrite")
        if save_to_cells:
            save_live_payload_to_cells(mpn, payload)
            build_z2_spec_cache_for_mpn(mpn)
    upsert_unified_part_for_mpn(mpn)
    return {"mpn": mpn, "source": source, "status": "saved" if payload is not None else "unified_only"}


def enqueue_scrub_queue_from_upload(file_obj):
    if not file_obj:
        return {"queued": 0, "skipped": 0, "error": "No file selected"}
    try:
        if str(file_obj.name).lower().endswith(".csv"):
            df = pd.read_csv(file_obj)
        else:
            df = pd.read_excel(file_obj)
    except Exception as ex:
        return {"queued": 0, "skipped": 0, "error": f"Read failed: {ex}"}
    if df.empty:
        return {"queued": 0, "skipped": 0, "error": "File has no rows"}

    ensure_scrub_queue_tables()
    cols = list(df.columns)
    mpn_col = cols[0]
    make_col = cols[1] if len(cols) > 1 else None
    queued = 0
    skipped = 0
    now_utc = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        for _, row in df.iterrows():
            mpn = normalize_mpn(row.get(mpn_col, ""))
            if not mpn or mpn.lower() == "nan":
                skipped += 1
                continue
            make_val = str(row.get(make_col, "")).strip() if make_col else ""
            if make_val.lower() == "nan":
                make_val = ""
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO scrub_queue (mpn, manufacturer, status, source, last_error, updated_at_utc)
                VALUES (?, ?, 'pending', '', '', ?)
                """,
                (mpn, make_val, now_utc),
            )
            # If manufacturer now available, backfill for existing queue row.
            if make_val:
                conn.execute(
                    "UPDATE scrub_queue SET manufacturer = COALESCE(NULLIF(manufacturer,''), ?), updated_at_utc=? WHERE mpn=?",
                    (make_val, now_utc, mpn),
                )
            if getattr(cur, "rowcount", 0) == 1:
                queued += 1
                log_scrub_history(
                    mpn,
                    step="queue_add",
                    status="pending",
                    message="Added from file upload into scrub_queue.",
                    conn=conn,
                )
            else:
                skipped += 1
        conn.commit()
    return {"queued": queued, "skipped": skipped, "error": ""}


def process_scrub_queue_batch(batch_size, mouser_key="", digikey_id="", digikey_secret="", digikey_scope="", fill_empty_from_fallback=True):
    ensure_scrub_queue_tables()
    batch_size = max(1, int(batch_size or 1))
    now_utc = datetime.now(timezone.utc).isoformat()
    processed = []
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT mpn FROM scrub_queue WHERE status IN ('pending', 'error') ORDER BY updated_at_utc, mpn LIMIT ?",
            (batch_size,),
        ).fetchall()
        mpns = [str(r[0]).strip() for r in rows if str(r[0]).strip()]

    for i, mpn in enumerate(mpns, start=1):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE scrub_queue SET status='in_progress', updated_at_utc=? WHERE mpn=?", (now_utc, mpn))
            conn.commit()
        log_scrub_history(mpn, step="process_start", status="in_progress", message=f"Batch item {i}/{len(mpns)} started.")
        try:
            result = fetch_live_into_db_for_mpn(
                mpn,
                mouser_key=mouser_key,
                digikey_id=digikey_id,
                digikey_secret=digikey_secret,
                digikey_scope=digikey_scope,
                priority_order=_rotating_priority_for_index(i - 1, split_mode=False),
                save_to_cells=True,
                fill_empty_from_fallback=fill_empty_from_fallback,
            )
            source = str(result.get("source", "")).strip()
            save_status = str(result.get("status", "")).strip()
            if save_status not in ("saved", "unified_only"):
                raise RuntimeError(f"No DB save completed (status={save_status or 'unknown'}).")
            if save_status == "unified_only":
                log_scrub_history(
                    mpn,
                    step="fetch_warning",
                    status="warning",
                    source=source,
                    message="No live payload found; only unified cache was refreshed.",
                )
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE scrub_queue SET status='done', source=?, last_error='', updated_at_utc=? WHERE mpn=?",
                    (source or save_status, datetime.now(timezone.utc).isoformat(), mpn),
                )
                conn.execute(
                    """
                    UPDATE scrub_queue_state
                    SET last_mpn=?, last_status='done', processed_count=processed_count+1, updated_at_utc=?
                    WHERE id=1
                    """,
                    (mpn, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            log_scrub_history(
                mpn,
                step="process_done",
                status="done",
                source=source or save_status,
                message="Queue item completed and status updated to done.",
            )
            processed.append({"mpn": mpn, "status": "done", "source": source or save_status, "result": save_status})
        except Exception as ex:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "UPDATE scrub_queue SET status='error', last_error=?, updated_at_utc=? WHERE mpn=?",
                    (str(ex), datetime.now(timezone.utc).isoformat(), mpn),
                )
                conn.execute(
                    "UPDATE scrub_queue_state SET last_mpn=?, last_status='error', updated_at_utc=? WHERE id=1",
                    (mpn, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            log_scrub_history(
                mpn,
                step="process_error",
                status="error",
                message=str(ex),
            )
            processed.append({"mpn": mpn, "status": "error", "error": str(ex)})
    return processed


def routine_check_db_mpns(limit, mouser_key="", digikey_id="", digikey_secret="", digikey_scope="", fill_empty_from_fallback=True):
    ensure_unified_parts_table()
    mpns = get_available_db_mpns()
    if limit and int(limit) > 0:
        mpns = mpns[: int(limit)]
    results = []
    for i, m in enumerate(mpns, start=1):
        result = fetch_live_into_db_for_mpn(
            m,
            mouser_key=mouser_key,
            digikey_id=digikey_id,
            digikey_secret=digikey_secret,
            digikey_scope=digikey_scope,
            priority_order=_rotating_priority_for_index(i - 1, split_mode=False),
            save_to_cells=True,
            fill_empty_from_fallback=fill_empty_from_fallback,
        )
        results.append(result)
    return results

# ==========================================
# 3. INTERFACE
# ==========================================

run_mode = show_sidebar_logo()
ensure_base_scraper_tables()
ensure_live_cache_table()
ensure_unified_parts_table()
ensure_z2_spec_tables()
ensure_scrub_queue_tables()

st.title("🛡️ Component Engineer")
ui_tabs = st.tabs([
    "🚀 Scraper",
    "⚙️ DB Processor",
    "📊 Live Viewer",
    "📥 Advanced Master Export",
    "🏆 Price Comparison",
    "🛒 Mouser API Live Feed",
])

with ui_tabs[0]:
    st.subheader("Z2 Scraper (Separate)")
    c1, c2 = st.columns(2)
    u_name = c1.text_input("Z2 Username")
    p_word = c2.text_input("Z2 Password", type="password")
    up_file = st.file_uploader("Upload MPN List for Scrubbing", type=["xlsx"], key="scrub_key")
    db_section_items = []
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                db_section_items = (
                    pd.read_sql("SELECT DISTINCT section_name FROM tables WHERE TRIM(COALESCE(section_name,'')) <> '' ORDER BY section_name", conn)
                    .iloc[:, 0]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .tolist()
                )
        except Exception:
            db_section_items = []
    scrub_choices = list(dict.fromkeys(TABS + db_section_items + ["Parametric"]))
    selected_scrub_items = st.multiselect(
        "Select tabs/items to scrub",
        options=scrub_choices,
        default=["Overview", "Manufacturing", "Package/Packing", "Lifecycle/Forecast"],
        key="z2_selected_scrub_items",
        help="Only selected items are scraped from Z2.",
    )
    if st.button("Start Automation"):
        if not selected_scrub_items:
            st.error("Please select at least one tab/item to scrub.")
        elif up_file and u_name and p_word:
            run_scrubbing(
                pd.read_excel(up_file).iloc[:,0].dropna().tolist(),
                u_name,
                p_word,
                run_mode,
                selected_tabs=selected_scrub_items,
            )

    st.markdown("---")
    st.subheader("Live Combo Scraper → DB Cells")
    st.caption("Priority used: Digi-Key → Mouser. Results are written directly to DB cells.")
    live_up = st.file_uploader("Upload MPN List for Live Combo Scraper", type=["xlsx", "csv"], key="live_combo_upload")
    live_manual = st.text_area("Or enter MPNs (comma/newline separated)", key="live_combo_manual")
    lc1, lc2 = st.columns(2)
    live_dk_id = lc1.text_input("Digi-Key Client ID", value=os.getenv("DIGIKEY_CLIENT_ID", DIGIKEY_CLIENT_ID_FALLBACK), key="live_combo_dk_id")
    live_dk_secret = lc1.text_input("Digi-Key Client Secret", value=os.getenv("DIGIKEY_CLIENT_SECRET", DIGIKEY_CLIENT_SECRET_FALLBACK), type="password", key="live_combo_dk_secret")
    live_dk_scope = lc1.text_input("Digi-Key Scope (Optional)", value=os.getenv("DIGIKEY_SCOPE", ""), key="live_combo_dk_scope")
    live_mouser = lc2.text_input("Mouser API Key (Optional)", value=os.getenv("MOUSER_API_KEY", MOUSER_API_KEY_FALLBACK), type="password", key="live_combo_mouser")
    live_split_mode = st.toggle("Fast split mode (Round-robin first hit: Digi-Key → Mouser)", value=False, key="live_combo_split_mode")
    live_fill_empty = st.toggle("Fill empty fields from next providers (fallback)", value=True, key="live_combo_fill_empty")
    if st.button("Start Live Combo Scraper", key="live_combo_run"):
        live_mpns = []
        live_mpns.extend(_read_mpn_list_from_upload(live_up))
        if live_manual.strip():
            live_mpns.extend([x.strip() for x in re.split(r"[\n,]+", live_manual) if x.strip()])
        live_mpns = list(dict.fromkeys([m for m in live_mpns if str(m).strip()]))
        if not live_mpns:
            st.error("Please upload or enter at least one MPN.")
        else:
            bar = st.progress(0)
            status_txt = st.empty()
            rows = []
            for i, m in enumerate(live_mpns, start=1):
                remaining = len(live_mpns) - i
                status_txt.info(f"Processing {i}/{len(live_mpns)} | Pending: {remaining}")
                rows.append(
                    fetch_live_into_db_for_mpn(
                        m,
                        mouser_key=live_mouser,
                        digikey_id=live_dk_id,
                        digikey_secret=live_dk_secret,
                        digikey_scope=live_dk_scope,
                        priority_order=_rotating_priority_for_index(i - 1, split_mode=live_split_mode),
                        save_to_cells=True,
                        fill_empty_from_fallback=live_fill_empty,
                    )
                )
                bar.progress(i / len(live_mpns))
            status_txt.success(f"Completed {len(live_mpns)}/{len(live_mpns)} | Pending: 0")
            st.dataframe(pd.DataFrame(rows), width="stretch")
            st.success("Live combo scrape complete and saved to DB cells.")

    st.markdown("---")
    st.subheader("Large File Background Queue (Resume Supported)")
    st.caption("For 50k+ rows: upload once, process in batches, stop anytime, and resume from last processed MPN.")
    bgc1, bgc2 = st.columns(2)
    bg_mouser = bgc1.text_input("Queue Mouser API Key", value=os.getenv("MOUSER_API_KEY", MOUSER_API_KEY_FALLBACK), type="password", key="bg_mouser")
    bg_dk_id = bgc1.text_input("Queue Digi-Key Client ID", value=os.getenv("DIGIKEY_CLIENT_ID", DIGIKEY_CLIENT_ID_FALLBACK), key="bg_dk_id")
    bg_dk_secret = bgc1.text_input("Queue Digi-Key Client Secret", value=os.getenv("DIGIKEY_CLIENT_SECRET", DIGIKEY_CLIENT_SECRET_FALLBACK), type="password", key="bg_dk_secret")
    bg_dk_scope = bgc1.text_input("Queue Digi-Key Scope", value=os.getenv("DIGIKEY_SCOPE", ""), key="bg_dk_scope")
    bg_fill_empty = bgc2.toggle("Queue fallback fill empty fields", value=True, key="bg_fill_empty")
    auto_run_on_enqueue = bgc2.toggle("Auto-run queue after adding file", value=True, key="bg_auto_run_on_enqueue")

    bg_file = st.file_uploader("Upload full MPN file (col1=MPN, col2=Manufacturer/Make optional)", type=["xlsx", "csv"], key="bg_queue_upload")
    b1, b2, b3 = st.columns(3)
    bg_batch_size = b2.number_input("Batch size", min_value=1, max_value=5000, value=200, step=50, key="bg_batch_size")
    run_batch = b3.button("▶ Run next batch", key="bg_run_batch_btn")
    if b1.button("📥 Add file to queue", key="bg_enqueue_btn"):
        out = enqueue_scrub_queue_from_upload(bg_file)
        if out.get("error"):
            st.error(out["error"])
        else:
            st.success(f"Queued {out['queued']} MPNs. Skipped {out['skipped']} duplicates/blank rows.")
            if auto_run_on_enqueue and int(out.get("queued", 0) or 0) > 0:
                processed = process_scrub_queue_batch(
                    int(bg_batch_size),
                    mouser_key=bg_mouser,
                    digikey_id=bg_dk_id,
                    digikey_secret=bg_dk_secret,
                    digikey_scope=bg_dk_scope,
                    fill_empty_from_fallback=bg_fill_empty,
                )
                if processed:
                    st.success(f"Auto-run started immediately and processed {len(processed)} queued rows.")
                    st.dataframe(pd.DataFrame(processed), width="stretch")
                else:
                    st.info("Auto-run was enabled, but no rows were processed in the first batch.")

    if run_batch:
        processed = process_scrub_queue_batch(
            int(bg_batch_size),
            mouser_key=bg_mouser,
            digikey_id=bg_dk_id,
            digikey_secret=bg_dk_secret,
            digikey_scope=bg_dk_scope,
            fill_empty_from_fallback=bg_fill_empty,
        )
        if not processed:
            st.info("No pending queue rows. Queue is idle.")
        else:
            st.success(f"Processed {len(processed)} queue rows in this run.")
            st.dataframe(pd.DataFrame(processed), width="stretch")

    with sqlite3.connect(DB_PATH) as conn:
        qstats = pd.read_sql("SELECT status, COUNT(1) AS cnt FROM scrub_queue GROUP BY status ORDER BY status", conn)
        qstate = pd.read_sql("SELECT * FROM scrub_queue_state WHERE id=1", conn)
        qlive = pd.read_sql("SELECT mpn, manufacturer, status, source, updated_at_utc, last_error FROM scrub_queue ORDER BY updated_at_utc DESC LIMIT 20", conn)
        qhist = pd.read_sql(
            "SELECT mpn, step, status, source, message, created_at_utc FROM scrub_queue_history ORDER BY id DESC LIMIT 200",
            conn,
        )
    st.markdown("#### Queue Status")
    q_col1, q_col2, q_col3 = st.columns(3)
    done_count = int(qstats.loc[qstats["status"].astype(str).str.lower() == "done", "cnt"].sum()) if not qstats.empty else 0
    in_progress_count = int(qstats.loc[qstats["status"].astype(str).str.lower().isin(["running", "in_progress", "processing"]), "cnt"].sum()) if not qstats.empty else 0
    pending_count = int(qstats.loc[qstats["status"].astype(str).str.lower().isin(["pending", "queued"]), "cnt"].sum()) if not qstats.empty else 0
    q_col1.metric("✅ Done", done_count)
    q_col2.metric("🔄 In Progress", in_progress_count)
    q_col3.metric("🕒 Pending", pending_count)
    if not qstats.empty:
        st.dataframe(qstats, width="stretch", hide_index=True)
    if not qstate.empty:
        st.info(
            f"Last processed MPN: {qstate.iloc[0].get('last_mpn','')} | "
            f"Last status: {qstate.iloc[0].get('last_status','')} | "
            f"Total processed: {int(qstate.iloc[0].get('processed_count',0) or 0)}"
        )
    st.markdown("#### Live Queue Viewer (latest 20)")
    if qlive.empty:
        st.caption("Queue is empty.")
    else:
        st.dataframe(qlive, width="stretch", hide_index=True)
    st.markdown("#### Process History (step-wise)")
    h1, h2 = st.columns([2, 1])
    hist_filter_mpn = h1.text_input("Filter history by MPN (optional)", key="queue_history_filter_mpn")
    hist_limit = int(h2.number_input("History rows", min_value=20, max_value=1000, value=200, step=20, key="queue_history_limit"))
    if not qhist.empty:
        qhist_show = qhist.copy()
        if hist_filter_mpn.strip():
            qhist_show = qhist_show[qhist_show["mpn"].astype(str).str.upper() == hist_filter_mpn.strip().upper()]
        qhist_show = qhist_show.head(hist_limit)
        if qhist_show.empty:
            st.caption("No history rows for this MPN filter.")
        else:
            st.dataframe(qhist_show, width="stretch", hide_index=True)
            st.download_button(
                "⬇️ Download Process History CSV",
                data=qhist_show.to_csv(index=False).encode("utf-8"),
                file_name="queue_process_history.csv",
                mime="text/csv",
                key="queue_history_download_btn",
            )
    else:
        st.caption("No process history recorded yet.")
    show_footer()

with ui_tabs[1]:
    st.subheader("Database Management")
    if st.button("🔨 Rebuild DB from Saved HTML (Optional)"):
        import html_to_sqlite
        html_to_sqlite.main()
        st.success("Database rebuilt from HTML files!")
    if st.button("🧩 Build Unified Cache (Scraper + Live Sources)"):
        rebuild_unified_cache_for_all_mpns()
        st.success("Unified part cache generated in DB (table: unified_part_cache).")
    if st.button("🧪 Build Z2 Specification Cache"):
        with sqlite3.connect(DB_PATH) as conn:
            mpns = pd.read_sql("SELECT DISTINCT mpn FROM sections", conn)["mpn"].dropna().astype(str).str.strip().tolist()
        for m in mpns:
            build_z2_spec_cache_for_mpn(m)
        st.success("Z2 specification cache built (tables: z2_spec_cache, z2_parametric_cache).")
    st.markdown("---")
    st.markdown("#### Routine Check (Auto refresh from DB MPN list)")
    rc1, rc2 = st.columns(2)
    rc_mouser = rc1.text_input("Routine Mouser API Key (Optional)", value=os.getenv("MOUSER_API_KEY", MOUSER_API_KEY_FALLBACK), type="password", key="routine_mouser")
    rc_dk_id = rc1.text_input("Routine Digi-Key Client ID (Optional)", value=os.getenv("DIGIKEY_CLIENT_ID", DIGIKEY_CLIENT_ID_FALLBACK), key="routine_dk_id")
    rc_dk_secret = rc1.text_input("Routine Digi-Key Client Secret (Optional)", value=os.getenv("DIGIKEY_CLIENT_SECRET", DIGIKEY_CLIENT_SECRET_FALLBACK), type="password", key="routine_dk_secret")
    rc_dk_scope = rc1.text_input("Routine Digi-Key Scope (Optional)", value=os.getenv("DIGIKEY_SCOPE", ""), key="routine_dk_scope")
    rc_fill = rc2.toggle("Routine fallback fill empty fields", value=True, key="routine_fill_empty")
    rc_limit = st.number_input("Routine MPN limit from DB", min_value=1, max_value=50000, value=200, step=50, key="routine_limit")
    if st.button("🔄 Run Routine Check Now", key="routine_check_run_btn"):
        routine_results = routine_check_db_mpns(
            rc_limit,
            mouser_key=rc_mouser,
            digikey_id=rc_dk_id,
            digikey_secret=rc_dk_secret,
            digikey_scope=rc_dk_scope,
            fill_empty_from_fallback=rc_fill,
        )
        st.success(f"Routine check completed for {len(routine_results)} MPN(s).")
        st.dataframe(pd.DataFrame(routine_results), width="stretch")
    show_footer()

with ui_tabs[2]:
    st.subheader("Scraper View (Enter MPN and View Data)")
    if "pending_mpns" not in st.session_state:
        st.session_state["pending_mpns"] = []
    view_mpn = st.text_input("Enter MPN", key="scraper_view_mpn")

    with st.expander("Pending List → Live Fetch to Same DB", expanded=False):
        p1, p2 = st.columns(2)
        pending_mouser = p1.text_input(
            "Mouser API Key (Optional)",
            value=os.getenv("MOUSER_API_KEY", MOUSER_API_KEY_FALLBACK),
            type="password",
            key="pending_mouser_key",
        )
        pending_digikey_id = p2.text_input(
            "Digi-Key Client ID (Optional)",
            value=os.getenv("DIGIKEY_CLIENT_ID", DIGIKEY_CLIENT_ID_FALLBACK),
            key="pending_digikey_id",
        )
        pending_digikey_secret = p2.text_input(
            "Digi-Key Client Secret (Optional)",
            value=os.getenv("DIGIKEY_CLIENT_SECRET", DIGIKEY_CLIENT_SECRET_FALLBACK),
            type="password",
            key="pending_digikey_secret",
        )
        pending_digikey_scope = p2.text_input(
            "Digi-Key Scope (Optional)",
            value=os.getenv("DIGIKEY_SCOPE", ""),
            key="pending_digikey_scope",
        )
        pending_split_mode = st.toggle("Fast split mode (Round-robin first hit: Digi-Key → Mouser)", value=False, key="pending_split_mode")
        pending_fill_empty = st.toggle("Fill empty fields from next providers (fallback)", value=True, key="pending_fill_empty")
        st.write("Pending MPNs:", st.session_state.get("pending_mpns", []))
        if st.button("▶ Fetch Pending MPNs (Digi-Key → Mouser)", key="fetch_pending_mpns"):
            pending = st.session_state.get("pending_mpns", [])
            if not pending:
                st.info("Pending list is empty.")
            else:
                out = []
                bar = st.progress(0)
                status_txt = st.empty()
                for i, pm in enumerate(pending, start=1):
                    remaining = len(pending) - i
                    status_txt.info(f"Processing {i}/{len(pending)} | Pending: {remaining}")
                    out.append(
                        fetch_live_into_db_for_mpn(
                            pm,
                            mouser_key=pending_mouser,
                            digikey_id=pending_digikey_id,
                            digikey_secret=pending_digikey_secret,
                            digikey_scope=pending_digikey_scope,
                            priority_order=_rotating_priority_for_index(i - 1, split_mode=pending_split_mode),
                            save_to_cells=True,
                            fill_empty_from_fallback=pending_fill_empty,
                        )
                    )
                    bar.progress(i / len(pending))
                status_txt.success(f"Completed {len(pending)}/{len(pending)} | Pending: 0")
                st.dataframe(pd.DataFrame(out), width="stretch")
                st.session_state["pending_mpns"] = []
                st.success("Pending list processed and saved to DB.")

    if st.button("🔍 View MPN Data", key="scraper_view_btn"):
        if not view_mpn.strip():
            st.error("Please enter MPN.")
        elif not DB_PATH.exists():
            st.info("Database missing. Run scraper first.")
        else:
            build_z2_spec_cache_for_mpn(view_mpn.strip())
            with sqlite3.connect(DB_PATH) as conn:
                st.session_state["last_tables_df"] = pd.read_sql(
                    "SELECT id, section_name, title FROM tables WHERE mpn LIKE ? ORDER BY section_name, table_index",
                    conn,
                    params=(f"%{view_mpn.strip()}%",),
                )
                st.session_state["last_spec_df"] = pd.read_sql(
                    "SELECT * FROM z2_spec_cache WHERE mpn = ?",
                    conn,
                    params=(view_mpn.strip(),),
                )
                st.session_state["last_param_df"] = pd.read_sql(
                    "SELECT section_name, table_title, row_index, header, value FROM z2_parametric_cache WHERE mpn = ? ORDER BY section_name, table_title, row_index",
                    conn,
                    params=(view_mpn.strip(),),
                )
            st.session_state["last_view_mpn"] = view_mpn.strip()

    tables_df = st.session_state.get("last_tables_df", pd.DataFrame())
    spec_df = st.session_state.get("last_spec_df", pd.DataFrame())
    param_df = st.session_state.get("last_param_df", pd.DataFrame())
    last_view_mpn = st.session_state.get("last_view_mpn", "")

    if last_view_mpn:
        st.markdown("### Page 1: All Z2 Tables/Columns")
        if tables_df.empty:
            st.info("No table data found for this MPN in scraper DB.")
            if st.button("➕ Add this MPN to Pending List", key="add_pending_mpn_btn"):
                if last_view_mpn and last_view_mpn not in st.session_state["pending_mpns"]:
                    st.session_state["pending_mpns"].append(last_view_mpn)
                    st.success(f"Added {last_view_mpn} to pending list.")
                else:
                    st.info("MPN already present in pending list.")
        else:
            for _, row in tables_df.iterrows():
                st.write(f"#### {row['section_name']} → {row['title']}")
                with sqlite3.connect(DB_PATH) as conn:
                    cells = pd.read_sql(
                        "SELECT header, value, row_index FROM cells WHERE table_id = ? ORDER BY row_index, col_index",
                        conn,
                        params=(row['id'],),
                    )
                if not cells.empty:
                    st.dataframe(pivot_data(cells), width="stretch")

        st.markdown("### Page 2: Specification Details")
        if spec_df.empty:
            st.info("No Z2 specification summary found for this MPN.")
        else:
            st.dataframe(spec_df, width="stretch")

        if param_df.empty:
            st.info("No parametric/specification rows found for this MPN.")
        else:
            st.dataframe(param_df, width="stretch")
    show_footer()

with ui_tabs[3]:
    st.subheader("Single Export Option (Fill DB + Export)")
    st.caption("Export only DB data. No live API calls are used in this window.")

    st.markdown("#### Excel Load → Direct Save to DB")
    direct_db_file = st.file_uploader(
        "Upload Excel/CSV to save data directly into DB columns",
        type=["xlsx", "csv"],
        key="direct_db_upload",
    )
    if st.button("⬆️ Load Excel to DB", key="direct_db_load_btn"):
        result = import_unified_from_excel(direct_db_file)
        if result.get("error"):
            st.error(result["error"])
        else:
            st.success(f"Loaded {result['loaded']} rows to unified DB. Skipped {result['skipped']} rows.")

    mpn_file = st.file_uploader(
        "Upload MPN list (Excel/CSV, first column used)",
        type=["xlsx", "csv"],
        key="single_option_upload",
    )
    one_mpn_view = st.text_input("Enter one MPN to view details", key="single_option_one_mpn")

    if st.button("🚀 Export from DB", key="single_option_run"):
        mpn_list = []
        mpn_list.extend(_read_mpn_list_from_upload(mpn_file))
        if one_mpn_view.strip():
            mpn_list.append(one_mpn_view.strip())

        mpn_list = list(dict.fromkeys([m for m in mpn_list if str(m).strip()]))
        if not mpn_list:
            st.error("Please upload file or enter at least one MPN.")
        else:
            ensure_unified_parts_table()
            progress = st.progress(0)
            total = len(mpn_list)

            for idx, mpn in enumerate(mpn_list, start=1):
                upsert_unified_part_for_mpn(mpn)
                progress.progress(idx / total)

            with sqlite3.connect(DB_PATH) as conn:
                placeholders = ",".join(["?"] * len(mpn_list))
                udf = pd.read_sql(
                    f"SELECT * FROM unified_part_cache WHERE mpn IN ({placeholders}) ORDER BY mpn",
                    conn,
                    params=mpn_list,
                )
            found_mpns = set([str(x).strip().upper() for x in udf.get("mpn", []).tolist() if str(x).strip()])
            requested_mpns = set([str(x).strip().upper() for x in mpn_list if str(x).strip()])
            missing_mpns = sorted(requested_mpns - found_mpns)

            rename_map = {
                "manufacturer_part_number": "Manufacture part number",
                "manufacturer": "Manufacture",
                "lifecycle_status": "Lifecycle",
                "rohs": "ROHS",
                "description": "Description",
                "msd_level": "MSD LEVEL",
                "datasheet_url": "DATASHEETLINK",
                "reflow_soldering_temperature": "REFLOW SOLDERING TEMPERATURE",
                "thermal_cycle": "THERMAL CYCLE",
                "wave_soldering_temperature": "WAVE SOLDERING TEMPERATURE",
                "lsl_details": "LSL DETAILS",
                "package_details": "PACKAGE",
                "price_details": "PRICE DETAILS",
                "operating_temperature": "OPERATING TEMPERATURE",
                "component_thickness": "Component thickness",
                "reach": "Reach",
                "reflow_soldering_time": "Reflow soldering time",
                "wave_soldering_time": "Wave soldering time",
                "body_mark": "Body mark",
            }
            export_df = udf.rename(columns=rename_map)
            export_cols = [
                "mpn",
                "Manufacture part number",
                "Manufacture",
                "Lifecycle",
                "ROHS",
                "Description",
                "MSD LEVEL",
                "DATASHEETLINK",
                "REFLOW SOLDERING TEMPERATURE",
                "THERMAL CYCLE",
                "WAVE SOLDERING TEMPERATURE",
                "LSL DETAILS",
                "PACKAGE",
                "PRICE DETAILS",
                "OPERATING TEMPERATURE",
                "Component thickness",
                "Reach",
                "Reflow soldering time",
                "Wave soldering time",
                "Body mark",
            ]
            for c in export_cols:
                if c not in export_df.columns:
                    export_df[c] = ""
            export_df = export_df[export_cols]

            st.success("DB filled and export prepared from available DB values.")
            st.caption(f"Requested MPNs: {len(mpn_list)} | Found in unified DB: {len(found_mpns)} | Missing: {len(missing_mpns)}")
            if missing_mpns:
                st.warning("Some requested MPNs are missing from unified DB. See list below.")
                st.dataframe(pd.DataFrame({"missing_mpn": missing_mpns}), width="stretch", hide_index=True)
            if one_mpn_view.strip():
                one = export_df[export_df["mpn"].astype(str).str.strip().str.upper() == one_mpn_view.strip().upper()]
                st.markdown("#### One MPN Detail View")
                if one.empty:
                    st.info("Entered MPN not found in fetched results.")
                else:
                    st.dataframe(one, width="stretch")

            st.markdown("#### Export Preview")
            st.dataframe(export_df, width="stretch")
            st.download_button(
                "📥 Download Export CSV",
                data=export_df.to_csv(index=False).encode("utf-8"),
                file_name="db_export.csv",
                mime="text/csv",
                key="single_option_download_csv",
            )

    st.markdown("---")
    st.subheader("Manual Data Entry (Direct to Unified DB)")
    with st.form("manual_unified_entry_form"):
        m1, m2, m3 = st.columns(3)
        manual_mpn = m1.text_input("MPN *")
        manual_mfr = m1.text_input("Manufacture")
        manual_mpn_mfr = m1.text_input("Manufacture part number")
        manual_lifecycle = m2.text_input("Lifecycle")
        manual_rohs = m2.text_input("ROHS")
        manual_desc = m2.text_input("Description")
        manual_msd = m3.text_input("MSD LEVEL")
        manual_datasheet = m3.text_input("DATASHEETLINK")
        manual_reflow = m3.text_input("REFLOW SOLDERING TEMPERATURE")

        n1, n2, n3 = st.columns(3)
        manual_thermal = n1.text_input("THERMAL CYCLE")
        manual_wave = n1.text_input("WAVE SOLDERING TEMPERATURE")
        manual_lsl = n1.text_input("LSL DETAILS")
        manual_package = n2.text_input("PACKAGE")
        manual_price = n2.text_input("PRICE DETAILS")
        manual_operating = n2.text_input("OPERATING TEMPERATURE")
        manual_component_thickness = n3.text_input("Component thickness")
        manual_reach = n3.text_input("Reach")
        manual_reflow_time = n3.text_input("Reflow soldering time")
        manual_wave_time = n3.text_input("Wave soldering time")
        manual_body_mark = n3.text_input("Body mark")
        manual_source = n3.text_input("Source Trace", value="ManualEntry")
        save_manual = st.form_submit_button("💾 Save Manual Entry")

    if save_manual:
        if not manual_mpn.strip():
            st.error("MPN is required for manual save.")
        else:
            ensure_unified_parts_table()
            with sqlite3.connect(DB_PATH) as conn:
                existing = conn.execute(
                    """
                    SELECT supplier_part_number, category, stock, product_url
                    FROM unified_part_cache WHERE mpn=?
                    """,
                    (manual_mpn.strip(),),
                ).fetchone()
                supplier_part_number, category, stock, product_url = existing if existing else ("", "", "", "")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO unified_part_cache
                    (mpn, manufacturer, manufacturer_part_number, supplier_part_number, description, category, lifecycle_status, rohs, stock, datasheet_url, product_url, msd_level, reflow_soldering_temperature, thermal_cycle, wave_soldering_temperature, lsl_details, package_details, price_details, operating_temperature, component_thickness, reach, reflow_soldering_time, wave_soldering_time, body_mark, source_trace, updated_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manual_mpn.strip(),
                        manual_mfr.strip(),
                        manual_mpn_mfr.strip(),
                        supplier_part_number,
                        manual_desc.strip(),
                        category,
                        manual_lifecycle.strip(),
                        manual_rohs.strip(),
                        stock,
                        manual_datasheet.strip(),
                        product_url,
                        manual_msd.strip(),
                        manual_reflow.strip(),
                        manual_thermal.strip(),
                        manual_wave.strip(),
                        manual_lsl.strip(),
                        manual_package.strip(),
                        manual_price.strip(),
                        manual_operating.strip(),
                        manual_component_thickness.strip(),
                        manual_reach.strip(),
                        manual_reflow_time.strip(),
                        manual_wave_time.strip(),
                        manual_body_mark.strip(),
                        manual_source.strip() or "ManualEntry",
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                conn.commit()
            st.success(f"Manual data saved for MPN: {manual_mpn.strip()}")
    show_footer()

with ui_tabs[4]:
    st.subheader("🏆 Live Price Comparison")
    c1, c2 = st.columns(2)
    cmp_source_a_key = c1.text_input(
        f"{SOURCE_MOUSER} API Key",
        value=os.getenv("MOUSER_API_KEY", MOUSER_API_KEY_FALLBACK),
        type="password",
        key="cmp_source_a_key",
    )
    cmp_source_b_id = c2.text_input(
        f"{SOURCE_DIGIKEY} Client ID",
        value=os.getenv("DIGIKEY_CLIENT_ID", DIGIKEY_CLIENT_ID_FALLBACK),
        key="cmp_source_b_id",
    )
    cmp_source_b_secret = c2.text_input(
        f"{SOURCE_DIGIKEY} Client Secret",
        value=os.getenv("DIGIKEY_CLIENT_SECRET", DIGIKEY_CLIENT_SECRET_FALLBACK),
        type="password",
        key="cmp_source_b_secret",
    )
    cmp_scope = c2.text_input(
        f"{SOURCE_DIGIKEY} Scope",
        value=os.getenv("DIGIKEY_SCOPE", ""),
        key="cmp_source_b_scope",
    )
    cmp_sandbox = c2.toggle(f"Use {SOURCE_DIGIKEY} Sandbox", value=False, key="cmp_source_b_sandbox")
    cmp_mpn = st.text_input("Enter MPN", key="cmp_mpn")

    if st.button("Compare Price", key="compare_price_btn"):
        if not cmp_mpn.strip():
            st.error("Enter MPN")
        elif not cmp_source_a_key.strip():
            st.error(f"Enter {SOURCE_MOUSER} API Key")
        elif not cmp_source_b_id.strip() or not cmp_source_b_secret.strip():
            st.error(f"Enter {SOURCE_DIGIKEY} credentials")
        else:
            comparison = compare_suppliers_price(
                cmp_mpn.strip(),
                source_a_key=cmp_source_a_key.strip(),
                source_b_id=cmp_source_b_id.strip(),
                source_b_secret=cmp_source_b_secret.strip(),
                source_b_scope=cmp_scope.strip() or None,
                source_b_sandbox=cmp_sandbox,
            )
            st.dataframe(pd.DataFrame([comparison]), width="stretch")
            st.markdown("#### Detailed Comparison")
            st.dataframe(build_comparison_details(comparison), width="stretch", hide_index=True)
            if comparison["Best Source"]:
                st.success(
                    f"🏆 Best Source: {comparison['Best Source']} | "
                    f"Lifecycle: {comparison.get('Best Lifecycle')} | "
                    f"Price: {comparison.get('Best Price')}"
                )
            else:
                st.warning("No valid price data found from live sources.")
    show_footer()
    show_footer()

with ui_tabs[4]:
    st.subheader("Mouser API Realtime Pricing + Datasheet Feed")
    st.caption("Tip: Set environment variable `MOUSER_API_KEY` and avoid hardcoding secrets in source code.")

    env_key = os.getenv("MOUSER_API_KEY", "")
    api_key = st.text_input(
        "Mouser API Key",
        value=env_key,
        type="password",
        help="Your key is used only for API requests from this running session.",
    )

    source_option = st.radio(
        "Choose part source",
        ["Use MPNs from database", "Upload Excel/CSV file"],
        horizontal=True,
    )

    selected_mpns = []
    if source_option == "Use MPNs from database":
        if not DB_PATH.exists():
            st.info("Database missing. Upload file mode is still available.")
        else:
            db_mpns = get_available_db_mpns()
            if not db_mpns:
                st.info("No MPNs found yet. Run scraper/import first, or use Upload mode.")
            selected_mpns = st.multiselect("Select MPNs to fetch from Mouser:", db_mpns)
    else:
        up = st.file_uploader("Upload MPN list (Excel/CSV, first column used)", type=["xlsx", "csv"], key="mouser_upload")
        if up:
            if up.name.lower().endswith(".csv"):
                selected_mpns = pd.read_csv(up).iloc[:, 0].dropna().astype(str).str.strip().tolist()
            else:
                selected_mpns = pd.read_excel(up).iloc[:, 0].dropna().astype(str).str.strip().tolist()
            st.write(f"Loaded {len(selected_mpns)} part numbers.")

    if st.button("🔄 Fetch Live Mouser Data"):
        if not api_key.strip():
            st.error("Please provide a valid Mouser API key.")
        elif not selected_mpns:
            st.error("Please provide at least one MPN.")
        else:
            results = []
            prog = st.progress(0)
            status = st.empty()
            total = len(selected_mpns)
            for i, mpn in enumerate(selected_mpns, start=1):
                status.info(f"Fetching {i}/{total}: {mpn}")
                try:
                    mouser_data = fetch_mouser_part_data(mpn, api_key.strip())
                    part_rows = mouser_data.get("parts", []) if isinstance(mouser_data, dict) else []
                    if part_rows:
                        results.extend(part_rows)
                    else:
                        results.append(
                            {
                                "Requested MPN": mpn,
                                "Mouser Part Number": "",
                                "Manufacturer Part Number": "",
                                "Manufacturer": "",
                                "Description": "No result found from Mouser",
                                "Category": "",
                                "Lifecycle Status": "",
                                "Availability": "",
                                "Stock": "",
                                "Lead Time": "",
                                "ROHS": "",
                                "Data Sheet URL": "",
                                "Product URL": "",
                                "Image URL": "",
                            }
                        )
                except HTTPError as http_err:
                    results.append({"Requested MPN": mpn, "Description": f"HTTP error: {http_err}"})
                except Exception as ex:
                    results.append({"Requested MPN": mpn, "Description": f"Error: {ex}"})
                prog.progress(i / total)

            out_df = pd.DataFrame(results)
            if out_df.empty:
                st.warning("No data returned.")
            else:
                st.success(f"Fetched {len(out_df)} row(s) from Mouser.")
                st.dataframe(out_df, use_container_width=True)
                st.download_button(
                    "📥 Download Mouser Feed (CSV)",
                    out_df.to_csv(index=False).encode("utf-8"),
                    file_name="mouser_live_feed.csv",
                    mime="text/csv",
                )
    show_footer()
