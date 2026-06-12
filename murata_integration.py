import requests
import json
import os
from datetime import datetime, timezone

# Murata API Configuration
MURATA_API_BASE_URL = "https://api.murata.com"
MURATA_API_VERSION = "v1"

def get_murata_headers(api_key):
    """Create headers for Murata API requests"""
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "ComponentEngineer/1.0"
    }


def search_murata_component(mpn, api_key, timeout=30):
    """Search for a component on Murata using MPN"""
    if not api_key or not mpn:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/search"
        
        params = {
            "q": str(mpn).strip(),
            "limit": 10
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as ex:
        print(f"Murata API search error: {ex}")
        return None


def get_murata_component_details(mpn, api_key, timeout=30):
    """
    Get comprehensive component details from Murata normalized to match existing structure
    Returns format compatible with Mouser/Digi-Key payloads
    """
    if not api_key or not mpn:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/products/{str(mpn).strip()}"
        
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        
        if data:
            # Normalize response to match existing part structure
            normalized = {
                "parts": [{
                    "Requested MPN": mpn,
                    "Supplier Part Number": data.get('partNumber', ''),
                    "Manufacturer Part Number": data.get('partNumber', ''),
                    "Manufacturer": "Murata",
                    "Description": data.get('productName', ''),
                    "Category": data.get('category', ''),
                    "Lifecycle Status": data.get('lifecycleStatus', ''),
                    "Quantity Available": int(data.get('inventory', {}).get('available', 0) if isinstance(data.get('inventory'), dict) else 0),
                    "Lead Time Weeks": data.get('leadTime', ''),
                    "Data Sheet URL": data.get('datastoreUrl', ''),
                    "Product URL": data.get('productUrl', ''),
                    "ROHS": data.get('rohsStatus', ''),
                }],
                "pricing": [],
                "attributes": [],
                "documents": [],
            }
            
            # Add pricing if available
            pricing = data.get('pricing', {})
            if isinstance(pricing, dict):
                for qty, price in pricing.items():
                    normalized["pricing"].append({
                        "Requested MPN": mpn,
                        "Break Quantity": qty,
                        "Unit Price": price,
                        "Currency": "USD"
                    })
            
            # Add specifications as attributes
            specs = data.get('specifications', {})
            if isinstance(specs, dict):
                for key, value in specs.items():
                    normalized["attributes"].append({
                        "Requested MPN": mpn,
                        "Attribute": str(key),
                        "Value": str(value),
                    })
            
            # Add datasheet to documents
            if data.get('datastoreUrl'):
                normalized["documents"].append({
                    "Requested MPN": mpn,
                    "Type": "Datasheet",
                    "URL": data.get('datastoreUrl', '')
                })
            
            return normalized
        return None
    except requests.exceptions.RequestException as ex:
        print(f"Murata API details error: {ex}")
        return None


def get_murata_stock(mpn, api_key, timeout=30):
    """Get stock information for a Murata component"""
    if not api_key or not mpn:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/products/{str(mpn).strip()}/inventory"
        
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as ex:
        print(f"Murata API stock error: {ex}")
        return None


def check_murata_availability(mpn, api_key, timeout=30):
    """
    Check if component is available in Murata
    Returns: (is_available: bool, stock_quantity: int, error_msg: str)
    """
    try:
        details = get_murata_component_details(mpn, api_key, timeout)
        if details and details.get("parts"):
            part = details["parts"][0]
            qty = int(part.get("Quantity Available", 0) or 0)
            return qty > 0, qty, ""
        return False, 0, "Component not found in Murata"
    except Exception as ex:
        return False, 0, str(ex)


def get_murata_pricing(mpn, api_key, quantity=1, timeout=30):
    """Get pricing information for a Murata component"""
    if not api_key or not mpn:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/products/{str(mpn).strip()}/pricing"
        
        params = {
            "quantity": int(quantity)
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as ex:
        print(f"Murata API pricing error: {ex}")
        return None


def get_murata_datasheet(mpn, api_key, timeout=30):
    """Get datasheet URL for a Murata component"""
    if not api_key or not mpn:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/products/{str(mpn).strip()}/datasheet"
        
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data.get('datastoreUrl', None)
    except requests.exceptions.RequestException as ex:
        print(f"Murata API datasheet error: {ex}")
        return None


def search_murata_by_category(category, api_key, timeout=30):
    """Search Murata components by category"""
    if not api_key or not category:
        return None
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/search"
        
        params = {
            "category": str(category).strip(),
            "limit": 25
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        return data
    except requests.exceptions.RequestException as ex:
        print(f"Murata API category search error: {ex}")
        return None


def validate_murata_api_key(api_key, timeout=30):
    """Validate Murata API Key"""
    if not api_key:
        return False
    
    try:
        headers = get_murata_headers(api_key)
        url = f"{MURATA_API_BASE_URL}/{MURATA_API_VERSION}/validate"
        
        response = requests.get(url, headers=headers, timeout=timeout)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


if __name__ == "__main__":
    # Test the integration
    test_api_key = os.getenv('MURATA_API_KEY', '')
    test_mpn = "GRM155R60J105KE19L"  # Example Murata capacitor
    
    if test_api_key:
        print(f"Testing Murata API with MPN: {test_mpn}\n")
        
        # Validate API key
        if validate_murata_api_key(test_api_key):
            print("✓ API Key is valid\n")
            
            # Get component details
            details = get_murata_component_details(test_mpn, test_api_key)
            if details:
                print("Component Details:")
                print(json.dumps(details, indent=2))
            else:
                print("Component not found")
        else:
            print("✗ API Key is invalid")
    else:
        print("Please set MURATA_API_KEY environment variable")
