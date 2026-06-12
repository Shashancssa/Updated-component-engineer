import requests
import json
import os
from datetime import datetime, timezone

# Murata API Configuration
MURATA_API_BASE_URL = "https://api.murata.com"
MURATA_API_VERSION = "v1"

def get_murata_headers(api_key):
    """
    Create headers for Murata API requests
    
    Args:
        api_key: Murata API Key
        
    Returns:
        dict: Headers for API requests
    """
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "ComponentEngineer/1.0"
    }


def search_murata_component(mpn, api_key, timeout=30):
    """
    Search for a component on Murata using MPN
    
    Args:
        mpn: Manufacturer Part Number
        api_key: Murata API Key
        timeout: Request timeout in seconds
        
    Returns:
        dict: Component details from Murata API or None
    """
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
    Get comprehensive component details from Murata
    
    Args:
        mpn: Manufacturer Part Number
        api_key: Murata API Key
        timeout: Request timeout in seconds
        
    Returns:
        dict: Component details including specifications, pricing, stock
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
            return {
                'mpn': mpn,
                'murata_part_number': data.get('partNumber', ''),
                'product_name': data.get('productName', ''),
                'category': data.get('category', ''),
                'description': data.get('description', ''),
                'manufacturer': data.get('manufacturer', 'Murata'),
                'specifications': data.get('specifications', {}),
                'datasheet_url': data.get('datastoreUrl', ''),
                'product_url': data.get('productUrl', ''),
                'pricing': data.get('pricing', {}),
                'stock_info': data.get('inventory', {}),
                'rohs_status': data.get('rohsStatus', ''),
                'lifecycle_status': data.get('lifecycleStatus', ''),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
        return None
    except requests.exceptions.RequestException as ex:
        print(f"Murata API details error: {ex}")
        return None


def get_murata_stock(mpn, api_key, timeout=30):
    """
    Get stock information for a Murata component
    
    Args:
        mpn: Manufacturer Part Number
        api_key: Murata API Key
        timeout: Request timeout in seconds
        
    Returns:
        dict: Stock information or None
    """
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


def get_murata_pricing(mpn, api_key, quantity=1, timeout=30):
    """
    Get pricing information for a Murata component
    
    Args:
        mpn: Manufacturer Part Number
        api_key: Murata API Key
        quantity: Quantity for pricing
        timeout: Request timeout in seconds
        
    Returns:
        dict: Pricing information or None
    """
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
    """
    Get datasheet URL for a Murata component
    
    Args:
        mpn: Manufacturer Part Number
        api_key: Murata API Key
        timeout: Request timeout in seconds
        
    Returns:
        str: Datasheet URL or None
    """
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
    """
    Search Murata components by category
    
    Args:
        category: Product category
        api_key: Murata API Key
        timeout: Request timeout in seconds
        
    Returns:
        dict: Search results or None
    """
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
    """
    Validate Murata API Key
    
    Args:
        api_key: Murata API Key to validate
        timeout: Request timeout in seconds
        
    Returns:
        bool: True if valid, False otherwise
    """
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
