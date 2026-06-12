import requests
import os
import json
import time
from datetime import datetime, timezone
from urllib import parse, request
from urllib.error import HTTPError

# Digi-Key API Configuration
DIGIKEY_API_KEY = os.getenv('DIGIKEY_API_KEY', '')
DIGIKEY_CLIENT_ID = os.getenv('DIGIKEY_CLIENT_ID', '')
DIGIKEY_CLIENT_SECRET = os.getenv('DIGIKEY_CLIENT_SECRET', '')
DIGIKEY_API_URL = "https://api.digikey.com/products/v4/search"
DIGIKEY_TOKEN_URL = "https://api.digikey.com/v1/oauth2/token"

_DIGIKEY_TOKEN_CACHE = {}

def get_digikey_access_token(client_id, client_secret, use_sandbox=False, timeout=30, scope=None):
    """
    Get Digi-Key OAuth2 access token
    
    Args:
        client_id: Digi-Key Client ID
        client_secret: Digi-Key Client Secret
        use_sandbox: Use sandbox API if True
        timeout: Request timeout
        scope: OAuth scope
        
    Returns:
        str: Access token
    """
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
        raise RuntimeError(f"Digi-Key token error {e.code}: {detail[:400]}") from e
    
    token = token_data.get("access_token")
    if not token:
        raise ValueError("Digi-Key access token not returned.")
    
    expires_in = int(token_data.get("expires_in", 900) or 900)
    _DIGIKEY_TOKEN_CACHE[cache_key] = {
        "token": str(token),
        "expires_at": time.time() + max(60, expires_in - 30),
    }
    return token


def search_component_digikey(mpn, client_id, client_secret, use_sandbox=False):
    """
    Search for a component on Digi-Key using MPN
    
    Args:
        mpn: Manufacturer Part Number
        client_id: Digi-Key Client ID
        client_secret: Digi-Key Client Secret
        use_sandbox: Use sandbox API
        
    Returns:
        dict: Component details from Digi-Key API
    """
    try:
        token = get_digikey_access_token(client_id, client_secret, use_sandbox=use_sandbox)
        host = "sandbox-api.digikey.com" if use_sandbox else "api.digikey.com"
        url = f"https://{host}/products/v4/search/keyword"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "X-DIGIKEY-Client-Id": str(client_id).strip(),
            "X-DIGIKEY-Locale-Site": "US",
            "X-DIGIKEY-Locale-Currency": "USD",
            "X-DIGIKEY-Locale-Language": "en",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        payload = {
            "Keywords": str(mpn).strip(),
            "RecordCount": 25,
        }
        
        req = request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        
        with request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        
        return data
    except Exception as ex:
        print(f"Digi-Key search error: {ex}")
        return None


def get_component_price(mpn, client_id, client_secret, use_sandbox=False):
    """
    Get real-time pricing from Digi-Key
    
    Args:
        mpn: Manufacturer Part Number
        client_id: Digi-Key Client ID
        client_secret: Digi-Key Client Secret
        use_sandbox: Use sandbox API
        
    Returns:
        dict: Pricing information or None
    """
    data = search_component_digikey(mpn, client_id, client_secret, use_sandbox)
    if data and isinstance(data, dict):
        products = data.get('Products', [])
        if products and isinstance(products[0], dict):
            pricing = products[0].get('StandardPricing', [])
            if pricing and isinstance(pricing[0], dict):
                return {
                    'quantity': pricing[0].get('BreakQuantity'),
                    'unit_price': pricing[0].get('UnitPrice'),
                    'total_price': pricing[0].get('TotalPrice'),
                    'currency': 'USD'
                }
    return None


def check_stock_digikey(mpn, client_id, client_secret, use_sandbox=False):
    """
    Check stock availability on Digi-Key
    
    Args:
        mpn: Manufacturer Part Number
        client_id: Digi-Key Client ID
        client_secret: Digi-Key Client Secret
        use_sandbox: Use sandbox API
        
    Returns:
        int: Quantity in stock or 0
    """
    data = search_component_digikey(mpn, client_id, client_secret, use_sandbox)
    if data and isinstance(data, dict):
        products = data.get('Products', [])
        if products and isinstance(products[0], dict):
            return int(products[0].get('QuantityAvailable', 0) or 0)
    return 0


def get_component_details(mpn, client_id, client_secret, use_sandbox=False):
    """
    Get comprehensive component details from Digi-Key
    
    Args:
        mpn: Manufacturer Part Number
        client_id: Digi-Key Client ID
        client_secret: Digi-Key Client Secret
        use_sandbox: Use sandbox API
        
    Returns:
        dict: Component details including specifications, pricing, stock
    """
    data = search_component_digikey(mpn, client_id, client_secret, use_sandbox)
    if data and isinstance(data, dict):
        products = data.get('Products', [])
        if products and isinstance(products[0], dict):
            product = products[0]
            
            # Extract pricing
            pricing_info = {}
            pricing = product.get('StandardPricing', [])
            if pricing and isinstance(pricing[0], dict):
                pricing_info = {
                    'quantity': pricing[0].get('BreakQuantity'),
                    'unit_price': pricing[0].get('UnitPrice'),
                    'total_price': pricing[0].get('TotalPrice'),
                }
            
            # Extract specifications
            specs = {}
            parameters = product.get('Parameters', [])
            if isinstance(parameters, list):
                for param in parameters[:20]:  # Limit to first 20 params
                    if isinstance(param, dict):
                        param_text = str(param.get('ParameterText', ''))
                        value_text = str(param.get('ValueText', ''))
                        if param_text and value_text:
                            specs[param_text] = value_text
            
            return {
                'mpn': mpn,
                'digikey_part_number': str(product.get('DigiKeyPartNumber', '')),
                'manufacturer': str(product.get('Manufacturer', {}).get('Name', '') if isinstance(product.get('Manufacturer', {}), dict) else product.get('Manufacturer', '')),
                'manufacturer_part_number': str(product.get('ManufacturerPartNumber', '')),
                'description': str(product.get('ProductDescription', '') or product.get('Description', '')),
                'category': str(product.get('Category', {}).get('Name', '') if isinstance(product.get('Category', {}), dict) else product.get('Category', '')),
                'quantity_available': int(product.get('QuantityAvailable', 0) or 0),
                'lead_time_weeks': str(product.get('ManufacturerLeadWeeks', '')),
                'pricing': pricing_info,
                'specifications': specs,
                'datasheet_url': str(product.get('DatasheetUrl', '')),
                'product_url': str(product.get('ProductUrl', '')),
                'rohs_status': str(product.get('RohsStatus', '') or product.get('RoHSStatus', '')),
                'lifecycle_status': str(product.get('ProductStatus', '') or product.get('PartStatus', '')),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
    
    return None


def compare_suppliers_on_stock_price(mpn, mouser_key="", digikey_id="", digikey_secret="", digikey_sandbox=False):
    """
    Compare stock and pricing between Mouser and Digi-Key
    
    Args:
        mpn: Manufacturer Part Number
        mouser_key: Mouser API Key (optional)
        digikey_id: Digi-Key Client ID
        digikey_secret: Digi-Key Client Secret
        digikey_sandbox: Use Digi-Key sandbox
        
    Returns:
        dict: Comparison data
    """
    result = {
        'mpn': mpn,
        'digikey': {
            'stock': 0,
            'price': None,
            'status': 'not_checked'
        }
    }
    
    # Get Digi-Key data
    if digikey_id and digikey_secret:
        try:
            result['digikey']['stock'] = check_stock_digikey(mpn, digikey_id, digikey_secret, digikey_sandbox)
            price_data = get_component_price(mpn, digikey_id, digikey_secret, digikey_sandbox)
            if price_data:
                result['digikey']['price'] = price_data.get('unit_price')
                result['digikey']['status'] = 'available'
            else:
                result['digikey']['status'] = 'no_price_data'
        except Exception as ex:
            result['digikey']['status'] = f'error: {str(ex)[:50]}'
    
    return result


if __name__ == "__main__":
    # Test the integration
    test_mpn = "STM32F407VG"
    client_id = os.getenv('DIGIKEY_CLIENT_ID')
    client_secret = os.getenv('DIGIKEY_CLIENT_SECRET')
    
    if client_id and client_secret:
        print(f"Testing Digi-Key API with MPN: {test_mpn}\n")
        
        details = get_component_details(test_mpn, client_id, client_secret)
        if details:
            print(json.dumps(details, indent=2))
        else:
            print("No component found")
    else:
        print("Please set DIGIKEY_CLIENT_ID and DIGIKEY_CLIENT_SECRET environment variables")
