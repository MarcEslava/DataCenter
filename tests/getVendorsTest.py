import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dags.utils.clsZohoInput import ZohoCRMConnector, ZohoTokenManager
import time
import pandas as pd


# Credencials Zoho: surten de l'entorn, MAI hardcodejades al codi.
CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET", "")
REDIRECT_URL = os.environ.get("ZOHO_REDIRECT_URL", "https://www.ecoceutics.com/wp-json/bitgfzc/redirect")
REFRESH_ID = os.environ.get("ZOHO_REFRESH_TOKEN", "")
AUTHORITATION_CODE = os.environ.get("ZOHO_AUTH_CODE", "")

def extract_vendors() -> list[dict]:
    """Extract vendors from Zoho and return one dict per client group."""
    # Get access token
    token_manager = ZohoTokenManager(CLIENT_ID, CLIENT_SECRET)
    access_token = token_manager.get_refresh_token(REFRESH_ID)
    print (f"code: {access_token}")

    # Fetch products data
    connector = ZohoCRMConnector(access_token)
    page = 0
    raw_products = []
    while True:
        try:
            params = {'page': page, 'per_page': 200}
            page_products = connector.fetch_module_data('Vendors', params=params)
            raw_products.extend(page_products)
        except Exception:
            print(f"Finished fetching vendors")
            break
        # print(f"Fetched page {page} with {len(page_products)} vendors")
        page += 1
        time.sleep(0.3)
    print(f"Total vendors fetched: {len(raw_products)}")
        # Process and export data
    raw_df = pd.DataFrame(raw_products)
    print(raw_df.columns)
    owner_df = pd.json_normalize(raw_df['Owner']) # Normalize the 'Owner' column
    owner_df.columns = [f"Owner.{col}" for col in owner_df.columns] # Prefix owner columns
    owner_df = pd.concat([raw_df.drop('Owner', axis=1), owner_df], axis=1) # Flatten the 'Owner' column
    owner_df = owner_df[['Vendor_Name', 'Owner.name', 'Owner.id', 'Owner.email']] # Select relevant columns
    print(owner_df.head())
extract_vendors()