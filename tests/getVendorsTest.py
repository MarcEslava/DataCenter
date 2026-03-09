import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from dags.utils.clsZohoInput import ZohoCRMConnector, ZohoTokenManager
import time
import pandas as pd


CLIENT_ID = "1000.TB4P58146F209UUX572QQK5DGD2G6V"
CLIENT_SECRET = "813e36e342c089dbc89cf14b42e1f1e61c50df47d6"
REDIRECT_URL = "https://www.ecoceutics.com/wp-json/bitgfzc/redirect"
REFRESH_ID = "1000.a969c1c21564d6fdf99ddb1ea08fd395.ef9c552deb5ab38ce55d45224117493e"
AUTHORITATION_CODE = '1000.7a95ae9d24d67431d979138cf32694da.c2f272295627bd21cf4aad6a92b4f785'

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