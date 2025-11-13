import hashlib, base64, requests, pandas as pd
from time import sleep

def generate_token():
    data = "pK3c76MsxY73eS9F2ke9gAvfBb2x84"
    digest = hashlib.sha256(data.encode("utf-8")).digest()
    token = base64.b64encode(digest).decode("utf-8")
    # print(f"Generated token: {token}")
    return token

def call_api(ti):
    token = ti
    url = "https://api.logicommerce.net/v1/orders"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {token}",
        "countryCode": "ES",
        "appid": "pK3c76MsxY"
    }
    response = requests.get(url, headers=headers)
    print("API Response:", response.status_code)
    response.raise_for_status()

    try:
        data = response.json()
        print(f"Fetched {len(data)} records")
    except Exception as e:
        print("Error parsing JSON:", e)
        data = []
    return data

def transform_data(ti):
    data = ti
    # print("Raw Data:", data)
    df = pd.json_normalize(data.get("ORDERS", []))
    # print("Transformed Data:", df['DOCUMENTNUMBER'])
    return df['DOCUMENTNUMBER'].tolist()

def call_api_id(ti, token = None):
    data = ti
    token = token
    results = []
    # print("Order IDs to process:", data)
    try:
        for x in data:
            print(f"Processing order ID: {x}")
            url = f"https://api.logicommerce.net/v1/orders/getid/{x}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
                "countryCode": "ES",
                "appid": "pK3c76MsxY"
            }
            response = requests.get(url, headers=headers)
            print("API Response:", response.status_code)
            response.raise_for_status()
            results.append(response.json())
            sleep(0.3)  # To avoid hitting rate limits
    except Exception as e:
        print("Error during API call for order IDs:", e)

    return results
def transform_data_id(ti):
    data = ti
    df = pd.DataFrame(data)
    print("Transformed Data for IDs:", df)
    return df

def call_api_id(ti, token = None):
    data = ti
    token = token
    results = []
    # print("Order IDs to process:", data)
    try:
        for x in data:
            print(f"Processing order ID: {x}")
            url = f"https://api.logicommerce.net/v1/orders/getid/{x}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
                "countryCode": "ES",
                "appid": "pK3c76MsxY"
            }
            response = requests.get(url, headers=headers)
            print("API Response:", response.status_code)
            response.raise_for_status()
            results.append(response.json())
            sleep(0.3)  # To avoid hitting rate limits
    except Exception as e:
        print("Error during API call for order IDs:", e)
def call_api_NumPedido(ti, token = None):
    data = ti
    token = token
    results = pd.DataFrame()
    try:
        for x in data:
            print(f"Processing order Number: {x}")
            url = f"https://api.logicommerce.net/v1/orders/{x}"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
                "countryCode": "ES",
                "appid": "pK3c76MsxY"
            }
            response = requests.get(url, headers=headers)
            print("API Response:", response.status_code)
            response.raise_for_status()
            response = response.json()
            response_df = pd.json_normalize(
                response,
                record_path=["ORDERS", "DETAILS"],
                meta=[["ORDERS","DATE"], ["ORDERS","ORDERID"], ["ORDERS","ORDERUSERS", "NIF"]],
                errors="ignore"
            ).rename(columns={"ORDERS.ORDERID": "PEDIDO",
                            "ORDERS.DATE": "DATE",
                            "ORDERS.ORDERUSERS.NIF": "NIF"})
            response_df["DISCOUNTVALUE"] = response_df["DISCOUNTS"].apply(
                lambda d: (
                    d[0].get("DISCOUNTVALUE")
                    if isinstance(d, list) and d and isinstance(d[0], dict)
                    else 0
                )
            )
            pedido_df = response_df[[
                "PEDIDO",
                "DATE",
                "SKU",
                "QUANTITY",
                "PRICE",
                "DISCOUNTVALUE",
                "NIF"
            ]]
            print(pedido_df)

            results = pd.concat([results, pedido_df], ignore_index=True)

            sleep(0.3)  # To avoid hitting rate limits
    except Exception as e:
        print("Error during API call:", e)
    print("Fetched order Numbers data:", results)
    NIF_DF = results['NIF'].drop_duplicates().reset_index(drop=True)
    print("Fetched NIF data:", NIF_DF)
    
    return results

def call_api_NIF(ti, token = None):
    data = ti
    token = token
    results = []
    # print("Order IDs to process:", data)
    try:
        for x in data:
            print(f"Processing order ID: {x}")
            url = f"https://apifidfarma.ecoceutics.com/v1/unit/{x}/fid/?api_key=657A8288P7156"
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {token}",
                "countryCode": "ES",
                "appid": "pK3c76MsxY"
            }
            response = requests.get(url, headers=headers)
            print("API Response:", response.status_code)
            response.raise_for_status()
            results.append(response.json())
            sleep(0.3)  # To avoid hitting rate limits
    except Exception as e:
        print("Error during API call for order IDs:", e)

    return results

# Example usage:
gen_token = generate_token()
api_data = call_api(ti=gen_token)
transformed_df = transform_data(api_data)
order_ids = call_api_id(ti=transformed_df, token=gen_token)
transformed_id_df = transform_data_id(order_ids)
pedidos_df = call_api_NumPedido(ti=transformed_df, token=gen_token)


