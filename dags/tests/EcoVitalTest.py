import hashlib, base64, requests, pandas as pd
from time import sleep
import json, ast

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
            print(response_df.columns)
            pedido_df = response_df[[
                "PEDIDO",
                "DATE",
                "SKU",
                "QUANTITY",
                "PRICE",
                "DISCOUNTVALUE",
                "NIF",
                "TAXES"
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
    # try:
    for unit in data:
        item = [0,unit]
        print(f"Processing order NIF: {unit}")
        url = f"https://apifidfarma.ecoceutics.com/v1/unit/{unit}/fid/?api_key=657A8288P7156"
        headers = {
            "Accept": "application/json",
            "countryCode": "ES",
        }
        response = requests.get(url, headers=headers)
        print("API Response:", response.text)
        payload  = response.json()
        item[0] = payload
        response.raise_for_status()
        print(item)
        results.append(item)
        sleep(0.3)  # To avoid hitting rate limits
    # except Exception as e:
    #     print("Error during API call for order IDs:", e)

    return results

def getUsers(ti):
    token = ti
    url = "https://api.logicommerce.net/v1/users"
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

# Example usage:
gen_token = generate_token()
api_data = call_api(ti=gen_token)
transformed_df = transform_data(api_data)
order_ids = call_api_id(ti=transformed_df, token=gen_token)
transformed_id_df = transform_data_id(order_ids)
pedidos_df = call_api_NumPedido(ti=transformed_df, token=gen_token)
df_NIF = pd.DataFrame(call_api_NIF(ti=pedidos_df['NIF'].drop_duplicates().reset_index(drop=True), token=gen_token))
df = df_NIF.rename(columns={0: 'raw', 1: 'nif'})
df['id'] = df['raw'].str[0].str.get('id')
df = df[['id', 'nif']]
df.drop_duplicates(['id'],inplace=True)  # revisar mañana si funciona 24/11/25
df = df.merge(pedidos_df, left_on='nif', right_on='NIF', how='right')
df.drop(columns=['nif'], inplace=True)
mapping = {
    "id": "NIF2",
    "SKU": "PRODUCTO",
    "PRICE": "PRECIO",
    "QUANTITY": "UNIDADES",
    "DISCOUNTVALUE": "DTO",
    "DATE": "FECHA",
}
df.rename(columns=mapping, inplace=True)
print(df)

users = getUsers(ti=gen_token)
users = pd.json_normalize(users.get("USERS", []))

col = "ADDRESSBOOK.BILLINGADDRESS"
if col in users.columns:
    def _parse_cell(x):
        if x == None:
            return None
        if isinstance(x, (dict, list)):
            return x
        s = str(x)
        try:
            return json.loads(s)
        except Exception:
            try:
                return ast.literal_eval(s)
            except Exception:
                return None

    parsed = users[col].apply(_parse_cell)

    # Prepare a list of dicts for json_normalize
    normalized_items = []
    for item in parsed:
        if isinstance(item, list):
            if len(item) == 0:
                normalized_items.append({})
            elif isinstance(item[0], dict):
                normalized_items.append(item[0])
            else:
                normalized_items.append({"value": item})
        elif isinstance(item, dict):
            normalized_items.append(item)
        else:
            normalized_items.append({})

    billing_df = pd.json_normalize(normalized_items)
    billing_df.index = users.index  # keep alignment with original users rows

    print("Billing address dataframe:")
else:
    print(f"Column '{col}' not found in users dataframe")

fact_df = df.merge(billing_df, left_on='NIF', right_on='NIF', how='left')

mapping = {
    "PEDIDO": "Pedido",
    "FECHA": "F.Pedido",
    "COMPANY": "Farmacia",
    "ADDRESS": "Direcion",
    "CITY": "Poblacion",
    "ZIP": "Codigo Postal",
    "CITY": "Poblacion",
    "STATE": "Provincia",
    "PRODUCTO": "Codigo Producto",
    "UNIDADES": "C.Pedida",
    "PRECIO": "Precio",
    "DTO": "Descuento",
    "SKU": "PRODUCTO",
    "NIF": "CustomerCifId",
    "TAXES": "TAXES"
}

fact_df.rename(columns=mapping, inplace=True)
fact_df['Precio'] = fact_df['Precio'].round(2)
fact_df.to_excel("EcoVital_FactTable.xlsx", index=False, sheet_name="in")

