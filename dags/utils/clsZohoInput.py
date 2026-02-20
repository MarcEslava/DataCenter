import requests
import pandas as pd
from typing import List, Dict, Optional, Union
import json


class ZohoTokenManager:
    """Handles Zoho authentication token operations with better error handling"""
    TOKEN_URL = "https://accounts.zoho.eu/oauth/v2/token"
    
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_refresh_token(self, refresh_token: str,) -> str:
        """Get initial access token using authorization code"""
        params = {
            "grant_type": "refresh_token", #access_token use it for genereting the first refresh token
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        return self._request_token(params)


    def _base_params(self) -> Dict[str, str]:
        return {
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

    def _request_token(self, params: Dict[str, str]) -> str:
        # print(f"Requesting token with params: {params}")
        # print(f"Token URL: {self.TOKEN_URL}")
        response = requests.post(self.TOKEN_URL, data=params)
        
        # Debugging output
        # print(f"Token request status: {response.status_code}")
        # print(f"Response content: {response.text}")
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Provide more detailed error information
            error_msg = f"Token request failed: {e}"
            try:
                error_data = response.json()
                error_msg += f"\nError details: {json.dumps(error_data, indent=2)}"
            except:
                error_msg += f"\nResponse text: {response.text}"
            raise Exception(error_msg)
        
        data = response.json()
        
        if 'access_token' not in data:
            error_details = json.dumps(data, indent=2) if data else "No error details"
            raise Exception(f"Access token not found in response: {error_details}")
        
        return data['access_token']


class ZohoMailer:
    """Send emails via Zoho Mail API.

    Requires OAuth scope: ZohoMail.messages.CREATE
    Docs: https://www.zoho.com/mail/help/api/post-send-an-email.html
    """

    # EU domain — change to mail.zoho.com for non-EU accounts
    BASE_URL = "https://mail.zoho.eu/api/accounts"

    def __init__(self, access_token: str, account_id: str):
        self.account_id = account_id
        self.headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def send(
        self,
        from_address: str,
        to_address: Union[str, List[str]],
        subject: str,
        content: str,
        cc: Union[str, List[str], None] = None,
        bcc: Union[str, List[str], None] = None,
        mail_format: str = "html",
        ask_receipt: bool = False,
    ) -> Dict:
        """Send an email and return the API response."""
        if isinstance(to_address, list):
            to_address = ",".join(to_address)
        if isinstance(cc, list):
            cc = ",".join(cc)
        if isinstance(bcc, list):
            bcc = ",".join(bcc)

        url = f"{self.BASE_URL}/{self.account_id}/messages"
        payload = {
            "fromAddress": from_address,
            "toAddress": to_address,
            "subject": subject,
            "content": content,
            "mailFormat": mail_format,
            "askReceipt": "yes" if ask_receipt else "no",
        }
        if cc:
            payload["ccAddress"] = cc
        if bcc:
            payload["bccAddress"] = bcc

        response = requests.post(url, headers=self.headers, json=payload)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_msg = f"Zoho Mail send failed: {e}"
            try:
                error_msg += f"\nDetails: {json.dumps(response.json(), indent=2)}"
            except Exception:
                error_msg += f"\nResponse: {response.text}"
            raise Exception(error_msg)

        return response.json()


class ZohoCRMConnector:
    """Handles Zoho CRM API interactions"""
    
    def __init__(self, access_token: str, base_url: str = 'https://www.zohoapis.eu/crm/v2/'):
        self.base_url = base_url.rstrip('/') + '/'  # Ensure trailing slash
        self.headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'Content-Type': 'application/json'
        }

    def fetch_module_data(self, module_name: str, params: Optional[Dict] = None) -> List[Dict]:
        """Fetch data from specified Zoho CRM module"""
        url = f'{self.base_url}{module_name}'
        response = requests.get(url, headers=self.headers, params=params)
        
        
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_msg = f"API request failed: {e}"
            try:
                error_data = response.json()
                error_msg += f"\nError details: {json.dumps(error_data, indent=2)}"
            except:
                error_msg += f"\nResponse text: {response.text}"
            raise Exception(error_msg)
        
        data = response.json()
        
        if 'data' not in data:
            error_details = json.dumps(data, indent=2) if data else "No data in response"
            raise Exception(f"Data not found in response: {error_details}")

        return data['data']


