import base64
import json
import requests

CONN_ID = "zepto_mail"
ZEPTO_API_URL = "https://api.zeptomail.eu/v1.1/email"


def _get_conn_params() -> tuple[str, str]:
    """Return (api_key, from_address) from the Airflow connection."""
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(CONN_ID)
    from_address = (json.loads(conn.extra or "{}")).get("from_address", "noreply@ecoceutics.net")
    return conn.password, from_address


class ZohoMailer:
    """Send transactional emails via Zoho ZeptoMail."""

    def __init__(self):
        api_key, from_address = _get_conn_params()
        self.from_address = from_address
        self.headers = {
            "accept":        "application/json",
            "content-type":  "application/json",
            "authorization": api_key,
        }

    def send(self, to: list[dict], subject: str, html_body: str, attachments: list[dict] | None = None) -> dict:
        """
        attachments: list of dicts with keys:
            - content: str (CSV or text content)
            - name: str (filename, e.g. "productos.csv")
            - mime_type: str (e.g. "text/csv")
        """
        """Send an email.

        Args:
            to: list of recipients, each a dict with 'address' and optionally 'name'.
                e.g. [{"address": "user@example.com", "name": "User"}]
            subject: email subject line.
            html_body: HTML content of the email.

        Returns:
            Parsed JSON response from ZeptoMail.
        """
        payload = {
            "from": {"address": self.from_address},
            "to": [
                {"email_address": {"address": r["address"], "name": r.get("name", "")}}
                for r in to
            ],
            "subject":  subject,
            "htmlbody": html_body,
        }
        if attachments:
            payload["attachments"] = [
                {
                    "content":   base64.b64encode(a["content"] if isinstance(a["content"], bytes) else a["content"].encode()).decode(),
                    "mime_type": a.get("mime_type", "text/csv"),
                    "name":      a["name"],
                }
                for a in attachments
            ]
        response = requests.post(ZEPTO_API_URL, json=payload, headers=self.headers)
        response.raise_for_status()
        return response.json()
