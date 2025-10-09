"""
Token refresh utility for API authentication
"""

import requests


def refresh_token() -> dict:
    """Refresh access token for Tenda API"""
    url = "https://api.tendaatacado.com.br/api/public/oauth/access-token"
    data = {
        "refresh_token": "a69e2cdc780eb1b283661452278c7ca8",
        "client_id": "79ggnm96dwlojly6mqulzval0h4b94gc",
        "client_secret": "ix2tid1exrsvc8u4ta2tys1p495sa3sk3h6o6fgp0kdpu7xgmb595b8525m9rfvj",
        "grant_type": "refresh_token"
    }
    
    response = requests.post(url, data=data, timeout=30)
    return response.json()


if __name__ == "__main__":
    result = refresh_token()
    print(result)
