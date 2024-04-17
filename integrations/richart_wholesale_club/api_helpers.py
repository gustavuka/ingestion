import json
import time

import requests
from requests.models import Response

BASE_URL = "http://localhost:5000"


# Load sensitive information using a .env file
# loca.env format -> GRANT_TYPE=xxxxxxx
def load_env_vars(path_to_env: str) -> dict:
    env_vars = {}
    with open(path_to_env) as file:
        for line in file:
            if line.startswith("#"):
                continue
            key, value = line.strip().split("=", 1)
            env_vars[key] = value

    return env_vars


def get_credentials(path_to_env: str) -> str:
    env_vars = load_env_vars(path_to_env)
    GRANT_TYPE = env_vars.get("GRANT_TYPE")
    CLIENT_ID = env_vars.get("CLIENT_ID")
    CLIENT_SECRET = env_vars.get("CLIENT_SECRET")

    response = requests.post(
        f"{BASE_URL}/oauth/token?client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&grant_type={GRANT_TYPE}"
    )
    time.sleep(1)

    return response.json().get("access_token")


def get_merchant_id(name: str, token: str) -> str:
    path = f"{BASE_URL}/api/merchants"
    headers = {"token": f"Bearer {token}"}

    response = requests.get(path, headers=headers)

    return [
        merchant.get("id")
        for merchant in response.json().get("merchants")
        if merchant.get("name") == name
    ][0]


def update_merchant(
    id: str,
    token: str,
    name: str,
    can_be_deleted: bool = True,
    can_be_updated: bool = True,
    is_active: bool = False,
) -> Response:
    path = f"{BASE_URL}/api/merchants/{id}"
    headers = {"token": f"Bearer {token}", "Content-type": "application/json"}

    payload = json.dumps(
        {
            "id": id,
            "name": name,
            "can_be_deleted": can_be_deleted,
            "can_be_updated": can_be_updated,
            "is_active": is_active,
        }
    )

    return requests.put(path, data=payload, headers=headers)


def delete_store(name: str, token: str) -> Response:
    store_id = get_merchant_id(name, token)
    path = f"{BASE_URL}/api/merchants/{store_id}"
    headers = {"token": f"Bearer {token}"}

    return requests.delete(path, headers=headers)


def send_products_info(payload: dict, token: str) -> Response:
    path = f"{BASE_URL}/api/products"
    headers = {"token": f"Bearer {token}", "Content-type": "application/json"}

    return requests.post(path, data=json.dumps(payload), headers=headers)
