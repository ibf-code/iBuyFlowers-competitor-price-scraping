import os
import requests
from dotenv import load_dotenv

load_dotenv()

LOGIN_URL = "https://www.mayesh.com/api/auth/login"
DATES_URL = "https://www.mayesh.com/api/auth/dates"

def get_earliest_eta_date():

    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")

    # Step 1: Login
    session = requests.Session()
    payload = {"email": email, "password": password}
    headers = {"Content-Type": "application/json"}

    try:
        login_response = session.post(LOGIN_URL, json=payload, headers=headers)
        login_response.raise_for_status()
        token = login_response.json()["data"]["token"]
        headers["Authorization"] = f"Bearer {token}"
    except Exception as e:
        raise Exception(f"❌ Login failed: {e}")

    # Step 2: Fetch dates
    try:
        dates_response = session.post(DATES_URL, json={}, headers=headers)
        dates_response.raise_for_status()
        dates = dates_response.json().get("dates", [])
        farm_direct = [d["delivery_date"] for d in dates if d.get("program_id") == 5]

        if not farm_direct:
            raise ValueError("❌ No Farm Direct delivery dates found.")

        earliest = min(farm_direct)
        print(f"🗓️ Earliest ETA date: {earliest}")
        return earliest

    except Exception as e:
        raise Exception(f"❌ Failed to fetch ETA dates: {e}")