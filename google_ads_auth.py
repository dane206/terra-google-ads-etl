#!/usr/bin/env python3
"""
Terra Health Essentials — Google Ads OAuth Setup
=================================================
Run once to generate a refresh token for the Google Ads API.

Usage:
  python google_ads_auth.py

Steps:
  1. Opens a browser for you to log in with your Google Ads account
  2. Prints a refresh token
  3. Add the refresh token to config.ini

Requirements:
  pip install google-auth-oauthlib
"""

import os, configparser
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/adwords"]

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

CLIENT_ID     = config["google_ads"]["client_id"]
CLIENT_SECRET = config["google_ads"]["client_secret"]

client_config = {
    "installed": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}

def main():
    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    credentials = flow.run_local_server(port=8080, prompt="consent")

    print("\n" + "="*60)
    print("SUCCESS — Add this to your config.ini under [google_ads]:")
    print("="*60)
    print(f"refresh_token = {credentials.refresh_token}")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
