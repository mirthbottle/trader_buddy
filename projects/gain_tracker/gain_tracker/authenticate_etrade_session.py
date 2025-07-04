"""
Executable script to authenticate an E-Trade session and save the session details.

run from projects/gain_tracker/gain_tracker
"""


import dotenv
import os
import pandas as pd
from datetime import date, datetime, timedelta

from resources.etrade_api import ETradeAPI
import json

load_dotenv = dotenv.load_dotenv()

consumer_key = os.getenv("ETRADE_KEY")
consumer_secret = os.getenv("ETRADE_SECRET")

if __name__ == "__main__":
    etrader = ETradeAPI("prod")
    etrader.authenticate_session()

    print(etrader.dagster_config_input_string())