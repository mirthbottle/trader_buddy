"""Pull Economic Indicators

BLS = US Bureau of Labor Statistics
FRED = Federal Reserve
"""

import os
import json
import pandas as pd
import requests
import pygsheets
from dagster import asset, AssetIn


@asset(io_manager_key="fs_io_manager")
def inflation_data() -> pd.DataFrame:
    """
    Pull inflation data from BLS
    """
    headers = {"Content-type": "application/json"}
    data = json.dumps({"seriesid": ["CUUR0000SA0"], "startyear": "2024", "endyear": "2024"})
    p = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)
    json_data = json.loads(p.text)
    df = pd.DataFrame.from_dict(json_data["Results"]["series"][0]["data"])
    df = df.astype({"year":'int32',"period": 'string',"periodName": 'string',"value":'float'})
    if 'footnotes' in df.columns:
        df.drop(columns=['footnotes'], axis=1, inplace=True)
    return df


GOOGLE_SERVICE_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

@asset(io_manager_key="fs_io_manager")
def inflation_gsheet(context, inflation_data: pd.DataFrame):
    gc = pygsheets.authorize(service_file=GOOGLE_SERVICE_FILE)
    sh = gc.open_by_key('1DdXB7q9AX_0ftpl6U0vHQ1oGSY_fZ3NX4VysbcxtlcY')
    wks = sh[0]
    wks.clear()
    wks.title = "Inflation Data"
    wks.set_dataframe(inflation_data, (1, 1))
    context.add_output_metadata({"row_count": len(inflation_data)})