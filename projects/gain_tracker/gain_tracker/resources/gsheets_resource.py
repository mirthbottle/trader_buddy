"""Open and update google spreadsheets

wrapper for pygsheets
"""

import pygsheets
from dagster import ConfigurableResource



class GSheetsResource(ConfigurableResource):
    google_service_file_loc: str

    def open_sheet_first_tab(
            self, sheet_key: str
            )->pygsheets.Worksheet:
        """Authorize gsheets
        """
        gc = pygsheets.authorize(service_file=self.google_service_file_loc)
        sh = gc.open_by_key(sheet_key)
        if sh:
            return sh[0]