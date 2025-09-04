"""Authenticate ETrade API
"""
import os
import re
from dagster import ConfigurableResource, InitResourceContext

from .etrade_api import ETradeAPI


env = os.getenv("ENV", "dev")
# env = "prod" # always pull real etrade data

def camel_to_snake(camel_case):
    """convert to something bigquery-friendly

    move this to a formatting io location, maybe in resources?
    ideally it would go in a new lib project. not sure how to import that
    """
    # Use regular expressions to split the string at capital letters
    cc_adj = camel_case[0].upper()+camel_case[1:]
    words = re.findall(r'[A-Z][a-z0-9]*', cc_adj)
    # Join the words with underscores and convert to lowercase
    snake_case = '_'.join(words).lower()
    return snake_case



class ETrader(ConfigurableResource):
    session_token: str
    session_token_secret: str

    def create_resource(self, context: InitResourceContext) -> ETradeAPI:
        """Use existing ETradeAPI object and return an instance
        """
        etrader = ETradeAPI(
            env, session_token=self.session_token,
            session_token_secret=self.session_token_secret)
        session = etrader.create_authenticated_session()
        etrader.renew_access_token()
        return etrader