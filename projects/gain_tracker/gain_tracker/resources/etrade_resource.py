"""Authenticate ETrade API
"""
import os
from dagster import ConfigurableResource, InitResourceContext

from .etrade_api import ETradeAPI


env = os.getenv("ENV", "dev")
env = "prod" # always pull real etrade data
        
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