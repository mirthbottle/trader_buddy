"""ETrade API Calls

"""

import os
import logging
from typing import Optional
import pandas as pd
import webbrowser
from rauth import OAuth1Service, OAuth1Session

# Set up logging
logger = logging.getLogger(__name__)

class ETradeAPI:
    """:description: Performs authorization for OAuth 1.0a

       :param client_key: Client key provided by Etrade
       :type client_key: str, required
       :param client_secret: Client secret provided by Etrade
       :type client_secret: str, required
       :param callback_url: Callback URL passed to OAuth mod, defaults to "oob"
       :type callback_url: str, optional
       :EtradeRef: https://apisb.etrade.com/docs/api/authorization/request_token.html

    """

    def __init__(
            self, environment:str="dev", callback_url:str="oob",
            session_token:Optional[str]='', 
            session_token_secret:Optional[str]=''):

        self.environment = environment

        if environment == "dev":
            consumer_key = os.getenv("ETRADE_SANDBOX_KEY")
            consumer_secret = os.getenv("ETRADE_SANDBOX_SECRET")
            self.base_url = r"https://apisb.etrade.com"
        elif environment == "prod":
            consumer_key = os.getenv("ETRADE_KEY")
            consumer_secret = os.getenv("ETRADE_SECRET")
            self.base_url = r"https://api.etrade.com"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
 
        self.req_token_url = f"{self.base_url}/oauth/request_token"
        self.auth_url = r"https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
        self.access_token_url = f"{self.base_url}/oauth/access_token"
        self.callback_url = callback_url
        self.access_token = None
        self.resource_owner_key = None
        self.session = None
        self.session_token = session_token
        self.session_token_secret = session_token_secret

    def authenticate_session(self):
        """Authenticate with consumer key and consumer secret
        Opens web browser and user inputs the verfier 
        """
        # Set up session
        service = OAuth1Service(
            name="etrade",
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            request_token_url=self.req_token_url,
            access_token_url=self.access_token_url,
            authorize_url=self.auth_url,
            base_url=self.base_url)

        # get request token
        oauth_token, oauth_token_secret = service.get_request_token(
            params={"oauth_callback": self.callback_url, "format": "json"})
        
        # get authorization url
        # etrade format: url?key&token
        authorize_url = service.authorize_url.format(
            self.consumer_key, oauth_token)
        webbrowser.open(authorize_url)
        oauth_verifier = input("Please accept agreement and enter verification code from browser: ")
        
        # get request token
        # oauth_token, oauth_token_secret = service.get_request_token(
        #     params={"oauth_callback": self.callback_url, "format": "json"})
        
        session = service.get_auth_session(
            oauth_token, oauth_token_secret,
            params={"oauth_verifier": oauth_verifier})
        self.session = session

        self.session_token = session.access_token
        self.session_token_secret = session.access_token_secret
        return session
    
    def create_authenticated_session(self):
        """create Session that has been authenticated already
        """
        self.session = OAuth1Session(
            self.consumer_key, self.consumer_secret,
            access_token = self.session_token,
            access_token_secret = self.session_token_secret)

        return self.session
    
    def renew_access_token(self):
        """Renew token

        token expires after 2 hrs, can be renewed up to 1 day
        """
        response = self.session.get(f"{self.base_url}/oauth/renew_access_token")

        # check status and if it's not 200, then request new token
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            logger.info("Reauthorize session")
            self.authenticate_session()
            return None

        return response

    def list_accounts(self):
        """GET request to list accounts
        Accounts - list accounts
        """
        json_path = "/v1/accounts/list.json"

        request_url = f"{self.base_url}{json_path}"
        response = self.session.get(request_url, params={"format": "json"})

        # check reponse code is 200
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            return None

        # if there are any accounts, they will be here
        accounts = response.json()["AccountListResponse"]["Accounts"]["Account"]

        return accounts
    
    def account_balance(
            self, account_id_key:str, inst_type:str="BROKERAGE"):
        """GET request for the account balance

        not able to retrieve for a particular time (no history)
        """
        account_balances_url = f"{self.base_url}/v1/accounts/{account_id_key}/balance.json"

        response = self.session.get(
            account_balances_url, params = {"instType": inst_type})
        
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            return None

        balance = response.json()["BalanceResponse"]
        return balance
    
    def view_portfolio(
            self, account_id_key:str, totals_required:bool=True,
            page_number:int=0
    ) -> Optional[pd.DataFrame]:
        """GET request for an account's portfolio holdings

        can't request history
        returns 50 by default

        always returns:
        positionId
        symbolDescription
        dateAcquired
        pricePaid
        quantity
        positionType
        daysGainPct
        totalGainPct
        totalCost
        costPerShare

        QUICK view
        FUNDAMENTAL view has marketCap, eps, and some other indicators
        COMPLETE view
        """

        view_portfolio_url = f"{self.base_url}/v1/accounts/{account_id_key}/portfolio.json"

        response = self.session.get(
            view_portfolio_url, 
            params={
                "totalsRequired": totals_required, "pageNumber": page_number,
                "lotsRequired": True}
        )
        
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            return None

        portfolio = response.json()["PortfolioResponse"]["AccountPortfolio"][0]
        positions = portfolio["Position"]

        if page_number < portfolio["totalPages"] - 1:
            # get the rest of pages
            # no need for totals in requests
            positions = positions + self.view_portfolio(
                account_id_key, False, page_number=page_number+1)
        # totals = portfolio
        
        p_lots = []
        for p in positions:
            # request details of lots
            # the same positionId may have more than one positionLot
            # each has its own positionLotId with a different price
            # and acquiredDate
            l_resp = self.session.get(p["lotsDetails"])
            l_data = l_resp.json()["PositionLotsResponse"]["PositionLot"]
            p_lots = p_lots+l_data

        if len(positions) > 0:
            ps = pd.DataFrame(positions).set_index("positionId").drop("marketValue", axis=1)
            pls = pd.DataFrame(p_lots).set_index("positionId")
            positions_df = ps.join(pls[[
                "price", "acquiredDate", "positionLotId",
                "marketValue", "originalQty", "remainingQty"]])
            positions_df.loc[:, "pricePaid"] = positions_df["price"]
            positions_df.loc[:, "dateAcquired"] = positions_df["acquiredDate"]
            positions_df.loc[:, "quantity"] = positions_df["remainingQty"]
            return positions_df.reset_index()
        else:
            return pd.DataFrame([])

    def get_transactions(
            self, account_id_key:str,
            date_range: Optional[tuple[str, str]]=None
    ) -> Optional[pd.DataFrame]:
        """Retrieve transactions data

        enter in start_date MMDDYYYY
        """
        transactions_url = f"{self.base_url}/v1/accounts/{account_id_key}/transactions.json"
        params = {}
        if date_range:
            start_date, end_date = date_range
            params["startDate"] = start_date
            params["endDate"] = end_date
        print(params)
        response = self.session.get(transactions_url, params=params)
        
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            return None
        
        transactions = response.json()["TransactionListResponse"]["Transaction"]
        
        if len(transactions) == 50:
            print("Warning: There are probably additional transactions")
        
        if len(transactions) > 0:
            ts = pd.DataFrame(transactions)
            brok_details = ts["brokerage"].apply(pd.Series)
            ts = pd.concat([ts, brok_details], axis=1)
            return ts
        else:
            return None
