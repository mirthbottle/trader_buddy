"""ETrade API Calls

"""

import logging
import webbrowser
from rauth import OAuth1Service

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
            self, consumer_key: str, consumer_secret: str, 
            environment:str="dev", callback_url:str="oob"):

        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.environment = environment

        if environment == "dev":
            self.base_url = r"https://api.etrade.com"
        elif environment == "prod":
            self.base_url = r"https://apisb.etrade.com"

        self.req_token_url = f"{self.base_url}/oauth/request_token"
        self.auth_url = r"https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
        self.access_token_url = f"{self.base_url}/oauth/access_token"
        self.callback_url = callback_url
        self.access_token = None
        self.resource_owner_key = None
        self.session = None

    def get_session(self):
        """:description: Obtains the token URL from Etrade.

           :param None: Takes no parameters
           :return: Formatted Authorization URL (Access this to obtain taken)
           :rtype: str
           :EtradeRef: https://apisb.etrade.com/docs/api/authorization/request_token.html

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
        text_code = input("Please accept agreement and enter verification code from browser: ")

        session = service.get_auth_session(
            oauth_token, oauth_token_secret,
            params={"oauth_verifier": text_code})
        self.session = session
        return session
    
    def renew_access_token(self):
        """Renew token

        token expires after 2 hrs, can be renewed up to 1 day
        """
        response = self.session.get(f"{self.base_url}/oauth/renew_access_token")

        # check status and if it's not 200, then request new token
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            logger.info("Reauthorize session")
            self.get_session()
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
    ):
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
                "totalsRequired": totals_required, "pageNumber": page_number}
        )
        
        if response is None or response.status_code != 200:
            logger.debug("Response Body: %s", response)
            return None

        portfolio = response.json()["PortfolioResponse"]["AccountPortfolio"]
        positions = portfolio["Position"]

        if page_number < portfolio["totalPages"] - 1:
            # get the rest of pages
            # no need for totals in requests
            positions = positions + self.view_portfolio(
                account_id_key, False, page_number=page_number+1)
        # totals = portfolio
        return positions
