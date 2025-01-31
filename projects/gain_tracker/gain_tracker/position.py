"""A class for a Position

A trade with recommendations about when to exit
implement with etrade and BigQuery?

in the future, use Pydantic for dataframe rows
and then dataclass for having dataframe vars
"""
from enum import Enum
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, getcontext
getcontext().prec = 28

from . import position_gain as pg

class PositionStatus(Enum):
	"""enum class for the Position

	CLOSED may be very similar to WATCHING, but there is a closed price
	"""
	WATCHING = 0
	OPEN_LONG = 1
	OPEN_SHORT = 2
	CLOSED = -1

class SymbolType(Enum):
    """"""
    """finite set of ID types
    """
    TICKER=0
    ISIN=1

@dataclass
class GainMetrics:
	percent_price_gain: Decimal
	gain: Decimal
	percent_gain: Decimal
	annualized_pct_gain: Decimal
	days_held: int

@dataclass
class Position:
	"""Position properties
	"""
	position_lot_id: int
	account_id_key: str
	instrument_symbol: str
	price_paid: Optional[float] = 0
	date_acquired: Optional[date] = None
	quantity: Optional[float] = 0
	original_quantity: Optional[float] = 0
	market_value: Optional[float] = None
	transaction_fee: float = 0
	instrument_symbol_type: SymbolType = SymbolType.TICKER
	exchange: str = "PCX"
	status: PositionStatus = PositionStatus.WATCHING
	
	def market_price(self) -> float:
		if self.market_value:
			return self.market_value/self.quantity
		else:
			return None
		
	def compute_gains(self, pricing_date: date) -> GainMetrics:
		"""compute all the gains needed
		"""
		market_price = self.market_value/self.quantity
		# need to get historical market prices from yahoo finance
		percent_price_gain = pg.compute_percent_price_gain(
			self.price_paid, market_price)

		gain = pg.compute_gain(
			percent_price_gain, self.quantity, self.price_paid,
				Decimal(str(-1*self.transaction_fee)))
		
		percent_gain = pg.compute_percent_gain(
			gain, self.quantity, self.price_paid)
		
		annualized_pct_gain, days_held = pg.compute_annualized_percent_gain(
				percent_gain, self.date_acquired, pricing_date
			)

		gm = GainMetrics(
			percent_price_gain, gain, percent_gain,
			annualized_pct_gain, days_held)
		return gm

@dataclass
class OpenPosition(Position):
	status: PositionStatus = PositionStatus.OPEN_LONG

@dataclass
class ClosedPosition(Position):
	transaction_id: int = -1
	transaction_fee: float = 0
	date_closed: date = date.today()
	status: PositionStatus = PositionStatus.CLOSED

	def compute_gains(self):
		"""Compute gain with closing date per row
		"""
		gm = super().compute_gains(self.date_closed)
		return gm
	
def recommend_enter_long(
            position: Position, current_price, timeframe_months: int=1):
	"""Evaluate a long position

	Rubric:
	- Relative strength is < 30
	- MACD is low
	- check recent large orders (like in WeBull)
	- check 1 month high-low range or other timeframe
	"""
	pass   

def enter_long(position: Position, current_price, target_value: float):
	"""Execute buy

	target_value is the target position size
	used to compute how many shares to purchase?
	in real life it may depend on uncertainty

	buy may be ordered but it may not be executed for a long time, huh
	"""   
	shares = round(target_value/current_price, 0)

	# trading_platform.buy(shares)



def recommend_exit_long(
		position: Position, current_price: Decimal, market_rate: Optional[Decimal]=Decimal("0.08")):
	"""Evaluate whether it's a good time to exit long position
	
	Rubric:
	- annualized_pnl > market_rate
	- relative strength > 70
	- MACD is high
	- sell orders are increasing
	- total abs gain > $50
	- total percent gain > 10%
	"""
	
	gain = pg.compute_percent_gain(position.entry_price, current_price)

	annualized_gain, position_days = pg.compute_annualized_percent_gain(
		gain, self.position_entry_date, datetime.today())
	
	# good_time = False
	good_time = annualized_gain > market_rate
	
	good_time = good_time and gain > Decimal("0.1")            

	return good_time, annualized_gain
    

class Benchmark:
    """A kind of fake position
    
    The performance can be compared with a position or a portfolio?
    Is a portfolio a kind of position?

    not sure if a separate class is needed or if it has the same properties of a Position
    """
    def __init__(self):
        pass
