import cProfile
import math
from decimal import Decimal, getcontext


f = 1.2 - 1
f != 0.2

f = sum([float(0.1)]*10)
f != 1

math.fsum([0.1]*10) == 1

cProfile.run("sum([Decimal('0.1')]*1000)")

