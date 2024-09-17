
from gain_tracker import position

class TestPosition:
    def test_init(self):
        my_p = position.Position(
            123, "acc1", "T")
        print(my_p.status)
        assert my_p.status.value == 0
        assert my_p.instrument_symbol == "T"